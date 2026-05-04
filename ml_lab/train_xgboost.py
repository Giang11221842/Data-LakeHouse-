import configparser
from pathlib import Path

import s3fs

import numpy as np
import pandas as pd
import xgboost as xgb
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql import Window
from sklearn.metrics import mean_squared_error


# ─────────────────────────────────────────────────────────────────────────────
# CUTOFFS (mirror baseline_ml.ipynb)
# ─────────────────────────────────────────────────────────────────────────────
CUTOFF_TRAIN = "2025-07-01"
CUTOFF_VAL = "2025-11-01"
CUTOFF_TEST = "2026-03-01"

FEATURES = [
    "PULocationID",
    "day_of_week",
    "is_weekend",
    "month",
    "lag_28d",
    "lag_35d",
    "rolling_28d_avg",
    "is_holiday",
    "feature_year",
]
TARGET = "target_demand"


def _load_config(config_path: str = "config/config.conf") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    project_root = Path(__file__).resolve().parents[1]
    loaded = config.read(project_root / config_path)
    if not loaded:
        raise FileNotFoundError(f"Could not read config file at {project_root / config_path}")
    return config


def _build_spark_session_for_ml(config: configparser.ConfigParser) -> SparkSession:
    minio_access_key = config.get("minio", "access_key")
    minio_secret_key = config.get("minio", "secret_key")

    # Chạy local theo baseline notebook
    local_endpoint = "http://localhost:9010"

    return (
        SparkSession.builder.appName("train_xgboost_dual_zone")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.28.15",
        )
        .config("spark.hadoop.fs.s3a.endpoint", local_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def _read_ml_path(config: configparser.ConfigParser) -> str:
    return "s3a://gold/ml/training/daily_demand_features/"


def load_and_prepare_dataframe() -> pd.DataFrame:
    print("[1] Load feature data from MinIO via Spark...")
    config = _load_config()
    spark = _build_spark_session_for_ml(config)

    input_path = _read_ml_path(config)
    silver_df: DataFrame = spark.read.option("mergeSchema", "true").parquet(input_path)

    # Ensure trip_date is date type
    silver_df = silver_df.withColumn("trip_date", F.to_date(F.col("trip_date")))

    # zone_type dynamic (mirror baseline notebook)
    window_zone = Window.partitionBy("PULocationID").orderBy("trip_date").rowsBetween(-89, 0)
    silver_df = silver_df.withColumn("rolling_median", F.avg("rolling_28d_avg").over(window_zone))

    threshold = silver_df.approxQuantile("rolling_median", [0.75], 0.01)[0]
    silver_df = silver_df.withColumn(
        "zone_type",
        F.when(F.col("rolling_median") > threshold, F.lit(1)).otherwise(F.lit(0)),
    )

    print(f"  -> zone_type threshold (75th): {threshold}")
    silver_df.groupBy("zone_type").count().show()

    # Keep only required columns + target/date/zone
    required_cols = FEATURES + [TARGET, "trip_date", "zone_type"]
    df = silver_df.select(*required_cols).toPandas()
    df["trip_date"] = pd.to_datetime(df["trip_date"])
    df = df.sort_values("trip_date").reset_index(drop=True)

    spark.stop()
    print(f"  -> Loaded {len(df):,} rows")
    return df


def split_by_zone_and_time(df: pd.DataFrame):
    print("[2] Split by zone_type and time windows...")

    high_df = df[df["zone_type"] == 1].copy()
    low_df = df[df["zone_type"] == 0].copy()

    # Train / val with log1p target
    high_train = high_df[high_df["trip_date"] < CUTOFF_TRAIN].copy()
    high_val = high_df[(high_df["trip_date"] >= CUTOFF_TRAIN) & (high_df["trip_date"] < CUTOFF_VAL)].copy()

    low_train = low_df[low_df["trip_date"] < CUTOFF_TRAIN].copy()
    low_val = low_df[(low_df["trip_date"] >= CUTOFF_TRAIN) & (low_df["trip_date"] < CUTOFF_VAL)].copy()

    # Test = from CUTOFF_VAL onward (same as baseline notebook)
    df_test = df[df["trip_date"] >= CUTOFF_VAL].copy()

    high_train[TARGET] = np.log1p(high_train[TARGET])
    high_val[TARGET] = np.log1p(high_val[TARGET])
    low_train[TARGET] = np.log1p(low_train[TARGET])
    low_val[TARGET] = np.log1p(low_val[TARGET])

    X_train_high, y_train_high = high_train[FEATURES], high_train[TARGET]
    X_val_high, y_val_high = high_val[FEATURES], high_val[TARGET]

    X_train_low, y_train_low = low_train[FEATURES], low_train[TARGET]
    X_val_low, y_val_low = low_val[FEATURES], low_val[TARGET]

    X_test = df_test[FEATURES]

    print(f"  -> high_train: {len(high_train):,}, high_val: {len(high_val):,}")
    print(f"  -> low_train: {len(low_train):,}, low_val: {len(low_val):,}")
    print(f"  -> test: {len(df_test):,}")

    return (
        X_train_high, y_train_high, X_val_high, y_val_high,
        X_train_low, y_train_low, X_val_low, y_val_low,
        X_test, df_test
    )


def train_dual_models(
    X_train_high, y_train_high, X_val_high, y_val_high,
    X_train_low, y_train_low, X_val_low, y_val_low
):
    print("[3] Train model_high...")
    model_high = xgb.XGBRegressor(
        n_estimators=1500,
        learning_rate=0.03,
        max_depth=8,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        objective="reg:tweedie",
        random_state=42,
        n_jobs=-1,
    )
    model_high.fit(
        X_train_high, y_train_high,
        eval_set=[(X_train_high, y_train_high), (X_val_high, y_val_high)],
        early_stopping_rounds=50,
        verbose=100,
    )

    print("[4] Train model_low...")
    model_low = xgb.XGBRegressor(
        n_estimators=1000,
        learning_rate=0.03,
        max_depth=7,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        objective="reg:tweedie",
        random_state=42,
        n_jobs=-1,
    )
    model_low.fit(
        X_train_low, y_train_low,
        eval_set=[(X_train_low, y_train_low), (X_val_low, y_val_low)],
        early_stopping_rounds=50,
        verbose=100,
    )

    return model_high, model_low


def evaluate_dual_models(model_high, model_low, X_test: pd.DataFrame, test_df: pd.DataFrame):
    print("[5] Evaluate dual models...")
    test_df = test_df.copy()

    test_df["pred_high"] = model_high.predict(X_test)
    test_df["pred_low"] = model_low.predict(X_test)

    test_df["prediction"] = np.where(
        test_df["zone_type"] == 1,
        np.expm1(test_df["pred_high"]),
        np.expm1(test_df["pred_low"]),
    )

    high_test = test_df[test_df["zone_type"] == 1]
    low_test = test_df[test_df["zone_type"] == 0]

    high_rmse = np.sqrt(mean_squared_error(high_test[TARGET], high_test["prediction"])) if len(high_test) else np.nan
    low_rmse = np.sqrt(mean_squared_error(low_test[TARGET], low_test["prediction"])) if len(low_test) else np.nan
    combined_rmse = np.sqrt(mean_squared_error(test_df[TARGET], test_df["prediction"]))

    print(f"  -> High RMSE: {high_rmse:.4f}")
    print(f"  -> Low RMSE: {low_rmse:.4f}")
    print(f"  -> Combined RMSE: {combined_rmse:.4f}")

    return test_df


def retrain_full_and_save(model_high, model_low, df: pd.DataFrame, config: configparser.ConfigParser):
    print("[6] Retrain full data by zone and save models...")

    high_all = df[df["zone_type"] == 1].copy()
    low_all = df[df["zone_type"] == 0].copy()

    high_all[TARGET] = np.log1p(high_all[TARGET])
    low_all[TARGET] = np.log1p(low_all[TARGET])

    X_high_all, y_high_all = high_all[FEATURES], high_all[TARGET]
    X_low_all, y_low_all = low_all[FEATURES], low_all[TARGET]

    high_best_n = (model_high.best_iteration + 1) if model_high.best_iteration is not None else 500
    low_best_n = (model_low.best_iteration + 1) if model_low.best_iteration is not None else 500

    final_high = xgb.XGBRegressor(
        n_estimators=high_best_n,
        learning_rate=0.03,
        max_depth=8,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        objective="reg:tweedie",
        random_state=42,
        n_jobs=-1,
    )
    final_low = xgb.XGBRegressor(
        n_estimators=low_best_n,
        learning_rate=0.03,
        max_depth=7,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        objective="reg:tweedie",
        random_state=42,
        n_jobs=-1,
    )

    final_high.fit(X_high_all, y_high_all, verbose=False)
    final_low.fit(X_low_all, y_low_all, verbose=False)

    # Save local first
    high_path = "xgboost_demand_high.json"
    low_path = "xgboost_demand_low.json"
    final_high.save_model(high_path)
    final_low.save_model(low_path)

    print(f"  -> Saved local: {high_path}")
    print(f"  -> Saved local: {low_path}")

    # Upload to MinIO by s3fs (avoid Hadoop copyFromLocalFile path issue)
    minio_endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    gold_bucket = config.get("minio", "gold_bucket", fallback="gold")

    # Convert internal docker endpoint -> local mapped endpoint for local run
    endpoint_url = "http://localhost:9010" if "minio:9000" in minio_endpoint else f"http://{minio_endpoint}"

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint_url},
    )

    remote_dir = f"{gold_bucket}/ml/model"
    fs.makedirs(remote_dir, exist_ok=True)

    with open(high_path, "rb") as f_high:
        with fs.open(f"{remote_dir}/xgboost_demand_high.json", "wb") as out_high:
            out_high.write(f_high.read())

    with open(low_path, "rb") as f_low:
        with fs.open(f"{remote_dir}/xgboost_demand_low.json", "wb") as out_low:
            out_low.write(f_low.read())

    print(f"  -> Uploaded to s3://{gold_bucket}/ml/model/")


def run():
    config = _load_config()
    df = load_and_prepare_dataframe()
    (
        X_train_high, y_train_high, X_val_high, y_val_high,
        X_train_low, y_train_low, X_val_low, y_val_low,
        X_test, df_test
    ) = split_by_zone_and_time(df)

    model_high, model_low = train_dual_models(
        X_train_high, y_train_high, X_val_high, y_val_high,
        X_train_low, y_train_low, X_val_low, y_val_low
    )

    evaluate_dual_models(model_high, model_low, X_test, df_test)
    retrain_full_and_save(model_high, model_low, df, config)


if __name__ == "__main__":
    run()
