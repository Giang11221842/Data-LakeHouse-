"""
XGBoost Taxi Demand Prediction – Comprehensive Evaluation Script
================================================================
Loads feature data from MinIO via PySpark, trains dual-zone XGBoost models
(high-demand vs low-demand zones), and produces a full evaluation suite:

Metrics: RMSE, MAE, MAPE, R², Explained Variance
Visualizations:
  1. Actual vs Predicted scatter plots per zone
  2. Residual distribution histograms
  3. Time-series comparison (daily aggregated)
  4. Feature importance bar charts
  5. Learning curves
  6. Error by day-of-week heatmap

All outputs are saved to ml_lab/evaluation_output/.
"""

import configparser
import json
import os
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    explained_variance_score,
    mean_absolute_percentage_error,
)

warnings.filterwarnings("ignore")

# ── Project paths ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT_DIR = Path(__file__).resolve().parent / "evaluation_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Constants (mirror train_xgboost.py) ────────────────────────────────────────
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

# ── Styling ────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
COLORS = {
    "high": "#FF6B6B",
    "low": "#4ECDC4",
    "combined": "#45B7D1",
    "accent": "#F9A825",
    "dark": "#2C3E50",
}


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def _load_config():
    config = configparser.ConfigParser()
    loaded = config.read(PROJECT_ROOT / "config" / "config.conf")
    if not loaded:
        raise FileNotFoundError("config/config.conf not found")
    return config


def load_data_from_minio() -> pd.DataFrame:
    """Load ML feature data from MinIO via PySpark, return pandas DataFrame."""
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql import Window

    config = _load_config()

    print("=" * 70)
    print("  ĐANG TẢI DỮ LIỆU TỪ MINIO…")
    print("=" * 70)

    spark = (
        SparkSession.builder.appName("xgboost_evaluation")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.28.15",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9010")
        .config("spark.hadoop.fs.s3a.access.key", config.get("minio", "access_key"))
        .config("spark.hadoop.fs.s3a.secret.key", config.get("minio", "secret_key"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    input_path = "s3a://gold/ml/training/daily_demand_features/"
    silver_df = spark.read.option("mergeSchema", "true").parquet(input_path)
    silver_df = silver_df.withColumn("trip_date", F.to_date(F.col("trip_date")))

    # Compute zone_type (same logic as train_xgboost.py)
    window_zone = Window.partitionBy("PULocationID").orderBy("trip_date").rowsBetween(-89, 0)
    silver_df = silver_df.withColumn("rolling_median", F.avg("rolling_28d_avg").over(window_zone))

    threshold = silver_df.approxQuantile("rolling_median", [0.75], 0.01)[0]
    silver_df = silver_df.withColumn(
        "zone_type",
        F.when(F.col("rolling_median") > threshold, F.lit(1)).otherwise(F.lit(0)),
    )

    print(f"  → Zone type threshold (Q75): {threshold:.2f}")
    silver_df.groupBy("zone_type").count().show()

    required_cols = FEATURES + [TARGET, "trip_date", "zone_type"]
    df = silver_df.select(*required_cols).toPandas()
    df["trip_date"] = pd.to_datetime(df["trip_date"])
    df = df.sort_values("trip_date").reset_index(drop=True)

    spark.stop()
    print(f"  → Loaded {len(df):,} rows spanning {df['trip_date'].min()} → {df['trip_date'].max()}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. TRAIN / VALIDATE / TEST SPLIT
# ═══════════════════════════════════════════════════════════════════════════════

def split_data(df: pd.DataFrame):
    """Split into train/val/test by zone_type and time."""
    high = df[df["zone_type"] == 1].copy()
    low = df[df["zone_type"] == 0].copy()

    splits = {}
    for label, zone_df in [("high", high), ("low", low)]:
        train = zone_df[zone_df["trip_date"] < CUTOFF_TRAIN].copy()
        val = zone_df[(zone_df["trip_date"] >= CUTOFF_TRAIN) & (zone_df["trip_date"] < CUTOFF_VAL)].copy()
        test = zone_df[zone_df["trip_date"] >= CUTOFF_VAL].copy()

        # Log1p transform for training/validation targets
        train[TARGET] = np.log1p(train[TARGET])
        val[TARGET] = np.log1p(val[TARGET])

        splits[label] = {
            "train": train,
            "val": val,
            "test": test,
        }

    print("\n📊 Data Split:")
    print(f"  Train: < {CUTOFF_TRAIN}")
    print(f"  Val:   {CUTOFF_TRAIN} → {CUTOFF_VAL}")
    print(f"  Test:  ≥ {CUTOFF_VAL}")
    for label in ["high", "low"]:
        s = splits[label]
        print(f"  [{label.upper()}] Train: {len(s['train']):,} | Val: {len(s['val']):,} | Test: {len(s['test']):,}")

    return splits


# ═══════════════════════════════════════════════════════════════════════════════
# 3. MODEL TRAINING
# ═══════════════════════════════════════════════════════════════════════════════

def train_models(splits: dict):
    """Train XGBoost for high and low demand zones with eval logging."""
    models = {}
    evals_results = {}

    for label, params in [
        ("high", {"n_estimators": 1500, "max_depth": 8}),
        ("low", {"n_estimators": 1000, "max_depth": 7}),
    ]:
        print(f"\n🔧 Training model_{label}…")
        s = splits[label]

        X_train, y_train = s["train"][FEATURES], s["train"][TARGET]
        X_val, y_val = s["val"][FEATURES], s["val"][TARGET]

        model = xgb.XGBRegressor(
            n_estimators=params["n_estimators"],
            learning_rate=0.03,
            max_depth=params["max_depth"],
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            reg_alpha=0.1,
            reg_lambda=1.0,
            objective="reg:tweedie",
            random_state=42,
            n_jobs=-1,
        )

        eval_result = {}
        model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_val, y_val)],
            early_stopping_rounds=50,
            verbose=100,
        )
        
        # Extract learning curves from model
        evals_result = model.evals_result()

        models[label] = model
        evals_results[label] = evals_result
        print(f"  → Best iteration: {model.best_iteration}, Best score: {model.best_score:.6f}")

    return models, evals_results


# ═══════════════════════════════════════════════════════════════════════════════
# 4. PREDICTION & EVALUATION METRICS
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate(models: dict, splits: dict) -> dict:
    """Run predictions on test set and compute comprehensive metrics."""
    all_results = {}

    for label in ["high", "low"]:
        model = models[label]
        test = splits[label]["test"].copy()

        if len(test) == 0:
            print(f"  [WARN] No test data for {label} zone")
            continue

        X_test = test[FEATURES]
        y_true = test[TARGET].values

        # Predict in log space, then expm1 back
        y_pred_log = model.predict(X_test)
        y_pred = np.expm1(y_pred_log)
        y_pred = np.clip(y_pred, 0, None)

        # Metrics
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        mae = mean_absolute_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)
        ev = explained_variance_score(y_true, y_pred)

        # MAPE (avoid division by zero)
        mask = y_true > 0
        if mask.sum() > 0:
            mape = mean_absolute_percentage_error(y_true[mask], y_pred[mask]) * 100
        else:
            mape = np.nan

        # Symmetric MAPE
        smape = 100 * np.mean(2 * np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred) + 1e-8))

        all_results[label] = {
            "y_true": y_true,
            "y_pred": y_pred,
            "test_df": test,
            "rmse": rmse,
            "mae": mae,
            "mape": mape,
            "smape": smape,
            "r2": r2,
            "explained_variance": ev,
            "n_samples": len(y_true),
        }

        print(f"\n📈 [{label.upper()} Zone] Test Metrics:")
        print(f"  RMSE:               {rmse:>12.2f}")
        print(f"  MAE:                {mae:>12.2f}")
        print(f"  MAPE:               {mape:>11.2f}%")
        print(f"  sMAPE:              {smape:>11.2f}%")
        print(f"  R²:                 {r2:>12.4f}")
        print(f"  Explained Variance: {ev:>12.4f}")
        print(f"  N samples:          {all_results[label]['n_samples']:>12,}")

    # Combined metrics
    y_true_all = np.concatenate([r["y_true"] for r in all_results.values()])
    y_pred_all = np.concatenate([r["y_pred"] for r in all_results.values()])

    combined_rmse = np.sqrt(mean_squared_error(y_true_all, y_pred_all))
    combined_mae = mean_absolute_error(y_true_all, y_pred_all)
    combined_r2 = r2_score(y_true_all, y_pred_all)
    mask_all = y_true_all > 0
    combined_mape = mean_absolute_percentage_error(y_true_all[mask_all], y_pred_all[mask_all]) * 100 if mask_all.sum() > 0 else np.nan
    combined_smape = 100 * np.mean(2 * np.abs(y_pred_all - y_true_all) / (np.abs(y_true_all) + np.abs(y_pred_all) + 1e-8))

    all_results["combined"] = {
        "y_true": y_true_all,
        "y_pred": y_pred_all,
        "rmse": combined_rmse,
        "mae": combined_mae,
        "mape": combined_mape,
        "smape": combined_smape,
        "r2": combined_r2,
        "n_samples": len(y_true_all),
    }

    print(f"\n{'='*50}")
    print(f"📊 COMBINED Test Metrics:")
    print(f"  RMSE:     {combined_rmse:>12.2f}")
    print(f"  MAE:      {combined_mae:>12.2f}")
    print(f"  MAPE:     {combined_mape:>11.2f}%")
    print(f"  sMAPE:    {combined_smape:>11.2f}%")
    print(f"  R²:       {combined_r2:>12.4f}")
    print(f"  N:        {len(y_true_all):>12,}")
    print(f"{'='*50}")

    return all_results


# ═══════════════════════════════════════════════════════════════════════════════
# 5. VISUALIZATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def plot_actual_vs_predicted(results: dict):
    """Scatter plot of actual vs predicted for each zone."""
    fig, axes = plt.subplots(1, 3, figsize=(20, 6))

    for idx, (label, color) in enumerate([
        ("high", COLORS["high"]),
        ("low", COLORS["low"]),
        ("combined", COLORS["combined"]),
    ]):
        if label not in results:
            continue
        r = results[label]
        ax = axes[idx]

        ax.scatter(r["y_true"], r["y_pred"], alpha=0.15, s=8, color=color, edgecolors="none")

        # Perfect prediction line
        max_val = max(r["y_true"].max(), r["y_pred"].max())
        ax.plot([0, max_val], [0, max_val], "k--", linewidth=1.5, alpha=0.7, label="Perfect prediction")

        ax.set_xlabel("Actual Demand", fontsize=12)
        ax.set_ylabel("Predicted Demand", fontsize=12)
        title = f"{'Vùng nhu cầu cao' if label == 'high' else 'Vùng nhu cầu thấp' if label == 'low' else 'Tổng hợp'}"
        ax.set_title(f"{title}\nR² = {r['r2']:.4f} | RMSE = {r['rmse']:.1f}", fontsize=13, fontweight="bold")
        ax.legend(fontsize=10)

    plt.suptitle("Biểu đồ Actual vs Predicted – Dự đoán nhu cầu Taxi", fontsize=15, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "01_actual_vs_predicted.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


def plot_residual_distribution(results: dict):
    """Histogram of residuals (error distribution)."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    for idx, (label, color) in enumerate([("high", COLORS["high"]), ("low", COLORS["low"])]):
        if label not in results:
            continue
        r = results[label]
        residuals = r["y_pred"] - r["y_true"]

        ax = axes[idx]
        ax.hist(residuals, bins=80, color=color, alpha=0.7, edgecolor="white", linewidth=0.5)
        ax.axvline(x=0, color="black", linestyle="--", linewidth=1.5)
        ax.axvline(x=residuals.mean(), color=COLORS["accent"], linestyle="-", linewidth=2, label=f"Mean = {residuals.mean():.1f}")

        zone_name = "Vùng nhu cầu cao" if label == "high" else "Vùng nhu cầu thấp"
        ax.set_title(f"Phân bố sai số – {zone_name}\nStd = {residuals.std():.1f}", fontsize=13, fontweight="bold")
        ax.set_xlabel("Residual (Predicted − Actual)", fontsize=12)
        ax.set_ylabel("Frequency", fontsize=12)
        ax.legend(fontsize=10)

    plt.suptitle("Phân bố Residuals – XGBoost Taxi Demand", fontsize=15, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "02_residual_distribution.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


def plot_time_series_comparison(results: dict):
    """Time series: daily actual vs predicted demand (aggregated)."""
    fig, axes = plt.subplots(2, 1, figsize=(18, 10), sharex=True)

    for idx, (label, color) in enumerate([("high", COLORS["high"]), ("low", COLORS["low"])]):
        if label not in results or "test_df" not in results[label]:
            continue
        r = results[label]
        test_df = r["test_df"].copy()
        test_df["prediction"] = r["y_pred"]

        daily_actual = test_df.groupby("trip_date")[TARGET].sum().reset_index()
        daily_pred = test_df.groupby("trip_date")["prediction"].sum().reset_index()
        daily = daily_actual.merge(daily_pred, on="trip_date")

        ax = axes[idx]
        ax.plot(daily["trip_date"], daily[TARGET], label="Actual", color=COLORS["dark"], linewidth=1.5, alpha=0.8)
        ax.plot(daily["trip_date"], daily["prediction"], label="Predicted", color=color, linewidth=1.5, alpha=0.8, linestyle="--")
        ax.fill_between(daily["trip_date"], daily[TARGET], daily["prediction"], alpha=0.15, color=color)

        zone_name = "Vùng nhu cầu cao" if label == "high" else "Vùng nhu cầu thấp"
        ax.set_title(f"Chuỗi thời gian – {zone_name}", fontsize=13, fontweight="bold")
        ax.set_ylabel("Tổng nhu cầu theo ngày", fontsize=12)
        ax.legend(fontsize=11)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())

    axes[-1].set_xlabel("Ngày", fontsize=12)
    plt.suptitle("So sánh chuỗi thời gian Actual vs Predicted – Test Set", fontsize=15, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "03_time_series_comparison.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


def plot_feature_importance(models: dict):
    """Horizontal bar chart of feature importance."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    for idx, (label, color) in enumerate([("high", COLORS["high"]), ("low", COLORS["low"])]):
        model = models[label]
        importance = model.feature_importances_
        sorted_idx = np.argsort(importance)

        ax = axes[idx]
        feature_names = np.array(FEATURES)
        ax.barh(feature_names[sorted_idx], importance[sorted_idx], color=color, alpha=0.85)

        zone_name = "Vùng nhu cầu cao" if label == "high" else "Vùng nhu cầu thấp"
        ax.set_title(f"Feature Importance – {zone_name}", fontsize=13, fontweight="bold")
        ax.set_xlabel("Importance (Gain)", fontsize=12)

    plt.suptitle("Tầm quan trọng của các đặc trưng (Features) – XGBoost", fontsize=15, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "04_feature_importance.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


def plot_learning_curves(evals_results: dict):
    """Training and validation loss curves."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    for idx, (label, color) in enumerate([("high", COLORS["high"]), ("low", COLORS["low"])]):
        if label not in evals_results:
            continue
        
        er = evals_results[label]
        train_metric = list(er.get("validation_0", {}).values())[0]
        val_metric = list(er.get("validation_1", {}).values())[0]

        ax = axes[idx]
        epochs = range(1, len(train_metric) + 1)
        ax.plot(epochs, train_metric, label="Train", color=COLORS["dark"], linewidth=1.2, alpha=0.8)
        ax.plot(epochs, val_metric, label="Validation", color=color, linewidth=1.5)

        zone_name = "Vùng nhu cầu cao" if label == "high" else "Vùng nhu cầu thấp"
        ax.set_title(f"Learning Curve – {zone_name}", fontsize=13, fontweight="bold")
        ax.set_xlabel("Iterations", fontsize=12)
        ax.set_ylabel("Loss (Tweedie)", fontsize=12)
        ax.legend(fontsize=11)

    plt.suptitle("Đường cong học tập – XGBoost Dual Zone Models", fontsize=15, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "05_learning_curves.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


def plot_error_by_day_of_week(results: dict):
    """Box plot of absolute errors grouped by day of week."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    for idx, (label, color) in enumerate([("high", COLORS["high"]), ("low", COLORS["low"])]):
        if label not in results or "test_df" not in results[label]:
            continue
        r = results[label]
        test_df = r["test_df"].copy()
        test_df["prediction"] = r["y_pred"]
        test_df["abs_error"] = np.abs(test_df["prediction"] - test_df[TARGET])

        ax = axes[idx]
        # Matplotlib dayofweek: Mon=0..Sun=6; Spark dayofweek: Sun=1..Sat=7
        test_df["dow_label"] = test_df["day_of_week"].map(
            {2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat", 1: "Sun"}
        )
        order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        sns.boxplot(data=test_df, x="dow_label", y="abs_error", order=order, color=color, ax=ax, fliersize=2)

        zone_name = "Vùng nhu cầu cao" if label == "high" else "Vùng nhu cầu thấp"
        ax.set_title(f"Sai số tuyệt đối theo ngày trong tuần – {zone_name}", fontsize=12, fontweight="bold")
        ax.set_xlabel("Ngày trong tuần", fontsize=11)
        ax.set_ylabel("Absolute Error", fontsize=11)

    plt.suptitle("Phân tích sai số theo ngày trong tuần – XGBoost", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "06_error_by_day_of_week.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


def plot_prediction_error_map(results: dict):
    """Top 20 zones with highest prediction error."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))

    for idx, (label, color) in enumerate([("high", COLORS["high"]), ("low", COLORS["low"])]):
        if label not in results or "test_df" not in results[label]:
            continue
        r = results[label]
        test_df = r["test_df"].copy()
        test_df["prediction"] = r["y_pred"]
        test_df["abs_error"] = np.abs(test_df["prediction"] - test_df[TARGET])

        zone_errors = test_df.groupby("PULocationID")["abs_error"].mean().sort_values(ascending=False).head(20)

        ax = axes[idx]
        zone_errors.plot(kind="barh", color=color, alpha=0.85, ax=ax)
        zone_name = "Vùng nhu cầu cao" if label == "high" else "Vùng nhu cầu thấp"
        ax.set_title(f"Top 20 Zone sai số lớn nhất – {zone_name}", fontsize=12, fontweight="bold")
        ax.set_xlabel("Mean Absolute Error", fontsize=11)
        ax.set_ylabel("PULocationID", fontsize=11)
        ax.invert_yaxis()

    plt.suptitle("Phân tích sai số theo Zone – XGBoost", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "07_error_by_zone.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  → Saved: {path}")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def generate_report(results: dict, models: dict, df: pd.DataFrame):
    """Write a structured evaluation report as JSON and markdown."""
    report = {
        "model": "XGBoost Dual-Zone (High/Low Demand)",
        "objective": "reg:tweedie",
        "target": "Daily taxi demand per PULocationID",
        "features": FEATURES,
        "data_split": {
            "train": f"< {CUTOFF_TRAIN}",
            "validation": f"{CUTOFF_TRAIN} ~ {CUTOFF_VAL}",
            "test": f"≥ {CUTOFF_VAL}",
        },
        "date_range": {
            "min": str(df["trip_date"].min().date()),
            "max": str(df["trip_date"].max().date()),
        },
        "total_rows": len(df),
    }

    for label in ["high", "low", "combined"]:
        if label in results:
            r = results[label]
            report[f"metrics_{label}"] = {
                "RMSE": round(r["rmse"], 4),
                "MAE": round(r["mae"], 4),
                "MAPE_%": round(r["mape"], 2),
                "sMAPE_%": round(r["smape"], 2),
                "R2": round(r["r2"], 4),
                "N_samples": r["n_samples"],
            }

    for label in ["high", "low"]:
        m = models[label]
        report[f"model_{label}_params"] = {
            "best_iteration": m.best_iteration,
            "best_score": round(m.best_score, 6),
            "n_estimators_used": m.best_iteration + 1,
            "max_depth": m.get_params()["max_depth"],
            "learning_rate": m.get_params()["learning_rate"],
        }

    # Save JSON
    json_path = OUTPUT_DIR / "evaluation_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  → Saved: {json_path}")

    return report


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "═" * 70)
    print("  XGBOOST TAXI DEMAND PREDICTION – COMPREHENSIVE EVALUATION")
    print("═" * 70 + "\n")

    # 1. Load data
    df = load_data_from_minio()

    # 2. Split
    splits = split_data(df)

    # 3. Train
    models, evals_results = train_models(splits)

    # 4. Evaluate
    print("\n" + "═" * 70)
    print("  ĐÁNH GIÁ MÔ HÌNH TRÊN TẬP TEST")
    print("═" * 70)
    results = evaluate(models, splits)

    # 5. Visualize
    print("\n📊 Generating visualizations…")
    plot_actual_vs_predicted(results)
    plot_residual_distribution(results)
    plot_time_series_comparison(results)
    plot_feature_importance(models)
    plot_learning_curves(evals_results)
    plot_error_by_day_of_week(results)
    plot_prediction_error_map(results)

    # 6. Report
    print("\n📝 Generating report…")
    report = generate_report(results, models, df)

    print("\n" + "═" * 70)
    print("  ✅ EVALUATION COMPLETE")
    print(f"  All outputs saved to: {OUTPUT_DIR}")
    print("═" * 70 + "\n")

    return report, results


if __name__ == "__main__":
    main()
