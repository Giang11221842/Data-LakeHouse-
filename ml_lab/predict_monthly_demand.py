import argparse
import configparser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4
import os

import holidays
import numpy as np
import pandas as pd
import psycopg2
import s3fs
import xgboost as xgb
import sys
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Fix ModuleNotFoundError: add project root to sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from etls.gold_transform import _load_minio_config_gold  # Reuse gold_transform helper


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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_str_exc(exc: BaseException) -> str:
    try:
        return str(exc)
    except Exception:
        try:
            return repr(exc)
        except Exception:
            return f"<{type(exc).__name__}: str() and repr() both failed>"


def _load_config(config_path: str = "config/config.conf") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    project_root = Path(__file__).resolve().parents[1]
    loaded = config.read(project_root / config_path)
    if not loaded:
        raise FileNotFoundError(f"Could not read config file at {project_root / config_path}")
    return config


def build_spark_session_ml(config: configparser.ConfigParser) -> SparkSession:
    minio_cfg = _load_minio_config_gold(config)  # Reuse gold_transform helper
    endpoint = minio_cfg["endpoint"]
    nessie_uri = config.get("nessie", "uri", fallback="http://nessie:19120/api/v1")
    nessie_ref = config.get("nessie", "ref", fallback="main")
    gold_bucket = config.get("minio", "gold_bucket", fallback="gold")

    spark_driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    spark_executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    spark_shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200")

    return (
        SparkSession.builder.appName("predict_monthly_demand_batch")
        .config("spark.driver.memory", spark_driver_memory)
        .config("spark.executor.memory", spark_executor_memory)
        .config("spark.sql.shuffle.partitions", spark_shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", os.getenv("SPARK_SQL_ADAPTIVE_MIN_PARTITIONS", "50"))
        .config("spark.sql.autoBroadcastJoinThreshold", os.getenv("SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD", "20MB"))
        .config("spark.sql.broadcastTimeout", os.getenv("SPARK_SQL_BROADCAST_TIMEOUT", "600"))
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
            "/opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.76.3.jar",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkExtensions"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio_cfg["secure"]).lower())
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.gold.uri", nessie_uri)
        .config("spark.sql.catalog.gold.ref", nessie_ref)
        .config("spark.sql.catalog.gold.warehouse", f"s3://{gold_bucket}/warehouse")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )


def _get_db_connection(config: configparser.ConfigParser):
    db_cfg = config["database"]
    return psycopg2.connect(
        host=db_cfg["database_host"],
        dbname=db_cfg["database_name"],
        user=db_cfg["database_username"],
        password=db_cfg["database_password"],
        port=int(db_cfg["database_port"]),
    )


def _ensure_ml_metadata_table_exists(conn, config: configparser.ConfigParser) -> None:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    table = db_cfg.get("ml_data_log_table", "ml_data_log")
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        pipeline_name TEXT NOT NULL,
        year INTEGER,
        month INTEGER,
        status TEXT NOT NULL,
        table_name TEXT,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        error_message TEXT,
        spark_app_id TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _log_ml_run(config: configparser.ConfigParser, run_payload: Dict[str, Any]) -> None:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    log_table = db_cfg.get("ml_data_log_table", "ml_data_log")
    conn = _get_db_connection(config)

    try:
        _ensure_ml_metadata_table_exists(conn, config)
        insert_sql = f"""
        INSERT INTO {schema}.{log_table} (
            run_id, pipeline_name, year, month, status, table_name,
            started_at, finished_at, error_message, spark_app_id
        )
        VALUES (
            %(run_id)s, %(pipeline_name)s, %(year)s, %(month)s, %(status)s, %(table_name)s,
            %(started_at)s, %(finished_at)s, %(error_message)s, %(spark_app_id)s
        );
        """
        with conn.cursor() as cur:
            cur.execute(insert_sql, run_payload)
        conn.commit()
    except Exception as e:
        print(f"[WARN] Error logging ML run: {e}")
    finally:
        conn.close()


def _check_ml_run_already_succeeded(
    config: configparser.ConfigParser,
    pipeline_name: str,
    year: int,
    month: int,
) -> bool:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    log_table = db_cfg.get("ml_data_log_table", "ml_data_log")

    try:
        conn = _get_db_connection(config)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {schema}.{log_table} "
                    "WHERE pipeline_name = %s AND year = %s AND month = %s "
                    "AND status = 'SUCCESS' LIMIT 1",
                    (pipeline_name, year, month),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] Error checking ML run status: {e!r}. Continue running.")
        return False


def _resolve_feature_path(config: configparser.ConfigParser) -> str:
    gold_bucket = config.get("minio", "gold_bucket", fallback="gold")
    return f"s3a://{gold_bucket}/ml/training/daily_demand_features/"


def _resolve_prediction_output_path(config: configparser.ConfigParser) -> str:
    gold_bucket = config.get("minio", "gold_bucket", fallback="gold")
    return f"s3a://{gold_bucket}/ml/predictions/monthly_demand_forecast/"


def load_month_feature_data(
    spark: SparkSession,
    config: configparser.ConfigParser,
    year: int,
    month: int,
) -> DataFrame:
    """Kiến tạo dữ liệu đặc trưng cho tháng dự đoán N.
    Tối ưu hóa: chỉ nạp 90 ngày lịch sử và tự động điền các ngày thiếu bằng Forward Fill.
    """
    input_path = _resolve_feature_path(config)
    
    # 1. Nạp dữ liệu lịch sử (90 ngày trước dải dự đoán)
    first_day_str = f"{year}-{month:02d}-01"
    lookback_date = F.date_sub(F.lit(first_day_str).cast("date"), 90)
    
    full_df = spark.read.option("mergeSchema", "true").parquet(input_path)
    
    history_df = full_df.filter(
        (F.col("trip_date") >= lookback_date) & 
        (F.col("trip_date") < F.lit(first_day_str).cast("date"))
    )

    if history_df.limit(1).count() == 0:
        print(f"[WARN] No features in last 90 days. Falling back to all historical features.")
        history_df = full_df.filter(F.col("trip_date") < F.lit(first_day_str).cast("date"))

    if history_df.limit(1).count() == 0:
        raise ValueError(f"No historical features available before {year}-{month:02d}")

    # 2. Tạo khung ngày (Skeleton) cho tháng dự đoán
    last_day_expr = F.last_day(F.lit(first_day_str).cast("date"))
    date_skeleton = spark.range(1).select(
        F.explode(F.sequence(F.lit(first_day_str).cast("date"), last_day_expr, F.expr("interval 1 day"))).alias("trip_date")
    )
    
    location_ids = history_df.select("PULocationID").distinct()
    # Dùng null để last_value ignorenulls hoạt động chính xác
    skeleton_df = date_skeleton.crossJoin(location_ids).withColumn("target_demand", F.lit(None).cast("double"))

    # 3. Hợp nhất Lịch sử và Skeleton
    actuals = history_df.select("trip_date", "PULocationID", F.col(TARGET).alias("target_demand"))
    combined_df = actuals.unionByName(skeleton_df)

    # 4. Tính toán Lags & Forward Fill
    days_to_sec = lambda d: d * 86400
    combined_unix = combined_df.withColumn("unix_ts", F.unix_timestamp("trip_date").cast("long"))

    w_28d = Window.partitionBy("PULocationID").orderBy("unix_ts").rangeBetween(-days_to_sec(28), -days_to_sec(28))
    w_35d = Window.partitionBy("PULocationID").orderBy("unix_ts").rangeBetween(-days_to_sec(35), -days_to_sec(35))
    w_rolling = Window.partitionBy("PULocationID").orderBy("unix_ts").rangeBetween(-days_to_sec(28), -days_to_sec(1))
    w_fill = Window.partitionBy("PULocationID").orderBy("unix_ts").rowsBetween(Window.unboundedPreceding, -1)

    featured_all = (
        combined_unix
        .withColumn("raw_lag_28d", F.max("target_demand").over(w_28d))
        .withColumn("raw_lag_35d", F.max("target_demand").over(w_35d))
        .withColumn("raw_rolling_28d_avg", F.avg("target_demand").over(w_rolling))
        # Forward fill cho các ngày rơi vào tương lai chưa có actuals
        .withColumn("lag_28d", F.coalesce(F.col("raw_lag_28d"), F.last_value("target_demand", True).over(w_fill)))
        .withColumn("lag_35d", F.coalesce(F.col("raw_lag_35d"), F.last_value("target_demand", True).over(w_fill)))
        .withColumn("rolling_28d_avg", F.coalesce(F.col("raw_rolling_28d_avg"), F.last_value("target_demand", True).over(w_fill)))
        .withColumnRenamed("target_demand", "historical_demand")
    )

    # 5. Cập nhật Đặc trưng Chu kỳ
    us_holidays = holidays.US(years=range(2021, 2027))
    holiday_list = [d.strftime("%Y-%m-%d") for d in us_holidays.keys()]

    final_feat_df = (
        featured_all
        .withColumn("day_of_week", F.dayofweek("trip_date"))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0))
        .withColumn("month", F.month("trip_date"))
        .withColumn("feature_year", F.year("trip_date"))
        .withColumn("feature_month", F.month("trip_date"))
        .withColumn("is_holiday", F.when(F.col("trip_date").cast("string").isin(holiday_list), 1).otherwise(0))
    )

    # 6. Tính toán zone_type động (tính trên 90 ngày history để ổn định)
    window_zone = Window.partitionBy("PULocationID").orderBy("trip_date").rowsBetween(-89, 0)
    final_feat_df = final_feat_df.withColumn("rolling_median", F.avg("rolling_28d_avg").over(window_zone))

    # Lấy threshold từ dữ liệu lịch sử để đảm bảo nhất quán với Train
    q = final_feat_df.approxQuantile("rolling_median", [0.75], 0.01)
    threshold = q[0] if q else 0.0
    
    final_feat_df = final_feat_df.withColumn(
        "zone_type",
        F.when(F.col("rolling_median") > F.lit(threshold), F.lit(1)).otherwise(F.lit(0))
    )

    # 7. Lọc kết quả duy nhất tháng dự đoán
    target_df = final_feat_df.filter((F.col("feature_year") == year) & (F.col("feature_month") == month)).drop("unix_ts", "rolling_median", "raw_lag_28d", "raw_lag_35d", "raw_rolling_28d_avg")

    print(f"[INFO] Generated {target_df.count():,} rows for prediction for {year}-{month:02d}")
    return target_df


def load_models(config: configparser.ConfigParser) -> tuple[xgb.Booster, xgb.Booster]:
    minio_endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    gold_bucket = config.get("minio", "gold_bucket", fallback="gold")

    endpoint_url = f"http://{minio_endpoint}"
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint_url},
    )

    remote_high = f"{gold_bucket}/ml/model/xgboost_demand_high.json"
    remote_low = f"{gold_bucket}/ml/model/xgboost_demand_low.json"

    project_root = Path(__file__).resolve().parents[1]
    models_dir = project_root / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    local_high = models_dir / "xgboost_demand_high.json"
    local_low = models_dir / "xgboost_demand_low.json"

    with fs.open(remote_high, "rb") as src, open(local_high, "wb") as dst:
        dst.write(src.read())
    with fs.open(remote_low, "rb") as src, open(local_low, "wb") as dst:
        dst.write(src.read())

    high_size = local_high.stat().st_size if local_high.exists() else 0
    low_size = local_low.stat().st_size if local_low.exists() else 0
    print(f"[INFO] Downloaded model files: high={local_high} ({high_size} bytes), low={local_low} ({low_size} bytes)")

    if high_size == 0 or low_size == 0:
        raise FileNotFoundError(
            f"Downloaded model file is empty or missing: "
            f"high={local_high} ({high_size}), low={local_low} ({low_size})"
        )

    model_high = xgb.Booster()
    model_high.load_model(str(local_high))

    model_low = xgb.Booster()
    model_low.load_model(str(local_low))

    return model_high, model_low


def _nessie_drop_table_via_rest(
    nessie_uri: str,
    namespace: list[str],
    table_name: str,
    branch: str = "main",
) -> bool:
    """Delete a stale Nessie table reference via the Nessie REST API v1.
    Imported from gold_transform.py to recover from stale metadata (BUG-029).
    """
    try:
        import requests
    except ImportError:
        print("[WARN] _nessie_drop_table_via_rest: 'requests' not available. Skipping REST fallback.")
        return False

    ref_url = f"{nessie_uri}/trees/tree/{branch}"
    commit_url = f"{nessie_uri}/trees/branch/{branch}/commit"
    commit_payload = {
        "commitMeta": {
            "message": f"Drop stale Iceberg table {'.'.join(namespace)}.{table_name} (automated recovery from ML pipeline)"
        },
        "operations": [
            {
                "type": "DELETE",
                "key": {"elements": namespace + [table_name]},
            }
        ],
    }

    def _attempt(expected_hash: str) -> int:
        resp = requests.post(
            commit_url,
            params={"expectedHash": expected_hash},
            json=commit_payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        return resp.status_code

    try:
        ref_resp = requests.get(ref_url, timeout=30)
        if ref_resp.status_code == 404:
            return False
        ref_resp.raise_for_status()
        current_hash = ref_resp.json()["hash"]

        status = _attempt(current_hash)
        if status in (200, 204, 404):
            return True
        if status == 409:
            ref_resp2 = requests.get(ref_url, timeout=30)
            ref_resp2.raise_for_status()
            status2 = _attempt(ref_resp2.json()["hash"])
            return status2 in (200, 204, 404)
        return False
    except Exception as e:
        print(f"[WARN] _nessie_drop_table_via_rest: REST call failed: {e!r}")
        return False


def predict_monthly_demand(df_spark: DataFrame, model_high: xgb.Booster, model_low: xgb.Booster) -> pd.DataFrame:
    pdf = df_spark.select(*(FEATURES + ["trip_date", "zone_type", "historical_demand", "feature_month"])).toPandas()
    if pdf.empty:
        print("[INFO] Empty feature DataFrame - no predictions generated.")
        return pdf

    # Chia tách dữ liệu để Inference độc lập
    pdf_high = pdf[pdf["zone_type"] == 1].copy()
    pdf_low = pdf[pdf["zone_type"] == 0].copy() 

    # Danh sách các mẩu dữ liệu nhỏ để concat sau cùng
    results = []

    if not pdf_high.empty:
        dmatrix_high = xgb.DMatrix(pdf_high[FEATURES])
        pred_high_log = model_high.predict(dmatrix_high)
        pdf_high["prediction"] = np.expm1(pred_high_log)
        results.append(pdf_high)

    if not pdf_low.empty:
        dmatrix_low = xgb.DMatrix(pdf_low[FEATURES])
        pred_low_log = model_low.predict(dmatrix_low)
        pdf_low["prediction"] = np.expm1(pred_low_log)
        results.append(pdf_low)
    
    if not results:
        return pd.DataFrame()

    pdf_final = pd.concat(results).drop(columns=["historical_demand"])
    pdf_final["prediction"] = np.clip(pdf_final["prediction"], a_min=0, a_max=None)
    print(f"[INFO] Generated {len(pdf_final)} predictions ({len(pdf_high)} high, {len(pdf_low)} low)")
    return pdf_final


def save_predictions(
    spark: SparkSession,
    prediction_pdf: pd.DataFrame,
    config: configparser.ConfigParser,
    year: int,
    month: int,
) -> None:
    if prediction_pdf.empty:
        print(f"[INFO] No predictions to write for {year}-{month:02d}.")
        return

    out_cols = [
        "trip_date",
        "PULocationID",
        "zone_type",
        "prediction",
        "day_of_week",
        "is_weekend",
        "month",
        "lag_28d",
        "lag_35d",
        "rolling_28d_avg",
        "is_holiday",
        "feature_year",
        "feature_month",
    ]
    final_pdf = prediction_pdf[out_cols].copy()
    final_pdf["prediction_year"] = year
    final_pdf["prediction_month"] = month

    out_df = spark.createDataFrame(final_pdf)
    table_fqn = "gold.predictions.monthly_demand_forecast"

    # Ensure namespaces exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold.predictions")

    # BUG-029 recovery: check if table is accessible
    table_exists = False
    try:
        spark.table(table_fqn)
        table_exists = True
    except Exception as e:
        err_str = _safe_str_exc(e)
        if "NotFoundException" in err_str or "FileNotFoundException" in err_str:
            print(f"[WARN] stale Nessie metadata detected for {table_fqn} — dropping stale reference.")
            # Step 1: SQL drop
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
                print(f"[INFO] SQL DROP succeeded for {table_fqn}")
            except Exception:
                # Step 2: REST fallback
                nessie_uri = spark.conf.get("spark.sql.catalog.gold.uri")
                nessie_ref = spark.conf.get("spark.sql.catalog.gold.ref", "main")
                _nessie_drop_table_via_rest(nessie_uri, ["predictions"], "monthly_demand_forecast", nessie_ref)
            
            # Step 3: Evict cache 
            try:
                spark.catalog.refreshTable(table_fqn)
            except Exception:
                pass
        else:
            # Table genuinely doesn't exist yet, which is fine
            pass

    if not table_exists:
        print(f"[INFO] Creating new Iceberg table {table_fqn}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_fqn} (
                trip_date DATE,
                PULocationID INT,
                zone_type INT,
                prediction DOUBLE,
                day_of_week INT,
                is_weekend INT,
                month INT,
                lag_28d DOUBLE,
                lag_35d DOUBLE,
                rolling_28d_avg DOUBLE,
                is_holiday INT,
                feature_year INT,
                feature_month INT,
                prediction_year INT,
                prediction_month INT
            )
            USING iceberg
            PARTITIONED BY (prediction_year, prediction_month)
            """
        )

    # BUG-024: Use Iceberg overwritePartitions for atomic updates
    (
        out_df.writeTo(table_fqn)
        .overwritePartitions()
    )

    print(
        f"[INFO] Wrote {len(final_pdf)} predictions to Iceberg table "
        f"{table_fqn} (year={year}, month={month})"
    )


def get_latest_feature_period(
    spark: SparkSession, config: configparser.ConfigParser
) -> tuple[int, int]:
    """Get the latest available feature year/month from parquet."""
    input_path = _resolve_feature_path(config)
    df = spark.read.option("mergeSchema", "true").parquet(input_path)
    max_row = df.agg(
        F.max("feature_year").alias("max_year"),
        F.max("feature_month").alias("max_month")
    ).collect()[0]
    latest_year = max_row["max_year"]
    latest_month = max_row["max_month"]
    print(f"[INFO] Latest features: {latest_year}-{latest_month:02d}")
    return latest_year, latest_month


def run(year: int | None, month: int | None, auto_next: bool, force: bool = False) -> None:
    pipeline_name = "monthly_demand_forecast"
    config = _load_config()

    run_payload: Dict[str, Any] = {
        "run_id": str(uuid4()),
        "pipeline_name": pipeline_name,
        "year": None,
        "month": None,
        "status": "RUNNING",
        "table_name": "monthly_demand_forecast",
        "started_at": _now_utc(),
        "finished_at": None,
        "error_message": None,
        "spark_app_id": None,
    }

    from etls.gold_transform import _load_minio_config_gold
    
    spark = build_spark_session_ml(config)
    run_payload["spark_app_id"] = spark.sparkContext.applicationId

    try:
        if auto_next and year is None and month is None:
            latest_year, latest_month = get_latest_feature_period(spark, config)
            year = latest_year + 1 if latest_month == 12 else latest_year
            month = 1 if latest_month == 12 else latest_month + 1
            print(f"[INFO] Auto mode: Predicting {year}-{month:02d} using features from previous period.")
            run_payload["year"] = year
            run_payload["month"] = month
        else:
            # Đảm bảo payload luôn có thông tin thời gian để ghi log chính xác
            run_payload["year"] = year
            run_payload["month"] = month

        if not force and _check_ml_run_already_succeeded(config, pipeline_name, year, month):
            run_payload["status"] = "SUCCESS"
            print(f"[INFO] {pipeline_name} already SUCCESS for {year}-{month:02d}. Skipping.")
        else:
            # load_month_feature_data tự xác định lịch sử và điền khuyết (Forward Fill) dựa trên Target Month
            df_month = load_month_feature_data(spark, config, year, month)
            model_high, model_low = load_models(config)
            pred_pdf = predict_monthly_demand(df_month, model_high, model_low)
            save_predictions(spark, pred_pdf, config, year, month)

            run_payload["status"] = "SUCCESS"
            print(f"[INFO] Monthly batch prediction completed for {year}-{month:02d}")
    except Exception as e:
        run_payload["status"] = "FAILED"
        run_payload["error_message"] = _safe_str_exc(e)
        raise
    finally:
        run_payload["finished_at"] = _now_utc()
        if spark is not None:
            try:
                spark.stop()
            except Exception as e:
                print(f"[WARN] spark.stop() failed: {e!r}")
        _log_ml_run(config, run_payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch predict monthly taxi demand")
    parser.add_argument("--year", type=int, help="Target year, e.g. 2026")
    parser.add_argument("--month", type=int, choices=range(1, 13), help="Target month 1..12")
    parser.add_argument("--auto-next", action="store_true", default=True, help="Auto-detect latest features and predict N+1 month")
    parser.add_argument("--force", action="store_true", default=False, help="Force re-run even if already succeeded in database")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.year, args.month, args.auto_next, args.force)
