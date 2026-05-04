import configparser
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window

import psycopg2
import holidays
from datetime import date

from etls.gold_transform import _now_vn


def _load_config(config_path: str = "config/config.conf") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    project_root = Path(__file__).resolve().parents[1]
    loaded = config.read(project_root / config_path)
    if not loaded:
        raise FileNotFoundError(f"Could not read config file at {project_root / config_path}")
    return config


def _build_spark_session_for_ml(config: configparser.ConfigParser) -> SparkSession:
    minio_endpoint = config.get("minio", "endpoint")
    minio_access_key = config.get("minio", "access_key")
    minio_secret_key = config.get("minio", "secret_key")
    minio_secure = config.getboolean("minio", "secure", fallback=False)

    return (
        SparkSession.builder.appName("ml_feature_engineering")
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio_secure).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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



def _resolve_silver_root_path(config: configparser.ConfigParser) -> str:
    silver_bucket = config.get("minio", "silver_bucket", fallback="silver")
    # Đọc TOÀN BỘ thư mục root Silver thay vì chỉ đọc 1 tháng phân vùng
    return f"s3a://{silver_bucket}/silver/taxi_trips/"


def _resolve_ml_output_path(config: configparser.ConfigParser) -> str:
    gold_bucket = config.get("minio", "gold_bucket", fallback="gold")
    # dedicated ML folder inside Gold layer
    return f"s3a://{gold_bucket}/ml/training/daily_demand_features/"

def _safe_str_exc(exc: BaseException) -> str:
    """Safely convert an exception to a string without crashing the error-handling path.

    ``Py4JJavaError.__str__()`` makes a round-trip call back into the JVM to fetch
    the Java stack trace.  If the Py4J gateway has already been torn down — e.g.
    after a JVM crash during a Spark write — that call raises another
    ``py4j.protocol.Py4JError``, masking the original exception and crashing the
    entire ``except`` block (BUG-019).

    Fallback chain:
      1. ``str(exc)``          — normal path, works for all non-Py4J exceptions.
      2. ``repr(exc)``         — safe fallback; never calls back into the JVM.
      3. sentinel string       — last resort if even ``repr()`` somehow fails.
    """
    try:
        return str(exc)
    except Exception:  # noqa: BLE001
        try:
            return repr(exc)
        except Exception:  # noqa: BLE001
            return f"<{type(exc).__name__}: str() and repr() both failed>"

def _ensure_ml_metadata_table_exists(conn, config: configparser.ConfigParser) -> None:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    table = db_cfg.get("ml_data_log_table", "ml_data_log")
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id BIGSERIAL PRIMARY KEY,
        run_id  TEXT NOT NULL,
        pipeline_name TEXT NOT NULL,
        year  INTERGER,
        month INTERGER,
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
            run_id, pipeline_name, year, month, status, table_name, started_at, finished_at, error_message, spark_app_id
        ) 
        VALUES 
        (%(run_id)s, %(pipeline_name)s, %(year)s, %(month)s, 
        %(status)s, %(table_name)s, %(started_at)s, %(finished_at)s, 
        %(error_message)s, %(spark_app_id)s);
        """
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (
                    run_payload.get("run_id"),
                    run_payload.get("pipeline_name"),
                    run_payload.get("year"),
                    run_payload.get("month"),
                    run_payload.get("status"),
                    run_payload.get("table_name"),
                    run_payload.get("started_at"),
                    run_payload.get("finished_at"),
                    run_payload.get("error_message"),
                    run_payload.get("spark_app_id"),
                ),
            )
        conn.commit()
    except Exception as e:
        print(f"[WARN] Error logging ML run: {e}")
    finally:
        conn.close()

def _check_ml_run_already_succeeded(config: configparser.ConfigParser, 
    year: Optional[int], month: Optional[int]) -> bool:
    if year is None or month is None:
        return False  # Nếu không có year/month thì không thể check, cứ chạy bình thường
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    log_table = db_cfg.get("ml_data_log_table", "ml_data_log")
    
    try:
        conn = _get_db_connection(config)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {schema}.{log_table} "
                    "WHERE pipeline_name = %s AND year = %s AND month = %s AND status = 'success' LIMIT 1",
                    (year,month),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as e:
        print(
            f"[WARN] Error checking ML run status: {e}"
            f"({e!r}) - proceeding with transform"
            )
        return False

def run_ml_feature_engineering(
    pipeline_name: str = "nyc_taxi_ml_features",
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> None:
    # Không dùng year/month rào scope File đọc đầu vào vì Lag Features yêu cầu lục lịch sử.

    config = _load_config()
    spark = _build_spark_session_for_ml(config)

    if _check_ml_run_already_succeeded(config, year, month):
        month_str = f"{month:02d}" if month else "all_months"
        print(
            f"[INFO] ML data already exists for {pipeline_name} year={year} month={month_str}, skipping transform."

        )
        return
    run_payload: Dict[str, Any] = {
    "run_id":        str(uuid4()),
    "pipeline_name": pipeline_name,
    "year":          year,
    "month":         month,
    "status":        "RUNNING",
    "table_name":    "daily_demand_features",
    "started_at":    _now_vn(),
}

    try:
        input_path = _resolve_silver_root_path(config)
        output_path = _resolve_ml_output_path(config)

        # Merge Schema và Load Toàn bộ Silver Layer
        silver_df: DataFrame = spark.read.option("mergeSchema", "true").parquet(input_path)

        # 1. Pipeline Silver hiện tại đã dọn cấu trúc và đổi tên rạch ròi về 'pickup_datetime'
        if "pickup_datetime" not in silver_df.columns:
            raise ValueError("No pickup_datetime column found in silver data.")

        if "PULocationID" not in silver_df.columns:
            raise ValueError("Column PULocationID not found in silver data.")

        base_df = (
            silver_df
            .withColumn("pickup_ts", F.to_timestamp(F.col("pickup_datetime")))
            .withColumn("trip_date", F.to_date(F.col("pickup_ts")))
            .filter(F.col("trip_date").isNotNull())
        )

        # 2. Xây dựng Daily Target Demand
        demand_df = (
            base_df.groupBy("trip_date", "PULocationID")
            .agg(F.count(F.lit(1)).alias("target_demand"))
            .withColumn("day_of_week", F.dayofweek(F.col("trip_date")))
            .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week").isin([1, 7]), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn("month", F.month(F.col("trip_date")))
        )

        # 3. Kỹ thuật Lag bằng Unix Time giúp xử lý lỗi thiếu ngày (Missing Data Data)
        days_to_sec = lambda d: d * 86400
        demand_df_unix = demand_df.withColumn("unix_date_long", F.unix_timestamp("trip_date").cast("long"))

        w_28d = Window.partitionBy("PULocationID").orderBy("unix_date_long").rangeBetween(-days_to_sec(28), -days_to_sec(28))
        w_35d = Window.partitionBy("PULocationID").orderBy("unix_date_long").rangeBetween(-days_to_sec(35), -days_to_sec(35))
        w_rolling_28d = Window.partitionBy("PULocationID").orderBy("unix_date_long").rangeBetween(-days_to_sec(28), -days_to_sec(1))

        featured_df = (
            demand_df_unix.withColumn("lag_28d", F.max("target_demand").over(w_28d))
            .withColumn("lag_35d", F.max("target_demand").over(w_35d))
            .withColumn("rolling_28d_avg", F.avg("target_demand").over(w_rolling_28d))
            .withColumn("feature_year", F.year(F.col("trip_date")))
            .withColumn("feature_month", F.month(F.col("trip_date")))
            .withColumn("is_holiday", F.when(F.col("trip_date").isin([F.lit(d) for d in holidays.US(years=range(2021, 2026))]), F.lit(1)).otherwise(F.lit(0)))
        ).drop("unix_date_long")

        # 4. Filter những dòng tính toán đựợc đầy đủ Lag Features (Cắt bỏ tháng đầu tiên ko có dữ kiện gốc)
        final_df = featured_df.filter(
            F.col("lag_28d").isNotNull() & F.col("lag_35d").isNotNull()
        )

        # 5. Ghi Output, nên dùng Mode Overwrite Partition tự động đè lại thay vì Append nối đuôi đè lên nhau
        (
            final_df.write
            .mode("overwrite")
            .partitionBy("feature_year", "feature_month")
            .parquet(output_path)
        )
        run_payload["status"] = "SUCCESS"
    except Exception as e:
        run_payload["status"] = "FAILED"
        run_payload["error_message"] = _safe_str_exc(e)
        raise
    finally:
        run_payload["finished_at"] = _now_vn()
        if spark:
            try:
                spark.stop()
            except Exception as e:
                print(
                    f"[WARN] spark.stop() raised {e!r} — "
                    "JVM may already be dead (SIGTERM / crash). "
                    "Continuing to log metadata."
                )
        _log_ml_run(config, run_payload)
        month_str = f"{month:02d}" if month else "all"
        print(f"ML data was ingested {run_payload['status']} for year={year}, month={month_str}")

