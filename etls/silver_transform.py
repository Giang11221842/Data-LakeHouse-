from __future__ import annotations

import configparser
import os
import ast
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg2
from minio import Minio
from psycopg2.extras import Json

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.conf"
GE_ROOT = Path(__file__).resolve().parents[1] / "great_expectations"
EXPECTATION_SUITE_NAME = "silver_taxi_suite"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
BRONZE_OBJECT_PATTERN = re.compile(
    r"^bronze/(?P<service_type>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{1,2})/(?P<file_name>.+\.parquet)$"
)
SILVER_OBJECT_PATTERN = re.compile(
    r"^silver/taxi_trips/year=(?P<year>\d{4})/month=(?P<month>\d{1,2})/(?P<file_name>.+\.parquet)$"
)


def _now_vn() -> datetime:
    return datetime.now(VN_TZ)


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


def _load_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config


def _load_minio_config(config: configparser.ConfigParser) -> dict:
    minio_cfg = config["minio"]
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", minio_cfg.get("endpoint", "minio:9000")),
        "access_key": os.getenv("MINIO_ACCESS_KEY", minio_cfg.get("access_key", "minioadmin")),
        "secret_key": os.getenv("MINIO_SECRET_KEY", minio_cfg.get("secret_key", "minioadmin")),
        "secure": os.getenv("MINIO_SECURE", minio_cfg.get("secure", "false")).lower() == "true",
        "bronze_bucket": os.getenv("BRONZE_BUCKET", minio_cfg.get("bronze_bucket", "bronze")),
        "silver_bucket": os.getenv("SILVER_BUCKET", minio_cfg.get("silver_bucket", "silver")),
    }


def _load_service_types(config: configparser.ConfigParser) -> List[str]:
    raw_value = config.get("taxi_type", "service_type", fallback="['yellow']")

    try:
        parsed = ast.literal_eval(raw_value)
        if isinstance(parsed, list):
            return [str(item).strip().lower() for item in parsed if str(item).strip()]
    except (SyntaxError, ValueError):
        pass

    return [value.strip().lower() for value in raw_value.split(",") if value.strip()]


def _get_db_connection(config: configparser.ConfigParser):
    db_cfg = config["database"]
    return psycopg2.connect(
        host=db_cfg["database_host"],
        dbname=db_cfg["database_name"],
        user=db_cfg["database_username"],
        password=db_cfg["database_password"],
        port=int(db_cfg["database_port"]),
    )


def _ensure_silver_metadata_tables(
    conn,
    schema: str,
    silver_log_table: str,
    dq_log_table: str,
) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.{silver_log_table} (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        pipeline_name TEXT NOT NULL,
        load_mode TEXT,
        status TEXT NOT NULL,
        error_message TEXT,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        spark_app_id TEXT,
        spark_version TEXT,
        bronze_file_count INTEGER,
        bronze_max_ingestion_id BIGINT,
        bronze_yellow_rows BIGINT,
        bronze_green_rows BIGINT,
        silver_output_rows BIGINT,
        silver_path TEXT,
        ge_suite_name TEXT,
        ge_success BOOLEAN,
        ge_total_expectations INTEGER,
        ge_successful_expectations INTEGER,
        ge_failed_expectations INTEGER
    );

    CREATE TABLE IF NOT EXISTS {schema}.{dq_log_table} (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        expectation_suite_name TEXT NOT NULL,
        success BOOLEAN NOT NULL,
        total_expectations INTEGER NOT NULL,
        successful_expectations INTEGER NOT NULL,
        failed_expectations INTEGER NOT NULL,
        validation_result JSONB,
        created_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS {schema}.silver_file_log (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        service_type TEXT NOT NULL,
        year INTEGER,
        month INTEGER,
        bronze_ingestion_id BIGINT,
        bronze_object_key TEXT,
        status TEXT NOT NULL,
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(
            f"ALTER TABLE {schema}.{silver_log_table} "
            "ADD COLUMN IF NOT EXISTS load_mode TEXT"
        )
        cur.execute(
            f"ALTER TABLE {schema}.{silver_log_table} "
            "ADD COLUMN IF NOT EXISTS bronze_file_count INTEGER"
        )
        cur.execute(
            f"ALTER TABLE {schema}.{silver_log_table} "
            "ADD COLUMN IF NOT EXISTS bronze_max_ingestion_id BIGINT"
        )
        cur.execute(
            f"ALTER TABLE {schema}.silver_file_log "
            "ADD COLUMN IF NOT EXISTS year INTEGER"
        )
        cur.execute(
            f"ALTER TABLE {schema}.silver_file_log "
            "ADD COLUMN IF NOT EXISTS month INTEGER"
        )
    conn.commit()


def _get_minio_client(config: configparser.ConfigParser) -> Minio:
    minio_cfg = _load_minio_config(config)
    return Minio(
        endpoint=minio_cfg["endpoint"],
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=minio_cfg["secure"],
    )


def _parse_bronze_object_key(object_key: str) -> Optional[Dict[str, Any]]:
    match = BRONZE_OBJECT_PATTERN.match(object_key)
    if not match:
        return None
    return {
        "service_type": str(match.group("service_type")).lower(),
        "year": int(match.group("year")),
        "month": int(match.group("month")),
    }


def _list_bronze_objects(config: configparser.ConfigParser) -> List[Dict[str, Any]]:
    minio_cfg = _load_minio_config(config)
    bronze_bucket = minio_cfg["bronze_bucket"]
    allowed_services = set(_load_service_types(config))
    client = _get_minio_client(config)

    records: List[Dict[str, Any]] = []
    for obj in client.list_objects(bronze_bucket, prefix="bronze/", recursive=True):
        object_key = obj.object_name
        if not object_key.endswith(".parquet"):
            continue

        parsed = _parse_bronze_object_key(object_key)
        if parsed is None:
            continue
        if allowed_services and parsed["service_type"] not in allowed_services:
            continue

        records.append(
            {
                "minio_bucket": bronze_bucket,
                "minio_object_key": object_key,
                "service_type": parsed["service_type"],
                "year": parsed["year"],
                "month": parsed["month"],
            }
        )

    records.sort(key=lambda x: (x["year"], x["month"], x["service_type"], x["minio_object_key"]))
    return records


def _list_silver_partitions(config: configparser.ConfigParser) -> set[Tuple[int, int]]:
    minio_cfg = _load_minio_config(config)
    silver_bucket = minio_cfg["silver_bucket"]
    client = _get_minio_client(config)

    partitions: set[Tuple[int, int]] = set()
    for obj in client.list_objects(silver_bucket, prefix="silver/taxi_trips/", recursive=True):
        match = SILVER_OBJECT_PATTERN.match(obj.object_name)
        if match is None:
            continue
        partitions.add((int(match.group("year")), int(match.group("month"))))

    return partitions


def _verify_silver_output_partitions(
    config: configparser.ConfigParser,
    expected_partitions: set[Tuple[int, int]],
) -> None:
    actual_partitions = _list_silver_partitions(config)
    missing_partitions = sorted(expected_partitions - actual_partitions)
    if missing_partitions:
        missing_text = ", ".join(f"{year}-{month:02d}" for year, month in missing_partitions)
        raise ValueError(
            "Silver write verification failed; missing materialized partitions: "
            f"{missing_text}"
        )


def _get_latest_bronze_ingestion_metadata_by_object(
    config: configparser.ConfigParser,
    object_keys: List[str],
) -> Dict[str, Dict[str, Any]]:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    table = db_cfg.get("database_log_table", "ingestion_log")

    if not object_keys:
        return {}

    conn = _get_db_connection(config)
    try:
        query = f"""
        SELECT DISTINCT ON (minio_object_key)
            id,
            minio_object_key,
            service_type,
            year,
            month,
            finished_at
        FROM {schema}.{table}
        WHERE layer_name = 'bronze'
          AND status = 'SUCCESS'
          AND minio_object_key = ANY(%s)
        ORDER BY minio_object_key, id DESC
        """
        with conn.cursor() as cur:
            cur.execute(query, (object_keys,))
            rows = cur.fetchall()
            return {
                str(row[1]): {
                    "id": int(row[0]),
                    "service_type": str(row[2]).lower() if row[2] is not None else None,
                    "year": int(row[3]) if row[3] is not None else None,
                    "month": int(row[4]) if row[4] is not None else None,
                    "finished_at": row[5],
                }
                for row in rows
            }
    finally:
        conn.close()


def _build_bronze_records_by_service(
    bronze_logs: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    records: Dict[str, List[Dict[str, Any]]] = {}
    for log in bronze_logs:
        service = str(log["service_type"]).lower()
        records.setdefault(service, []).append(log)
    return records


def _discover_bronze_records(config: configparser.ConfigParser) -> List[Dict[str, Any]]:
    storage_records = _list_bronze_objects(config)
    object_keys = [str(item["minio_object_key"]) for item in storage_records]
    metadata_by_key = _get_latest_bronze_ingestion_metadata_by_object(config, object_keys)

    records: List[Dict[str, Any]] = []
    for item in storage_records:
        meta = metadata_by_key.get(str(item["minio_object_key"]), {})
        records.append(
            {
                "id": meta.get("id"),
                "service_type": item["service_type"],
                "year": item["year"],
                "month": item["month"],
                "minio_bucket": item["minio_bucket"],
                "minio_object_key": item["minio_object_key"],
                "finished_at": meta.get("finished_at"),
            }
        )
    return records


def _get_successful_silver_files(config: configparser.ConfigParser) -> Tuple[set[tuple[str, int]], set[str]]:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    silver_file_table = db_cfg.get("silver_file_log_table", "silver_file_log")
    silver_log_table = db_cfg.get("silver_transform_log_table", "silver_transform_log")
    dq_log_table = db_cfg.get("dq_log_table", "data_quality_log")

    conn = _get_db_connection(config)
    try:
        _ensure_silver_metadata_tables(conn, schema, silver_log_table, dq_log_table)
        query = f"""
        SELECT bronze_object_key, bronze_ingestion_id
        FROM {schema}.{silver_file_table}
        WHERE status = 'SUCCESS'
          AND bronze_object_key IS NOT NULL
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            pair_set = {
                (str(r[0]), int(r[1]))
                for r in rows
                if r[0] is not None and r[1] is not None
            }
            key_set = {str(r[0]) for r in rows if r[0] is not None}
            return pair_set, key_set
    finally:
        conn.close()


def _log_silver_file_runs(
    config: configparser.ConfigParser,
    run_id: str,
    file_logs: List[Dict[str, Any]],
) -> None:
    if not file_logs:
        return

    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    silver_file_table = db_cfg.get("silver_file_log_table", "silver_file_log")
    silver_log_table = db_cfg.get("silver_transform_log_table", "silver_transform_log")
    dq_log_table = db_cfg.get("dq_log_table", "data_quality_log")

    conn = _get_db_connection(config)
    try:
        _ensure_silver_metadata_tables(conn, schema, silver_log_table, dq_log_table)
        insert_sql = f"""
        INSERT INTO {schema}.{silver_file_table}
        (
            run_id,
            service_type,
            year,
            month,
            bronze_ingestion_id,
            bronze_object_key,
            status,
            error_message,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cur:
            for item in file_logs:
                cur.execute(
                    insert_sql,
                    (
                        run_id,
                        item.get("service_type"),
                        item.get("year"),
                        item.get("month"),
                        item.get("bronze_ingestion_id"),
                        item.get("bronze_object_key"),
                        item.get("status", "UNKNOWN"),
                        item.get("error_message"),
                        _now_vn(),
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def _log_silver_run(
    config: configparser.ConfigParser,
    run_payload: Dict[str, Any],
    ge_payload: Dict[str, Any] | None = None,
) -> None:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    silver_log_table = db_cfg.get("silver_transform_log_table", "silver_transform_log")
    dq_log_table = db_cfg.get("dq_log_table", "data_quality_log")

    conn = _get_db_connection(config)
    try:
        _ensure_silver_metadata_tables(
            conn=conn,
            schema=schema,
            silver_log_table=silver_log_table,
            dq_log_table=dq_log_table,
        )

        silver_insert_sql = f"""
        INSERT INTO {schema}.{silver_log_table}
        (
            run_id,
            pipeline_name,
            load_mode,
            status,
            error_message,
            started_at,
            finished_at,
            spark_app_id,
            spark_version,
            bronze_file_count,
            bronze_max_ingestion_id,
            bronze_yellow_rows,
            bronze_green_rows,
            silver_output_rows,
            silver_path,
            ge_suite_name,
            ge_success,
            ge_total_expectations,
            ge_successful_expectations,
            ge_failed_expectations
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with conn.cursor() as cur:
            cur.execute(
                silver_insert_sql,
                (
                    run_payload["run_id"],
                    run_payload["pipeline_name"],
                    run_payload.get("load_mode"),
                    run_payload["status"],
                    run_payload.get("error_message"),
                    run_payload["started_at"],
                    run_payload.get("finished_at"),
                    run_payload.get("spark_app_id"),
                    run_payload.get("spark_version"),
                    run_payload.get("bronze_file_count"),
                    run_payload.get("bronze_max_ingestion_id"),
                    run_payload.get("bronze_yellow_rows"),
                    run_payload.get("bronze_green_rows"),
                    run_payload.get("silver_output_rows"),
                    run_payload.get("silver_path"),
                    run_payload.get("ge_suite_name"),
                    run_payload.get("ge_success"),
                    run_payload.get("ge_total_expectations"),
                    run_payload.get("ge_successful_expectations"),
                    run_payload.get("ge_failed_expectations"),
                ),
            )

        if ge_payload is not None:
            dq_insert_sql = f"""
            INSERT INTO {schema}.{dq_log_table}
            (
                run_id,
                expectation_suite_name,
                success,
                total_expectations,
                successful_expectations,
                failed_expectations,
                validation_result,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            with conn.cursor() as cur:
                cur.execute(
                    dq_insert_sql,
                    (
                        run_payload["run_id"],
                        ge_payload["expectation_suite_name"],
                        ge_payload["success"],
                        ge_payload["total_expectations"],
                        ge_payload["successful_expectations"],
                        ge_payload["failed_expectations"],
                        Json(ge_payload["validation_result"]),
                        _now_vn(),
                    ),
                )

        conn.commit()
    finally:
        conn.close()


def _df_count_with_timeout(df: "DataFrame", timeout_seconds: int = 60) -> Optional[int]:
    """
    DataFrame-native time-limited count. Runs ``df.count()`` in a daemon thread and
    returns ``None`` if it does not complete within *timeout_seconds*.

    This replaces ``df.rdd.countApprox()`` throughout the pipeline.  Accessing
    ``.rdd`` on a DataFrame calls ``javaToPython()`` on the underlying Java object,
    which raises ``Py4JJavaError`` when the query plan has issues — most commonly a
    deep union tree (one union per file) that causes a JVM ``StackOverflowError``
    during query planning.  ``df.count()`` is a DataFrame-native action that does
    not go through the Py4J ``javaToPython`` bridge.

    Row counts are metadata-only; returning ``None`` on timeout keeps the pipeline
    moving without blocking on large scans.
    """
    import threading

    result: list = [None]
    exc: list = [None]

    def _count() -> None:
        try:
            result[0] = int(df.count())
        except Exception as e:  # noqa: BLE001
            exc[0] = e

    t = threading.Thread(target=_count, daemon=True)
    t.start()
    t.join(timeout=timeout_seconds)

    if t.is_alive():
        print(
            f"[WARN] _df_count_with_timeout: df.count() did not finish within "
            f"{timeout_seconds}s — returning None (pipeline continues)."
        )
        return None
    if exc[0] is not None:
        print(f"[WARN] _df_count_with_timeout: df.count() raised {exc[0]!r} — returning None.")
        return None
    return result[0]


def build_spark_session() -> SparkSession:
    from pyspark.sql import SparkSession

    config = _load_config()
    minio_cfg = _load_minio_config(config)

    endpoint = minio_cfg["endpoint"]
    spark_driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    spark_executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    spark_shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "64")
    spark_max_partition_bytes = os.getenv("SPARK_SQL_MAX_PARTITION_BYTES", "134217728")  # 128 MB (was 32 MB — BUG-010)

    return (
        SparkSession.builder
        .appName("Taxi Silver Layer")
        .config("spark.driver.memory", spark_driver_memory)
        .config("spark.executor.memory", spark_executor_memory)
        .config("spark.sql.shuffle.partitions", spark_shuffle_partitions)
        .config("spark.sql.files.maxPartitionBytes", spark_max_partition_bytes)
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", os.getenv("SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD", "20MB"))
        .config("spark.sql.broadcastTimeout", os.getenv("SPARK_SQL_BROADCAST_TIMEOUT", "600"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.instances", "1")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.rpc.message.maxSize", "256")
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio_cfg["secure"]).lower())
        # Bronze parquet files can have type drift between partitions (e.g., int32 vs int64).
        # Use schema merge and disable vectorized reader to avoid conversion failures.
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )


def _yellow_schema() -> StructType:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
        ]
    )


def _green_schema() -> StructType:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("lpep_pickup_datetime", TimestampType(), True),
            StructField("lpep_dropoff_datetime", TimestampType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("ehail_fee", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("payment_type", LongType(), True),
            StructField("trip_type", LongType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
        ]
    )


def _load_bronze_dataframes(
    spark: SparkSession,
    bronze_records: Dict[str, List[Dict[str, Any]]],
    load_mode: str,
) -> Tuple[DataFrame, DataFrame]:
    from collections import defaultdict
    from pyspark.sql.functions import lit

    reader = spark.read.option("ignoreCorruptFiles", "true")

    yellow_df = spark.createDataFrame([], schema=_yellow_schema())
    green_df = spark.createDataFrame([], schema=_green_schema())

    def _load_service_dfs(records: List[Dict[str, Any]]) -> List[Any]:
        """
        Group bronze records by (year, month) and read each period's files together
        via a single ``spark.read.parquet(*paths)`` call.

        The previous approach unioned files one-by-one, creating an N-deep union tree
        (one node per file).  For 9 years × 12 months = 108 files per service type,
        Spark's recursive query planner hits a JVM ``StackOverflowError``, which
        surfaces as ``Py4JJavaError`` when ``javaToPython`` is subsequently called.
        Grouping by period reduces the union tree depth from N files → M distinct
        periods (at most 12 per year for monthly data).

        Source period is still attached from bronze folder metadata (not from
        pickup_datetime) to prevent partition drift on anomalous dates.
        """
        period_groups: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)
        for record in records:
            period_groups[(int(record["year"]), int(record["month"]))].append(record)

        period_dfs: List[Any] = []
        for (yr, mo), group_records in sorted(period_groups.items()):
            paths = [
                f"s3a://{r['minio_bucket']}/{r['minio_object_key']}"
                for r in group_records
            ]
            period_df = (
                reader.parquet(*paths)
                .withColumn("source_year", lit(yr))
                .withColumn("source_month", lit(mo))
            )
            period_dfs.append(period_df)
        return period_dfs

    if "yellow" in bronze_records and bronze_records["yellow"]:
        period_dfs = _load_service_dfs(bronze_records["yellow"])
        if period_dfs:
            yellow_df = period_dfs[0]
            for df in period_dfs[1:]:
                yellow_df = yellow_df.unionByName(df, allowMissingColumns=True)

    if "green" in bronze_records and bronze_records["green"]:
        period_dfs = _load_service_dfs(bronze_records["green"])
        if period_dfs:
            green_df = period_dfs[0]
            for df in period_dfs[1:]:
                green_df = green_df.unionByName(df, allowMissingColumns=True)

    return yellow_df, green_df


def _transform_yellow(df_yellow: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, lit, to_timestamp

    # Normalize airport fee column across source variants.
    if "airport_fee" in df_yellow.columns:
        normalized_df = df_yellow
    elif "Airport_fee" in df_yellow.columns:
        normalized_df = df_yellow.withColumnRenamed("Airport_fee", "airport_fee")
    else:
        normalized_df = df_yellow.withColumn("airport_fee", lit(0.0))

    return (
        normalized_df
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
        .withColumn("service_type", lit("yellow"))
        .withColumn("RatecodeID", col("RatecodeID").cast("int"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("payment_type", col("payment_type").cast("int"))
        .withColumn("trip_type", lit(0).cast("int"))
        .withColumn("airport_fee", col("airport_fee").cast("double"))
        .fillna(
            {
                "store_and_fwd_flag": "N",
                "RatecodeID": -1,
                "passenger_count": 1,
                "congestion_surcharge": 0.0,
                "airport_fee": 0.0,
            }
        )
    )


def _transform_green(df_green: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, lit, to_timestamp

    return (
        df_green
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
        .withColumn("service_type", lit("green"))
        .drop("ehail_fee")
        .withColumn("RatecodeID", col("RatecodeID").cast("int"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("trip_type", col("trip_type").cast("int"))
        .withColumn("payment_type", col("payment_type").cast("int"))
        .withColumn("airport_fee", lit(0.0))
        .fillna(
            {
                "store_and_fwd_flag": "N",
                "RatecodeID": 0,
                "passenger_count": 0,
                "congestion_surcharge": 0.0,
                "payment_type": 0,
                "trip_type": -1,
            }
        )
    )


def _normalize_taxi_schema(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, TimestampType

    cast_map = {
        "VendorID": LongType(),
        "pickup_datetime": TimestampType(),
        "dropoff_datetime": TimestampType(),
        "passenger_count": IntegerType(),
        "trip_distance": DoubleType(),
        "RatecodeID": IntegerType(),
        "store_and_fwd_flag": StringType(),
        "PULocationID": LongType(),
        "DOLocationID": LongType(),
        "payment_type": IntegerType(),
        "fare_amount": DoubleType(),
        "extra": DoubleType(),
        "mta_tax": DoubleType(),
        "tip_amount": DoubleType(),
        "tolls_amount": DoubleType(),
        "improvement_surcharge": DoubleType(),
        "total_amount": DoubleType(),
        "congestion_surcharge": DoubleType(),
        "airport_fee": DoubleType(),
        "service_type": StringType(),
        "trip_type": IntegerType(),
        "trip_duration_minutes": DoubleType(),
    }

    normalized_df = df
    for field_name, target_type in cast_map.items():
        if field_name in normalized_df.columns:
            normalized_df = normalized_df.withColumn(field_name, col(field_name).cast(target_type))

    return normalized_df


def _build_silver_dataframe(df_yellow: DataFrame, df_green: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, isnan, unix_timestamp, year

    yellow_transformed = _normalize_taxi_schema(_transform_yellow(df_yellow))
    green_transformed = _normalize_taxi_schema(_transform_green(df_green))

    silver_df = yellow_transformed.unionByName(green_transformed, allowMissingColumns=True)

    silver_df = silver_df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0,
    )

    pickup_year_col = year(col("pickup_datetime"))
    dropoff_year_col = year(col("dropoff_datetime"))
    #max_valid_year = _now_vn().year + 1

    silver_df = silver_df.filter(
        (col("pickup_datetime").isNotNull())
        & (col("dropoff_datetime").isNotNull())
        & (~isnan(col("trip_distance")))
        & (col("trip_distance") >= 0.01)
        #& (col("trip_distance") <= 200)
        & (col("total_amount") >= 0)
        & (~isnan(col("trip_duration_minutes")))
        & (col("trip_duration_minutes") > 0)
        #& (col("trip_duration_minutes") < 600)
        & (col("dropoff_datetime") >= col("pickup_datetime"))
        & (col("service_type").isin("yellow", "green"))
        & (pickup_year_col >= 2021)
        #& (pickup_year_col <= max_valid_year)
        & (dropoff_year_col >= 2021)
        #& (dropoff_year_col <= max_valid_year)
        & (col("pickup_datetime")<_now_vn())
        & (col("dropoff_datetime")<_now_vn())  
        & (col("source_year").isNotNull())
        & (col("source_month").isNotNull())
    )

    silver_df = silver_df.withColumn("pickup_year", pickup_year_col)
    silver_df = silver_df.withColumn("dropoff_year", dropoff_year_col)
    silver_df = silver_df.withColumn("year", col("source_year").cast("int"))
    silver_df = silver_df.withColumn("month", col("source_month").cast("int"))

    return silver_df


def _validate_with_great_expectations(silver_df: DataFrame) -> Dict[str, Any]:
    import great_expectations as gx

    ge_sample_rows = int(os.getenv("GE_VALIDATION_SAMPLE_ROWS", "300000"))
    validation_df = silver_df.limit(ge_sample_rows)

    context = gx.get_context(context_root_dir=str(GE_ROOT))
    context.add_or_update_expectation_suite(expectation_suite_name=EXPECTATION_SUITE_NAME)

    try:
        datasource = context.get_datasource("silver_spark_source")
    except Exception:
        datasource = context.sources.add_or_update_spark(name="silver_spark_source")

    try:
        dataframe_asset = datasource.get_asset("silver_taxi_df_asset")
    except Exception:
        dataframe_asset = datasource.add_dataframe_asset(name="silver_taxi_df_asset")
    batch_request = dataframe_asset.build_batch_request(dataframe=validation_df)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=EXPECTATION_SUITE_NAME,
    )

    validator.expect_column_values_to_not_be_null("pickup_datetime")
    validator.expect_column_values_to_not_be_null("dropoff_datetime")
    validator.expect_column_values_to_be_between("trip_distance", min_value=0.01)
    validator.expect_column_values_to_be_between("trip_duration_minutes", min_value=0)
    validator.expect_column_values_to_be_in_set("service_type", ["yellow", "green"])
    validator.expect_column_values_to_be_between("pickup_year", min_value=2021)
    validator.expect_column_values_to_not_be_null("year")
    validator.expect_column_values_to_not_be_null("month")

    validator.save_expectation_suite(discard_failed_expectations=False)
    results = validator.validate()
    results_json = results.to_json_dict()
    total_expectations = len(results_json.get("results", []))
    successful_expectations = sum(
        1 for item in results_json.get("results", []) if item.get("success") is True
    )
    failed_expectations = total_expectations - successful_expectations

    return {
        "expectation_suite_name": EXPECTATION_SUITE_NAME,
        "success": bool(results_json.get("success", False)),
        "total_expectations": total_expectations,
        "successful_expectations": successful_expectations,
        "failed_expectations": failed_expectations,
        "validation_result": results_json,
    }


def _summarize_failed_expectations(validation_result: Dict[str, Any]) -> str:
    failed = []
    for item in validation_result.get("results", []):
        if item.get("success") is True:
            continue
        expectation_config = item.get("expectation_config", {})
        expectation_type = expectation_config.get("expectation_type", "unknown_expectation")
        kwargs = expectation_config.get("kwargs", {})
        column_name = kwargs.get("column")
        if column_name:
            failed.append(f"{expectation_type}(column={column_name})")
        else:
            failed.append(expectation_type)

    if not failed:
        return "unknown"
    return ", ".join(failed)


def _distinct_year_month_pairs(df: DataFrame) -> List[Tuple[int, int]]:
    from pyspark.sql.functions import col

    if "year" not in df.columns or "month" not in df.columns:
        return []

    try:
        rows = (
            df.select(col("year").cast("int").alias("year"), col("month").cast("int").alias("month"))
            .distinct()
            .collect()
        )
    except Exception as exc:  # noqa: BLE001
        # .collect() uses javaToPython internally; guard against Py4J bridge failures
        # so that an incremental merge failure does not abort the entire silver run.
        print(f"[WARN] _distinct_year_month_pairs: collect() failed: {exc!r} — returning []")
        return []

    pairs: List[Tuple[int, int]] = []
    for row in rows:
        if row["year"] is None or row["month"] is None:
            continue
        pairs.append((int(row["year"]), int(row["month"])))
    pairs.sort()
    return pairs


def _read_silver_partition(
    spark: SparkSession,
    silver_path: str,
    partition_year: int,
    partition_month: int,
) -> Optional[DataFrame]:
    from pyspark.sql.functions import lit
    from pyspark.sql.utils import AnalysisException

    partition_path = f"{silver_path.rstrip('/')}/year={partition_year}/month={partition_month}"
    try:
        df = spark.read.parquet(partition_path)
    except AnalysisException:
        return None
    except Exception:
        # On object stores (S3A/MinIO), missing paths can surface as generic Java exceptions.
        return None

    # Ensure partition columns exist even when reading a leaf partition path.
    if "year" not in df.columns:
        df = df.withColumn("year", lit(partition_year).cast("int"))
    if "month" not in df.columns:
        df = df.withColumn("month", lit(partition_month).cast("int"))
    return df


def _df_has_any_row_with_timeout(df: "DataFrame", timeout_seconds: int = 60) -> Optional[bool]:
    """
    Safe non-blocking emptiness check using ``limit(1).count()`` in a daemon thread.

    Returns:
    - True: dataframe has at least 1 row
    - False: dataframe is empty
    - None: check failed or timed out (caller decides fallback strategy)
    """
    import threading

    result: list = [None]
    exc: list = [None]

    def _check() -> None:
        try:
            result[0] = bool(df.limit(1).count() > 0)
        except Exception as e:  # noqa: BLE001
            exc[0] = e

    t = threading.Thread(target=_check, daemon=True)
    t.start()
    t.join(timeout=timeout_seconds)

    if t.is_alive():
        print(
            f"[WARN] _df_has_any_row_with_timeout: limit(1).count() did not finish within "
            f"{timeout_seconds}s — returning None."
        )
        return None
    if exc[0] is not None:
        print(
            f"[WARN] _df_has_any_row_with_timeout: limit(1).count() raised {exc[0]!r} "
            "— returning None."
        )
        return None
    return result[0]


def _write_to_silver(silver_df: DataFrame, load_mode: str) -> str:
    config = _load_config()
    minio_cfg = _load_minio_config(config)
    silver_bucket = minio_cfg["silver_bucket"]
    silver_path = f"s3a://{silver_bucket}/silver/taxi_trips/"

    write_mode = "overwrite"
    write_df = silver_df.drop("source_year", "source_month")
    cols_to_drop = [c for c in ["pickup_year", "dropoff_year"] if c in write_df.columns]
    silver_df_to_write = write_df.drop(*cols_to_drop)
    (
        silver_df_to_write.write.mode(write_mode)
        .option("partitionOverwriteMode", "dynamic")
        .option("maxRecordsPerFile", 5000000)
        .option("compression", "snappy")
        .option("parquet.block.size", 128 * 1024 * 1024)
        .option("parquet.page.size", 1 * 1024 * 1024)
        .partitionBy("year", "month")
        .parquet(silver_path)
    )
    return silver_path


def run_silver_transform(
    pipeline_name: str = "nyc_taxi_silver_transform",
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> Dict[str, Any]:
    config = _load_config()
    successful_file_pairs, successful_file_keys = _get_successful_silver_files(config=config)
    is_first_run = len(successful_file_keys) == 0
    load_mode = "FULL" if is_first_run else "INCREMENTAL"

    all_bronze_logs = _discover_bronze_records(config=config)

    # BUG-020: Scope to specific year/month when provided by DAG context.
    # Prevents processing ALL years when silver_file_log is out of sync.
    if year is not None and month is not None:
        all_bronze_logs = [
            log for log in all_bronze_logs
            if log.get("year") is not None
            and log.get("month") is not None
            and int(log["year"]) == year
            and int(log["month"]) == month
        ]

    # BUG-020: Use object_key only for deduplication (more robust than
    # (object_key, ingestion_id) pair which breaks when bronze is re-ingested
    # and creates a new SUCCESS entry with a new ID).
    bronze_logs = [
        log for log in all_bronze_logs
        if str(log["minio_object_key"]) not in successful_file_keys
    ]

    bronze_records = _build_bronze_records_by_service(bronze_logs)
    bronze_max_ingestion_id = max(
        [int(log["id"]) for log in bronze_logs if log.get("id") is not None],
        default=0,
    )
    file_log_entries = [
        {
            "service_type": log["service_type"],
            "year": log["year"],
            "month": log["month"],
            "bronze_ingestion_id": log["id"],
            "bronze_object_key": log["minio_object_key"],
            "status": "PENDING",
            "error_message": None,
        }
        for log in bronze_logs
    ]

    run_payload: Dict[str, Any] = {
        "run_id": str(uuid4()),
        "pipeline_name": pipeline_name,
        "load_mode": load_mode,
        "status": "FAILED",
        "error_message": None,
        "started_at": _now_vn(),
        "finished_at": None,
        "spark_app_id": None,
        "spark_version": None,
        "bronze_file_count": len(bronze_logs),
        "bronze_max_ingestion_id": bronze_max_ingestion_id,
        "bronze_yellow_rows": None,
        "bronze_green_rows": None,
        "silver_output_rows": None,
        "silver_path": None,
        "ge_suite_name": EXPECTATION_SUITE_NAME,
        "ge_success": None,
        "ge_total_expectations": None,
        "ge_successful_expectations": None,
        "ge_failed_expectations": None,
    }
    ge_payload: Dict[str, Any] | None = None

    spark = None
    silver_df = None
    try:
        if not bronze_logs:
            run_payload["status"] = "SKIPPED_NO_NEW_BRONZE"
            run_payload["error_message"] = (
                "No bronze parquet files pending for silver transform "
                "(all discovered bronze object keys already processed)."
            )
            return run_payload

        # BUG-020: Secondary guard — check Silver MinIO partition existence before
        # starting Spark. Handles the case where silver_file_log is out of sync
        # with MinIO Silver (e.g. after DB reset or a failed _log_silver_file_runs call).
        # If the partition already exists in MinIO, skip Spark entirely and backfill
        # silver_file_log so future runs are fast.
        if year is not None and month is not None:
            existing_silver_partitions = _list_silver_partitions(config)
            if (year, month) in existing_silver_partitions:
                print(
                    f"[INFO] BUG-020: Silver partition year={year}/month={month:02d} "
                    f"already exists in MinIO. Skipping Spark for {len(bronze_logs)} "
                    "bronze file(s) (silver_file_log was out of sync — backfilling entries)."
                )
                for item in file_log_entries:
                    item["status"] = "SUCCESS"
                    item["error_message"] = (
                        f"Backfilled: Silver partition year={year}/month={month:02d} "
                        "already existed in MinIO (silver_file_log was out of sync)."
                    )
                run_payload["status"] = "SKIPPED_ALREADY_IN_SILVER"
                run_payload["error_message"] = (
                    f"Silver partition year={year}/month={month:02d} already exists in MinIO. "
                    f"silver_file_log was out of sync for {len(bronze_logs)} file(s) — "
                    "entries backfilled as SUCCESS."
                )
                return run_payload

        spark = build_spark_session()
        run_payload["spark_app_id"] = spark.sparkContext.applicationId
        run_payload["spark_version"] = spark.version

        df_yellow, df_green = _load_bronze_dataframes(
            spark=spark,
            bronze_records=bronze_records,
            load_mode=load_mode,
        )
        # Bronze row counts removed (BUG-010): full scans on raw bronze DataFrames are
        # expensive Spark jobs that only produce metadata. Set to None (best-effort).
        run_payload["bronze_yellow_rows"] = None
        run_payload["bronze_green_rows"] = None

        silver_df = _build_silver_dataframe(df_yellow, df_green)
        from pyspark.sql.functions import col, lit

        silver_df = silver_df.filter(
            (col("pickup_year").isNotNull()) & (col("pickup_year") >= 2018)
        )

        # Cache silver_df before GE validation + write to avoid recomputing the full
        # transform pipeline multiple times (GE sample, write, incremental merge).
        silver_df.cache()

        # Safe emptiness check: don't let a brittle count crash the whole task.
        has_any_row = _df_has_any_row_with_timeout(silver_df, timeout_seconds=60)
        is_silver_empty = has_any_row is False

        if has_any_row is None:
            print(
                "[WARN] Could not determine silver_df emptiness reliably (count timeout/error). "
                "Proceeding to GE/write path to avoid false-negative pipeline failure."
            )

        # Silver output row count removed (BUG-010): full scan is expensive and
        # metadata-only. Emptiness is already checked via limit(1).count() above.
        run_payload["silver_output_rows"] = 0 if is_silver_empty else None
        if is_silver_empty:
            run_payload["status"] = "SKIPPED_EMPTY_AFTER_FILTERS"
            run_payload["error_message"] = (
                "Silver dataframe is empty after transform filters "
                f"(bronze_yellow_rows={run_payload['bronze_yellow_rows']}, "
                f"bronze_green_rows={run_payload['bronze_green_rows']})."
            )
            for item in file_log_entries:
                item["status"] = "SKIPPED"
                item["error_message"] = run_payload["error_message"]
            return run_payload

        ge_payload = _validate_with_great_expectations(silver_df)
        run_payload["ge_success"] = ge_payload["success"]
        run_payload["ge_total_expectations"] = ge_payload["total_expectations"]
        run_payload["ge_successful_expectations"] = ge_payload["successful_expectations"]
        run_payload["ge_failed_expectations"] = ge_payload["failed_expectations"]

        if not ge_payload["success"]:
            failed_expectation_summary = _summarize_failed_expectations(ge_payload["validation_result"])
            raise ValueError(
                "Great Expectations validation failed for silver_taxi_suite "
                f"(failed={ge_payload['failed_expectations']}; "
                f"failed_expectations={failed_expectation_summary})."
            )

        if load_mode == "INCREMENTAL":
            # BUG-010 fix: write each (year, month) partition separately using the
            # existing _write_to_silver() with dynamic partition overwrite.
            # The previous approach collected all merged partitions into a single
            # union tree (N-deep) before writing — same root cause as BUG-009.
            #
            # New approach:
            # 1. Derive year/month pairs from bronze_logs metadata (no .collect() action).
            # 2. For each partition: filter incoming, union with existing, write directly.
            # 3. _write_to_silver uses partitionOverwriteMode=dynamic so only the
            #    target partition is overwritten on each call.
            config = _load_config()
            minio_cfg = _load_minio_config(config)
            silver_bucket = minio_cfg["silver_bucket"]
            silver_path = f"s3a://{silver_bucket}/silver/taxi_trips/"

            incoming_write_df = silver_df.drop("source_year", "source_month")
            incoming_write_df = (
                incoming_write_df.drop("pickup_year")
                if "pickup_year" in incoming_write_df.columns
                else incoming_write_df
            )

            # Derive partitions from bronze_logs metadata — avoids .collect() on DataFrame
            partitions = sorted({
                (int(log["year"]), int(log["month"]))
                for log in bronze_logs
                if log.get("year") is not None and log.get("month") is not None
            })

            written_any = False
            for partition_year, partition_month in partitions:
                incoming_partition_df = incoming_write_df.filter(
                    (col("year") == lit(partition_year)) & (col("month") == lit(partition_month))
                )

                existing_partition_df = _read_silver_partition(
                    spark=spark,
                    silver_path=silver_path,
                    partition_year=partition_year,
                    partition_month=partition_month,
                )

                if existing_partition_df is not None:
                    merged_partition_df = incoming_partition_df.unionByName(
                        existing_partition_df, allowMissingColumns=True
                    ).dropDuplicates()
                else:
                    merged_partition_df = incoming_partition_df

                # Write this partition only — dynamic overwrite touches only year=X/month=Y
                _write_to_silver(merged_partition_df, load_mode=load_mode)
                written_any = True
                print(
                    f"[INFO] INCREMENTAL write complete for "
                    f"year={partition_year}, month={partition_month:02d}"
                )

            run_payload["silver_path"] = silver_path if written_any else None
        else:
            run_payload["silver_path"] = _write_to_silver(silver_df, load_mode=load_mode)
        run_payload["error_message"] = None
        run_payload["status"] = "SUCCESS"

        for item in file_log_entries:
            item["status"] = "SUCCESS"

        return run_payload
    except Exception as exc:
        source_hint = ""
        if bronze_logs:
            source_hint = (
                " | bronze_sources="
                + ",".join(
                    [
                        f"{log['service_type']}:{log['year']}-{log['month']:02d}:{log['minio_object_key']}"
                        for log in bronze_logs
                    ]
                )
            )
        _exc_str = _safe_str_exc(exc)
        run_payload["error_message"] = f"{_exc_str}{source_hint}"
        for item in file_log_entries:
            item["status"] = "FAILED"
            item["error_message"] = _exc_str
        raise
    finally:
        if ge_payload is None:
            ge_payload = {
                "expectation_suite_name": EXPECTATION_SUITE_NAME,
                "success": False,
                "total_expectations": 0,
                "successful_expectations": 0,
                "failed_expectations": 0,
                "validation_result": {
                    "status": "NOT_EXECUTED",
                    "reason": run_payload.get("error_message"),
                },
            }

        run_payload["finished_at"] = _now_vn()
        # Unpersist cached silver_df before stopping Spark to free memory cleanly.
        if silver_df is not None:
            try:
                silver_df.unpersist()
            except Exception:
                pass
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        _log_silver_run(config=config, run_payload=run_payload, ge_payload=ge_payload)
        _log_silver_file_runs(config=config, run_id=run_payload["run_id"], file_logs=file_log_entries)
        print(
            "Silver transform finished "
            f"(status={run_payload['status']}, run_id={run_payload['run_id']}, "
            f"output_rows={run_payload['silver_output_rows']})"
        )


if __name__ == "__main__":
    run_silver_transform()
