from __future__ import annotations

import configparser
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo


import psycopg2
from psycopg2.extras import Json
from minio import Minio
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, concat, concat_ws, sha2, hour, dayofmonth, month, year,
    dayofweek, monotonically_increasing_id, date_trunc, broadcast
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.conf"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# STEP 1:Config Static Lookup Data 
VENDOR_LOOKUP = {1: "Creative Mobile Technologies, LLC", 2: "Curb Mobility, LLC", 6: "Myle Technologies Inc", 7: "Helix (yellow)" }
RATECODE_LOOKUP = {
    1: "Standard rate", 2: "JFK", 3: "Newark",
    4: "Nassau or Westchester", 5: "Negotiated fare",
    6: "Group ride", 99: "Unknown"
}
PAYMENT_LOOKUP = {
    0: "Unknown", 1: "Credit card", 2: "Cash",
    3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"
}
TRIP_TYPE_LOOKUP = {-1: "Unknown", 0: "N/A (Yellow)", 1: "Street-hail", 2: "Dispatch"}
SERVICE_TYPE_LOOKUP = {"yellow": "Yellow Taxi", "green": "Green Taxi"}
STORE_AND_FWD_FLAG_LOOKUP = {"Y": "Store and forward", "N": "Not a store and forward"}

# STEP 2: Gold Transform Helpers
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


def _df_count_with_timeout(df: DataFrame, timeout_seconds: int = 60) -> Optional[int]:
    """
    DataFrame-native time-limited count. Runs ``df.count()`` in a daemon thread and
    returns ``None`` if it does not complete within *timeout_seconds*.

    Replaces ``df.rdd.countApprox()`` — accessing ``.rdd`` calls ``javaToPython()``
    on the underlying Java object, which raises ``Py4JJavaError`` when the query plan
    has issues (deep union trees causing JVM StackOverflow, S3A connection errors).
    ``df.count()`` is a DataFrame-native action that bypasses the Py4J bridge.

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


def _load_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config


def _load_minio_config_gold(config: configparser.ConfigParser) -> dict:
    minio_cfg = config["minio"]
    return {
        "endpoint":      os.getenv("MINIO_ENDPOINT",   minio_cfg.get("endpoint",   "minio:9000")),
        "access_key":    os.getenv("MINIO_ACCESS_KEY", minio_cfg.get("access_key", "minioadmin")),
        "secret_key":    os.getenv("MINIO_SECRET_KEY", minio_cfg.get("secret_key", "minioadmin")),
        "secure":        os.getenv("MINIO_SECURE",     minio_cfg.get("secure",     "false")).lower() == "true",
        "bronze_bucket": os.getenv("BRONZE_BUCKET",    minio_cfg.get("bronze_bucket", "bronze")),
        "silver_bucket": os.getenv("SILVER_BUCKET",    minio_cfg.get("silver_bucket", "silver")),
        "gold_bucket":   os.getenv("GOLD_BUCKET",      minio_cfg.get("gold_bucket",   "gold")),
    }


def _get_db_connection(config: configparser.ConfigParser):
    db_cfg = config["database"]
    return psycopg2.connect(
        host=db_cfg["database_host"],
        dbname=db_cfg["database_name"],
        user=db_cfg["database_username"],
        password=db_cfg["database_password"],
        port=int(db_cfg["database_port"]),
    )


def _get_minio_client_gold(config: configparser.ConfigParser) -> Minio:
    minio_cfg = _load_minio_config_gold(config)
    return Minio(
        endpoint=minio_cfg["endpoint"],
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=minio_cfg["secure"],
    )


def _check_gold_already_succeeded(
    config: configparser.ConfigParser,
    year: Optional[int],
    month: Optional[int],
) -> bool:
    """Return True if a SUCCESS entry already exists in gold_transform_log for (year, month).

    Used as an early-exit guard in run_gold_transform to avoid re-running the full
    Spark gold transform for periods that have already been successfully processed.
    Mirrors the silver MinIO partition skip guard introduced in BUG-020.

    Returns False (do not skip) on any DB error so the pipeline proceeds safely.
    """
    if year is None or month is None:
        # Cannot determine period — do not skip.
        return False
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema", "metadata")
    table = db_cfg.get("gold_transform_log_table", "gold_transform_log")
    try:
        conn = _get_db_connection(config)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {schema}.{table} "
                    "WHERE year = %s AND month = %s AND status = 'SUCCESS' LIMIT 1",
                    (year, month),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001
        print(
            f"[WARN] _check_gold_already_succeeded: could not query metadata "
            f"({e!r}) — proceeding with transform."
        )
        return False


# Spark Catalog Config 
def build_spark_session_gold(config: configparser.ConfigParser) -> SparkSession:
    minio_cfg = _load_minio_config_gold(config)
    endpoint = minio_cfg["endpoint"]
    spark_driver_memory    = os.getenv("SPARK_DRIVER_MEMORY",            "4g")
    spark_executor_memory  = os.getenv("SPARK_EXECUTOR_MEMORY",          "4g")
    # Increased from 64 → 200 (BUG-021): 64 shuffle partitions caused per-partition
    # hash aggregation OOM when running .distinct() on large silver DataFrames.
    # More partitions = smaller chunks per partition = less memory per aggregation task.
    spark_shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200")

    nessie_uri = config["nessie"]["uri"]
    nessie_ref = config["nessie"].get("ref", "main")

    spark = (
        SparkSession.builder
        .appName("Taxi Gold Layer")
        .config("spark.driver.memory",   spark_driver_memory)
        .config("spark.executor.memory", spark_executor_memory)
        .config("spark.sql.shuffle.partitions", spark_shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # BUG-021: Prevent AQE from coalescing the 200 shuffle partitions back down
        # to a handful, which would defeat the per-partition memory reduction fix.
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum",
                os.getenv("SPARK_SQL_ADAPTIVE_MIN_PARTITIONS", "50"))
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
        .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint",                 endpoint)
        .config("spark.hadoop.fs.s3a.access.key",               minio_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key",               minio_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access",        "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   str(minio_cfg["secure"]).lower())
        # Map s3:// → S3AFileSystem so the gold catalog warehouse uses s3:// URIs,
        # which Trino's native S3 filesystem can resolve directly (no s3a:// mismatch).
        # S3AFileSystem reuses all fs.s3a.* credentials above for both schemes.
        .config("spark.hadoop.fs.s3.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.gold",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.gold.uri",       nessie_uri)
        .config("spark.sql.catalog.gold.ref",       nessie_ref)
        .config("spark.sql.catalog.gold.warehouse", f"s3://{minio_cfg['gold_bucket']}/warehouse")
        .config("spark.sql.parquet.mergeSchema",    "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )
    return spark


# STEP 3: Dim Builders


def build_dim_vendor(spark: SparkSession) -> DataFrame:
    data = [(k, v) for k, v in VENDOR_LOOKUP.items()]
    df = spark.createDataFrame(data, ["vendor_id", "vendor_name"])
    return df.withColumn("vendor_key", sha2(df["vendor_id"].cast("string"), 256))


def build_dim_service_type(spark: SparkSession) -> DataFrame:
    data = [(k, v) for k, v in SERVICE_TYPE_LOOKUP.items()]
    df = spark.createDataFrame(data, ["service_type", "service_type_name"])
    return df.withColumn("service_type_key", sha2(df["service_type"].cast("string"), 256))


def build_dim_payment(spark: SparkSession) -> DataFrame:
    data = [(k, v) for k, v in PAYMENT_LOOKUP.items()]
    df = spark.createDataFrame(data, ["payment_code", "description"])
    return df.withColumn("payment_type_key", sha2(df["payment_code"].cast("string"), 256))


def build_dim_ratecode(spark: SparkSession) -> DataFrame:
    data = [(k, v) for k, v in RATECODE_LOOKUP.items()]
    df = spark.createDataFrame(data, ["ratecode_id", "description"])
    return df.withColumn("ratecode_key", sha2(df["ratecode_id"].cast("string"), 256))


def build_dim_trip_type(spark: SparkSession) -> DataFrame:
    data = [(k, v) for k, v in TRIP_TYPE_LOOKUP.items()]
    df = spark.createDataFrame(data, ["trip_type_id", "description"])
    return df.withColumn("trip_type_key", sha2(df["trip_type_id"].cast("string"), 256))


def build_dim_location(spark: SparkSession) -> DataFrame:
    candidate_paths = [
        Path("/opt/airflow/data/resources/taxi_zone_lookup.csv"),
        Path("/opt/airflow/resources/taxi_zone_lookup.csv"),
        Path(__file__).resolve().parents[1] / "data" / "resources" / "taxi_zone_lookup.csv",
    ]
    local_csv_path = next((path for path in candidate_paths if path.exists()), None)
    if local_csv_path is None:
        searched = ", ".join(str(path) for path in candidate_paths)
        raise FileNotFoundError(f"taxi_zone_lookup.csv not found. Searched: {searched}")

    df = spark.read.csv(str(local_csv_path), header=True)
    return df.select(
        col("LocationID").cast("long").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone").alias("service_zone")
    ).withColumn("location_key", sha2(col("location_id").cast("string"), 256))




def build_dim_date(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    # BUG-021: Repartition by pickup_hour BEFORE .distinct() to fix OOM.
    # After .select() the data is already reduced to one column.
    # Repartitioning by the aggregation key ensures even distribution across
    # 200 partitions so each partition's hash table holds ~1/200 of the rows
    # instead of ~1/64, preventing SparkOutOfMemoryError during hash aggregation.
    return (
        silver_df
        .select(date_trunc("hour", col("pickup_datetime")).alias("pickup_hour"))
        .repartition(200, col("pickup_hour"))
        .distinct()
        .withColumn("datetime_key", sha2(col("pickup_hour").cast("string"), 256))
        .withColumn("hour",       hour(col("pickup_hour")))
        .withColumn("day",        dayofmonth(col("pickup_hour")))
        .withColumn("month",      month(col("pickup_hour")))
        .withColumn("year",       year(col("pickup_hour")))
        .withColumn("weekday",    dayofweek(col("pickup_hour")))
        .withColumn("is_weekend", when(dayofweek(col("pickup_hour")).isin(1, 7), 1).otherwise(0))
        .withColumnRenamed("pickup_hour", "full_datetime")
    )


# STEP 4: Fact Builder - read silver with optional year/month filters
def _read_silver_layer(
    spark: SparkSession,
    config: configparser.ConfigParser,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> DataFrame:
    minio_cfg = _load_minio_config_gold(config)
    silver_bucket = minio_cfg["silver_bucket"]
    base = f"s3a://{silver_bucket}/silver/taxi_trips"
    # PERF: Use direct partition path for S3-side partition pruning.
    # Avoids reading the entire silver table and filtering in Spark —
    # S3/MinIO only lists and reads the relevant prefix, reducing I/O significantly.
    if year is not None and month is not None:
        silver_path = f"{base}/year={year}/month={month}/"
    elif year is not None:
        silver_path = f"{base}/year={year}/"
    else:
        silver_path = f"{base}/"
    return spark.read.parquet(silver_path)



def build_fact_table(
    spark: SparkSession,
    silver_df: DataFrame,
    dim_vendor: DataFrame,
    dim_service_type: DataFrame,
    dim_payment: DataFrame,
    dim_ratecode: DataFrame,
    dim_trip_type: DataFrame,
    dim_location: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    if "cbd_congestion_fee" not in silver_df.columns:
        silver_df = silver_df.withColumn("cbd_congestion_fee", lit(0.0).cast("double"))

    dim_location_pu = dim_location.select(
        col("location_id").alias("pu_location_id"),
        col("location_key").alias("pickup_location_key"),
    )
    dim_location_do = dim_location.select(
        col("location_id").alias("do_location_id"),
        col("location_key").alias("dropoff_location_key"),
    )

    dim_datetime_pickup = dim_date.select(
        col("full_datetime").alias("pickup_full_datetime"),
        col("datetime_key").alias("pickup_datetime_key"),
    )
    dim_datetime_dropoff = dim_date.select(
        col("full_datetime").alias("dropoff_full_datetime"),
        col("datetime_key").alias("dropoff_datetime_key"),
    )

    # PERF: broadcast() hints for small dimension tables (< a few MB each).
    # Spark will send a full copy of each dim to every executor, eliminating
    # shuffle joins (SortMergeJoin → BroadcastHashJoin). Typical speedup: 3-10x
    # for the fact table build step on large silver DataFrames.
    fact_df = (
        silver_df.alias("s")
        .join(broadcast(dim_vendor).alias("dv"),
              col("s.VendorID") == col("dv.vendor_id"), "left")
        .join(broadcast(dim_service_type).alias("dst"),
              col("s.service_type") == col("dst.service_type"), "left")
        .join(broadcast(dim_payment).alias("dp"),
              col("s.payment_type") == col("dp.payment_code"), "left")
        .join(broadcast(dim_ratecode).alias("dr"),
              col("s.RatecodeID") == col("dr.ratecode_id"), "left")
        .join(broadcast(dim_trip_type).alias("dtt"),
              col("s.trip_type") == col("dtt.trip_type_id"), "left")
        .join(broadcast(dim_location_pu),
              col("s.PULocationID") == col("pu_location_id"), "left")
        .join(broadcast(dim_location_do),
              col("s.DOLocationID") == col("do_location_id"), "left")
        .join(broadcast(dim_datetime_pickup),
              date_trunc("hour", col("s.pickup_datetime")) == col("pickup_full_datetime"), "left")
        .join(broadcast(dim_datetime_dropoff),
              date_trunc("hour", col("s.dropoff_datetime")) == col("dropoff_full_datetime"), "left")
        .select(
            monotonically_increasing_id().alias("trip_key"),
            # BUG-026: Stable content-based hash for deduplication in write_fact_to_gold.
            # Uses key identifying fields so the hash is deterministic across re-runs.
            # monotonically_increasing_id() is non-deterministic and cannot be used for dedup.
            sha2(concat_ws("|",
                col("s.pickup_datetime").cast("string"),
                col("s.dropoff_datetime").cast("string"),
                col("s.PULocationID").cast("string"),
                col("s.DOLocationID").cast("string"),
                col("s.service_type"),
                col("s.total_amount").cast("string"),
                col("s.fare_amount").cast("string"),
                col("s.trip_distance").cast("string"),
            ), 256).alias("trip_hash"),
            col("dv.vendor_key"),
            col("dst.service_type_key"),
            col("dp.payment_type_key"),
            col("dr.ratecode_key"),
            col("dtt.trip_type_key"),
            col("pickup_location_key"),
            col("dropoff_location_key"),
            col("pickup_datetime_key"),
            col("dropoff_datetime_key"),
            col("s.passenger_count"),
            col("s.trip_distance"),
            col("s.total_amount"),
            col("s.trip_duration_minutes"),
            col("s.fare_amount"),
            col("s.extra"),
            col("s.mta_tax"),
            col("s.tip_amount"),
            col("s.tolls_amount"),
            col("s.improvement_surcharge"),
            col("s.congestion_surcharge"),
            col("s.airport_fee"),
            col("s.cbd_congestion_fee"),
            year(col("s.pickup_datetime")).alias("year"),
            month(col("s.pickup_datetime")).alias("month"),
        )
    )
    return fact_df


# STEP 5: Iceberg Writes


def _ensure_gold_schema(spark: SparkSession) -> None:
    """Create the 'gold' namespace in Nessie if it does not already exist.

    Root cause of BUG-011: writing tables as ``gold.<table>`` (2-part name) places
    them in the root namespace of the Nessie catalog.  Trino's
    ``SHOW SCHEMAS FROM iceberg`` only lists *named* namespaces — root-level tables
    are invisible.  Creating the ``gold`` namespace and writing to the 3-part name
    ``gold.gold.<table>`` (catalog=gold, schema=gold, table=<table>) makes the
    schema and its tables visible to Trino.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold.gold")


def _nessie_drop_table_via_rest(
    nessie_uri: str,
    namespace: str,
    table_name: str,
    branch: str = "main",
) -> bool:
    """Delete a stale Nessie table reference via the Nessie REST API v1.

    BUG-029 fix: `spark.sql("DROP TABLE IF EXISTS ...")` can fail with
    `Py4JJavaError` when Nessie raises `NessieConflictException` (branch hash
    conflict) during `client.commitMultipleOperations()`.  When that happens the
    stale catalog reference is never removed, so the subsequent `saveAsTable`
    retry hits the same `NotFoundException` and the task fails permanently.

    This helper bypasses the Spark/Iceberg/Py4J stack entirely and calls the
    Nessie REST API directly:
      1. GET  /api/v1/trees/tree/{branch}          → fetch current branch hash
      2. POST /api/v1/trees/branch/{branch}/commit  → commit a DELETE operation
                                                       for the content key

    The DELETE operation is a pure Nessie metadata operation — it removes the
    catalog reference without touching any S3/MinIO data files.

    Returns True  if the reference was deleted (or was already absent).
    Returns False if the REST call failed (caller should still attempt the retry).
    """
    try:
        import requests as _requests  # lazy import — avoid top-level dep for non-REST paths
    except ImportError:
        print(
            "[WARN] _nessie_drop_table_via_rest: 'requests' library not available "
            "— skipping REST fallback."
        )
        return False

    ref_url    = f"{nessie_uri}/trees/tree/{branch}"
    commit_url = f"{nessie_uri}/trees/branch/{branch}/commit"
    commit_payload = {
        "commitMeta": {
            "message": (
                f"Drop stale Iceberg table {namespace}.{table_name} "
                "(automated recovery from NotFoundException — BUG-029)"
            )
        },
        "operations": [
            {
                "type": "DELETE",
                "key": {"elements": [namespace, table_name]},
            }
        ],
    }

    def _attempt(expected_hash: str) -> int:
        """POST the commit and return the HTTP status code."""
        resp = _requests.post(
            commit_url,
            params={"expectedHash": expected_hash},
            json=commit_payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        return resp.status_code

    try:
        # Step 1 — get current branch hash
        ref_resp = _requests.get(ref_url, timeout=30)
        if ref_resp.status_code == 404:
            print(
                f"[WARN] _nessie_drop_table_via_rest: branch '{branch}' not found "
                f"at {ref_url}."
            )
            return False
        ref_resp.raise_for_status()
        current_hash = ref_resp.json()["hash"]

        # Step 2 — commit the DELETE
        status = _attempt(current_hash)

        if status in (200, 204):
            return True
        if status == 404:
            # Content key already absent — treat as success.
            return True
        if status == 409:
            # Hash conflict: another operation committed between our GET and POST.
            # Fetch the latest hash and retry once.
            print(
                f"[INFO] _nessie_drop_table_via_rest: 409 Conflict for "
                f"{namespace}.{table_name} — retrying with fresh hash."
            )
            ref_resp2 = _requests.get(ref_url, timeout=30)
            ref_resp2.raise_for_status()
            status2 = _attempt(ref_resp2.json()["hash"])
            if status2 in (200, 204, 404):
                return True
            print(
                f"[WARN] _nessie_drop_table_via_rest: retry also failed with "
                f"HTTP {status2} for {namespace}.{table_name}."
            )
            return False

        print(
            f"[WARN] _nessie_drop_table_via_rest: unexpected HTTP {status} "
            f"for {namespace}.{table_name}."
        )
        return False

    except Exception as rest_err:  # noqa: BLE001
        print(
            f"[WARN] _nessie_drop_table_via_rest: REST call raised "
            f"{rest_err!r} for {namespace}.{table_name}."
        )
        return False


def write_dim_to_gold(df: DataFrame, table_name: str, spark: SparkSession) -> None:
    """Overwrite small dimension tables (full refresh).

    BUG-028 fix: catch-and-retry on NotFoundException (stale Nessie metadata).
    Nessie may hold a stale pointer to a metadata file that no longer exists in
    S3/MinIO (catalog-storage inconsistency, e.g. MinIO reset without Nessie reset,
    or a partial write that left Nessie pointing to a file never fully committed).

    When saveAsTable tries to load the existing table, the call chain is:
      saveAsTable → SparkCatalog.loadTable → NessieTableOperations.doRefresh
      → refreshFromMetadataLocation → S3 open(metadata.json) → FileNotFoundException

    BUG-029 fix: the original BUG-028 recovery used only `spark.sql("DROP TABLE IF
    EXISTS ...")`.  In Nessie 0.76.3, `NessieCatalog.dropTable()` calls
    `client.commitMultipleOperations()` which can raise `NessieConflictException`
    (branch hash conflict) → propagates as `Py4JJavaError` through the Py4J bridge.
    When that happens the stale reference is never removed and the retry
    `saveAsTable` hits the same `NotFoundException`.

    Additionally, Iceberg's `CachingCatalog` may still hold the stale table entry
    in its in-memory cache even after a successful DROP.  `spark.catalog.refreshTable()`
    evicts the entry so the retry `saveAsTable` does not re-use the stale reference.

    Recovery order (happy path has zero overhead — unchanged behaviour):
      1. `spark.sql("DROP TABLE IF EXISTS ...")` — fast Nessie metadata-only op.
      2. If (1) fails with Py4JJavaError → `_nessie_drop_table_via_rest()` — calls
         Nessie REST API directly, bypassing Spark/Iceberg/Py4J entirely.
      3. After any successful drop → `spark.catalog.refreshTable()` to evict the
         stale CachingCatalog entry.
      4. Retry `saveAsTable` — table absent from Nessie → creates fresh metadata chain.
    """
    table_fqn = f"gold.gold.{table_name}"
    try:
        (df.write
            .format("iceberg")
            .mode("overwrite")
            .saveAsTable(table_fqn))
    except Exception as e:
        err_str = _safe_str_exc(e)
        if "NotFoundException" in err_str or "FileNotFoundException" in err_str:
            print(
                f"[WARN] write_dim_to_gold: stale Nessie metadata detected for "
                f"{table_fqn} — dropping table and retrying. "
                f"Error: {err_str[:300]}"
            )

            # ── Step 1: try spark.sql DROP ────────────────────────────────────
            drop_succeeded = False
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
                drop_succeeded = True
                print(
                    f"[INFO] write_dim_to_gold: spark.sql DROP succeeded for {table_fqn}."
                )
            except Exception as drop_err:
                # BUG-029: spark.sql DROP can fail with Py4JJavaError when Nessie
                # raises NessieConflictException during commitMultipleOperations().
                # Fall through to the REST API fallback below.
                print(
                    f"[WARN] write_dim_to_gold: DROP TABLE IF EXISTS {table_fqn} "
                    f"raised {drop_err!r} — attempting Nessie REST API fallback."
                )

            # ── Step 2: REST API fallback if spark.sql DROP failed ────────────
            if not drop_succeeded:
                try:
                    nessie_uri = spark.conf.get("spark.sql.catalog.gold.uri")
                    nessie_ref = spark.conf.get("spark.sql.catalog.gold.ref", "main")
                    drop_succeeded = _nessie_drop_table_via_rest(
                        nessie_uri, "gold", table_name, nessie_ref
                    )
                    if drop_succeeded:
                        print(
                            f"[INFO] write_dim_to_gold: Nessie REST API drop "
                            f"succeeded for {table_fqn}."
                        )
                    else:
                        print(
                            f"[WARN] write_dim_to_gold: Nessie REST API drop also "
                            f"failed for {table_fqn} — retrying saveAsTable anyway."
                        )
                except Exception as rest_err:  # noqa: BLE001
                    print(
                        f"[WARN] write_dim_to_gold: REST API fallback raised "
                        f"{rest_err!r} for {table_fqn}."
                    )

            # ── Step 3: evict stale CachingCatalog entry ──────────────────────
            # BUG-029: even after a successful DROP, Iceberg's CachingCatalog may
            # still hold the old table reference in memory.  refreshTable() calls
            # SparkCatalog.invalidateTable() → CachingCatalog.invalidateTable()
            # which removes the entry from the Caffeine cache so the retry
            # saveAsTable does not re-use the stale metadata location.
            if drop_succeeded:
                try:
                    spark.catalog.refreshTable(table_fqn)
                except Exception:  # noqa: BLE001
                    # refreshTable may raise if the table is already gone from the
                    # catalog — that is exactly what we want, so ignore the error.
                    pass

            # ── Step 4: retry saveAsTable ─────────────────────────────────────
            # Table is absent from Nessie (or cache evicted) → saveAsTable creates
            # it fresh with a valid metadata chain.
            (df.write
                .format("iceberg")
                .mode("overwrite")
                .saveAsTable(table_fqn))
        else:
            raise


def write_fact_to_gold(
    df: DataFrame,
    spark: SparkSession,
    source_year: Optional[int] = None,
    source_month: Optional[int] = None,
) -> None:
    """Write fact_trips to Iceberg using a two-phase write strategy.

    BUG-022 fix (preserved):
        Empty-DataFrame guard — if *df* has no rows, skip the write entirely so
        that a run for a month with no silver data cannot erase previously written
        partitions.

    BUG-024 fix (preserved):
        Use Iceberg DataFrameWriterV2 API (``writeTo().overwritePartitions()``) for
        correct dynamic partition overwrite instead of the V1 ``saveAsTable`` path.

    BUG-026 fix (new):
        ``overwritePartitions()`` replaces ALL ``(year, month)`` partitions present
        in *df*, including partitions from other years when silver data contains rows
        with anomalous ``pickup_datetime`` values (e.g. silver 2024/01 has rows whose
        ``pickup_datetime`` falls in 2023).  This silently wiped the 37.9 M-row
        year=2023 partition down to a few dozen anomalous rows on every 2024+ run.

        Fix — two-phase write keyed on *source_year*:

        Phase 1 — ``main_df`` (``year == source_year``):
            Use ``overwritePartitions()`` — idempotent for the current batch, only
            touches partitions where ``year == source_year``.  All other partitions
            are untouched.

        Phase 2 — ``anomalous_df`` (``year != source_year``):
            Rows whose ``pickup_datetime`` falls in a different year than the source
            batch.  These are appended with **partition-pruned deduplication** via
            ``trip_hash`` so existing data in other years' partitions is never
            overwritten.

        Performance:
        - Fast path (no anomalous rows, the common case): Phase 2 exits after a
          single ``limit(1).count()`` call (~milliseconds, zero Spark job overhead).
        - Slow path (anomalous rows exist): only the specific ``(year, month)``
          partitions affected are scanned for ``trip_hash`` lookup — Iceberg
          partition pruning ensures no full-table scan occurs.

        Schema evolution:
        - If the existing table pre-dates this fix (no ``trip_hash`` column), an
          ``ALTER TABLE … ADD COLUMN`` is issued before Phase 1 so that the column
          is present for both the overwrite and the dedup join.  Iceberg adds the
          column as metadata-only; existing data files are not rewritten.
    """
    # BUG-022 guard: never overwrite existing data with an empty DataFrame.
    if df.limit(1).count() == 0:
        print(
            "[WARN] write_fact_to_gold: fact_df is empty for this period — "
            "skipping Iceberg write to preserve existing partitions."
        )
        return

    table_fqn = "gold.gold.fact_trips"

    # Detect first run (table does not exist yet) vs subsequent runs.
    # BUG-029: distinguish between "table genuinely absent" (NoSuchTableException)
    # and "stale Nessie metadata" (NotFoundException — table reference exists in
    # Nessie but the metadata file is missing in S3).
    #
    # Without this distinction, the original code catches ALL exceptions and sets
    # table_exists=False, then calls writeTo().create().  But create() internally
    # calls AtomicCreateTableAsSelectExec → tableExists() → loadTable() → doRefresh()
    # → reads the same missing S3 metadata file → same NotFoundException → task fails.
    #
    # Fix: when NotFoundException is detected, apply the same 3-step recovery as
    # write_dim_to_gold (spark.sql DROP → REST API fallback → cache invalidation)
    # before proceeding with the first-run create() path.
    table_exists = False
    try:
        spark.table(table_fqn)
        table_exists = True
    except Exception as check_err:
        check_err_str = _safe_str_exc(check_err)
        if "NotFoundException" in check_err_str or "FileNotFoundException" in check_err_str:
            # Stale Nessie metadata detected — drop the stale reference so that
            # the subsequent create() call does not hit the same NotFoundException
            # inside tableExists() → loadTable() → doRefresh().
            print(
                f"[WARN] write_fact_to_gold: stale Nessie metadata detected for "
                f"{table_fqn} — dropping stale reference before create. "
                f"Error: {check_err_str[:300]}"
            )
            # Step 1: try spark.sql DROP
            drop_succeeded = False
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
                drop_succeeded = True
                print(
                    f"[INFO] write_fact_to_gold: spark.sql DROP succeeded for {table_fqn}."
                )
            except Exception as drop_err:
                print(
                    f"[WARN] write_fact_to_gold: DROP TABLE IF EXISTS {table_fqn} "
                    f"raised {drop_err!r} — attempting Nessie REST API fallback."
                )
            # Step 2: REST API fallback if spark.sql DROP failed
            if not drop_succeeded:
                try:
                    nessie_uri = spark.conf.get("spark.sql.catalog.gold.uri")
                    nessie_ref = spark.conf.get("spark.sql.catalog.gold.ref", "main")
                    drop_succeeded = _nessie_drop_table_via_rest(
                        nessie_uri, "gold", "fact_trips", nessie_ref
                    )
                    if drop_succeeded:
                        print(
                            f"[INFO] write_fact_to_gold: Nessie REST API drop "
                            f"succeeded for {table_fqn}."
                        )
                    else:
                        print(
                            f"[WARN] write_fact_to_gold: Nessie REST API drop also "
                            f"failed for {table_fqn} — proceeding with create() anyway."
                        )
                except Exception as rest_err:  # noqa: BLE001
                    print(
                        f"[WARN] write_fact_to_gold: REST API fallback raised "
                        f"{rest_err!r} for {table_fqn}."
                    )
            # Step 3: evict stale CachingCatalog entry so create() does not
            # re-use the stale metadata location from the Caffeine cache.
            if drop_succeeded:
                try:
                    spark.catalog.refreshTable(table_fqn)
                except Exception:  # noqa: BLE001
                    pass
            # After drop, table is absent from Nessie → proceed with first-run path.
            table_exists = False
        else:
            # Genuinely absent (NoSuchTableException) or other non-metadata error.
            table_exists = False

    if not table_exists:
        # First run: create the table with the correct partition spec (year, month).
        # writeTo().create() is safe here because we only reach this branch when
        # spark.table() raised an exception (table truly does not exist, or stale
        # reference was dropped above).
        print(
            f"[INFO] write_fact_to_gold: table {table_fqn} does not exist — "
            "creating with partition spec (year, month)."
        )
        (df.writeTo(table_fqn)
            .partitionedBy(col("year"), col("month"))
            .create())
        return

    # BUG-026 + cross-month fix: Two-phase write split by (source_year, source_month).
    # Filtering by BOTH year AND month ensures overwritePartitions() only touches
    # the exact (year=source_year, month=source_month) partition, preventing
    # anomalous pickup_datetime rows from overwriting other months within the same year.
    if source_year is not None and source_month is not None:
        main_df      = df.filter(
            (col("year") == source_year) & (col("month") == source_month)
        )
        anomalous_df = df.filter(
            (col("year") != source_year) | (col("month") != source_month)
        )
    elif source_year is not None:
        # source_month not provided — year-only filter (original BUG-026 behaviour)
        main_df      = df.filter(col("year") == source_year)
        anomalous_df = df.filter(col("year") != source_year)
    else:
        # source_year unknown — fall back to single-phase overwritePartitions.
        print(
            "[INFO] write_fact_to_gold: source_year not provided — "
            "falling back to single-phase overwritePartitions (original behaviour)."
        )
        df_coalesced = df.coalesce(2)
        (df_coalesced.writeTo(table_fqn).overwritePartitions())
        return

    # ── Phase 1: overwrite only current-year partitions ──────────────────────
    if main_df.limit(1).count() > 0:
        # BUG-026 schema evolution: add trip_hash to existing table if missing.
        # Iceberg ADD COLUMN is metadata-only — existing data files are not rewritten.
        existing_cols = [f.name for f in spark.table(table_fqn).schema.fields]
        if "trip_hash" not in existing_cols:
            print(
                f"[INFO] write_fact_to_gold: adding trip_hash column to {table_fqn} "
                "(schema evolution — existing data files are not rewritten)."
            )
            spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMN trip_hash STRING")

        print(
            f"[INFO] write_fact_to_gold: Phase 1 — overwritePartitions for "
            f"year={source_year} (main_df only, anomalous rows excluded)."
        )
        main_df = main_df.coalesce(2) 
        (main_df.writeTo(table_fqn).overwritePartitions())
    else:
        print(
            f"[WARN] write_fact_to_gold: Phase 1 — main_df (year={source_year}) is "
            "empty after filtering. Skipping overwrite for current year."
        )

    # ── Phase 2: append anomalous rows directly into their correct (year, month) partitions ──
    # Anomalous rows (pickup_datetime year != source_year OR month != source_month)
    # are appended directly into fact_trips at their correct (year, month) partition
    # (derived from pickup_datetime, not from the source batch).
    #
    # BUG-033 fix: removes the fact_trips_anomalous separate table approach which
    # caused NotFoundException when Nessie held a stale metadata reference to a
    # metadata file no longer present in S3/MinIO. The bare `except Exception:`
    # block caught the NotFoundException from spark.table(anomalous_table) and
    # called .create(), which internally called tableExists() → loadTable() →
    # doRefresh() → read the same missing S3 metadata file → same NotFoundException.
    #
    # New strategy per (year, month) group:
    #   1. Filter anomalous_df to just this partition.
    #   2. Anti-join against existing trip_hash values in that partition
    #      (Iceberg partition-pruned scan — only reads the target partition).
    #   3. If new rows exist, writeTo(table_fqn).append() — never overwrites
    #      existing data in any partition.
    #
    # Properties:
    #   • Handles both cross-year and cross-month anomalous rows correctly.
    #   • Idempotent: trip_hash dedup prevents duplicates on re-runs.
    #   • Safe: append() never touches existing rows or other partitions.
    #   • Graceful fallback: if reading existing trip_hash fails, appends all
    #     anomalous rows without dedup (logs warning, does not raise).
    if anomalous_df.limit(1).count() == 0:
        print("[INFO] write_fact_to_gold: Phase 2 — no anomalous rows. Skipping.")
        return

    # Collect distinct (year, month) pairs — small aggregation, not full data collect.
    # Uses actual year/month from pickup_datetime (already computed as col("year")/col("month")
    # in build_fact_table), not source_year/source_month.
    try:
        anomalous_pairs = (
            anomalous_df
            .select(col("year").cast("int").alias("yr"), col("month").cast("int").alias("mo"))
            .distinct()
            .collect()
        )
    except Exception as collect_err:
        print(
            f"[WARN] write_fact_to_gold: Phase 2 — could not collect anomalous "
            f"(year, month) pairs: {_safe_str_exc(collect_err)[:200]}. Skipping Phase 2."
        )
        return

    if not anomalous_pairs:
        print("[INFO] write_fact_to_gold: Phase 2 — no anomalous (year, month) pairs found. Skipping.")
        return

    print(
        f"[WARN] write_fact_to_gold: Phase 2 — {len(anomalous_pairs)} anomalous "
        f"(year, month) partition(s) detected. Appending directly into {table_fqn}."
    )

    # Schema evolution guard: ensure trip_hash column exists in fact_trips before
    # attempting the anti-join dedup. Phase 1 adds it when main_df is non-empty,
    # but if main_df was empty (Phase 1 skipped), the column may be absent.
    try:
        existing_schema_cols = [f.name for f in spark.table(table_fqn).schema.fields]
        has_trip_hash = "trip_hash" in existing_schema_cols
    except Exception:
        has_trip_hash = False

    if not has_trip_hash:
        print(
            f"[WARN] write_fact_to_gold: Phase 2 — trip_hash column not found in "
            f"{table_fqn}. Adding column before dedup (schema evolution)."
        )
        try:
            spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMN trip_hash STRING")
            has_trip_hash = True
            print(
                f"[INFO] write_fact_to_gold: Phase 2 — trip_hash column added to {table_fqn}."
            )
        except Exception as alter_err:
            print(
                f"[WARN] write_fact_to_gold: Phase 2 — could not add trip_hash column: "
                f"{_safe_str_exc(alter_err)[:200]}. Appending without dedup."
            )

    for row in anomalous_pairs:
        anom_year  = int(row["yr"])
        anom_month = int(row["mo"])

        partition_df = anomalous_df.filter(
            (col("year") == anom_year) & (col("month") == anom_month)
        )

        # Dedup: anti-join against existing trip_hash values in this partition.
        # Iceberg partition pruning ensures only year=anom_year/month=anom_month
        # is scanned — no full table scan.
        if has_trip_hash:
            try:
                existing_hashes = (
                    spark.table(table_fqn)
                    .filter(
                        (col("year") == anom_year) & (col("month") == anom_month)
                    )
                    .select("trip_hash")
                )
                new_rows_df = partition_df.join(
                    existing_hashes, on="trip_hash", how="left_anti"
                )
            except Exception as read_err:
                print(
                    f"[WARN] write_fact_to_gold: Phase 2 — could not read existing "
                    f"trip_hash for year={anom_year}/month={anom_month:02d}: "
                    f"{_safe_str_exc(read_err)[:200]}. "
                    "Appending all anomalous rows without dedup."
                )
                new_rows_df = partition_df
        else:
            new_rows_df = partition_df

        if new_rows_df.limit(1).count() == 0:
            print(
                f"[INFO] write_fact_to_gold: Phase 2 — no new anomalous rows for "
                f"year={anom_year}/month={anom_month:02d} "
                "(all trip_hash values already exist). Skipping."
            )
            continue

        try:
            new_rows_df = new_rows_df.coalesce(1)
            (new_rows_df.writeTo(table_fqn).append())
            print(
                f"[INFO] write_fact_to_gold: Phase 2 — anomalous rows appended to "
                f"{table_fqn} year={anom_year}/month={anom_month:02d}."
            )
        except Exception as append_err:
            print(
                f"[ERROR] write_fact_to_gold: Phase 2 — failed to append anomalous "
                f"rows for year={anom_year}/month={anom_month:02d}: "
                f"{_safe_str_exc(append_err)[:300]}"
            )
            raise


def write_dim_date_to_gold(df: DataFrame, spark: SparkSession) -> None:
    """Append only new date entries to dim_date, preserving historical dates.

    Using mode('overwrite') on dim_date would erase all dates outside the current
    month on every run. Instead we do a left-anti join against the existing table
    so only genuinely new full_datetime values are inserted.

    BUG-028 fix: catch-and-retry on NotFoundException for the saveAsTable call.
    Same stale Nessie metadata pattern as write_dim_to_gold. If saveAsTable(append)
    fails with NotFoundException, drop the table from Nessie and fall back to a
    full write with all rows from the current df (since the existing data is already
    inaccessible due to the broken metadata chain — drop+recreate is the safest
    recovery path; dim_date will be repopulated incrementally on subsequent runs).
    """
    try:
        existing_dates = spark.table("gold.gold.dim_date").select("full_datetime")
        new_rows_df = df.join(existing_dates, on="full_datetime", how="left_anti")
    except Exception:
        # Table does not exist yet (or stale metadata on read) — write everything.
        new_rows_df = df

    # limit(1).count() reads at most one row — fast and avoids .rdd (javaToPython).
    if new_rows_df.limit(1).count() == 0:
        return

    try:
        (new_rows_df.write
            .format("iceberg")
            .mode("append")
            .saveAsTable("gold.gold.dim_date"))
    except Exception as e:
        err_str = _safe_str_exc(e)
        if "NotFoundException" in err_str or "FileNotFoundException" in err_str:
            print(
                "[WARN] write_dim_date_to_gold: stale Nessie metadata detected for "
                "gold.gold.dim_date — dropping table and recreating with current df. "
                f"Error: {err_str[:300]}"
            )

            # ── Step 1: try spark.sql DROP ────────────────────────────────────
            drop_succeeded = False
            try:
                spark.sql("DROP TABLE IF EXISTS gold.gold.dim_date")
                drop_succeeded = True
                print(
                    "[INFO] write_dim_date_to_gold: spark.sql DROP succeeded for "
                    "gold.gold.dim_date."
                )
            except Exception as drop_err:
                # BUG-029: same Py4JJavaError / NessieConflictException pattern as
                # write_dim_to_gold — fall through to REST API fallback.
                print(
                    f"[WARN] write_dim_date_to_gold: DROP TABLE IF EXISTS "
                    f"gold.gold.dim_date raised {drop_err!r} — attempting Nessie "
                    "REST API fallback."
                )

            # ── Step 2: REST API fallback if spark.sql DROP failed ────────────
            if not drop_succeeded:
                try:
                    nessie_uri = spark.conf.get("spark.sql.catalog.gold.uri")
                    nessie_ref = spark.conf.get("spark.sql.catalog.gold.ref", "main")
                    drop_succeeded = _nessie_drop_table_via_rest(
                        nessie_uri, "gold", "dim_date", nessie_ref
                    )
                    if drop_succeeded:
                        print(
                            "[INFO] write_dim_date_to_gold: Nessie REST API drop "
                            "succeeded for gold.gold.dim_date."
                        )
                    else:
                        print(
                            "[WARN] write_dim_date_to_gold: Nessie REST API drop "
                            "also failed for gold.gold.dim_date — retrying saveAsTable anyway."
                        )
                except Exception as rest_err:  # noqa: BLE001
                    print(
                        f"[WARN] write_dim_date_to_gold: REST API fallback raised "
                        f"{rest_err!r} for gold.gold.dim_date."
                    )

            # ── Step 3: evict stale CachingCatalog entry ──────────────────────
            if drop_succeeded:
                try:
                    spark.catalog.refreshTable("gold.gold.dim_date")
                except Exception:  # noqa: BLE001
                    pass

            # ── Step 4: retry with full df ────────────────────────────────────
            # Retry with full df (not new_rows_df): existing data is inaccessible
            # due to broken metadata chain, so recreate from current batch.
            # dim_date will be repopulated incrementally on subsequent monthly runs.
            (df.write
                .format("iceberg")
                .mode("overwrite")
                .saveAsTable("gold.gold.dim_date"))
        else:
            raise


# Step 6: Metadata Logging to Postgres
def _ensure_gold_metadata_table(conn, config: configparser.ConfigParser) -> None:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema",       "metadata")
    table  = db_cfg.get("gold_transform_log_table", "gold_transform_log")
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id              BIGSERIAL PRIMARY KEY,
        run_id          TEXT        NOT NULL,
        pipeline_name   TEXT        NOT NULL,
        year            INTEGER,
        month           INTEGER,
        status          TEXT,
        silver_rows     BIGINT,
        fact_rows       BIGINT,
        started_at      TIMESTAMPTZ NOT NULL,
        finished_at     TIMESTAMPTZ,
        error_message   TEXT,
        spark_app_id    TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


# Inert gold run metadata (success or failure) to Postgres for observability and debugging
def _log_gold_run(config: configparser.ConfigParser, run_payload: Dict[str, Any]) -> None:
    db_cfg = config["database"]
    schema = db_cfg.get("database_schema",          "metadata")
    table  = db_cfg.get("gold_transform_log_table", "gold_transform_log")
    conn = _get_db_connection(config)
    try:
        _ensure_gold_metadata_table(conn, config)
        insert_sql = f"""
            INSERT INTO {schema}.{table}
                (run_id, pipeline_name, year, month, status,
                 silver_rows, fact_rows,
                 started_at, finished_at, error_message, spark_app_id)
            VALUES
                (%(run_id)s, %(pipeline_name)s, %(year)s, %(month)s, %(status)s,
                 %(silver_rows)s, %(fact_rows)s,
                 %(started_at)s, %(finished_at)s, %(error_message)s, %(spark_app_id)s)
        """
        with conn.cursor() as cur:
            cur.execute(insert_sql, {
                "run_id":           run_payload.get("run_id"),
                "pipeline_name":    run_payload.get("pipeline_name"),
                "year":             run_payload.get("year"),
                "month":            run_payload.get("month"),
                "status":           run_payload.get("status"),
                "silver_rows":      run_payload.get("silver_rows"),
                "fact_rows":        run_payload.get("fact_rows"),
                "started_at":       run_payload.get("started_at"),
                "finished_at":      run_payload.get("finished_at"),
                "error_message":    run_payload.get("error"),
                "spark_app_id":     run_payload.get("spark_app_id"),
            })
        conn.commit()
    except Exception as log_err:
        print(f"[WARN] Failed to log gold run metadata: {log_err}")
    finally:
        conn.close()


# STEP 7: Main Orchestration Function
def run_gold_transform(
    pipeline_name: str = "taxi_gold_transform",
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> None:
    config = _load_config()

    # BUG-021: Early exit if this (year, month) was already successfully processed.
    # Avoids re-running the full Spark gold transform (Spark startup + all dim/fact
    # writes) for periods already in gold_transform_log with status='SUCCESS'.
    # Mirrors the silver MinIO partition skip guard introduced in BUG-020.
    if _check_gold_already_succeeded(config, year, month):
        month_str = f"{month:02d}" if month else "all"
        print(
            f"[INFO] Gold transform already succeeded for year={year}, "
            f"month={month_str} — skipping (no Spark session started)."
        )
        return

    run_payload: Dict[str, Any] = {
        "run_id":        str(uuid4()),
        "pipeline_name": pipeline_name,
        "year":          year,
        "month":         month,
        "status":        "RUNNING",
        "started_at":    _now_vn(),
    }

    spark = None
    silver_df = None
    dim_date = None  # BUG-021: declared here so finally block can unpersist safely
    try:
        spark = build_spark_session_gold(config)
        run_payload["spark_app_id"] = spark.sparkContext.applicationId

        # Ensure the 'gold' namespace exists in Nessie so Trino can see it via
        # SHOW SCHEMAS FROM iceberg (BUG-011).
        _ensure_gold_schema(spark)

        # Read silver
        silver_df = _read_silver_layer(spark, config, year, month)

        # Cache silver_df early — it is reused for dim_date build, fact joins, and count.
        # Without caching, Spark re-reads and re-processes the full silver layer each time.
        

        # DataFrame-native time-limited count — avoids .rdd which calls javaToPython()
        # and raises Py4JJavaError on deep union trees / S3A errors (BUG-009).
        run_payload["silver_rows"] = None

        # PERF: persist with MEMORY_AND_DISK — more memory-efficient than cache()
        # (MEMORY_ONLY) for large silver DataFrames.
        # BUG-031 fix: MEMORY_AND_DISK_SER is a Scala/Java Spark storage level that
        # does NOT exist in PySpark. In PySpark, MEMORY_AND_DISK already uses
        # serialized storage (deserialized=False) — it is the direct equivalent of
        # Scala's MEMORY_AND_DISK_SER. MEMORY_AND_DISK_DESER is the deserialized variant.
        from pyspark import StorageLevel
        silver_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Build all dims
        dim_vendor       = build_dim_vendor(spark)
        dim_service_type = build_dim_service_type(spark)
        dim_payment      = build_dim_payment(spark)
        dim_ratecode     = build_dim_ratecode(spark)
        dim_trip_type    = build_dim_trip_type(spark)
        dim_location     = build_dim_location(spark)
        dim_date         = build_dim_date(spark, silver_df)

        # BUG-021: Cache dim_date immediately after building.
        dim_date.cache()

        # PERF: Write static lookup dims in parallel using ThreadPoolExecutor.
        # These 6 tables are independent (no shared state) and each write is a
        # small Iceberg overwrite (~KB). Parallel submission cuts wall-clock time
        # from ~6 × T_write to ~1 × T_write (bounded by the slowest single write).
        # dim_date and fact_trips are written sequentially after (ordering matters).
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures import as_completed as _as_completed

        static_dims = [
            (dim_vendor,       "dim_vendor"),
            (dim_service_type, "dim_service_type"),
            (dim_payment,      "dim_payment"),
            (dim_ratecode,     "dim_ratecode"),
            (dim_trip_type,    "dim_trip_type"),
            (dim_location,     "dim_location"),
        ]

        def _write_dim(args):
            df_dim, name = args
            write_dim_to_gold(df_dim, name, spark)
            return name

        with ThreadPoolExecutor(max_workers=6) as _pool:
            futures = {_pool.submit(_write_dim, item): item[1] for item in static_dims}
            for fut in _as_completed(futures):
                name = futures[fut]
                try:
                    fut.result()
                    print(f"[INFO] run_gold_transform: dim '{name}' written successfully.")
                except Exception as dim_err:
                    print(f"[ERROR] run_gold_transform: dim '{name}' write failed: {dim_err!r}")
                    raise

        # dim_date: append-only, must run after static dims (sequential)
        write_dim_date_to_gold(dim_date, spark)

        # Build & write fact
        fact_df = build_fact_table(
            spark, silver_df,
            dim_vendor, dim_service_type, dim_payment,
            dim_ratecode, dim_trip_type, dim_location, dim_date,
        )
        # DataFrame-native time-limited count for fact rows (metadata-only, non-blocking).
        run_payload["fact_rows"] = None
        write_fact_to_gold(fact_df, spark, source_year=year, source_month=month)

        run_payload["status"] = "SUCCESS"

    except Exception as e:
        run_payload["status"] = "FAILED"
        run_payload["error"]  = _safe_str_exc(e)
        raise

    finally:
        run_payload["finished_at"] = _now_vn()
        # Unpersist cached DataFrames before stopping Spark to release memory cleanly.
        if dim_date is not None:
            try:
                dim_date.unpersist()
            except Exception:
                pass
        if silver_df is not None:
            try:
                silver_df.unpersist()
            except Exception:
                pass
        if spark:
            try:
                spark.stop()
            except Exception as stop_err:  # noqa: BLE001
                # BUG-023: spark.stop() raises Py4JError when the JVM is already dead
                # (e.g. after SIGTERM from Airflow externally marking the task as success,
                # or after a JVM crash mid-write). Without this guard the exception escapes
                # the finally block, preventing _log_gold_run from being called and
                # reporting a misleading "SparkSession does not exist in the JVM" error
                # instead of the real failure cause.
                print(
                    f"[WARN] spark.stop() raised {stop_err!r} — "
                    "JVM may already be dead (SIGTERM / crash). "
                    "Continuing to log metadata."
                )
        # Log always (success or failure)
        _log_gold_run(config, run_payload)
        month_str = f"{month:02d}" if month else "all"
        print(f"Gold transform {run_payload['status']} for year={year}, month={month_str}")


if __name__ == "__main__":
    run_gold_transform()
