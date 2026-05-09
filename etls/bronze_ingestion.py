import configparser
import os
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
import requests
from airflow.exceptions import AirflowSkipException
from minio import Minio
from minio.error import S3Error

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.conf"

# Read config 
def _load_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config

# Load service types
def _load_service_types(config: configparser.ConfigParser) -> List[str]:
    raw_value = config.get("taxi_type", "service_type", fallback="['yellow']")

    try:
        parsed = ast.literal_eval(raw_value) # Safely evaluate string representation of list
        if isinstance(parsed, list):
            return [str(item).strip().lower() for item in parsed if str(item).strip()]
    except (SyntaxError, ValueError):
        pass

    return [value.strip().lower() for value in raw_value.split(",") if value.strip()]

# Helper functions to construct URLs, file paths, and interact with MinIO and PostgreSQL for logging
def _source_url(config: configparser.ConfigParser, service_type: str, year: int, month: int) -> str:
    base_url = config["download"]["base_url"].rstrip("/")
    file_name = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
    return f"{base_url}/{file_name}"

# Construct local file path based on config and parameters
def _local_file_path(config: configparser.ConfigParser, service_type: str, year: int, month: int) -> str:
    input_path = config["file_paths"]["input_path"]
    return os.path.join(
        input_path,
        service_type,
        f"year={year}",
        f"month={month:02d}",
        "data.parquet",
    )

# Determine MinIO bucket name for the given layer, with fallback to layer name if not specified in config
def _layer_bucket(config: configparser.ConfigParser, layer: str) -> str:
    minio_cfg = config["minio"]
    bucket_key = f"{layer}_bucket"
    if bucket_key in minio_cfg:
        return minio_cfg[bucket_key]
    return layer

# Construct object key for MinIO based on layer, service type, year, and month
def _object_key(layer: str, service_type: str, year: int, month: int) -> str:
    return f"{layer}/{service_type}/year={year}/month={month:02d}/data.parquet"

# Initialize MinIO client using configuration parameters, with support for secure connections
def _get_minio_client(config: configparser.ConfigParser) -> Minio:
    minio_cfg = config["minio"]
    secure = minio_cfg.get("secure", "false").lower() == "true"
    return Minio(
        endpoint=minio_cfg["endpoint"],
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=secure,
    )

# Establish a connection to PostgreSQL using configuration parameters, with support for custom ports
def _get_db_connection(config: configparser.ConfigParser):
    db_cfg = config["database"]
    return psycopg2.connect(
        host=db_cfg["database_host"],
        dbname=db_cfg["database_name"],
        user=db_cfg["database_username"],
        password=db_cfg["database_password"],
        port=int(db_cfg["database_port"]),
    )


def _get_existing_layer_object_stat(
    config: configparser.ConfigParser,
    layer: str,
    service_type: str,
    year: int,
    month: int,
) -> Optional[Dict[str, Any]]:
    bucket_name = _layer_bucket(config, layer)
    object_key = _object_key(layer, service_type, year, month)
    client = _get_minio_client(config)

    try:
        if not client.bucket_exists(bucket_name):
            return None
        stat = client.stat_object(bucket_name, object_key)
        return {
            "minio_bucket": bucket_name,
            "minio_object_key": object_key,
            "file_size_bytes": int(getattr(stat, "size", 0) or 0),
        }
    except S3Error as exc:
        # Treat "not found" as absent; bubble up unexpected errors to avoid silent data loss.
        if getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return None
        raise


def _run_local_file_cleanup(config: configparser.ConfigParser, force: bool = False) -> Dict[str, Any]:
    cleanup_enabled = config.getboolean("cleanup", "enabled", fallback=False)
    retention_hours = config.getfloat("cleanup", "retention_hours", fallback=168.0)
    local_base_path = Path(config["file_paths"]["input_path"])

    stats: Dict[str, Any] = {
        "enabled": cleanup_enabled,
        "retention_hours": retention_hours,
        "base_path": str(local_base_path),
        "deleted_files": 0,
        "deleted_bytes": 0,
        "errors": [],
    }

    if not cleanup_enabled and not force:
        return stats

    if not local_base_path.exists():
        return stats

    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=retention_hours)

    for parquet_file in local_base_path.rglob("*.parquet"):
        try:
            modified_time = datetime.fromtimestamp(parquet_file.stat().st_mtime, tz=timezone.utc)
            if modified_time <= cutoff_time:
                file_size = parquet_file.stat().st_size
                parquet_file.unlink()
                stats["deleted_files"] += 1
                stats["deleted_bytes"] += file_size
        except OSError as exc:
            stats["errors"].append(f"{parquet_file}: {exc}")

    return stats


def cleanup_local_files(force: bool = False) -> Dict[str, Any]:
    config = _load_config()
    cleanup_stats = _run_local_file_cleanup(config=config, force=force)

    status_label = "forced" if force else "config-driven"
    print(
        "Local cleanup completed "
        f"({status_label}, retention_hours={cleanup_stats['retention_hours']}, "
        f"deleted_files={cleanup_stats['deleted_files']}, "
        f"deleted_bytes={cleanup_stats['deleted_bytes']})"
    )
    if cleanup_stats["errors"]:
        print(f"Local cleanup had {len(cleanup_stats['errors'])} errors.")

    return cleanup_stats

# Ensure that the ingestion log table exists in the database, creating it if necessary
def _ensure_log_table(conn, schema: str, table: str) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id BIGSERIAL PRIMARY KEY,
        pipeline_name TEXT NOT NULL,
        layer_name TEXT NOT NULL,
        service_type TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        source_url TEXT NOT NULL,
        local_path TEXT,
        minio_bucket TEXT,
        minio_object_key TEXT,
        file_size_bytes BIGINT,
        status TEXT NOT NULL,
        error_message TEXT,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


# Log the ingestion attempt to the database
def _log_ingestion(config: configparser.ConfigParser, payload: Dict[str, Any]) -> None:
    schema = config["database"].get("database_schema", "metadata")
    table = config["database"].get("database_log_table", "ingestion_log")

    conn = _get_db_connection(config)
    try:
        _ensure_log_table(conn, schema, table)
        insert_sql = f"""
        INSERT INTO {schema}.{table}
        (
            pipeline_name,
            layer_name,
            service_type,
            year,
            month,
            source_url,
            local_path,
            minio_bucket,
            minio_object_key,
            file_size_bytes,
            status,
            error_message,
            started_at,
            finished_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (
                    payload["pipeline_name"],
                    payload["layer_name"],
                    payload["service_type"],
                    payload["year"],
                    payload["month"],
                    payload["source_url"],
                    payload.get("local_path"),
                    payload.get("minio_bucket"),
                    payload.get("minio_object_key"),
                    payload.get("file_size_bytes"),
                    payload["status"],
                    payload.get("error_message"),
                    payload["started_at"],
                    payload["finished_at"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _get_latest_successful_ingestion(
    config: configparser.ConfigParser,
    layer: str,
    service_type: str,
    year: int,
    month: int,
) -> Optional[Dict[str, Any]]:
    schema = config["database"].get("database_schema", "metadata")
    table = config["database"].get("database_log_table", "ingestion_log") 

    conn = _get_db_connection(config)
    try:
        _ensure_log_table(conn, schema, table)
        query = f"""
        SELECT source_url, local_path, minio_bucket, minio_object_key, file_size_bytes, finished_at
        FROM {schema}.{table}
        WHERE layer_name = %s
          AND service_type = %s
          AND year = %s
          AND month = %s
          AND status = 'SUCCESS'
        ORDER BY finished_at DESC NULLS LAST, id DESC
        LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(query, (layer, service_type, year, month))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "source_url": row[0],
                "local_path": row[1],
                "minio_bucket": row[2],
                "minio_object_key": row[3],
                "file_size_bytes": row[4],
                "finished_at": row[5],
            }
    finally:
        conn.close()


def _get_latest_success_period_for_service(
    config: configparser.ConfigParser,
    layer: str,
    service_type: str,
) -> Optional[Tuple[int, int]]:
    schema = config["database"].get("database_schema", "metadata")
    table = config["database"].get("database_log_table", "ingestion_log")

    conn = _get_db_connection(config)
    try:
        _ensure_log_table(conn, schema, table)
        query = f"""
        SELECT year, month
        FROM {schema}.{table}
        WHERE layer_name = %s
          AND service_type = %s
          AND status = 'SUCCESS'
        ORDER BY year DESC, month DESC, id DESC
        LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(query, (layer, service_type))
            row = cur.fetchone()
            if not row:
                return None
            return int(row[0]), int(row[1])
    finally:
        conn.close()


def _next_year_month(year: int, month: int) -> Tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def infer_next_ingestion_period_from_metadata(
    layer: str = "bronze",
    start_year: int = 2022,
    start_month: int = 1,
) -> Tuple[int, int]:
    config = _load_config()
    service_types = _load_service_types(config)

    next_candidates: List[Tuple[int, int]] = []
    for service in service_types:
        latest = _get_latest_success_period_for_service(
            config=config,
            layer=layer,
            service_type=service,
        )
        if latest is None:
            next_candidates.append((start_year, start_month))
        else:
            next_candidates.append(_next_year_month(*latest))

    return min(next_candidates)


# Download the taxi data file from the source URL and save it locally, returning metadata about the download
def download_taxi(service_type: str, year: int, month: int) -> Dict[str, Any]:
    config = _load_config()
    url = _source_url(config, service_type, year, month)
    timeout = int(config["download"]["time_out"])
    save_path = _local_file_path(config, service_type, year, month)

    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    response = requests.get(url, stream=True, timeout=timeout)
    if response.status_code in (403, 404):
        raise AirflowSkipException(
            f"Source file not available yet: {service_type}_tripdata_{year}-{month:02d}.parquet"
        )
    response.raise_for_status()

    with open(save_path, "wb") as file_obj:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_obj.write(chunk)

    return {
        "source_url": url,
        "local_path": save_path,
        "file_size_bytes": os.path.getsize(save_path),
    }


# Upload the local file to the appropriate MinIO bucket and object key
def upload_file_to_layer(local_path: str, layer: str, service_type: str, year: int, month: int) -> Dict[str, str]:
    config = _load_config()
    bucket_name = _layer_bucket(config, layer)
    object_key = _object_key(layer, service_type, year, month)

    client = _get_minio_client(config)
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    client.fput_object(bucket_name, object_key, local_path)

    return {"minio_bucket": bucket_name, "minio_object_key": object_key}


# Ingest one service type to target layer and write ingestion metadata log
def _ingest_single_service(
    service_type: str,
    year: int,
    month: int,
    layer: str = "bronze",
    pipeline_name: str = "nyc_taxi_ingestion",
    force_reload: bool = False,
) -> Dict[str, Any]:
    config = _load_config()
    started_at = datetime.now(timezone.utc)

    log_payload: Dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "layer_name": layer,
        "service_type": service_type,
        "year": year,
        "month": month,
        "source_url": _source_url(config, service_type, year, month),
        "local_path": None,
        "minio_bucket": None,
        "minio_object_key": None,
        "file_size_bytes": None,
        "status": "FAILED",
        "error_message": None,
        "started_at": started_at,
        "finished_at": None,
    }

    try:
        if not force_reload:
            existing_success = _get_latest_successful_ingestion(
                config=config,
                layer=layer,
                service_type=service_type,
                year=year,
                month=month,
            )
            existing_object = _get_existing_layer_object_stat(
                config=config,
                layer=layer,
                service_type=service_type,
                year=year,
                month=month,
            )

            if existing_success and existing_object:
                # Both metadata SUCCESS and MinIO object exist → safe to skip
                log_payload["status"] = "SKIPPED"
                log_payload["error_message"] = (
                    "Skipped because a successful ingestion metadata record AND the target "
                    "MinIO object both already exist for this layer/service_type/year/month. "
                    "Set force_reload=True to reload."
                )
                log_payload.update(
                    {
                        "local_path": existing_success.get("local_path"),
                        "minio_bucket": existing_object.get("minio_bucket"),
                        "minio_object_key": existing_object.get("minio_object_key"),
                        "file_size_bytes": existing_object.get("file_size_bytes"),
                    }
                )
                return {
                    "layer": layer,
                    "service_type": service_type,
                    "year": year,
                    "month": month,
                    "status": "SKIPPED",
                    "reason": "existing_success_metadata_and_minio_object",
                    "source_url": existing_success.get("source_url", log_payload["source_url"]),
                    "minio_bucket": existing_object.get("minio_bucket"),
                    "minio_object_key": existing_object.get("minio_object_key"),
                    "finished_at": existing_success.get("finished_at"),
                }

            if existing_object and not existing_success:
                # Object exists in MinIO but no metadata log → skip (object already present)
                log_payload["status"] = "SKIPPED"
                log_payload["error_message"] = (
                    "Skipped because the target object already exists in MinIO "
                    "for this layer/service_type/year/month. Set force_reload=True to reload."
                )
                log_payload.update(existing_object)
                return {
                    "layer": layer,
                    "service_type": service_type,
                    "year": year,
                    "month": month,
                    "status": "SKIPPED",
                    "reason": "existing_minio_object",
                    "source_url": log_payload["source_url"],
                    **existing_object,
                }

            # existing_success present but existing_object is None → self-heal:
            # metadata says SUCCESS but object is gone from MinIO, re-ingest to restore.
            if existing_success and not existing_object:
                print(
                    f"[SELF-HEAL] Metadata SUCCESS found for {service_type} {year}-{month:02d} "
                    f"but MinIO object is missing. Re-ingesting to restore Bronze data."
                )

        download_result = download_taxi(service_type, year, month)
        upload_result = upload_file_to_layer(
            local_path=download_result["local_path"],
            layer=layer,
            service_type=service_type,
            year=year,
            month=month,
        )

        log_payload.update(download_result)
        log_payload.update(upload_result)
        log_payload["status"] = "SUCCESS"
        return {
            **download_result,
            **upload_result,
            "layer": layer,
            "status": "SUCCESS",
        }
    except Exception as exc:
        if isinstance(exc, AirflowSkipException):
            log_payload["status"] = "FAILED"
        log_payload["error_message"] = str(exc)
        raise
    finally:
        log_payload["finished_at"] = datetime.now(timezone.utc)
        _log_ingestion(config, log_payload)


# Orchestrate ingestion to layer. If service_type is None, load from config [taxi_type] service_type.
def ingest_taxi_to_layer(
    year: int,
    month: int,
    layer: str = "bronze",
    pipeline_name: str = "nyc_taxi_ingestion",
    service_type: Optional[str] = None,
    force_reload: bool = False,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    config = _load_config()
    service_types = [service_type.lower()] if service_type else _load_service_types(config)

    results: List[Dict[str, Any]] = []
    missing_sources: List[Dict[str, Any]] = []

    for service in service_types:
        try:
            results.append(
                _ingest_single_service(
                    service_type=service,
                    year=year,
                    month=month,
                    layer=layer,
                    pipeline_name=pipeline_name,
                    force_reload=force_reload,
                )
            )
        except AirflowSkipException as exc:
            # Keep observability in returned payload, but fail task after cleanup.
            skip_result = {
                "layer": layer,
                "service_type": service,
                "year": year,
                "month": month,
                "status": "FAILED",
                "reason": "source_not_available",
                "error_message": str(exc),
            }
            results.append(skip_result)
            missing_sources.append(skip_result)

    cleanup_stats = _run_local_file_cleanup(config=config)
    if cleanup_stats["enabled"]:
        print(
            "Local cleanup completed "
            f"(retention_hours={cleanup_stats['retention_hours']}, "
            f"deleted_files={cleanup_stats['deleted_files']}, "
            f"deleted_bytes={cleanup_stats['deleted_bytes']})"
        )
        if cleanup_stats["errors"]:
            print(f"Local cleanup had {len(cleanup_stats['errors'])} errors.")

    if missing_sources:
        missing_labels = ", ".join(
            f"{item['service_type']}:{item['year']}-{item['month']:02d}"
            for item in missing_sources
        )
        raise RuntimeError(
            "Bronze ingestion failed due to source files not available yet for: "
            f"{missing_labels}"
        )

    if len(results) == 1:
        return results[0]
    return results