#!/usr/bin/env python3
import argparse
import configparser
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, List, Optional

import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.conf"

VALID_TAXI_TYPES = ["green", "yellow"]


def load_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    return cfg


def resolve_endpoint(raw_endpoint: str) -> str:
    endpoint = (raw_endpoint or "").strip()
    if endpoint == "minio:9000":
        # "minio" is resolvable inside docker network, not from host shell.
        return "localhost:9010"
    return endpoint or "localhost:9010"


def get_minio_client(cfg: configparser.ConfigParser, endpoint_override: Optional[str]) -> Any:
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency: minio\n"
            "Install with: ./venv/bin/pip install minio"
        ) from exc

    minio_cfg = cfg["minio"]
    endpoint = resolve_endpoint(
        endpoint_override
        or os.getenv("MINIO_ENDPOINT", "")
        or minio_cfg.get("endpoint", "localhost:9010")
    )
    access_key = os.getenv("MINIO_ACCESS_KEY", minio_cfg.get("access_key", "minioadmin"))
    secret_key = os.getenv("MINIO_SECRET_KEY", minio_cfg.get("secret_key", "minioadmin"))
    secure = os.getenv("MINIO_SECURE", minio_cfg.get("secure", "false")).lower() == "true"
    return Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def pick_latest_parquet(client: Any, bucket: str, prefix: str) -> Optional[str]:
    latest_key = None
    latest_mtime = None
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith(".parquet"):
            continue
        if latest_mtime is None or (obj.last_modified and obj.last_modified > latest_mtime):
            latest_key = obj.object_name
            latest_mtime = obj.last_modified
    return latest_key


def list_parquet_objects(client: Any, bucket: str, prefix: str, limit: int) -> None:
    count = 0
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith(".parquet"):
            continue
        print(f"{obj.last_modified}\t{obj.size}\t{obj.object_name}")
        count += 1
        if count >= limit:
            break
    if count == 0:
        print("No parquet files found.")


def download_object(client: Any, bucket: str, object_key: str, output_path: Optional[str]) -> Path:
    try:
        from minio.error import S3Error
    except ModuleNotFoundError:
        S3Error = Exception  # fallback, dependency handled earlier

    if output_path:
        out = Path(output_path).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            client.fget_object(bucket, object_key, str(out))
        except S3Error as exc:
            if getattr(exc, "code", "") == "NoSuchKey":
                suggest_nearby_keys(client, bucket, object_key)
            raise
        return out

    fd, tmp_name = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        client.fget_object(bucket, object_key, tmp_name)
    except S3Error as exc:
        if getattr(exc, "code", "") == "NoSuchKey":
            suggest_nearby_keys(client, bucket, object_key)
        raise
    return Path(tmp_name)


def suggest_nearby_keys(client: Any, bucket: str, object_key: str) -> None:
    parent_prefix = object_key.rsplit("/", 1)[0] + "/"
    print("\nObject key not found. Nearby parquet keys:")
    printed = 0
    for obj in client.list_objects(bucket, prefix=parent_prefix, recursive=True):
        if not obj.object_name.endswith(".parquet"):
            continue
        print(f"- {obj.object_name}")
        printed += 1
        if printed >= 20:
            break

    if printed == 0:
        alt_prefix = parent_prefix.replace("month=1/", "month=01/").replace("month=01/", "month=1/")
        if alt_prefix != parent_prefix:
            print(f"No files under {parent_prefix}. Trying alternate prefix {alt_prefix}")
            for obj in client.list_objects(bucket, prefix=alt_prefix, recursive=True):
                if not obj.object_name.endswith(".parquet"):
                    continue
                print(f"- {obj.object_name}")
                printed += 1
                if printed >= 20:
                    break

    if printed == 0:
        print("No nearby parquet keys found. Use --list-only with a higher-level prefix.")


def show_parquet_preview(file_path: Path, rows: int) -> None:
    parquet_file = pq.ParquetFile(file_path)
    print(f"\nRows(metadata): {parquet_file.metadata.num_rows}")
    print(f"Row groups: {parquet_file.metadata.num_row_groups}")
    print("\nSchema:")
    for field in parquet_file.schema_arrow:
        print(f"- {field.name}: {field.type}")

    table = pq.read_table(file_path).slice(0, rows)
    df = table.to_pandas()
    print(f"\nSample top {min(rows, len(df))} rows:")
    if df.empty:
        print("(empty)")
        return
    print(df.to_string(index=False, max_colwidth=60))


# Sub-path mapping per bucket/layer:
#   bronze  →  bronze/{taxi_type}/year=.../month=.../data.parquet
#   silver  →  silver/taxi_trips/year=.../month=.../part-xxxx.parquet
#   gold    →  gold/...  (manual prefix recommended)
LAYER_SUB_PATH: dict = {
    "bronze": None,          # replaced by taxi_type at runtime
    "silver": "taxi_trips",  # fixed sub-path, no taxi_type split
}


def build_prefix(
    bucket: str,
    taxi_type: Optional[str],
    year: Optional[int],
    month: Optional[int],
) -> str:
    """Build MinIO object prefix from bucket, taxi_type, year, and optional month.

    Layer-aware prefix construction:
      bronze  →  bronze/{taxi_type}/year={year}/month={month:02d}/
      silver  →  silver/taxi_trips/year={year}/month={month:02d}/
      other   →  {bucket}/{taxi_type}/year={year}/month={month:02d}/
    """
    sub_path = LAYER_SUB_PATH.get(bucket)

    if sub_path is None:
        # bronze (or unknown): use taxi_type as sub-path
        sub_path = taxi_type or ""

    parts = [p for p in [bucket, sub_path] if p]
    if year is not None:
        parts.append(f"year={year}")
        if month is not None:
            parts.append(f"month={month:02d}")
    return "/".join(parts) + "/"


def collect_all_parquet_keys(client: Any, bucket: str, prefix: str) -> List[str]:
    """Return a list of all parquet object keys under the given prefix."""
    keys = []
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if obj.object_name.endswith(".parquet"):
            keys.append(obj.object_name)
    return keys


def compute_aggregation_from_keys(
    client: Any,
    bucket: str,
    object_keys: List[str],
    column: str,
    agg_type: str,
    filter_year: Optional[int] = None,
    filter_month: Optional[int] = None,
    filter_col: Optional[str] = None,
) -> None:
    """Download each parquet file, read the target column, and accumulate sum/count.

    Optional post-download filter:
      filter_col  — datetime column to filter on (e.g. 'pickup_datetime')
      filter_year — keep only rows where year(filter_col) == filter_year
      filter_month — keep only rows where month(filter_col) == filter_month
    This is useful to compare silver (partitioned by source_year) vs gold
    (partitioned by pickup_datetime year).
    """
    import pyarrow.compute as pc

    total_files = len(object_keys)
    if total_files == 0:
        print("No parquet files found for the given parameters.", file=sys.stderr)
        return

    accumulated_sum = 0.0
    accumulated_count = 0
    apply_datetime_filter = bool(filter_col and filter_year is not None)

    for idx, key in enumerate(object_keys, start=1):
        print(f"  Processing [{idx}/{total_files}]: {key}")
        fd, tmp_name = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        try:
            client.fget_object(bucket, key, tmp_name)
            pf = pq.ParquetFile(tmp_name)

            # Validate columns exist (only on first file)
            if idx == 1:
                schema_names = [f.name for f in pf.schema_arrow]
                if column not in schema_names:
                    print(
                        f"\nERROR: Column '{column}' not found in parquet schema.\n"
                        f"Available columns: {schema_names}",
                        file=sys.stderr,
                    )
                    return
                if apply_datetime_filter and filter_col not in schema_names:
                    print(
                        f"\nERROR: Filter column '{filter_col}' not found in parquet schema.\n"
                        f"Available columns: {schema_names}",
                        file=sys.stderr,
                    )
                    return

            # Read columns needed
            cols_to_read = [column]
            if apply_datetime_filter and filter_col != column:
                cols_to_read.append(filter_col)

            table = pq.read_table(tmp_name, columns=cols_to_read)

            # Apply datetime-based filter if requested
            if apply_datetime_filter:
                dt_col = table.column(filter_col)
                year_arr = pc.year(dt_col)
                mask = pc.equal(year_arr, filter_year)
                if filter_month is not None:
                    month_arr = pc.month(dt_col)
                    mask = pc.and_(mask, pc.equal(month_arr, filter_month))
                table = table.filter(mask)

            col_data = table.column(column)

            if agg_type == "count":
                accumulated_count += len(col_data)
            elif agg_type == "sum":
                col_sum = pc.sum(col_data).as_py()
                if col_sum is not None:
                    accumulated_sum += col_sum
        finally:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass

    result_value = accumulated_count if agg_type == "count" else accumulated_sum
    result_str = (
        f"{result_value:,}"
        if agg_type == "count"
        else f"{result_value:,.4f}"
    )

    filter_note = ""
    if apply_datetime_filter:
        filter_note = f"\n  Filter   : year({filter_col})={filter_year}"
        if filter_month is not None:
            filter_note += f", month({filter_col})={filter_month}"

    print()
    print("=" * 60)
    print("  Aggregation Result")
    print(f"  Bucket   : {bucket}")
    print(f"  Column   : {column}")
    print(f"  Operation: {agg_type.upper()}{filter_note}")
    print(f"  Result   : {result_str}")
    print(f"  Files    : {total_files}")
    print("=" * 60)


def run_aggregation(
    client: Any,
    bucket: str,
    taxi_types: List[str],
    year: Optional[int],
    month: Optional[int],
    column: str,
    agg_type: str,
    prefix_override: str,
    filter_col: Optional[str] = None,
    filter_year: Optional[int] = None,
    filter_month: Optional[int] = None,
) -> int:
    """Orchestrate aggregation across one or more taxi types."""
    all_keys: List[str] = []

    if prefix_override:
        # Manual prefix provided — use it directly
        print(f"Using manual prefix: {prefix_override}")
        all_keys = collect_all_parquet_keys(client, bucket, prefix_override)
    elif bucket == "silver":
        # Silver layer: single merged table, no taxi_type split
        prefix = build_prefix(bucket, None, year, month)
        print(f"Building prefix for silver layer: {prefix}")
        keys = collect_all_parquet_keys(client, bucket, prefix)
        print(f"  Found {len(keys)} parquet file(s).")
        all_keys.extend(keys)
        if taxi_types != VALID_TAXI_TYPES:
            print(
                f"  Note: --taxi-type is ignored for silver layer "
                f"(silver stores all taxi types merged under 'taxi_trips/')."
            )
    else:
        for tt in taxi_types:
            prefix = build_prefix(bucket, tt, year, month)
            print(f"Building prefix for '{tt}': {prefix}")
            keys = collect_all_parquet_keys(client, bucket, prefix)
            print(f"  Found {len(keys)} parquet file(s).")
            all_keys.extend(keys)

    if not all_keys:
        print("No parquet files found for the given parameters.", file=sys.stderr)
        return 1

    print(f"\nTotal parquet files to process: {len(all_keys)}\n")
    compute_aggregation_from_keys(
        client, bucket, all_keys, column, agg_type,
        filter_year=filter_year,
        filter_month=filter_month,
        filter_col=filter_col,
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read parquet files directly from MinIO: preview schema/data "
            "or compute aggregations (sum/count) on a column."
        )
    )
    parser.add_argument("--bucket", required=True, help="MinIO bucket name, e.g. bronze or silver")
    parser.add_argument(
        "--prefix",
        default="",
        help="Prefix to list/search parquet files, e.g. silver/taxi_trip/year=2024/",
    )
    parser.add_argument("--object-key", default=None, help="Exact object key parquet to read")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Pick latest parquet under --prefix (ignored if --object-key is set)",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only list parquet objects under prefix and exit",
    )
    parser.add_argument("--list-limit", type=int, default=50, help="Max objects to print in list mode")
    parser.add_argument("--rows", type=int, default=10, help="Number of preview rows")
    parser.add_argument(
        "--download-path",
        default=None,
        help="Optional local path to save parquet file (otherwise use a temp file)",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="MinIO endpoint host:port. Example: localhost:9010",
    )

    # ── Aggregation arguments ──────────────────────────────────────────────
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to filter data, e.g. 2018. Aggregates all months if --month is not set.",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Month to filter data (1-12). Requires --year.",
    )
    parser.add_argument(
        "--taxi-type",
        dest="taxi_type",
        default=None,
        choices=VALID_TAXI_TYPES,
        help=(
            "Taxi type to filter: 'green' or 'yellow'. "
            "Defaults to both green and yellow when not specified."
        ),
    )
    parser.add_argument(
        "--column",
        default=None,
        help="Column name to compute aggregation on, e.g. total_amount or trip_distance.",
    )
    parser.add_argument(
        "--agg",
        default=None,
        choices=["sum", "count"],
        help="Aggregation type: 'sum' or 'count'. Requires --column.",
    )
    parser.add_argument(
        "--filter-col",
        dest="filter_col",
        default=None,
        help=(
            "Datetime column to apply year/month filter on (e.g. 'pickup_datetime'). "
            "Useful to compare silver (partitioned by source_year) vs gold semantics "
            "(partitioned by pickup_datetime year). "
            "Example: --filter-col pickup_datetime --year 2023"
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    client = get_minio_client(cfg, args.endpoint)

    # ── Aggregation mode ───────────────────────────────────────────────────
    if args.agg is not None or args.column is not None:
        if not args.agg or not args.column:
            print(
                "ERROR: Both --agg (sum|count) and --column <name> are required together.",
                file=sys.stderr,
            )
            return 2

        if args.month is not None and args.year is None:
            print("ERROR: --month requires --year to be specified.", file=sys.stderr)
            return 2

        taxi_types = [args.taxi_type] if args.taxi_type else VALID_TAXI_TYPES

        # --filter-col: apply datetime-based row filter after download
        # (e.g. filter silver rows by pickup_datetime year to match gold semantics)
        filter_col = getattr(args, "filter_col", None)

        return run_aggregation(
            client=client,
            bucket=args.bucket,
            taxi_types=taxi_types,
            year=args.year,
            month=args.month,
            column=args.column,
            agg_type=args.agg,
            prefix_override=args.prefix,
            filter_col=filter_col,
            filter_year=args.year if filter_col else None,
            filter_month=args.month if filter_col else None,
        )

    # ── List-only mode ─────────────────────────────────────────────────────
    if args.list_only:
        list_parquet_objects(client, args.bucket, args.prefix, args.list_limit)
        return 0

    # ── Preview mode ───────────────────────────────────────────────────────
    object_key = args.object_key
    if not object_key:
        if not args.latest:
            print("Provide --object-key, or use --latest with --prefix.", file=sys.stderr)
            return 2
        object_key = pick_latest_parquet(client, args.bucket, args.prefix)
        if not object_key:
            print("No parquet file found for given prefix.", file=sys.stderr)
            return 1

    print(f"Reading: s3://{args.bucket}/{object_key}")
    local_file = download_object(client, args.bucket, object_key, args.download_path)
    print(f"Local file: {local_file}")
    show_parquet_preview(local_file, args.rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
