# Project Overview: NYC Taxi Data Pipeline (Airflow + MinIO + Spark + Great Expectations + Iceberg/Nessie + Trino + Superset)

## 1) What does this project do?

This project implements a production-oriented data platform for **NYC Taxi Trip Data** using a layered architecture:

- **Bronze layer**: download raw parquet files from public source, apply skip/self-heal logic, store to MinIO, and log ingestion metadata into PostgreSQL.
- **Silver layer**: read Bronze data with Spark, normalize schema + clean/filter data, validate quality with Great Expectations, write curated parquet to MinIO partitioned by `year/month`, and log run/file/DQ metadata.
- **Gold layer**: build dimensional model + fact table on Iceberg (Nessie catalog), apply incremental-safe write strategies, and log gold run metadata for observability.

Main goals:

- Automate orchestration with Airflow.
- Keep ingestion/transform status traceable by each period.
- Support incremental processing and operational visibility via Prometheus + Grafana.
- Expose analytics-ready Gold data via Trino and BI dashboards via Superset.

---

## 2) High-level architecture

Core system components:

- **Airflow**: workflow orchestration (DAG, scheduler, worker, web UI).
- **MinIO (S3-compatible)**: object storage for Bronze/Silver data and Iceberg warehouse files for Gold.
- **PostgreSQL**: stores Airflow metadata + ingestion/transform/DQ metadata tables.
- **Spark (PySpark)**: performs Bronze-to-Silver and Silver-to-Gold transformations.
- **Great Expectations (GE)**: validates Silver data quality before write.
- **Iceberg + Nessie**: Gold table format + catalog/versioned metadata.
- **Trino**: SQL query engine serving Gold analytical datasets.
- **Superset**: BI & dashboard layer connected to Trino.
- **Prometheus + Grafana + exporters**: monitoring and observability.

Related architecture files:

- `Architecture/System Architecture.drawio.png`
- `Architecture/Data Model.png`

---

## 3) End-to-end data flow

### Step A - Bronze ingestion

Data source:
- Base URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- Example filename: `{service_type}_tripdata_{year}-{month}.parquet`

Process (`etls/bronze_ingestion.py`):
1. Load configuration from `config/config.conf`.
2. Resolve service types (yellow/green).
3. Apply skip logic:
   - Existing SUCCESS ingestion metadata for the same period -> SKIPPED.
   - Existing object in MinIO for the same period -> SKIPPED.
4. Download parquet file to local mounted path.
5. Upload file to MinIO bucket for the target layer (default Bronze).
6. Write ingestion log to PostgreSQL (`metadata.ingestion_log`): SUCCESS/SKIPPED/FAILED.
7. Clean up local files based on retention settings (`[cleanup] retention_hours`).

### Step B - Silver transform

Process (`etls/silver_transform.py`):
1. Discover Bronze objects in MinIO and map object key metadata (`service_type/year/month`).
2. Join discovered objects with latest successful Bronze ingestion metadata.
3. Determine run mode:
   - **FULL**: if no successful Silver file history exists.
   - **INCREMENTAL**: process only Bronze object keys that are not yet marked as SUCCESS in Silver logs.
4. Apply robustness guards:
   - Skip when no new Bronze objects are discovered.
   - Skip/backfill when a Silver partition already exists in MinIO but logs are out-of-sync.
5. Read Bronze parquet with Spark (yellow + green), grouped by period to reduce deep union-tree risk.
6. Apply centralized schema normalization (`_normalize_taxi_schema`) for timestamp/ID/amount fields.
7. Apply cleaning/quality filters (null checks, range checks, logical time checks, source period checks).
8. Validate transformed data via Great Expectations (`silver_taxi_suite`).
9. Write Silver parquet to:
   - `s3a://silver/silver/taxi_trips/`
   - Partitioned by `year/month`
   - Dynamic partition overwrite, Snappy compression.
10. Log metadata into:
   - `metadata.silver_transform_log`
   - `metadata.data_quality_log`
   - `metadata.silver_file_log`

### Step C - Gold transform

Process (`etls/gold_transform.py`):
1. Early skip if `(year, month)` already has SUCCESS status in `metadata.gold_transform_log`.
2. Build Spark session for Gold + Iceberg/Nessie catalog configuration.
3. Ensure namespace `gold.gold` exists (so Trino can discover the schema correctly).
4. Read Silver using partition-aware paths (prefer pruning by `year/month`).
5. Build dimension tables:
   - `dim_vendor`, `dim_service_type`, `dim_payment`, `dim_ratecode`,
   - `dim_trip_type`, `dim_location`, `dim_date`.
6. Build `fact_trips`:
   - Join Silver with dimensions (broadcast joins for small dimension tables),
   - Generate deterministic `trip_hash` for safe deduplication.
7. Write Gold tables to Iceberg:
   - Static dimensions: overwrite (parallelized writes to reduce wall time),
   - `dim_date`: append-only/left-anti strategy to preserve history,
   - `fact_trips`: two-phase strategy (main partition overwrite + anomalous append dedup by `trip_hash`).
8. Reliability fallback for stale metadata/Nessie conflicts:
   - try `DROP TABLE` via Spark SQL,
   - fallback REST API to Nessie,
   - refresh table cache rồi retry write.
9. Log run metadata into `metadata.gold_transform_log`.

---

## 4) DAG orchestration

Main DAG file: `dags/taxi_dag.py`

- **DAG ID**: `nyc_taxi_pipeline_v1.9.4`
- **Schedule**: `@monthly`
- **catchup**: `True`
- **max_active_runs**: `1`
- Task flow:
  1. `ingest_taxi_to_bronze`
  2. `transform_bronze_to_silver`
  3. `transform_silver_to_gold_iceberg` (if enabled in the current DAG version)

### How the DAG determines the target period

Inside `run_bronze_ingestion`:
- If `dag_run.conf` includes `year` and `month` -> use those values.
- Otherwise -> use Airflow `logical_date`.
- If still unavailable -> infer next period from ingestion metadata.

`force_reload` is also supported through `dag_run.conf`.

Example manual trigger config:
```json
{
  "year": 2024,
  "month": 10,
  "force_reload": false
}
```

### How to clear task
```bash

docker compose exec airflow-webserver airflow tasks clear ml_inference_dag.v3.1 --yes
```

---

## 5) Important repository structure

```text
config/
  config.conf                  # centralized runtime configuration
  sql/init_metadata.sql        # metadata initialization SQL

dags/
  taxi_dag.py                  # main Airflow DAG

etls/
  bronze_ingestion.py          # source -> bronze ingestion + metadata logging + skip/self-heal
  silver_transform.py          # bronze -> silver via Spark + GE + incremental/file-log controls
  gold_transform.py            # silver -> gold dim/fact on Iceberg/Nessie + resilient write strategy

pipelines/
  taxi_pipeline.py             # callable helper pipeline (non-DAG usage)

scripts/
  read_minio_parquet.py        # utility to list/read parquet directly from MinIO

great_expectations/
  expectations/silver_taxi_suite.json
  checkpoints/silver_taxi_checkpoint.yml

monitoring/
  prometheus/...
  grafana/...
```

---

## 6) Key configuration (`config/config.conf`)

Critical sections:

- `[database]`
  - host, port, db name, username, password
  - metadata schema + log table names
- `[download]`
  - `base_url`, `time_out`
- `[taxi_type]`
  - `service_type = ['Yellow', 'Green']`
- `[minio]`
  - endpoint, access key, secret key
  - bronze/silver/gold bucket names
- `[cleanup]`
  - enable/disable local cleanup
  - retention in hours (currently `6`)

Notes:
- Inside Docker network, MinIO endpoint is typically `minio:9000`.
- From host machine, it is usually exposed as `localhost:9010`.

---

## 7) Metadata tables and status semantics

### Bronze ingestion log
Table: `metadata.ingestion_log`  
Purpose: track ingestion state by `(layer, service_type, year, month)`.

Common statuses:
- `SUCCESS`: ingestion completed successfully.
- `SKIPPED`: skipped due to existing data/metadata or source not yet available.
- `FAILED`: error occurred during download/upload/logging.

### Silver transform log
Table: `metadata.silver_transform_log`  
Stores:
- run_id, pipeline_name, load_mode (FULL/INCREMENTAL),
- Spark app information,
- input/output row counts,
- GE validation summary.

### Data quality log
Table: `metadata.data_quality_log`  
Stores GE validation details:
- expectation totals,
- pass/fail counts,
- full JSON validation result.

### Silver file log
Table: `metadata.silver_file_log`  
Tracks per-bronze-object processing state and supports incremental behavior.

### Gold transform log
Table: `metadata.gold_transform_log`  
Stores:
- run_id, pipeline_name, target year/month,
- status (SUCCESS/FAILED/SKIPPED pattern),
- silver_rows/fact_rows (best-effort metadata),
- started/finished timestamps,
- spark_app_id and error context.

---

## 8) Local run guide with Docker Compose

File: `docker-compose.yml`

Main services:
- `postgres`, `redis`, `minio`, `minio-init`
- `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `airflow-worker`
- `prometheus`, `grafana`, and exporters

### Suggested startup steps

1. Build image:
```bash
docker compose build
```

2. Initialize Airflow DB + create admin user:
```bash
docker compose up airflow-init
```
3. Start full stack:
```bash
docker compose up -d
```

4. Access UIs:
- Airflow: http://localhost:8080 (admin/admin)
- MinIO Console: http://localhost:9011
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

---

## 9) Inspect parquet data in MinIO

Utility script: `scripts/read_minio_parquet.py`

Examples:

List parquet objects in Silver bucket:
```bash
python scripts/read_minio_parquet.py --bucket silver --prefix silver/taxi_trips/ --list-only
```

Read latest parquet under a prefix:
```bash
python scripts/read_minio_parquet.py --bucket silver --prefix silver/taxi_trips/year=2024/ --latest --rows 20
```

Read a specific object key:
```bash
python scripts/read_minio_parquet.py --bucket bronze --object-key bronze/yellow/year=2024/month=10/data.parquet --rows 10


```

---

## 10) Monitoring, query serving and BI

### Monitoring and alerting

Monitoring stack:
- Prometheus scrapes metrics from statsd-exporter, node-exporter, cadvisor, postgres-exporter, and redis-exporter.
- Grafana dashboard file:
  - `monitoring/grafana/dashboards/airflow_pipeline_overview.json`

Alert rules:
- `monitoring/prometheus/rules/airflow_alerts.yml`

### Query serving (Trino)
- Trino đọc Gold Iceberg tables để phục vụ truy vấn phân tích.
- Tuning đã áp dụng:
  - memory: `query.max-memory`, `query.max-memory-per-node`, `query.max-total-memory`
  - execution/concurrency: `task.concurrency`, `task.max-worker-threads`, `query.max-hash-partition-count`
  - stability: `query.low-memory-killer.policy`, spill-to-disk, optimizer settings.
- JVM heap Trino tăng lên `-Xmx6G` để giảm spill và tăng tốc chart queries lớn.

#### Optimize table in iceberg
```bash
ALTER TABLE iceberg.gold.fact_trips EXECUTE optimize(file_size_threshold => '256MB');

```

### BI layer (Superset)
- Superset kết nối Trino cho SQL Lab và dashboards.
- Đã bật Redis-backed caches (metadata/data/filter/explore) để cache dùng chung giữa workers.
- Timeout SQL Lab/Webserver được tăng (`300s`) để phù hợp workload truy vấn nhiều năm dữ liệu taxi.

---

## 11) Common issues and quick troubleshooting

1. **Cannot access MinIO from host machine**
   - Use `localhost:9010` instead of `minio:9000`.

2. **Spark errors while reading parquet with schema drift**
   - The project already enables:
     - `spark.sql.parquet.mergeSchema=true`
     - `spark.sql.parquet.enableVectorizedReader=false`
   - Verify source parquet files for corruption.

3. **Great Expectations validation failure**
   - Check `data_quality_log.validation_result` for details.
   - Validate required fields: pickup/dropoff timestamps, trip_distance, service_type, year/month.

4. **DAG runs but many tasks are skipped**
   - Check `ingestion_log` for prior SUCCESS records.
   - Trigger with `force_reload=true` if re-ingestion is required.

5. **No new Silver output appears**
   - Check `silver_file_log` to confirm Bronze objects are marked SUCCESS.
   - Verify FULL vs INCREMENTAL mode and target `year/month` partitions.

6. **Gold write fails with Iceberg/Nessie metadata errors**
   - Triệu chứng thường gặp: `NotFoundException` / stale metadata chain.
   - Pipeline đã có fallback logic (DROP + Nessie REST API + cache refresh), nhưng vẫn cần:
     - kiểm tra trạng thái Nessie,
     - xác minh MinIO warehouse path,
     - soát log gold transform trong `metadata.gold_transform_log`.

7. **Superset dashboard query chậm/timeout**
   - Verify Trino health + memory config + JVM heap.
   - Verify Superset Redis cache hoạt động (cache miss/hit pattern).
   - Xác nhận timeout đồng bộ: Superset SQL Lab / gunicorn / Trino execution guard.

---

## 12) What to provide so others can fully understand this project

Quick checklist:

- [ ] Business objective + data scope (yellow/green, year range).
- [ ] Architecture diagram and each service responsibility.
- [ ] Bronze -> Silver flow with skip/reload logic.
- [ ] Current runtime configuration (`config.conf`).
- [ ] End-to-end local run instructions via Docker Compose.
- [ ] DAG trigger examples and `run_conf` usage.
- [ ] How to validate MinIO data + PostgreSQL metadata.
- [ ] Data quality rules (Great Expectations).
- [ ] Monitoring and alerting references.
- [ ] Known issues + runbook.

---

## 13) Recent updates (code logic & technology)

### Bronze ingestion - code logic highlights
- Multi-service ingestion (`yellow/green`) driven by config.
- Skip logic an toàn:
  - skip khi đã có SUCCESS metadata + object tồn tại,
  - skip khi object đã có dù thiếu metadata,
  - self-heal re-ingest khi metadata SUCCESS nhưng object bị mất.
- Metadata-driven next-period inference.
- Local parquet cleanup theo retention policy.
- Full ingestion logging (`SUCCESS/SKIPPED/FAILED`) vào `metadata.ingestion_log`.

### Silver transform - data processing method
- Discover Bronze objects từ MinIO rồi map theo metadata database.
- Full/Incremental mode bằng `silver_file_log`.
- Group-by-period reading để tránh deep union tree gây JVM stack overflow.
- Schema normalization tập trung + business/data quality filters.
- GE validation trước khi write.
- Partitioned Silver output (`year/month`) + dynamic overwrite.
- Log đầy đủ run/file/DQ để audit và vận hành.

### Gold transform - data processing method
- Read Silver theo partition-pruning path để giảm IO.
- Build star-schema-like model:
  - dimensions từ static lookups + date/location/service/payment/...
  - fact table `fact_trips` joined với dims.
- Dùng `trip_hash` để dedup idempotent và xử lý anomalous rows an toàn.
- Two-phase fact write tránh overwrite nhầm partitions ngoài scope run.
- Resilient Iceberg/Nessie handling khi gặp stale metadata conflicts.
- Logging gold run metadata cho observability và troubleshooting.

### New technologies integrated
- **Iceberg + Nessie** for Gold table format and catalog/versioned metadata.
- **Trino** as SQL serving layer on top of Iceberg.
- **Superset** as BI layer with Redis shared cache and tuned SQL timeout.
- **Redis** as distributed cache backend for Superset workers.

---

## 14) Quick onboarding suggestion for new contributors

1. Read this document first.
2. Open Airflow UI and manually trigger one period.
3. Verify new objects in Bronze/Silver buckets on MinIO.
4. Query metadata tables to understand pipeline status transitions.
5. Open Grafana dashboard and observe pipeline metrics.

This file is designed as an onboarding document and can be used as the foundation for a formal `README.md`.
