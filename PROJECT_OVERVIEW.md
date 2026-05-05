# NYC Taxi Data Lakehouse Platform: Project Overview

## 1. Introduction
This project is an end-to-end, production-grade Data Lakehouse platform designed to process, analyze, and predict trends for the **NYC Taxi & Limousine Commission (TLC)** dataset. 

By leveraging the **Medallion Architecture**, the platform transforms raw, messy parquet files into high-performance, versioned tables for BI analytics and Machine Learning forecasting.

### Core Objectives:
- **Scalability**: Handle years of taxi data using Apache Spark.
- **Reliability**: Implement metadata-driven ingestion with self-healing and skip logic.
- **Quality**: Enforce strict data quality guards via Great Expectations.
- **Analytics-Ready**: Provide a SQL-interface (Trino) and BI Dashboards (Superset).
- **ML-Powered**: Predict monthly demand using XGBoost.

---

## 2. Infrastructure Stack (The "Lakehouse")
The project uses a modern open-source stack orchestrated via Docker:

- **Storage**: **MinIO** (S3-compatible) serving as the physical storage layer.
- **Orchestration**: **Apache Airflow** managing the end-to-end DAG logic.
- **Compute**: **PySpark** for distributed data processing and transformations.
- **Quality Assurance**: **Great Expectations** for automated data validation at the Silver layer.
- **Table Format**: **Apache Iceberg** (on **Nessie** catalog) for the Gold layer, enabling ACID transactions and time travel.
- **Query Engine**: **Trino** for fast, distributed SQL queries on top of Iceberg.
- **Visualization**: **Apache Superset** for building interactive BI dashboards.
- **Monitoring**: **Prometheus & Grafana** for real-time observability.

---

## 3. Data Pipeline Architecture (Medallion Layers)

The data flows through three distinct stages, each logged and tracked in a **PostgreSQL metadata database**.

### 🥉 Bronze Layer: Raw Ingestion
- **Source**: NYC TLC Parquet files (`Yellow` & `Green` taxi).
- **Process**: `etls/bronze_ingestion.py`
- **Logic**: Downloads files, performs integrity checks, and uploads to MinIO.
- **Key Feature**: **Metadata-driven skip logic**. If a file is already processed or the source is unavailable, the system intelligently skips or logs the failure without breaking the pipeline.

### 🥈 Silver Layer: Cleaned & Validated
- **Process**: `etls/silver_transform.py`
- **Transforms**: Handles schema normalization (fixing INT/BIGINT drifts), data cleaning (removing invalid trips), and feature enrichment.
- **Quality Guard**: Runs **Great Expectations** suites. If data doesn't meet quality standards (e.g., negative fare amounts), it's caught before hitting the analytics layer.
- **Mode**: Supports **Incremental** (delta only) and **Full** refreshes.

### 🥇 Gold Layer: Analytical Modeling
- **Process**: `etls/gold_transform.py`
- **Architecture**: Transforms flattened Silver data into a **Star Schema** (Dimension and Fact tables).
- **Tables**: `fact_trips`, `dim_vendor`, `dim_location`, `dim_date`, etc.
- **Technology**: Built on **Apache Iceberg**. This ensures that complex joins are fast and BI tools see a consistent view of the world.

---

## 4. Machine Learning Integration
Beyond ETL, the platform includes a dedicated ML workspace:
- **Training**: `ml_lab/train_xgboost.py` trains models to predict taxi demand.
- **Inference**: `ml_lab/predict_monthly_demand.py` generates monthly forecasts based on Gold layer history.
- **Evaluation**: Comprehensive metrics (RMSE, MAE) and visualizations are stored for model auditing.

---

## 5. Repository Guide: Mapping Files to Functions

To understand the codebase, follow this structural map:

| Directory/File | Responsibility |
| :--- | :--- |
| **`dags/taxi_dag.py`** | The "Brain" - defines the monthly Airflow pipeline. |
| **`etls/`** | The "Muscle" - contains core Spark processing logic for each layer. |
| **`config/config.conf`** | The "Control Center" - handles all paths, credentials, and parameters. |
| **`great_expectations/`** | The "Guard" - contains data quality rules and checkpoint configs. |
| **`superset/`** | The "Face" - configuration and setup for BI dashboards. |
| **`monitoring/`** | The "Eyes" - Prometheus rules and Grafana dashboard templates. |


---

## 6. Operational Visibility
The system records every run into specialized metadata tables:
- `metadata.ingestion_log`: Tracks Bronze source status.
- `metadata.silver_file_log`: Tracks files processed for incremental logic.
- `metadata.data_quality_log`: Stores detailed validation results.
- `metadata.gold_transform_log`: Monitors Gold layer refresh health.

---

## 7. How to Get Started
1. **Infrastructure**: Build and start services using `docker compose up -d`.
2. **Setup**: Initialize Airflow metadata and admin user via `airflow-init`.
3. **Orchestrate**: Access the Airflow UI (`localhost:8080`) and trigger the `nyc_taxi_pipeline` DAG.
4. **Analyze**: Connect Trino or Superset to the Iceberg catalog to explore Gold data.

---
