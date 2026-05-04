from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


def run_bronze_ingestion(**context):
    # Lazy import to keep DAG parse fast; these modules pull heavy dependencies.
    from etls.bronze_ingestion import ingest_taxi_to_layer, infer_next_ingestion_period_from_metadata

    dag_run = context.get("dag_run")
    run_conf = dag_run.conf if dag_run and dag_run.conf else {}

    if "year" in run_conf and "month" in run_conf:
        year = int(run_conf["year"])
        month = int(run_conf["month"])
    else:
        logical_date = context.get("logical_date")
        if logical_date is not None:
            year = logical_date.year
            month = logical_date.month
        else:
            year, month = infer_next_ingestion_period_from_metadata(layer="bronze")

    force_reload = bool(run_conf.get("force_reload", False))

    ingestion_result = ingest_taxi_to_layer(
        year=year,
        month=month,
        layer="bronze",
        pipeline_name="nyc_taxi_bronze_ingestion",
        force_reload=force_reload,
    )
    return {
        "year": year,
        "month": month,
        "force_reload": force_reload,
        "ingestion_result": ingestion_result,
    }


def run_silver_layer(**context):
    # Lazy import to keep DAG parse fast; silver transform imports Spark + Great Expectations.
    from etls.silver_transform import run_silver_transform

    dag_run = context.get("dag_run")
    run_conf = dag_run.conf if dag_run and dag_run.conf else {}
    logical_date = context.get("logical_date")

    if "year" in run_conf and "month" in run_conf:
        year = int(run_conf["year"])
        month = int(run_conf["month"])
    elif logical_date is not None:
        year = logical_date.year
        month = logical_date.month
    else:
        year = None
        month = None

    run_silver_transform(
        pipeline_name="nyc_taxi_silver_transform",
        year=year,
        month=month,
    )

def run_gold_layer_iceberg(**context):
    from etls.gold_transform import run_gold_transform
    

    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    params = context.get("params") or {}
    logical_date = context.get("logical_date")

 
    year = dag_run_conf.get("year") or (logical_date.year if logical_date else None) or params.get("year")
    month = dag_run_conf.get("month") or (logical_date.month if logical_date else None) or params.get("month")


    if year is None or month is None:
        from etls.bronze_ingestion import _get_latest_successful_ingestion
        fallback_year, fallback_month = _get_latest_successful_ingestion(layer="bronze")
        year = year or fallback_year
        month = month or fallback_month


    if year and month:
        run_gold_transform(year=int(year), month=int(month))
    else:
        raise ValueError(f"Could not determine year and month. Got year={year}, month={month}")


def run_ml_feature_engineering_job(**context):
    from etls.ml_feature_engineering import run_ml_feature_engineering

    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    params = context.get("params") or {}
    logical_date = context.get("logical_date")

    year = dag_run_conf.get("year") or (logical_date.year if logical_date else None) or params.get("year")
    month = dag_run_conf.get("month") or (logical_date.month if logical_date else None) or params.get("month")

    if year is None or month is None:
        from etls.bronze_ingestion import _get_latest_successful_ingestion
        fallback_year, fallback_month = _get_latest_successful_ingestion(layer="bronze")
        year = year or fallback_year
        month = month or fallback_month

    if year and month:
        run_ml_feature_engineering(
            pipeline_name="nyc_taxi_ml_features",
            year=int(year),
            month=int(month)
        )
    else:
        raise ValueError(f"ML Feature Engineering could not determine year and month.")


with DAG(
    dag_id="nyc_taxi_pipeline_v2.5",
    start_date=datetime(2021, 1, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["nyc","taxi", "pipeline", "bronze", "silver", "gold", "iceberg", "ml"],
) as dag:
    bronze_ingestion_task = PythonOperator(
        task_id="ingest_taxi_to_bronze",
        python_callable=run_bronze_ingestion,
        execution_timeout=timedelta(hours=1),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    silver_transform_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=run_silver_layer,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=timedelta(hours=3),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    gold_transform_task = PythonOperator(
        task_id="transform_silver_to_gold_iceberg",
        python_callable=run_gold_layer_iceberg,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=timedelta(hours=2),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    ml_feature_task = PythonOperator(
        task_id="transform_silver_to_ml_features",
        python_callable=run_ml_feature_engineering_job,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=timedelta(hours=2),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    bronze_ingestion_task >> silver_transform_task
    silver_transform_task >> [gold_transform_task, ml_feature_task]
