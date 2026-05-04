from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ml_inference_dag.v3.1",
    description="DAG to run ML inference for monthly demand prediction",
    # schedule_interval="0 6 1 * *",  # Run on the 1st of every month at 6:00 AM
    start_date=datetime(2026, 3, 7),
    catchup=True,
) as dag:
    predict_monthly_batch = BashOperator(
        task_id="predict_monthly_batch",
        bash_command="""cd /opt/airflow && python ml_lab/predict_monthly_demand.py \\
  --year={{ dag_run.conf.get('year', execution_date.year) | int }} \\
  --month={{ dag_run.conf.get('month', execution_date.month) | int }} \\
  --auto-next""",
    )

#     validate_trino_read = BashOperator(
#         task_id="validate_trino_read",
#         bash_command="""docker exec trino trino --server http://trino:8085 --execute "
# SELECT prediction_year, prediction_month, count(*) AS row_count
# FROM iceberg.ml.predictions.monthly_demand_forecast
# GROUP BY 1,2
# ORDER BY 1 DESC, 2 DESC
# LIMIT 12;" """,
#     )

    predict_monthly_batch
