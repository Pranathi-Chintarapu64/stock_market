from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.fetcher.data_fetcher import run_pipeline

DEFAULT_ARGS = {
    "owner": "pranathi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

schedule = os.getenv("SCHEDULE_INTERVAL", "@daily")

with DAG(
    dag_id="stock_market_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock market data and upsert into Postgres",
    schedule_interval=schedule,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    run_etl = PythonOperator(
        task_id="run_stock_etl",
        python_callable=run_pipeline,
        op_kwargs={},
    )

    run_etl
