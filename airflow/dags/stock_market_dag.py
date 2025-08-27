from datetime import datetime, timedelta
import os
import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DEFAULT_ARGS = {
    "owner": "pranathi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

SCHEDULE = os.getenv("SCHEDULE_INTERVAL", "@daily")

def run_pipeline_task(**context):
    logger = logging.getLogger("airflow.task")
    logger.info("Starting run_pipeline()")
    from src.fetcher.data_fetcher import run_pipeline

    try:
        result = run_pipeline()
        logger.info("run_pipeline finished, result: %s", result)
        return result
    except Exception as exc:
        logger.exception("run_pipeline failed")
        raise

with DAG(
    dag_id="stock_market_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock market data and upsert into Postgres",
    schedule=SCHEDULE,                # airflow 2.x style (env-overridable)
    start_date=datetime(2025, 8, 1),  # must be <= today to allow runs (adjust if needed)
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "etl"],
) as dag:

    insert_to_postgres = PythonOperator(
        task_id="insert_to_postgres",
        python_callable=run_pipeline_task,
        # Optional: override retries for this task only
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    insert_to_postgres
