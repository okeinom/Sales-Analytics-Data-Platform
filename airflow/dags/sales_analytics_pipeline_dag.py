from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "okeino",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sales_analytics_batch_pipeline",
    description="Stub DAG: runs Sales Analytics batch pipeline end-to-end",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # change to None if you only want manual runs
    catchup=False,
    tags=["portfolio", "batch", "sales-analytics"],
) as dag:
    run_pipeline = BashOperator(
        task_id="run_sales_analytics_pipeline",
        bash_command="""
        set -e
        cd "{{ var.value.get('sales_analytics_repo_path', '.') }}"
        . .venv/Scripts/activate || true
        python -m sales_analytics.load.run_load
        """,
    )

    run_pipeline
