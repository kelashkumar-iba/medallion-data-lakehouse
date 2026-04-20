"""
Medallion weather pipeline — daily schedule.

Chain: bronze_ingest -> bronze_gate -> silver_transform -> silver_gate -> gold_transform

Every task is a BashOperator calling the same Python scripts we run manually.
Scripts read MINIO_* env vars — already injected via airflow-common-env in compose.
If any quality gate exits non-zero, downstream tasks are skipped.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "kelash",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="medallion_weather",
    default_args=default_args,
    description="Bronze -> GE gate -> Silver -> GE gate -> Gold (weather data)",
    start_date=datetime(2026, 4, 20),
    schedule="@daily",
    catchup=False,            # don't backfill historical dates
    max_active_runs=1,        # one run at a time — Silver/Gold read shared partitions
    tags=["medallion", "weather", "portfolio"],
) as dag:

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="python /opt/airflow/ingestion/bronze_ingest.py",
    )

    bronze_gate = BashOperator(
        task_id="bronze_gate",
        bash_command="python /opt/airflow/quality/bronze_expectations.py",
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/airflow/transforms/silver_transform.py",
    )

    silver_gate = BashOperator(
        task_id="silver_gate",
        bash_command="python /opt/airflow/quality/silver_expectations.py",
    )

    gold_transform = BashOperator(
        task_id="gold_transform",
        bash_command="python /opt/airflow/transforms/gold_transform.py",
    )

    bronze_ingest >> bronze_gate >> silver_transform >> silver_gate >> gold_transform
