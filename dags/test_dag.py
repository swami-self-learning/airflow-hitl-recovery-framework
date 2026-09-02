from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
        dag_id="test_dag",
        start_date=datetime(2026, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
):
    EmptyOperator(task_id="start")