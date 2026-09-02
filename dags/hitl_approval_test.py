from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import DAG
from pendulum import datetime


with DAG(
        dag_id="hitl_approval_test",
        description="Validate Airflow 3.1 Human-in-the-Loop approval",
        start_date=datetime(2026, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=["hackathon", "hitl", "test"],
) as dag:

    failure_detected = EmptyOperator(
        task_id="failure_detected",
    )

    approve_recovery = ApprovalOperator(
        task_id="approve_recovery",
        subject="Approve automated pipeline recovery",
        body="""
## Pipeline failure detected

**Pipeline:** Daily Sales Pipeline  
**Failed task:** Load Sales Data  
**Error:** Database connection timeout  
**Proposed fix:** Reset the connection and rerun the failed task

Please approve or reject the proposed recovery.
        """,
    )

    apply_fix = EmptyOperator(
        task_id="apply_fix",
    )

    rerun_pipeline = EmptyOperator(
        task_id="rerun_pipeline",
    )

    recovery_completed = EmptyOperator(
        task_id="recovery_completed",
    )

    (
            failure_detected
            >> approve_recovery
            >> apply_fix
            >> rerun_pipeline
            >> recovery_completed
    )