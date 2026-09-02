from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.sdk import DAG
from pendulum import datetime


with DAG(
        dag_id="hitl_branch_test",
        description="Validate human-driven pipeline recovery branching",
        start_date=datetime(2026, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=["hackathon", "hitl", "branch-test"],
) as dag:

    failure_detected = EmptyOperator(
        task_id="failure_detected",
    )

    choose_recovery_action = HITLBranchOperator(
        task_id="choose_recovery_action",
        subject="Choose pipeline recovery action",
        body="""
## Pipeline failure detected

**Pipeline:** Daily Sales Pipeline  
**Failed task:** Load Sales Data  
**Error code:** DB_CONNECTION_TIMEOUT  
**Error:** Database connection timed out after 30 seconds  
**Business impact:** Daily sales dashboard may be delayed  
**SLA remaining:** 45 minutes

**Known resolution:** Reset the database connection and rerun the failed task  
**Historical success rate:** 92%  
**Risk level:** Low

Choose how Airflow should proceed.
        """,
        options=[
            "apply_fix",
            "create_incident",
            "manual_investigation",
        ],
    )

    apply_fix = EmptyOperator(
        task_id="apply_fix",
    )

    rerun_pipeline = EmptyOperator(
        task_id="rerun_pipeline",
    )

    recovery_successful = EmptyOperator(
        task_id="recovery_successful",
    )

    create_incident = EmptyOperator(
        task_id="create_incident",
    )

    manual_investigation = EmptyOperator(
        task_id="manual_investigation",
    )

    failure_detected >> choose_recovery_action

    (
            choose_recovery_action
            >> apply_fix
            >> rerun_pipeline
            >> recovery_successful
    )

    choose_recovery_action >> create_incident
    choose_recovery_action >> manual_investigation