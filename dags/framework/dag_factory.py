import json
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.sdk import DAG, task
from pendulum import datetime


def create_recovery_dag(config: dict[str, Any]) -> DAG:
    team_config = config["team"]
    dag_config = config["dag"]
    pipeline_config = config["pipeline"]
    kedb_config = config["kedb"]
    hitl_config = config["hitl"]
    remediation_config = config["remediation"]
    incident_config = config["incident"]

    hitl_options = [
        action["task_id"]
        for action in hitl_config["actions"]
    ]

    hitl_action_descriptions = "\n".join(
        f"- `{action['task_id']}`: {action['label']}"
        for action in hitl_config["actions"]
    )

    with DAG(
            dag_id=dag_config["dag_id"],
            description=dag_config.get("description"),
            start_date=datetime(2026, 1, 1, tz="UTC"),
            schedule=dag_config.get("schedule"),
            catchup=False,
            tags=sorted(dag_config.get("tags", [])),
    ) as dag:

        @task(task_id="execute_pipeline")
        def execute_pipeline() -> dict[str, Any]:
            steps = pipeline_config["steps"]
            simulation = pipeline_config["simulation"]
            outcome = simulation["outcome"]

            print(
                f"Starting pipeline '{pipeline_config['name']}' "
                f"for team '{team_config['name']}'"
            )

            for step_name in steps:
                print(f"Executing pipeline step: {step_name}")

                if (
                        outcome == "failed"
                        and step_name == simulation.get("failed_step")
                ):
                    result = {
                        "team_id": team_config["id"],
                        "team_name": team_config["name"],
                        "pipeline_name": pipeline_config["name"],
                        "status": "failed",
                        "completed_steps": [
                            item
                            for item in steps
                            if steps.index(item) < steps.index(step_name)
                        ],
                        "failed_step": step_name,
                        "error_code": simulation["error_code"],
                        "error_message": simulation["error_message"],
                        "business_impact": pipeline_config[
                            "business_impact"
                        ],
                        "sla_minutes_remaining": pipeline_config[
                            "sla_minutes_remaining"
                        ],
                    }

                    print(f"Pipeline execution failed: {result}")
                    return result

            result = {
                "team_id": team_config["id"],
                "team_name": team_config["name"],
                "pipeline_name": pipeline_config["name"],
                "status": "success",
                "completed_steps": steps,
                "failed_step": None,
                "error_code": None,
                "error_message": None,
                "business_impact": None,
                "sla_minutes_remaining": pipeline_config[
                    "sla_minutes_remaining"
                ],
            }

            print(f"Pipeline execution succeeded: {result}")
            return result

        @task.branch(task_id="evaluate_pipeline_result")
        def evaluate_pipeline_result(
                pipeline_result: dict[str, Any],
        ) -> str:
            if pipeline_result["status"] == "failed":
                print(
                    "Pipeline failed. Routing execution to KEDB lookup."
                )
                return "search_kedb"

            print(
                "Pipeline succeeded. Recovery workflow is not required."
            )
            return "pipeline_completed"

        @task(task_id="pipeline_completed")
        def pipeline_completed(
                pipeline_result: dict[str, Any],
        ) -> dict[str, Any]:
            result = {
                "pipeline_name": pipeline_result["pipeline_name"],
                "status": "completed",
                "recovery_required": False,
            }

            print(f"Pipeline completed successfully: {result}")
            return result

        @task(task_id="search_kedb")
        def search_kedb(
                pipeline_result: dict[str, Any],
        ) -> dict[str, Any]:
            kedb_path = Path(kedb_config["location"])

            if not kedb_path.exists():
                raise FileNotFoundError(
                    f"KEDB file does not exist: {kedb_path}"
                )

            with kedb_path.open("r", encoding="utf-8") as kedb_file:
                known_errors = json.load(kedb_file)

            error_code = pipeline_result["error_code"]
            matched_error = known_errors.get(error_code)

            if matched_error is None:
                result = {
                    **pipeline_result,
                    "known_error": False,
                    "category": "Unknown",
                    "probable_cause": (
                        "No matching entry was found in the team KEDB"
                    ),
                    "recommended_fix": (
                        "Manual investigation is required"
                    ),
                    "risk_level": "Unknown",
                    "historical_success_rate": 0,
                }
            else:
                result = {
                    **pipeline_result,
                    **matched_error,
                }

            print(f"KEDB lookup result: {result}")
            return result

        pipeline_result = execute_pipeline()

        selected_path = evaluate_pipeline_result(
            pipeline_result
        )

        success_result = pipeline_completed(
            pipeline_result
        )

        kedb_result = search_kedb(
            pipeline_result
        )

        choose_recovery_action = HITLBranchOperator(
            task_id="choose_recovery_action",
            subject=hitl_config["subject"],
            body=f"""
## {hitl_config["title"]}

**Team:**  
`{{{{ ti.xcom_pull(task_ids='search_kedb')['team_name'] }}}}`

**Pipeline:**  
`{{{{ ti.xcom_pull(task_ids='search_kedb')['pipeline_name'] }}}}`

**Failed pipeline step:**  
`{{{{ ti.xcom_pull(task_ids='search_kedb')['failed_step'] }}}}`

**Error code:**  
`{{{{ ti.xcom_pull(task_ids='search_kedb')['error_code'] }}}}`

**Error message:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['error_message'] }}}}

**KEDB match found:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['known_error'] }}}}

**Probable cause:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['probable_cause'] }}}}

**Recommended remediation:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['recommended_fix'] }}}}

**Risk level:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['risk_level'] }}}}

**Historical success rate:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['historical_success_rate'] }}}}%

**Business impact:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['business_impact'] }}}}

**SLA remaining:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['sla_minutes_remaining'] }}}} minutes

### Available actions

{hitl_action_descriptions}

Select how Airflow should proceed.
            """,
            options=hitl_options,
        )

        @task(task_id="apply_fix")
        def apply_fix(
                kedb_match: dict[str, Any],
        ) -> dict[str, Any]:
            remediation_action = remediation_config["action"]

            print(
                f"Applying approved remediation "
                f"'{remediation_action}' for pipeline "
                f"'{kedb_match['pipeline_name']}'"
            )

            return {
                "pipeline_name": kedb_match["pipeline_name"],
                "fix_applied": True,
                "remediation_action": remediation_action,
                "approved_by_human": True,
            }

        @task(task_id="rerun_pipeline")
        def rerun_pipeline(
                remediation_result: dict[str, Any],
        ) -> dict[str, Any]:
            if not remediation_result["fix_applied"]:
                raise RuntimeError(
                    "The approved remediation was not applied"
                )

            rerun_outcome = remediation_config.get(
                "rerun_outcome",
                "success",
            )

            print(
                f"Rerunning pipeline "
                f"'{remediation_result['pipeline_name']}' after "
                f"'{remediation_result['remediation_action']}'"
            )

            if rerun_outcome == "failed":
                return {
                    **remediation_result,
                    "rerun_status": "failed",
                    "message": (
                        "Pipeline continued to fail after remediation"
                    ),
                }

            return {
                **remediation_result,
                "rerun_status": "success",
                "message": (
                    "Pipeline recovered successfully after remediation"
                ),
            }

        @task(task_id="recovery_completed")
        def recovery_completed(
                rerun_result: dict[str, Any],
        ) -> dict[str, Any]:
            if rerun_result["rerun_status"] != "success":
                raise RuntimeError(
                    "Pipeline recovery was not successful"
                )

            result = {
                "pipeline_name": rerun_result["pipeline_name"],
                "status": "recovered",
                "remediation_action": rerun_result[
                    "remediation_action"
                ],
            }

            print(f"Recovery completed: {result}")
            return result

        @task(task_id="create_incident")
        def create_incident(
                kedb_match: dict[str, Any],
        ) -> dict[str, Any]:
            if not incident_config.get("enabled", False):
                return {
                    "status": "disabled",
                    "pipeline_name": kedb_match["pipeline_name"],
                }

            incident = {
                "incident_id": (
                    f"MOCK-{team_config['id'].upper()}-0001"
                ),
                "status": "created",
                "provider": incident_config["provider"],
                "priority": incident_config["priority"],
                "assignment_group": incident_config[
                    "assignment_group"
                ],
                "team_name": kedb_match["team_name"],
                "pipeline_name": kedb_match["pipeline_name"],
                "failed_step": kedb_match["failed_step"],
                "error_code": kedb_match["error_code"],
                "error_message": kedb_match["error_message"],
                "business_impact": kedb_match["business_impact"],
            }

            print(f"Mock incident created: {incident}")
            return incident

        @task(task_id="manual_investigation")
        def manual_investigation(
                kedb_match: dict[str, Any],
        ) -> dict[str, Any]:
            investigation = {
                "status": "manual_investigation_requested",
                "team_id": kedb_match["team_id"],
                "team_name": kedb_match["team_name"],
                "pipeline_name": kedb_match["pipeline_name"],
                "error_code": kedb_match["error_code"],
                "automated_action_taken": False,
            }

            print(
                f"Manual investigation requested: {investigation}"
            )
            return investigation

        fix_result = apply_fix(kedb_result)
        pipeline_rerun_result = rerun_pipeline(fix_result)
        recovered_result = recovery_completed(
            pipeline_rerun_result
        )

        incident_result = create_incident(
            kedb_result
        )

        investigation_result = manual_investigation(
            kedb_result
        )

        selected_path >> success_result
        selected_path >> kedb_result

        kedb_result >> choose_recovery_action

        choose_recovery_action >> fix_result
        choose_recovery_action >> incident_result
        choose_recovery_action >> investigation_result

    return dag