import json
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.hitl import (
    HITLEntryOperator,
    HITLBranchOperator,
)
from airflow.sdk import DAG, Param, task
from pendulum import datetime


def create_recovery_dag(config: dict[str, Any]) -> DAG:
    """
    Create a team-specific pipeline recovery DAG from YAML configuration.

    Workflow:

    1. Execute a simulated data pipeline.
    2. Evaluate whether the pipeline succeeded or failed.
    3. Complete normally when the pipeline succeeds.
    4. Search the team's KEDB when the pipeline fails.
    5. Ask a human to select a recovery action.
    6. Apply remediation, create an incident, or collect manual
       investigation findings.
    """

    team_config = config["team"]
    dag_config = config["dag"]
    pipeline_config = config["pipeline"]
    kedb_config = config["kedb"]
    hitl_config = config["hitl"]
    remediation_config = config["remediation"]
    incident_config = config["incident"]

    # These task IDs must match the downstream branch task IDs.
    hitl_options = [
        action["task_id"]
        for action in hitl_config["actions"]
    ]

    # Display human-friendly action descriptions in the HITL screen.
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

        # ---------------------------------------------------------
        # 1. Execute the configured pipeline
        # ---------------------------------------------------------

        @task(task_id="execute_pipeline")
        def execute_pipeline() -> dict[str, Any]:
            """
            Simulate pipeline execution using values from team YAML.

            A failed operational result is returned instead of raising
            an Airflow exception. This allows the DAG to continue into
            the recovery workflow.
            """

            steps = pipeline_config["steps"]
            simulation = pipeline_config["simulation"]
            configured_outcome = simulation["outcome"]

            print(
                f"Starting pipeline '{pipeline_config['name']}' "
                f"for team '{team_config['name']}'"
            )

            completed_steps: list[str] = []

            for step_name in steps:
                print(f"Executing pipeline step: {step_name}")

                should_fail = (
                        configured_outcome == "failed"
                        and step_name == simulation.get("failed_step")
                )

                if should_fail:
                    result = {
                        "team_id": team_config["id"],
                        "team_name": team_config["name"],
                        "pipeline_name": pipeline_config["name"],
                        "status": "failed",
                        "completed_steps": completed_steps,
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

                completed_steps.append(step_name)

            result = {
                "team_id": team_config["id"],
                "team_name": team_config["name"],
                "pipeline_name": pipeline_config["name"],
                "status": "success",
                "completed_steps": completed_steps,
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

        # ---------------------------------------------------------
        # 2. Evaluate the pipeline outcome
        # ---------------------------------------------------------

        @task.branch(task_id="evaluate_pipeline_result")
        def evaluate_pipeline_result(
                pipeline_result: dict[str, Any],
        ) -> str:
            """
            Route successful pipelines to completion and failed
            pipelines to the KEDB lookup.
            """

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
            """
            Record normal pipeline completion.
            """

            result = {
                "team_id": pipeline_result["team_id"],
                "team_name": pipeline_result["team_name"],
                "pipeline_name": pipeline_result["pipeline_name"],
                "status": "completed",
                "recovery_required": False,
            }

            print(f"Pipeline completed successfully: {result}")
            return result

        # ---------------------------------------------------------
        # 3. Search the team's Known Error Database
        # ---------------------------------------------------------

        @task(task_id="search_kedb")
        def search_kedb(
                pipeline_result: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Search the team's local JSON KEDB using the error code.
            """

            kedb_path = Path(kedb_config["location"])

            if not kedb_path.exists():
                raise FileNotFoundError(
                    f"KEDB file does not exist: {kedb_path}"
                )

            with kedb_path.open(
                    "r",
                    encoding="utf-8",
            ) as kedb_file:
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

        # ---------------------------------------------------------
        # Instantiate pipeline execution tasks
        # ---------------------------------------------------------

        pipeline_result = execute_pipeline()

        selected_pipeline_path = evaluate_pipeline_result(
            pipeline_result
        )

        successful_pipeline_result = pipeline_completed(
            pipeline_result
        )

        kedb_result = search_kedb(
            pipeline_result
        )

        # ---------------------------------------------------------
        # 4. Human selects the recovery action
        # ---------------------------------------------------------

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

**Error category:**  
{{{{ ti.xcom_pull(task_ids='search_kedb')['category'] }}}}

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

### Available recovery actions

{hitl_action_descriptions}

Select how Airflow should proceed.
            """,
            options=hitl_options,
        )

        # ---------------------------------------------------------
        # 5A. Apply the KEDB remediation
        # ---------------------------------------------------------

        @task(task_id="apply_fix")
        def apply_fix(
                kedb_match: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Apply the configured remediation after human selection.
            """

            remediation_action = remediation_config["action"]

            print(
                f"Applying approved remediation "
                f"'{remediation_action}' for pipeline "
                f"'{kedb_match['pipeline_name']}'"
            )

            result = {
                "team_id": kedb_match["team_id"],
                "team_name": kedb_match["team_name"],
                "pipeline_name": kedb_match["pipeline_name"],
                "failed_step": kedb_match["failed_step"],
                "fix_applied": True,
                "remediation_action": remediation_action,
                "approved_by_human": True,
            }

            print(f"Remediation result: {result}")
            return result

        @task(task_id="rerun_pipeline")
        def rerun_pipeline(
                remediation_result: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Simulate rerunning the pipeline after remediation.
            """

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
                result = {
                    **remediation_result,
                    "rerun_status": "failed",
                    "message": (
                        "Pipeline continued to fail after remediation"
                    ),
                }

                print(f"Pipeline rerun failed: {result}")
                return result

            result = {
                **remediation_result,
                "rerun_status": "success",
                "message": (
                    "Pipeline recovered successfully after remediation"
                ),
            }

            print(f"Pipeline rerun succeeded: {result}")
            return result

        @task(task_id="recovery_completed")
        def recovery_completed(
                rerun_result: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Record successful pipeline recovery.
            """

            if rerun_result["rerun_status"] != "success":
                raise RuntimeError(
                    "Pipeline recovery was not successful"
                )

            result = {
                "team_id": rerun_result["team_id"],
                "team_name": rerun_result["team_name"],
                "pipeline_name": rerun_result["pipeline_name"],
                "status": "recovered",
                "remediation_action": rerun_result[
                    "remediation_action"
                ],
                "approved_by_human": rerun_result[
                    "approved_by_human"
                ],
            }

            print(f"Recovery completed: {result}")
            return result

        # ---------------------------------------------------------
        # 5B. Create a mock incident
        # ---------------------------------------------------------

        @task(task_id="create_incident")
        def create_incident(
                kedb_match: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Simulate incident creation for the selected team.
            """

            if not incident_config.get("enabled", False):
                result = {
                    "status": "disabled",
                    "team_id": kedb_match["team_id"],
                    "pipeline_name": kedb_match["pipeline_name"],
                }

                print(f"Incident creation disabled: {result}")
                return result

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
                "team_id": kedb_match["team_id"],
                "team_name": kedb_match["team_name"],
                "pipeline_name": kedb_match["pipeline_name"],
                "failed_step": kedb_match["failed_step"],
                "error_code": kedb_match["error_code"],
                "error_message": kedb_match["error_message"],
                "business_impact": kedb_match["business_impact"],
                "probable_cause": kedb_match["probable_cause"],
                "recommended_fix": kedb_match[
                    "recommended_fix"
                ],
            }

            print(f"Mock incident created: {incident}")
            return incident

        # ---------------------------------------------------------
        # 5C. Collect manual investigation findings
        # ---------------------------------------------------------

        manual_investigation = HITLEntryOperator(
            task_id="manual_investigation",
            subject="Provide manual investigation findings",
            body="""
## Manual pipeline investigation

**Team:**  
`{{ ti.xcom_pull(task_ids='search_kedb')['team_name'] }}`

**Pipeline:**  
`{{ ti.xcom_pull(task_ids='search_kedb')['pipeline_name'] }}`

**Failed pipeline step:**  
`{{ ti.xcom_pull(task_ids='search_kedb')['failed_step'] }}`

**Error code:**  
`{{ ti.xcom_pull(task_ids='search_kedb')['error_code'] }}`

**Error message:**  
{{ ti.xcom_pull(task_ids='search_kedb')['error_message'] }}

**KEDB probable cause:**  
{{ ti.xcom_pull(task_ids='search_kedb')['probable_cause'] }}

**KEDB recommendation:**  
{{ ti.xcom_pull(task_ids='search_kedb')['recommended_fix'] }}

Please review the failure and record your investigation findings.
            """,
            params={
                "investigation_notes": Param(
                    "Enter investigation details",
                    type="string",
                    title="Investigation notes",
                    description=(
                        "Describe the checks performed and observations."
                    ),
                    minLength=1,
                ),
                "suspected_root_cause": Param(
                    "Root cause under investigation",
                    type="string",
                    title="Suspected root cause",
                    description=(
                        "Enter the suspected technical root cause."
                    ),
                    minLength=1,
                ),
                "proposed_action": Param(
                    "Continue manual investigation",
                    type="string",
                    title="Proposed recovery action",
                    description=(
                        "Describe the action recommended by the engineer."
                    ),
                    minLength=1,
                ),
                "recovery_eta_minutes": Param(
                    30,
                    type="integer",
                    title="Estimated recovery time in minutes",
                    description=(
                        "Estimate the time required to recover "
                        "the pipeline."
                    ),
                    minimum=1,
                    maximum=1440,
                ),
                "notify_business": Param(
                    False,
                    type="boolean",
                    title="Business notification required",
                    description=(
                        "Select if affected business users "
                        "should be informed."
                    ),
                ),
            },
        )

        @task(task_id="record_investigation")
        def record_investigation(
                hitl_response: dict[str, Any],
                kedb_match: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Extract and record the values submitted through
            HITLEntryOperator.
            """

            form_input = hitl_response.get(
                "params_input",
                {},
            )

            investigation_record = {
                "status": "investigation_recorded",
                "team_id": kedb_match["team_id"],
                "team_name": kedb_match["team_name"],
                "pipeline_name": kedb_match["pipeline_name"],
                "failed_step": kedb_match["failed_step"],
                "error_code": kedb_match["error_code"],
                "error_message": kedb_match["error_message"],
                "investigation_notes": form_input.get(
                    "investigation_notes"
                ),
                "suspected_root_cause": form_input.get(
                    "suspected_root_cause"
                ),
                "proposed_action": form_input.get(
                    "proposed_action"
                ),
                "recovery_eta_minutes": form_input.get(
                    "recovery_eta_minutes"
                ),
                "notify_business": form_input.get(
                    "notify_business"
                ),
                "automated_action_taken": False,
            }

            print("Manual investigation submitted:")
            print(json.dumps(investigation_record, indent=2))

            return investigation_record

        # ---------------------------------------------------------
        # 6. Instantiate the recovery tasks
        # ---------------------------------------------------------

        fix_result = apply_fix(
            kedb_result
        )

        pipeline_rerun_result = rerun_pipeline(
            fix_result
        )

        recovered_result = recovery_completed(
            pipeline_rerun_result
        )

        incident_result = create_incident(
            kedb_result
        )

        investigation_result = record_investigation(
            manual_investigation.output,
            kedb_result,
        )

        # ---------------------------------------------------------
        # 7. Define branching dependencies
        # ---------------------------------------------------------

        selected_pipeline_path >> successful_pipeline_result
        selected_pipeline_path >> kedb_result

        kedb_result >> choose_recovery_action

        choose_recovery_action >> fix_result
        choose_recovery_action >> incident_result
        choose_recovery_action >> manual_investigation

        manual_investigation >> investigation_result

    return dag