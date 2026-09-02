from typing import Any


REQUIRED_SECTIONS = {
    "framework_version",
    "team",
    "dag",
    "pipeline",
    "kedb",
    "hitl",
    "remediation",
    "incident",
}

ALLOWED_PIPELINE_OUTCOMES = {
    "success",
    "failed",
}

ALLOWED_BRANCH_TASKS = {
    "apply_fix",
    "create_incident",
    "manual_investigation",
}

ALLOWED_REMEDIATIONS = {
    "reset_connection",
    "wait_for_source_file",
}

ALLOWED_RERUN_OUTCOMES = {
    "success",
    "failed",
}


def validate_config(config: dict[str, Any]) -> None:
    config_file = config.get("_config_file", "unknown configuration")

    missing_sections = REQUIRED_SECTIONS - config.keys()

    if missing_sections:
        raise ValueError(
            f"{config_file}: missing required sections "
            f"{sorted(missing_sections)}"
        )

    team_id = config["team"].get("id")

    if not team_id:
        raise ValueError(
            f"{config_file}: team.id is required"
        )

    dag_id = config["dag"].get("dag_id")

    if not dag_id:
        raise ValueError(
            f"{config_file}: dag.dag_id is required"
        )

    pipeline = config["pipeline"]

    if not pipeline.get("name"):
        raise ValueError(
            f"{config_file}: pipeline.name is required"
        )

    pipeline_steps = pipeline.get("steps", [])

    if not pipeline_steps:
        raise ValueError(
            f"{config_file}: pipeline.steps must contain at least one step"
        )

    simulation = pipeline.get("simulation")

    if not isinstance(simulation, dict):
        raise ValueError(
            f"{config_file}: pipeline.simulation is required"
        )

    pipeline_outcome = simulation.get("outcome")

    if pipeline_outcome not in ALLOWED_PIPELINE_OUTCOMES:
        raise ValueError(
            f"{config_file}: pipeline.simulation.outcome must be one of "
            f"{sorted(ALLOWED_PIPELINE_OUTCOMES)}"
        )

    if pipeline_outcome == "failed":
        required_failure_fields = {
            "failed_step",
            "error_code",
            "error_message",
        }

        missing_failure_fields = (
                required_failure_fields - simulation.keys()
        )

        if missing_failure_fields:
            raise ValueError(
                f"{config_file}: failed simulation is missing "
                f"{sorted(missing_failure_fields)}"
            )

    kedb = config["kedb"]

    if kedb.get("type") != "local_json":
        raise ValueError(
            f"{config_file}: only kedb.type=local_json is currently supported"
        )

    if not kedb.get("location"):
        raise ValueError(
            f"{config_file}: kedb.location is required"
        )

    hitl_actions = config["hitl"].get("actions", [])

    if not hitl_actions:
        raise ValueError(
            f"{config_file}: at least one HITL action is required"
        )

    seen_task_ids = set()
    seen_labels = set()

    for action in hitl_actions:
        task_id = action.get("task_id")
        label = action.get("label")

        if task_id not in ALLOWED_BRANCH_TASKS:
            raise ValueError(
                f"{config_file}: unsupported HITL task_id '{task_id}'"
            )

        if not label:
            raise ValueError(
                f"{config_file}: every HITL action requires a label"
            )

        if task_id in seen_task_ids:
            raise ValueError(
                f"{config_file}: duplicate HITL task_id '{task_id}'"
            )

        if label in seen_labels:
            raise ValueError(
                f"{config_file}: duplicate HITL label '{label}'"
            )

        seen_task_ids.add(task_id)
        seen_labels.add(label)

    remediation_action = config["remediation"].get("action")

    if remediation_action not in ALLOWED_REMEDIATIONS:
        raise ValueError(
            f"{config_file}: unsupported remediation action "
            f"'{remediation_action}'"
        )

    rerun_outcome = config["remediation"].get(
        "rerun_outcome",
        "success",
    )

    if rerun_outcome not in ALLOWED_RERUN_OUTCOMES:
        raise ValueError(
            f"{config_file}: remediation.rerun_outcome must be one of "
            f"{sorted(ALLOWED_RERUN_OUTCOMES)}"
        )