from framework.config_loader import load_team_configs
from framework.config_validator import validate_config
from framework.dag_factory import create_recovery_dag


CONFIG_DIRECTORY = "/opt/airflow/team-configs"

team_configs = load_team_configs(CONFIG_DIRECTORY)

seen_dag_ids: set[str] = set()


for team_config in team_configs:
    validate_config(team_config)

    dag_id = team_config["dag"]["dag_id"]

    if dag_id in seen_dag_ids:
        raise ValueError(
            f"Duplicate DAG ID detected: {dag_id}"
        )

    seen_dag_ids.add(dag_id)

    globals()[dag_id] = create_recovery_dag(team_config)