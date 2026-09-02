from pathlib import Path
from typing import Any

import yaml


def load_team_configs(config_directory: str) -> list[dict[str, Any]]:
    directory = Path(config_directory)

    if not directory.exists():
        raise FileNotFoundError(
            f"Configuration directory does not exist: {directory}"
        )

    configs: list[dict[str, Any]] = []

    for config_path in sorted(directory.glob("*.yaml")):
        with config_path.open("r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)

        if not isinstance(config, dict):
            raise ValueError(
                f"Configuration must contain a YAML object: {config_path}"
            )

        config["_config_file"] = str(config_path)
        configs.append(config)

    return configs