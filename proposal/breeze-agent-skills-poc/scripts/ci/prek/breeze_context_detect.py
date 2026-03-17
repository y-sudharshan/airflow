from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class ExecutionEnvironment(str, Enum):
    HOST = "host"
    BREEZE_CONTAINER = "breeze-container"
    CI = "ci"


class FailureReason(str, Enum):
    NONE = "none"
    MISSING_SYSTEM_DEPS = "missing_system_deps"
    CI_MISMATCH = "ci_mismatch"
    NEEDS_FULL_AIRFLOW_ENV = "needs_full_airflow_env"


@dataclass(frozen=True)
class EnvironmentEvidence:
    environment: ExecutionEnvironment
    reason: str


def detect_environment() -> EnvironmentEvidence:
    if os.getenv("AIRFLOW_BREEZE_CONTAINER") == "true":
        return EnvironmentEvidence(ExecutionEnvironment.BREEZE_CONTAINER, "AIRFLOW_BREEZE_CONTAINER")
    if Path("/.dockerenv").exists():
        return EnvironmentEvidence(ExecutionEnvironment.BREEZE_CONTAINER, "/.dockerenv")
    if Path("/opt/airflow").exists() and Path("/entrypoint_ci.sh").exists():
        return EnvironmentEvidence(ExecutionEnvironment.BREEZE_CONTAINER, "breeze-path-markers")
    if os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true":
        return EnvironmentEvidence(ExecutionEnvironment.CI, "ci-env")
    return EnvironmentEvidence(ExecutionEnvironment.HOST, "default")


def plan_command(skill: dict[str, Any], params: dict[str, str], failure_reason: FailureReason = FailureReason.NONE) -> str:
    evidence = detect_environment()

    command_template = skill["local"]
    if evidence.environment == ExecutionEnvironment.BREEZE_CONTAINER:
        if skill["id"] == "run-unit-tests":
            command_template = "pytest {test_path} -xvs"
    elif failure_reason == FailureReason.MISSING_SYSTEM_DEPS and skill.get("fallback"):
        command_template = skill["fallback"]
    elif failure_reason == FailureReason.NEEDS_FULL_AIRFLOW_ENV and skill.get("fallback"):
        command_template = skill["fallback"]
    elif failure_reason == FailureReason.CI_MISMATCH and skill.get("ci"):
        command_template = skill["ci"]

    command = command_template
    for key, value in params.items():
        command = command.replace(f"{{{key}}}", str(value))
    return " ".join(command.split())
