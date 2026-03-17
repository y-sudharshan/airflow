# AGENTS instructions

This file demonstrates an executable-document approach for Airflow contribution skills.
Human readers can use the guidance directly; extraction scripts read the structured blocks below.

## Environment Notes

- Run `prek` on the host.
- Prefer `uv run --project ... pytest ...` for targeted unit tests.
- Only escalate to Breeze when system dependencies are missing or CI behavior diverges.

<!-- agent-skill:start run-static-checks -->
id: run-static-checks
context: host
kind: workflow
summary: Run static checks on changed files using prek.
local: prek run ruff ruff-format mypy --files {files}
fallback:
fallback_condition:
prereqs: git-add
<!-- agent-skill:end run-static-checks -->

<!-- agent-skill:start run-unit-tests -->
id: run-unit-tests
context: either
kind: workflow
summary: Run targeted unit tests with a local-first strategy.
local: uv run --project {distribution_folder} pytest {test_path} -xvs
fallback: breeze exec pytest {test_path} -xvs
fallback_condition: missing_system_deps
ci: breeze testing tests {test_path} --python {python} --backend {backend}
prereqs: detect-environment
<!-- agent-skill:end run-unit-tests -->

<!-- agent-skill:start verify-dag -->
id: verify-dag
context: host
kind: workflow
summary: Start Airflow and verify DAG behavior as a stretch workflow.
local: uv run --project airflow airflow dags test {dag_id} {execution_date}
fallback: breeze start-airflow --backend {backend}
fallback_condition: needs_full_airflow_env
prereqs: detect-environment, run-unit-tests
<!-- agent-skill:end verify-dag -->
