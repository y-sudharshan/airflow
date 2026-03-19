.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

*************************
Contributor's Quick Start
*************************

Running Static Code Checks
##########################

Before committing your code, ensure all static checks pass locally:

``prek`` is the tool we use for running pre-commit checks. It allows you to run specific checks for selected files.

**Running all prek hooks:**

.. code-block:: bash

    prek

<!-- agent-skill:start run-static-checks -->
id: run-static-checks
context: host
kind: workflow
summary: Run all static code checks (ruff, mypy, black, etc.) using prek
local: prek
fallback: breeze exec prek
fallback_condition: missing_system_deps
ci: breeze static-checks
prereqs: detect-environment
<!-- agent-skill:end run-static-checks -->

**Running prek for selected files:**

.. code-block:: bash

    prek --files airflow-core/src/airflow/utils/decorators.py

**Running specific check only:**

.. code-block:: bash

    prek black --files airflow-core/src/airflow/utils/decorators.py

Running Unit Tests
##################

To run unit tests for your changes, use the ``uv run`` command in the distribution folder:

**Run all tests:**

.. code-block:: bash

    uv run --project airflow-core pytest

**Run specific test file:**

.. code-block:: bash

    uv run --project airflow-core pytest tests/utils/test_decorators.py -xvs

**Run with Breeze container (if local environment has missing dependencies):**

.. code-block:: bash

    breeze exec pytest tests/utils/test_decorators.py -xvs

<!-- agent-skill:start run-unit-tests -->
id: run-unit-tests
context: either
kind: workflow
summary: Run targeted unit tests with local-first strategy and Breeze fallback
local: uv run --project {distribution_folder} pytest {test_path} -xvs
fallback: breeze exec pytest {test_path} -xvs
fallback_condition: missing_system_deps
ci: breeze testing tests {test_path} --python {python} --backend {backend}
prereqs: detect-environment
<!-- agent-skill:end run-unit-tests -->

Verifying DAG Integrity
#######################

When modifying DAG definitions, verify they can be parsed and loaded correctly:

**Verify a specific DAG:**

.. code-block:: bash

    breeze start-airflow --backend postgres

<!-- agent-skill:start verify-dag -->
id: verify-dag
context: breeze-container
kind: workflow
summary: Verify DAG definition can be parsed and loaded in Airflow
local: python -c "from airflow import settings; settings.configure_logging()"
fallback: breeze start-airflow --backend {backend}
fallback_condition: needs_full_airflow_env
ci: breeze testing tests tests/core/test_dagbag.py
prereqs: detect-environment
<!-- agent-skill:end verify-dag -->

Summary of Skills in This Document
##################################

This document embeds three agent skills that AI assistants can extract and use:

1. **run-static-checks** - Execute prek for linting/formatting
2. **run-unit-tests** - Run pytest with local-first + Breeze fallback
3. **verify-dag** - Check DAG integrity in Breeze environment

The skills are marked with HTML comments (``<!-- agent-skill:start ... -->``), allowing tools to:
- Extract alongside contributing documentation
- Maintain synchronization between docs and agent instructions
- Keep contributor guides as the single source of truth
