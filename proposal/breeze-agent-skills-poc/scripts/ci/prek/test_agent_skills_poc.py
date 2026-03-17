from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import extract_agent_skills
import breeze_context_detect


class TestExtraction(unittest.TestCase):
    def test_parse_blocks_extracts_three_skills(self):
        text = extract_agent_skills.AGENTS_FILE.read_text(encoding="utf-8")
        skills = extract_agent_skills.parse_blocks(text)
        self.assertEqual(3, len(skills))
        self.assertEqual(["run-static-checks", "run-unit-tests", "verify-dag"], [skill["id"] for skill in skills])

    def test_local_first_contract_rejects_breeze_local(self):
        text = """<!-- agent-skill:start bad -->\nid: bad\ncontext: host\nkind: workflow\nsummary: bad\nlocal: breeze exec pytest tests/foo.py\nprereqs: detect-environment\n<!-- agent-skill:end bad -->\n"""
        with self.assertRaisesRegex(ValueError, "violates local-first contract"):
            extract_agent_skills.parse_blocks(text)

    def test_render_is_valid_json(self):
        text = extract_agent_skills.AGENTS_FILE.read_text(encoding="utf-8")
        payload = extract_agent_skills.render(extract_agent_skills.parse_blocks(text))
        parsed = json.loads(payload)
        self.assertIn("skills", parsed)
        self.assertEqual("run-unit-tests", parsed["skills"][1]["id"])

    def test_duplicate_skill_id_rejected(self):
        text = """<!-- agent-skill:start a -->\nid: same\ncontext: host\nkind: workflow\nsummary: one\nlocal: prek run ruff --files {files}\nprereqs: git-add\n<!-- agent-skill:end a -->\n\n<!-- agent-skill:start b -->\nid: same\ncontext: host\nkind: workflow\nsummary: two\nlocal: prek run mypy --files {files}\nprereqs: git-add\n<!-- agent-skill:end b -->\n"""
        with self.assertRaisesRegex(ValueError, "Duplicate skill id"):
            extract_agent_skills.parse_blocks(text)

    def test_invalid_context_rejected(self):
        text = """<!-- agent-skill:start bad -->\nid: bad\ncontext: unknown\nkind: workflow\nsummary: bad\nlocal: prek run ruff --files {files}\nprereqs: git-add\n<!-- agent-skill:end bad -->\n"""
        with self.assertRaisesRegex(ValueError, "invalid context"):
            extract_agent_skills.parse_blocks(text)

    def test_invalid_kind_rejected(self):
        text = """<!-- agent-skill:start bad -->\nid: bad\ncontext: host\nkind: command\nsummary: bad\nlocal: prek run ruff --files {files}\nprereqs: git-add\n<!-- agent-skill:end bad -->\n"""
        with self.assertRaisesRegex(ValueError, "invalid kind"):
            extract_agent_skills.parse_blocks(text)

    def test_invalid_fallback_condition_rejected(self):
        text = """<!-- agent-skill:start bad -->\nid: bad\ncontext: either\nkind: workflow\nsummary: bad\nlocal: uv run --project x pytest y\nfallback: breeze exec pytest y\nfallback_condition: random_reason\nprereqs: detect-environment\n<!-- agent-skill:end bad -->\n"""
        with self.assertRaisesRegex(ValueError, "invalid fallback_condition"):
            extract_agent_skills.parse_blocks(text)

    def test_empty_prereqs_rejected(self):
        text = """<!-- agent-skill:start bad -->\nid: bad\ncontext: host\nkind: workflow\nsummary: bad\nlocal: prek run ruff --files {files}\nprereqs:\n<!-- agent-skill:end bad -->\n"""
        with self.assertRaisesRegex(ValueError, "at least one prereq"):
            extract_agent_skills.parse_blocks(text)

    def test_no_skill_blocks_rejected(self):
        with self.assertRaisesRegex(ValueError, "No agent-skill blocks"):
            extract_agent_skills.parse_blocks("# no markers here")


class TestDriftCheck(unittest.TestCase):
    def test_check_mode_fails_when_output_is_out_of_sync(self):
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            agents_file = temp_root / "AGENTS.md"
            out_file = temp_root / "generated" / "skills.json"

            agents_file.write_text(
                """<!-- agent-skill:start x -->
id: x
context: host
kind: workflow
summary: x
local: prek run ruff --files {files}
prereqs: git-add
<!-- agent-skill:end x -->
""",
                encoding="utf-8",
            )
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text("{}\n", encoding="utf-8")

            with mock.patch.object(extract_agent_skills, "AGENTS_FILE", agents_file), mock.patch.object(
                extract_agent_skills, "OUTPUT_FILE", out_file
            ), mock.patch("sys.argv", ["extract_agent_skills.py", "--check"]):
                code = extract_agent_skills.main()
            self.assertEqual(1, code)

    def test_check_mode_passes_when_output_is_in_sync(self):
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            agents_file = temp_root / "AGENTS.md"
            out_file = temp_root / "generated" / "skills.json"

            agents_file.write_text(
                """<!-- agent-skill:start x -->
id: x
context: host
kind: workflow
summary: x
local: prek run ruff --files {files}
prereqs: git-add
<!-- agent-skill:end x -->
""",
                encoding="utf-8",
            )
            out_file.parent.mkdir(parents=True, exist_ok=True)
            rendered = extract_agent_skills.render(extract_agent_skills.parse_blocks(agents_file.read_text(encoding="utf-8")))
            out_file.write_text(rendered, encoding="utf-8")

            with mock.patch.object(extract_agent_skills, "AGENTS_FILE", agents_file), mock.patch.object(
                extract_agent_skills, "OUTPUT_FILE", out_file
            ), mock.patch("sys.argv", ["extract_agent_skills.py", "--check"]):
                code = extract_agent_skills.main()
            self.assertEqual(0, code)


class TestEnvironmentDetection(unittest.TestCase):
    def test_detects_host_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch("breeze_context_detect.Path.exists", return_value=False):
            evidence = breeze_context_detect.detect_environment()
            self.assertEqual(breeze_context_detect.ExecutionEnvironment.HOST, evidence.environment)

    def test_detects_breeze_by_env_var(self):
        with mock.patch.dict(os.environ, {"AIRFLOW_BREEZE_CONTAINER": "true"}, clear=True):
            evidence = breeze_context_detect.detect_environment()
            self.assertEqual(breeze_context_detect.ExecutionEnvironment.BREEZE_CONTAINER, evidence.environment)

    def test_detects_breeze_by_path_markers(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch("breeze_context_detect.Path.exists", side_effect=[False, True, True]):
            evidence = breeze_context_detect.detect_environment()
            self.assertEqual(breeze_context_detect.ExecutionEnvironment.BREEZE_CONTAINER, evidence.environment)

    def test_detects_ci_environment(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=True), mock.patch("breeze_context_detect.Path.exists", return_value=False):
            evidence = breeze_context_detect.detect_environment()
            self.assertEqual(breeze_context_detect.ExecutionEnvironment.CI, evidence.environment)


class TestCommandPlanning(unittest.TestCase):
    def setUp(self):
        text = extract_agent_skills.AGENTS_FILE.read_text(encoding="utf-8")
        self.skills = {skill["id"]: skill for skill in extract_agent_skills.parse_blocks(text)}

    def test_local_first_is_default(self):
        skill = self.skills["run-unit-tests"]
        with mock.patch("breeze_context_detect.detect_environment", return_value=breeze_context_detect.EnvironmentEvidence(breeze_context_detect.ExecutionEnvironment.HOST, "default")):
            command = breeze_context_detect.plan_command(skill, {"distribution_folder": "airflow-core", "test_path": "tests/utils/", "python": "3.12", "backend": "postgres"})
        self.assertTrue(command.startswith("uv run --project airflow-core pytest tests/utils/ -xvs"))

    def test_missing_deps_falls_back_to_breeze_exec(self):
        skill = self.skills["run-unit-tests"]
        with mock.patch("breeze_context_detect.detect_environment", return_value=breeze_context_detect.EnvironmentEvidence(breeze_context_detect.ExecutionEnvironment.HOST, "default")):
            command = breeze_context_detect.plan_command(skill, {"distribution_folder": "airflow-core", "test_path": "tests/utils/", "python": "3.12", "backend": "postgres"}, breeze_context_detect.FailureReason.MISSING_SYSTEM_DEPS)
        self.assertEqual("breeze exec pytest tests/utils/ -xvs", command)

    def test_container_uses_plain_pytest(self):
        skill = self.skills["run-unit-tests"]
        with mock.patch("breeze_context_detect.detect_environment", return_value=breeze_context_detect.EnvironmentEvidence(breeze_context_detect.ExecutionEnvironment.BREEZE_CONTAINER, "/.dockerenv")):
            command = breeze_context_detect.plan_command(skill, {"distribution_folder": "airflow-core", "test_path": "tests/utils/", "python": "3.12", "backend": "postgres"})
        self.assertEqual("pytest tests/utils/ -xvs", command)

    def test_ci_mismatch_uses_ci_command(self):
        skill = self.skills["run-unit-tests"]
        with mock.patch("breeze_context_detect.detect_environment", return_value=breeze_context_detect.EnvironmentEvidence(breeze_context_detect.ExecutionEnvironment.HOST, "default")):
            command = breeze_context_detect.plan_command(
                skill,
                {"distribution_folder": "airflow-core", "test_path": "tests/utils/", "python": "3.12", "backend": "postgres"},
                breeze_context_detect.FailureReason.CI_MISMATCH,
            )
        self.assertEqual("breeze testing tests tests/utils/ --python 3.12 --backend postgres", command)

    def test_verify_dag_uses_fallback_when_full_env_needed(self):
        skill = self.skills["verify-dag"]
        with mock.patch("breeze_context_detect.detect_environment", return_value=breeze_context_detect.EnvironmentEvidence(breeze_context_detect.ExecutionEnvironment.HOST, "default")):
            command = breeze_context_detect.plan_command(
                skill,
                {"dag_id": "example", "execution_date": "2026-03-17", "backend": "postgres"},
                breeze_context_detect.FailureReason.NEEDS_FULL_AIRFLOW_ENV,
            )
        self.assertEqual("breeze start-airflow --backend postgres", command)


if __name__ == "__main__":
    current_dir = Path(__file__).resolve().parent
    os.chdir(current_dir)
    import sys
    sys.path.insert(0, str(current_dir))
    unittest.main(verbosity=2)
