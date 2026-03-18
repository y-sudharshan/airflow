# PoC Demo Execution Report

Generated at: `2026-03-18T09:14:51.497234+00:00`
Overall status: `PASS`

## Step Results

### Generate skills manifest

- Command: `C:\Python313\python.exe scripts/ci/prek/extract_agent_skills.py`
- Status: `PASS`
- Exit code: `0`
- Duration: `0.1037s`

Output:
```text
Written 3 skill(s) to generated\skills.json
  - run-static-checks
  - run-unit-tests
  - verify-dag
```

### Check for drift

- Command: `C:\Python313\python.exe scripts/ci/prek/extract_agent_skills.py --check`
- Status: `PASS`
- Exit code: `0`
- Duration: `0.11s`

Output:
```text
OK: skills.json is in sync with AGENTS.md
```

### Run PoC tests

- Command: `C:\Python313\python.exe scripts/ci/prek/test_agent_skills_poc.py`
- Status: `PASS`
- Exit code: `0`
- Duration: `0.2976s`

Output:
```text
DRIFT: generated/skills.json is out of sync with AGENTS.md
OK: skills.json is in sync with AGENTS.md

test_ci_mismatch_uses_ci_command (__main__.TestCommandPlanning.test_ci_mismatch_uses_ci_command) ... ok
test_container_uses_plain_pytest (__main__.TestCommandPlanning.test_container_uses_plain_pytest) ... ok
test_local_first_is_default (__main__.TestCommandPlanning.test_local_first_is_default) ... ok
test_missing_deps_falls_back_to_breeze_exec (__main__.TestCommandPlanning.test_missing_deps_falls_back_to_breeze_exec) ... ok
test_verify_dag_uses_fallback_when_full_env_needed (__main__.TestCommandPlanning.test_verify_dag_uses_fallback_when_full_env_needed) ... ok
test_check_mode_fails_when_output_is_out_of_sync (__main__.TestDriftCheck.test_check_mode_fails_when_output_is_out_of_sync) ... ok
test_check_mode_passes_when_output_is_in_sync (__main__.TestDriftCheck.test_check_mode_passes_when_output_is_in_sync) ... ok
test_detects_breeze_by_env_var (__main__.TestEnvironmentDetection.test_detects_breeze_by_env_var) ... ok
test_detects_breeze_by_path_markers (__main__.TestEnvironmentDetection.test_detects_breeze_by_path_markers) ... ok
test_detects_ci_environment (__main__.TestEnvironmentDetection.test_detects_ci_environment) ... ok
test_detects_host_by_default (__main__.TestEnvironmentDetection.test_detects_host_by_default) ... ok
test_duplicate_skill_id_rejected (__main__.TestExtraction.test_duplicate_skill_id_rejected) ... ok
test_empty_prereqs_rejected (__main__.TestExtraction.test_empty_prereqs_rejected) ... ok
test_invalid_context_rejected (__main__.TestExtraction.test_invalid_context_rejected) ... ok
test_invalid_fallback_condition_rejected (__main__.TestExtraction.test_invalid_fallback_condition_rejected) ... ok
test_invalid_kind_rejected (__main__.TestExtraction.test_invalid_kind_rejected) ... ok
test_local_first_contract_rejects_breeze_local (__main__.TestExtraction.test_local_first_contract_rejects_breeze_local) ... ok
test_no_skill_blocks_rejected (__main__.TestExtraction.test_no_skill_blocks_rejected) ... ok
test_parse_blocks_extracts_three_skills (__main__.TestExtraction.test_parse_blocks_extracts_three_skills) ... ok
test_render_is_valid_json (__main__.TestExtraction.test_render_is_valid_json) ... ok
test_extractor_caches_metadata_from_help (__main__.TestMetadataExtraction.test_extractor_caches_metadata_from_help)
Verify metadata extraction caches results to avoid repeated CLI calls. ... ok
test_extractor_handles_missing_breeze_gracefully (__main__.TestMetadataExtraction.test_extractor_handles_missing_breeze_gracefully)
Verify extractor gracefully handles when breeze CLI is not available. ... ok
test_extractor_parses_subcommands_from_help (__main__.TestMetadataExtraction.test_extractor_parses_subcommands_from_help)
Verify extractor parses subcommands from --help output. ... ok
test_extractor_persists_cache_to_disk (__main__.TestMetadataExtraction.test_extractor_persists_cache_to_disk)
Verify cache is persisted to disk for reuse across sessions. ... ok
test_resolve_command_fallback_to_cli (__main__.TestMetadataExtraction.test_resolve_command_fallback_to_cli)
Verify resilient fallback: uses CLI extraction when manifest doesn't have command. ... ok
test_resolve_command_prefers_manifest (__main__.TestMetadataExtraction.test_resolve_command_prefers_manifest)
Verify manifest-first strategy: prefers manifest over CLI extraction. ... ok

----------------------------------------------------------------------
Ran 26 tests in 0.038s

OK
```
