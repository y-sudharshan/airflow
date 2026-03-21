# Breeze Agent Skills PoC

This PoC implements Breeze-aware agent workflows with contributor documentation as the only authoritative source.

## Source of Truth

Canonical source:

- contributing-docs/03_contributors_quick_start.rst

Embedded `agent-skill` blocks are extracted into `generated/skills.json`.

## Architecture

1. Documentation Layer
- Contributor workflow guidance lives in contributing-docs.
- Embedded agent-skill blocks keep human and agent guidance aligned.

2. Extraction Layer
- scripts/ci/prek/extract_agent_skills.py parses embedded blocks.
- Schema validation enforces required fields, context enums, and local-first behavior.

3. Drift Enforcement Layer
- extract_agent_skills.py --check fails when generated/skills.json diverges.
- Intended for pre-commit and CI gating.

4. Runtime Decision Layer
- scripts/ci/prek/breeze_context_detect.py detects host vs breeze-container vs CI.
- plan_command() selects local, fallback, or CI commands.
- resolve_command_from_metadata() is manifest-first with breeze --help fallback.

5. Verification Layer
- scripts/ci/prek/test_agent_skills_poc.py validates extraction, drift, environment detection, planning, and metadata fallback.
- run_poc_demo.py runs extraction, drift check, and tests and writes proof artifacts.

## Workflow Alignment

- Local-first is default (`uv`/`prek`).
- Breeze is used as fallback for missing system dependencies or environment mismatch.
- Skills are tied to contributor workflow docs instead of a separate standalone spec.

## Run

```bash
python proposal/breeze-agent-skills-poc/run_poc_demo.py
```

Output artifacts:
- proposal/breeze-agent-skills-poc/proof/run_report.md
- proposal/breeze-agent-skills-poc/proof/run_report.json

## Validation

- 28 tests passing
- RST source extraction enabled
- Drift check wired to contributing-docs source
- Manifest-first plus CLI fallback behavior tested

## Acceptance Criteria Covered

- Single source of truth in contributor docs
- Deterministic extraction from embedded blocks
- Drift detection for source/manifest consistency
- Runtime command planning by execution context
- Metadata fallback when command is absent from manifest
- End-to-end proof report with passing test run

## Included Components

- contributing-docs/03_contributors_quick_start.rst (embedded skill blocks)
- proposal/breeze-agent-skills-poc/scripts/ci/prek/extract_agent_skills.py
- proposal/breeze-agent-skills-poc/scripts/ci/prek/breeze_context_detect.py
- proposal/breeze-agent-skills-poc/scripts/ci/prek/test_agent_skills_poc.py
- proposal/breeze-agent-skills-poc/generated/skills.json
- proposal/breeze-agent-skills-poc/run_poc_demo.py
- proposal/breeze-agent-skills-poc/proof/run_report.md
- proposal/breeze-agent-skills-poc/proof/run_report.json
