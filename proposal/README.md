# Airflow Agent Skills Proposal Bundle

This folder contains a focused proposal package for Breeze-aware agent skills in Apache Airflow.

## Contents

- `breeze-agent-skills-poc/`: runnable proof-of-concept with tests, generated skill manifest, and execution evidence.
- `skills/`: supporting skill artifacts.

## Primary PoC Path

Use the PoC folder as the canonical artifact:

- `agent-skills-proposal/breeze-agent-skills-poc/README.md`

## Quick Validation

From `agent-skills-proposal/breeze-agent-skills-poc/`:

```bash
python run_poc_demo.py
```

This executes:

1. skill manifest generation
2. drift check
3. test suite

Outputs are written under `proof/`.
