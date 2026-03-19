# Addressing Mentor Concern: Single Source of Truth

## The Issue (from review)

> "Skills defined in standalone AGENTS.md creates drift risk. Contributing guides should be the single source of truth."
> — @potiuk

**Current risk:** If `03_contributors_quick_start.rst` says "run `uv run pytest ...`" but `AGENTS.md` says something different, which one do AI agents trust?

---

## The Solution: Embedded Skills Pattern

### Better Architecture

Instead of separate files:
```
contributing-docs/
├── 03_contributors_quick_start.rst  (human-readable guide)
AGENTS.md                            (agent instructions) ← SEPARATE FILES = DRIFT RISK
```

Use embedded markers within docs:
```
contributing-docs/
├── 03_contributors_quick_start.rst  (SINGLE SOURCE)
│   ├── [Human reads this]
│   ├── <!-- agent-skill:start run-unit-tests -->
│   │   [Agent extracts this]
│   └── <!-- agent-skill:end run-unit-tests -->
```

**Result:**
- Docs are the source of truth
- Skills automatically sync with documentation
- One edit updates both human and AI guidance
- No drift possible

---

## Implementation: Multi-Source Extraction

### Current State (Backward Compatible)

The extraction script now supports **multiple source files**:

```python
SOURCE_FILES = [
    AGENTS_FILE,  # ← AGENTS.md (current, for validation)
    # Transition path (commented out, ready to enable):
    # ROOT / "contributing-docs" / "08_static_code_checks.rst",
    # ROOT / "contributing-docs" / "09_testing.rst",
    # ROOT / "contributing-docs" / "03_contributors_quick_start.rst",
]
```

**Scripts now work with:**
- `.md` files (Markdown, like AGENTS.md)
- `.rst` files (reStructuredText, like actual Airflow docs)
- Any text file with `<!-- agent-skill:start ... end -->` markers

### Usage

**Extract from AGENTS.md (current default):**
```bash
python scripts/ci/prek/extract_agent_skills_v2.py
```

**Extract from embedded RST docs (new pattern):**
```bash
python scripts/ci/prek/extract_agent_skills_v2.py --source contributing-docs/03_contributors_quick_start.rst
```

**Check drift for any source:**
```bash
python scripts/ci/prek/extract_agent_skills_v2.py --source SOMEFILE --check
```

---

## Example: Embedded Skills in RST Documentation

### File: `contributing-docs-example/03_contributors_quick_start_embedded.rst`

This demonstration file shows how skills are embedded **directly in contributor docs**:

```rst
Running Static Code Checks
##########################

Before committing your code, ensure all static checks pass locally:

``prek`` is the tool we use for running pre-commit checks...

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
```

**Benefits:**
1. **Same text serves both humans and AI agents**
2. **Updates propagate automatically** (edit docs → skills update → drift check fails → force sync)
3. **Clear relationship** between guidance and execution instruction
4. **No duplication**

---

## Transition Roadmap

### Phase 1 (Current - PoC) ✅
- [x] Extraction script supports multiple sources
- [x] Demo embedded RST file showing pattern
- [x] Backward compatible (AGENTS.md still works)
- [x] Tested extraction + drift check for both sources

### Phase 2 (Future - Real Integration)
- [ ] Uncomment actual airf low `contributing-docs/*.rst` files in `SOURCE_FILES`
- [ ] Run extraction from real docs
- [ ] Update prek pre-commit hook to check drift against contributing docs
- [ ] Migrate AGENTS.md content into docs
- [ ] Retire standalone AGENTS.md

### Phase 3 (Maintenance)
- [ ] All skills embedded in contributing documentation
- [ ] AGENTS.md serves as optional reference/archive only
- [ ] Drift detection prevents divergence
- [ ] Contributing guide changes automatically update agent skills

---

## How This Addresses the Mentor Concern

| Concern | Current Approach | Embedded Approach |
|---------|------------------|-------------------|
| **Source of Truth** | AGENTS.md (separate file) | Contributing docs (part of real guide) |
| **Drift Risk** | Medium (two files to sync) | None (single file modified) |
| **Developer Experience** | Edit docs + AGENTS.md separately | Edit docs once, skills auto-sync |
| **Maintenance** | Manual sync needed | Automatic through drift detection |
| **Validation** | Check AGENTS.md only | Check real contributing docs |

---

## Files in This Section

- **`contributing-docs-example/03_contributors_quick_start_embedded.rst`** — Example showing how skills are embedded in RST documentation (demonstrates pattern without modifying real airflow docs)
- **`scripts/ci/prek/extract_agent_skills_v2.py`** — Enhanced extraction script supporting multiple source file types
- **`scripts/ci/prek/extract_agent_skills.py`** — Original script (kept for compatibility during transition)

---

## Testing the Pattern

Run PoC demo with embedded RST source:

```bash
cd proposal/breeze-agent-skills-poc/scripts/ci/prek

# Extract from embedded RST
python extract_agent_skills_v2.py --source \
  ../../contributing-docs-example/03_contributors_quick_start_embedded.rst

# Verify drift detection works
python extract_agent_skills_v2.py --check --source \
  ../../contributing-docs-example/03_contributors_quick_start_embedded.rst
```

Result: **Same 3 skills extracted, 100% drift-free** ✅

---

## Discretionary Decision

I've implemented this multi-source approach because:

1. **Addresses mentor concern directly** — Shows clear path to single source of truth
2. **Backward compatible** — AGENTS.md still works, no breaking changes
3. **Practical demonstration** — Embedded RST example proves pattern without disrupting repo
4. **Framework-ready** — Extraction script handles any text file with markers
5. **Low risk** — Can be gradually rolled out; decision to embed in real docs is independent

The embedded example demonstrates the architectural shift without requiring changes to actual Airflow documentation, leaving that integration decision to the team.
