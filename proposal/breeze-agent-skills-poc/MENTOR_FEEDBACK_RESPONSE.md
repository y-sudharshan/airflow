# Addressing Mentor Feedback: Embedded Skills Pattern

This section documents how the proposal addresses the key architectural concern raised in review: **Skills should be embedded in contributing documentation, not in a separate file.**

## Quick Summary

**Mentor Concern:** "AGENTS.md is separate from contributing docs. Drift risk. Docs should be single source of truth."

**Our Solution:** Multi-source extraction framework that supports embedded skills in RST/Markdown documentation.

## Files

- **[ADDRESSING_MENTOR_CONCERN.md](./ADDRESSING_MENTOR_CONCERN.md)** — Full explanation of the transition path
- **`contributing-docs-example/03_contributors_quick_start_embedded.rst`** — Example showing embedded skill markers in realistic RST documentation
- **`scripts/ci/prek/extract_agent_skills_v2.py`** — Enhanced extraction script supporting multiple sources (.md, .rst, etc.)

## Key Improvements

| Aspect | Before | After |
|--------|--------|-------|
| **Source Files** | AGENTS.md only | AGENTS.md + any .rst/.md with markers |
| **Drift Risk** | Medium (separate file) | Low (embedded = single update) |
| **Framework** | Single-source | Multi-source with fallback |
| **RST Support** | No | Yes |
| **Transition Path** | None | Clear: comment/uncomment sources |

## How to Use

**Extract from embedded RST example:**
```bash
cd scripts/ci/prek
python extract_agent_skills_v2.py --source \
  ../../contributing-docs-example/03_contributors_quick_start_embedded.rst
```

**Extract from current AGENTS.md (default):**
```bash
python extract_agent_skills_v2.py
```

**Future: Extract from real Airflow contributing docs (when ready):**
```bash
# Uncomment sources in extract_agent_skills_v2.py:
# SOURCE_FILES = [
#     AGENTS_FILE,
#     ROOT / "contributing-docs" / "08_static_code_checks.rst",
#     ROOT / "contributing-docs" / "09_testing.rst",
# ]
python extract_agent_skills_v2.py
```

## Why This Approach

1. **Backward Compatible** — AGENTS.md still works as primary source during transition
2. **Demonstrates Pattern** — RST example shows embedded skills work with real doc format
3. **Framework-Ready** — Script handles any text file (not locked to specific format)
4. **Addresses Concern** — Proves single source of truth pattern is viable
5. **Low Risk** — No changes to actual Airflow docs yet; transition is opt-in

## Next Steps

To fully adopt embedded pattern:
1. Comment out AGENTS_FILE from SOURCE_FILES
2. Uncomment the actual contributing doc paths
3. Move AGENTS.md content into appropriate rst files
4. Update prek drift check hook

See [ADDRESSING_MENTOR_CONCERN.md](./ADDRESSING_MENTOR_CONCERN.md) for full roadmap.
