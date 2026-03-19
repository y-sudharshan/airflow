from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
AGENTS_FILE = ROOT / "AGENTS.md"
OUTPUT_FILE = ROOT / "generated" / "skills.json"

# Source files for skill extraction (support multiple sources)
# Can include: AGENTS.md (backward compatible), contributing docs with embedded skills
SOURCE_FILES = [
    AGENTS_FILE,  # Standalone skills file (primary for now)
    # Transition path: embed skills directly in contributing docs
    # ROOT / "contributing-docs" / "08_static_code_checks.rst",
    # ROOT / "contributing-docs" / "09_testing.rst",
    # ROOT / "contributing-docs" / "03_contributors_quick_start.rst",
]

BLOCK_RE = re.compile(
    r"<!-- agent-skill:start (?P<id>[a-z0-9\-]+) -->\n(?P<body>.*?)<!-- agent-skill:end (?P=id) -->",
    re.DOTALL,
)
LINE_RE = re.compile(r"^(?P<key>[a-z_]+):\s*(?P<value>.*)$")
REQUIRED_KEYS = {"id", "context", "kind", "summary", "local", "prereqs"}
ALLOWED_CONTEXTS = {"host", "either", "breeze-container", "ci"}
ALLOWED_KINDS = {"workflow"}
ALLOWED_FAILURE_CONDITIONS = {"", "missing_system_deps", "ci_mismatch", "needs_full_airflow_env"}


def parse_blocks(text: str) -> list[dict[str, object]]:
    skills: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    for match in BLOCK_RE.finditer(text):
        body = match.group("body")
        parsed: dict[str, object] = {}
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            line_match = LINE_RE.match(line)
            if not line_match:
                raise ValueError(f"Invalid line in skill block: {line}")
            key = line_match.group("key")
            value = line_match.group("value")
            parsed[key] = value
        missing = REQUIRED_KEYS - parsed.keys()
        if missing:
            raise ValueError(f"Skill {parsed.get('id', match.group('id'))} missing required keys: {sorted(missing)}")

        skill_id = str(parsed["id"])
        if skill_id in seen_ids:
            raise ValueError(f"Duplicate skill id found: {skill_id}")
        seen_ids.add(skill_id)

        context = str(parsed["context"])
        if context not in ALLOWED_CONTEXTS:
            raise ValueError(f"Skill {skill_id} has invalid context: {context}")

        kind = str(parsed["kind"])
        if kind not in ALLOWED_KINDS:
            raise ValueError(f"Skill {skill_id} has invalid kind: {kind}")

        fallback_condition = str(parsed.get("fallback_condition", "")).strip()
        if fallback_condition not in ALLOWED_FAILURE_CONDITIONS:
            raise ValueError(f"Skill {skill_id} has invalid fallback_condition: {fallback_condition}")

        if str(parsed["local"]).startswith("breeze"):
            raise ValueError(f"Skill {parsed['id']} violates local-first contract: local starts with breeze")

        prereqs = [item.strip() for item in str(parsed["prereqs"]).split(",") if item.strip()]
        if not prereqs:
            raise ValueError(f"Skill {skill_id} must contain at least one prereq")
        parsed["prereqs"] = prereqs

        skills.append(parsed)

    if not skills:
        raise ValueError("No agent-skill blocks found in source files")

    return skills


def render(skills: list[dict[str, object]]) -> str:
    return json.dumps({"skills": skills}, indent=2) + "\n"


def get_source_files() -> list[Path]:
    """Get list of source files to extract skills from.
    
    Returns files that exist. Allows gradual migration from AGENTS.md
    to embedded skills in contributing documentation.
    """
    return [f for f in SOURCE_FILES if f.exists()]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract agent skills from documentation files (AGENTS.md, RST, Markdown, etc.)"
    )
    parser.add_argument("--check", action="store_true", help="Check if skills.json is in sync with sources")
    parser.add_argument("--source", help="Override source file path (default: AGENTS.md)")
    args = parser.parse_args()

    # Allow override of source file for flexibility
    if args.source:
        source_file = Path(args.source)
    else:
        available_sources = get_source_files()
        if not available_sources:
            print(f"ERROR: No source files found. Expected: {SOURCE_FILES}")
            return 1
        source_file = available_sources[0]

    if not source_file.exists():
        print(f"ERROR: Source file not found: {source_file}")
        return 1

    text = source_file.read_text(encoding="utf-8")
    skills = parse_blocks(text)
    rendered = render(skills)

    if args.check:
        current = OUTPUT_FILE.read_text(encoding="utf-8") if OUTPUT_FILE.exists() else ""
        if current != rendered:
            print(f"DRIFT: generated/skills.json is out of sync with {source_file.name}")
            return 1
        print(f"OK: skills.json is in sync with {source_file.name}")
        return 0

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(rendered, encoding="utf-8")
    print(f"Written {len(skills)} skill(s) to {OUTPUT_FILE.relative_to(ROOT)} (from {source_file.name})")
    for skill in skills:
        print(f"  - {skill['id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
