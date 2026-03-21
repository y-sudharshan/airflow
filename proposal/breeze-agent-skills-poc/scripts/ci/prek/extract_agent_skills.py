from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
AIRFLOW_ROOT = ROOT.parents[1]
AGENTS_FILE = AIRFLOW_ROOT / "contributing-docs" / "03_contributors_quick_start.rst"
OUTPUT_FILE = ROOT / "generated" / "skills.json"

BLOCK_RE = re.compile(
    r"\s*<!-- agent-skill:start (?P<id>[a-z0-9\-]+) -->\n(?P<body>.*?)\s*<!-- agent-skill:end (?P=id) -->",
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
        raise ValueError("No agent-skill blocks found in source document")

    return skills


def render(skills: list[dict[str, object]]) -> str:
    return json.dumps({"skills": skills}, indent=2) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--source", default=str(AGENTS_FILE))
    args = parser.parse_args()

    source_file = Path(args.source)
    if not source_file.exists():
        print(f"ERROR: source file not found: {source_file}")
        return 1

    text = source_file.read_text(encoding="utf-8")
    skills = parse_blocks(text)
    rendered = render(skills)

    if args.check:
        current = OUTPUT_FILE.read_text(encoding="utf-8") if OUTPUT_FILE.exists() else ""
        if current != rendered:
            print(f"DRIFT: generated/skills.json is out of sync with {source_file}")
            return 1
        print(f"OK: skills.json is in sync with {source_file}")
        return 0

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(rendered, encoding="utf-8")
    print(f"Written {len(skills)} skill(s) to {OUTPUT_FILE.relative_to(ROOT)}")
    print(f"Source of truth: {source_file}")
    for skill in skills:
        print(f"  - {skill['id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
