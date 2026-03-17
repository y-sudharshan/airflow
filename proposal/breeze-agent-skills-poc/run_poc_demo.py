from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROOF_DIR = ROOT / "proof"


def run_step(name: str, command: list[str]) -> dict:
    started = time.perf_counter()
    proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    ended = time.perf_counter()
    return {
        "name": name,
        "command": " ".join(command),
        "returncode": proc.returncode,
        "duration_seconds": round(ended - started, 4),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "ok": proc.returncode == 0,
    }


def write_reports(results: list[dict]) -> None:
    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(ROOT),
        "all_ok": all(step["ok"] for step in results),
        "steps": results,
    }

    (PROOF_DIR / "run_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# PoC Demo Execution Report")
    lines.append("")
    lines.append(f"Generated at: `{summary['generated_at']}`")
    lines.append(f"Overall status: `{'PASS' if summary['all_ok'] else 'FAIL'}`")
    lines.append("")
    lines.append("## Step Results")
    lines.append("")

    for step in results:
        status = "PASS" if step["ok"] else "FAIL"
        lines.append(f"### {step['name']}")
        lines.append("")
        lines.append(f"- Command: `{step['command']}`")
        lines.append(f"- Status: `{status}`")
        lines.append(f"- Exit code: `{step['returncode']}`")
        lines.append(f"- Duration: `{step['duration_seconds']}s`")
        lines.append("")
        lines.append("Output:")
        lines.append("```text")
        combined = (step["stdout"] + ("\n" + step["stderr"] if step["stderr"] else "")).strip()
        lines.append(combined if combined else "<no output>")
        lines.append("```")
        lines.append("")

    (PROOF_DIR / "run_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    python = sys.executable
    steps = [
        ("Generate skills manifest", [python, "scripts/ci/prek/extract_agent_skills.py"]),
        ("Check for drift", [python, "scripts/ci/prek/extract_agent_skills.py", "--check"]),
        ("Run PoC tests", [python, "scripts/ci/prek/test_agent_skills_poc.py"]),
    ]

    results = [run_step(name, cmd) for name, cmd in steps]
    write_reports(results)

    print("PoC demo finished. Reports written to:")
    print(f"- {PROOF_DIR / 'run_report.md'}")
    print(f"- {PROOF_DIR / 'run_report.json'}")

    return 0 if all(step["ok"] for step in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
