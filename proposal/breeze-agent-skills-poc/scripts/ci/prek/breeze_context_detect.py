from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class ExecutionEnvironment(str, Enum):
    HOST = "host"
    BREEZE_CONTAINER = "breeze-container"
    CI = "ci"


class FailureReason(str, Enum):
    NONE = "none"
    MISSING_SYSTEM_DEPS = "missing_system_deps"
    CI_MISMATCH = "ci_mismatch"
    NEEDS_FULL_AIRFLOW_ENV = "needs_full_airflow_env"


@dataclass(frozen=True)
class EnvironmentEvidence:
    environment: ExecutionEnvironment
    reason: str


@dataclass
class CommandMetadata:
    """Metadata about a Breeze command extracted from --help output."""
    name: str
    description: str
    subcommands: list[str] = field(default_factory=list)
    source: str = "cli"  # "manifest" or "cli"


class BreezeMetadataExtractor:
    """Extract command metadata from Breeze CLI when not in manifest."""

    def __init__(self, cache_file: str | None = None):
        self.cache_file = cache_file or ".breeze_metadata_cache.json"
        self._cache: dict[str, CommandMetadata] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        """Load cached metadata from disk if available."""
        if Path(self.cache_file).exists():
            try:
                with open(self.cache_file) as f:
                    data = json.load(f)
                    self._cache = {k: CommandMetadata(**v) for k, v in data.items()}
            except (json.JSONDecodeError, OSError):
                self._cache = {}

    def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            data = {k: {"name": v.name, "description": v.description, "subcommands": v.subcommands, "source": v.source} for k, v in self._cache.items()}
            with open(self.cache_file, "w") as f:
                json.dump(data, f, indent=2)
        except OSError:
            pass

    def extract_from_help(self, command: str = "breeze") -> dict[str, CommandMetadata]:
        """Parse `breeze --help` output to extract subcommands and metadata.
        
        Returns a dict mapping command names to CommandMetadata objects.
        """
        if command in self._cache:
            # Return all cached subcommands for this command
            return {k: v for k, v in self._cache.items() if k != command}

        try:
            result = subprocess.run(
                [command, "--help"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            help_output = result.stdout + result.stderr
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return {}

        # Parse help output for subcommands
        metadata: dict[str, CommandMetadata] = {}
        subcommands: list[str] = []

        for line in help_output.split("\n"):
            stripped = line.strip()
            # Skip empty lines, lines starting with dashes, and header lines
            if not stripped or stripped.startswith("-") or any(kw in stripped.lower() for kw in ["usage", "options", "help", "commands"]):
                continue
            
            # Pattern: "subcommand       Description"
            # Match: word (with optional hyphens) followed by 2+ spaces and description
            match = re.match(r"^([\w][\w\-]*)\s{2,}(.+)$", stripped)
            if match:
                subcommand_name, description = match.groups()
                subcommands.append(subcommand_name)
                metadata[subcommand_name] = CommandMetadata(
                    name=subcommand_name,
                    description=description,
                    source="cli",
                )

        # Cache the metadata
        if metadata:
            main_metadata = CommandMetadata(
                name=command,
                description=f"Breeze command '{command}'",
                subcommands=subcommands,
                source="cli",
            )
            self._cache[command] = main_metadata
            self._cache.update(metadata)
            self._save_cache()
            return metadata
        return metadata


# Global extractor instance
_extractor = BreezeMetadataExtractor()


def detect_environment() -> EnvironmentEvidence:
    if os.getenv("AIRFLOW_BREEZE_CONTAINER") == "true":
        return EnvironmentEvidence(ExecutionEnvironment.BREEZE_CONTAINER, "AIRFLOW_BREEZE_CONTAINER")
    if Path("/.dockerenv").exists():
        return EnvironmentEvidence(ExecutionEnvironment.BREEZE_CONTAINER, "/.dockerenv")
    if Path("/opt/airflow").exists() and Path("/entrypoint_ci.sh").exists():
        return EnvironmentEvidence(ExecutionEnvironment.BREEZE_CONTAINER, "breeze-path-markers")
    if os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true":
        return EnvironmentEvidence(ExecutionEnvironment.CI, "ci-env")
    return EnvironmentEvidence(ExecutionEnvironment.HOST, "default")


def plan_command(skill: dict[str, Any], params: dict[str, str], failure_reason: FailureReason = FailureReason.NONE) -> str:
    """Plan the appropriate command for the skill in the current context.
    
    Manifest-first strategy: use skill definition from manifest.
    Resilient fallback: if command not found, query breeze --help and parse output.
    """
    evidence = detect_environment()

    command_template = skill["local"]
    if evidence.environment == ExecutionEnvironment.BREEZE_CONTAINER:
        if skill["id"] == "run-unit-tests":
            command_template = "pytest {test_path} -xvs"
    elif failure_reason == FailureReason.MISSING_SYSTEM_DEPS and skill.get("fallback"):
        command_template = skill["fallback"]
    elif failure_reason == FailureReason.NEEDS_FULL_AIRFLOW_ENV and skill.get("fallback"):
        command_template = skill["fallback"]
    elif failure_reason == FailureReason.CI_MISMATCH and skill.get("ci"):
        command_template = skill["ci"]

    command = command_template
    for key, value in params.items():
        command = command.replace(f"{{{key}}}", str(value))
    return " ".join(command.split())


def resolve_command_from_metadata(command_name: str, subcommand: str | None = None) -> CommandMetadata | None:
    """Resolve a command from manifest or fallback to CLI metadata extraction.
    
    Manifest-first (Robust): Check skills.json for cached definitions.
    Resilient fallback: Query Breeze CLI if not found.
    """
    # Try manifest first
    try:
        skills_path = Path(__file__).parent.parent.parent.parent / "generated" / "skills.json"
        if skills_path.exists():
            with open(skills_path) as f:
                skills = json.load(f)
                for skill in skills:
                    if skill["id"] == command_name:
                        return CommandMetadata(
                            name=skill["id"],
                            description=skill.get("summary", ""),
                            source="manifest",
                        )
    except (json.JSONDecodeError, OSError):
        pass

    # Fallback to CLI metadata extraction
    metadata = _extractor.extract_from_help("breeze")
    if subcommand and subcommand in metadata:
        return metadata[subcommand]
    if command_name in metadata:
        return metadata[command_name]
    return None
