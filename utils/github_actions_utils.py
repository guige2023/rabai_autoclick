"""
GitHub Actions CI/CD Utilities.

Provides helpers for interacting with GitHub Actions API, workflow management,
secrets handling, and environment setup during action runs.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import hashlib
import hmac
import base64
from typing import Optional, Any
from pathlib import Path


# --------------------------------------------------------------------------- #
# GitHub Actions Environment Variables
# --------------------------------------------------------------------------- #

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "")
GITHUB_SHA = os.getenv("GITHUB_SHA", "")
GITHUB_REF = os.getenv("GITHUB_REF", "")
GITHUB_WORKFLOW = os.getenv("GITHUB_WORKFLOW", "")
GITHUB_ACTION = os.getenv("GITHUB_ACTION", "")
GITHUB_ACTOR = os.getenv("GITHUB_ACTOR", "")
GITHUB_EVENT_NAME = os.getenv("GITHUB_EVENT_NAME", "")
GITHUB_WORKSPACE = os.getenv("GITHUB_WORKSPACE", "")


def is_github_actions() -> bool:
    """Check if running inside a GitHub Actions environment."""
    return bool(GITHUB_REPOSITORY and GITHUB_ACTION)


def get_repository() -> tuple[str, str]:
    """Return (owner, repo) from GITHUB_REPOSITORY."""
    parts = GITHUB_REPOSITORY.split("/", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", "")


# --------------------------------------------------------------------------- #
# Output / State Management
# --------------------------------------------------------------------------- #

def set_output(name: str, value: str) -> None:
    """
    Write a step output value to GITHUB_OUTPUT.

    In GitHub Actions, outputs are written via the GITHUB_OUTPUT file
    in the format: name=value\\n
    """
    output_path = os.getenv("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(f"{name}={value}\\n")


def set_step_summary(content: str) -> None:
    """Append content to the step summary markdown file."""
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as f:
        f.write(content)


def set_env_var(name: str, value: str) -> None:
    """Export an environment variable to GITHUB_ENV."""
    env_path = os.getenv("GITHUB_ENV")
    if not env_path:
        return
    delimiter = hashlib.sha256(os.urandom(32)).hexdigest()[:16]
    with open(env_path, "a", encoding="utf-8") as f:
        f.write(f"{name}<<{delimiter}\\n{value}\\n{delimiter}\\n")


def add_path(directory: str) -> None:
    """Prepend a directory to PATH in GITHUB_ENV."""
    env_path = os.getenv("GITHUB_ENV")
    if not env_path:
        return
    with open(env_path, "a", encoding="utf-8") as f:
        f.write(f"PATH={directory}:$PATH\\n")


# --------------------------------------------------------------------------- #
# Artifact Management
# --------------------------------------------------------------------------- #

def artifact_url(artifact_name: str, run_id: Optional[str] = None) -> str:
    """Build the download URL for a workflow artifact."""
    rid = run_id or GITHUB_RUN_ID
    owner, repo = get_repository()
    return (
        f"https://api.github.com/repos/{owner}/{repo}"
        f"/actions/runs/{rid}/artifacts"
    )


def get_artifact_paths(
    artifact_dir: str = ".",
    pattern: str = "*.zip"
) -> list[Path]:
    """Find all artifact zip files matching a glob pattern."""
    return list(Path(artifact_dir).glob(pattern))


# --------------------------------------------------------------------------- #
# Markdown Report Helpers
# --------------------------------------------------------------------------- #

def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render a markdown table from headers and rows."""
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("|" + "|".join(" --- " for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\\n".join(lines)


def markdown_code_block(code: str, language: str = "") -> str:
    """Wrap text in a fenced code block."""
    return f"```{language}\\n{code}\\n```"


def markdown_collapsed_section(title: str, content: str) -> str:
    """Create a collapsed section using HTML <details>."""
    return (
        f"<details>\n<summary>{title}</summary>\n\n"
        f"{content}\n\n</details>"
    )


# --------------------------------------------------------------------------- #
# Webhook / Event Payloads
# --------------------------------------------------------------------------- #

def get_event_payload() -> dict[str, Any]:
    """Load and return the full event payload JSON from GITHUB_EVENT_PATH."""
    path = os.getenv("GITHUB_EVENT_PATH")
    if not path or not Path(path).exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def verify_github_webhook(
    payload: bytes,
    signature: str,
    secret: str
) -> bool:
    """
    Verify that a webhook payload matches its X-Hub-Signature-256 header.

    GitHub signs payloads with HMAC-SHA256 using the webhook secret.
    """
    if not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()
    return hmac.compare_digest(signature, expected)


def parse_pull_request_payload() -> dict[str, Any]:
    """Extract commonly-used fields from a pull_request event payload."""
    payload = get_event_payload()
    pr = payload.get("pull_request", {})
    return {
        "title": pr.get("title", ""),
        "body": pr.get("body", ""),
        "number": pr.get("number", 0),
        "state": pr.get("state", ""),
        "head_ref": pr.get("head", {}).get("ref", ""),
        "base_ref": pr.get("base", {}).get("ref", ""),
        "author": pr.get("user", {}).get("login", ""),
    }


def parse_workflow_dispatch(
    key: str,
    default: Optional[str] = None
) -> Optional[str]:
    """Extract an input value from a workflow_dispatch event."""
    payload = get_event_payload()
    inputs = payload.get("inputs", {})
    return inputs.get(key, {}).get("value", default)


# --------------------------------------------------------------------------- #
# Matrix Strategy Helpers
# --------------------------------------------------------------------------- #

def parse_matrix() -> dict[str, list[str]]:
    """Return the matrix context from the workflow dispatch payload."""
    payload = get_event_payload()
    matrix = payload.get("matrix", {})
    if not matrix:
        # Fallback to env vars for composite actions
        include_raw = os.getenv("INPUT_MATRIX_INCLUDE", "")
        if include_raw:
            try:
                matrix = json.loads(include_raw)
            except json.JSONDecodeError:
                pass
    return matrix


def should_run_for_matrix(
    include_paths: Optional[list[str]] = None,
    exclude_paths: Optional[list[str]] = None
) -> bool:
    """
    Determine if the current job should run based on changed files.

    Skips when GITHUB_EVENT_NAME is 'pull_request' and no relevant
    files were changed.
    """
    if GITHUB_EVENT_NAME != "pull_request":
        return True
    payload = get_event_payload()
    commits = payload.get("commits", [])
    changed = set()
    for commit in commits:
        changed.update(commit.get("changed_files", []))
        for file in commit.get("added", []) + commit.get("modified", []):
            changed.add(file)
    include = set(include_paths) if include_paths else None
    exclude = set(exclude_paths) if exclude_paths else set()
    if exclude:
        changed -= exclude
    if include:
        return bool(changed & include)
    return True
