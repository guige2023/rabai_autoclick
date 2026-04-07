"""
Buildkite CI/CD Utilities.

Helpers for triggering and monitoring Buildkite pipeline builds,
managing agents, artifacts, and interacting with the Buildkite API.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BUILDKITE_TOKEN = os.getenv("BUILDKITE_TOKEN", "")
BUILDKITE_API_BASE = "https://api.buildkite.com/v2"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {BUILDKITE_TOKEN}",
        "Content-Type": "application/json",
    }


def _api(
    method: str,
    path: str,
    params: Optional[dict[str, str]] = None,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{BUILDKITE_API_BASE}{path}"
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}?{qs}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise BuildkiteAPIError(exc.code, exc.read().decode()) from exc


class BuildkiteAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Buildkite API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Organizations
# --------------------------------------------------------------------------- #

def get_org(org_slug: str) -> dict[str, Any]:
    """Return organization details."""
    return _api("GET", f"/organizations/{org_slug}")


# --------------------------------------------------------------------------- #
# Pipelines
# --------------------------------------------------------------------------- #

def list_pipelines(org_slug: str) -> list[dict[str, Any]]:
    """Return all pipelines for an organization."""
    return _api("GET", f"/organizations/{org_slug}/pipelines") or []


def get_pipeline(org_slug: str, pipeline_slug: str) -> dict[str, Any]:
    """Fetch a pipeline by slug."""
    return _api("GET", f"/organizations/{org_slug}/pipelines/{pipeline_slug}")


def create_pipeline(
    org_slug: str,
    name: str,
    repository: str,
    config: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Create a new Buildkite pipeline.

    Args:
        org_slug: Organization slug.
        name: Pipeline name.
        repository: Git repository URL.
        config: Optional pipeline configuration (steps, etc.).

    Returns:
        Created pipeline object.
    """
    payload: dict[str, Any] = {
        "name": name,
        "repository": repository,
    }
    if config:
        payload.update(config)
    return _api(
        "POST",
        f"/organizations/{org_slug}/pipelines",
        body=payload,
    )


# --------------------------------------------------------------------------- #
# Builds
# --------------------------------------------------------------------------- #

def trigger_build(
    org_slug: str,
    pipeline_slug: str,
    branch: str = "main",
    commit: Optional[str] = None,
    message: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Trigger a new build on a pipeline.

    Args:
        org_slug: Organization slug.
        pipeline_slug: Pipeline slug.
        branch: Git branch to build.
        commit: Specific commit SHA.
        message: Build commit message.
        env: Environment variables for the build.

    Returns:
        Created build object.
    """
    payload: dict[str, Any] = {"branch": branch}
    if commit:
        payload["commit"] = commit
    if message:
        payload["message"] = message
    if env:
        payload["env"] = env
    return _api(
        "POST",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}/builds",
        body=payload,
    )


def list_builds(
    org_slug: str,
    pipeline_slug: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """List builds for a pipeline."""
    return _api(
        "GET",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}/builds",
        params={"per_page": str(limit)},
    ) or []


def get_build(org_slug: str, pipeline_slug: str, build_number: int) -> dict[str, Any]:
    """Fetch a specific build."""
    return _api(
        "GET",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}/builds/{build_number}",
    )


def cancel_build(
    org_slug: str,
    pipeline_slug: str,
    build_number: int,
) -> dict[str, Any]:
    """Cancel an in-progress build."""
    return _api(
        "POST",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}"
        f"/builds/{build_number}/cancel",
    )


def rebuild_build(
    org_slug: str,
    pipeline_slug: str,
    build_number: int,
) -> dict[str, Any]:
    """Rebuild a completed build."""
    return _api(
        "POST",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}"
        f"/builds/{build_number}/rebuild",
    )


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #

def get_build_jobs(
    org_slug: str,
    pipeline_slug: str,
    build_number: int,
) -> list[dict[str, Any]]:
    """Return all jobs for a build."""
    return _api(
        "GET",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}"
        f"/builds/{build_number}/jobs",
    ) or []


def retry_job(
    org_slug: str,
    pipeline_slug: str,
    build_number: int,
    job_id: str,
) -> dict[str, Any]:
    """Retry a failed job."""
    return _api(
        "POST",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}"
        f"/builds/{build_number}/jobs/{job_id}/retry",
    )


def cancel_job(
    org_slug: str,
    pipeline_slug: str,
    build_number: int,
    job_id: str,
) -> dict[str, Any]:
    """Cancel a running job."""
    return _api(
        "POST",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}"
        f"/builds/{build_number}/jobs/{job_id}/cancel",
    )


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #

def list_artifacts(
    org_slug: str,
    pipeline_slug: str,
    build_number: int,
) -> list[dict[str, Any]]:
    """List artifacts for a build."""
    return _api(
        "GET",
        f"/organizations/{org_slug}/pipelines/{pipeline_slug}"
        f"/builds/{build_number}/artifacts",
    ) or []


def download_artifact(
    artifact_url: str,
    output_path: str,
) -> None:
    """Download an artifact to disk."""
    import pathlib
    req = urllib.request.Request(
        artifact_url,
        headers={"Authorization": f"Bearer {BUILDKITE_TOKEN}"},
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        pathlib.Path(output_path).write_bytes(resp.read())


# --------------------------------------------------------------------------- #
# Agents
# --------------------------------------------------------------------------- #

def list_agents(
    org_slug: str,
    include: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """
    List build agents.

    Args:
        org_slug: Organization slug.
        include: Filter by agent state: 'busy', 'idle', 'stopped'.
    """
    params: dict[str, str] = {}
    if include:
        params["include"] = ",".join(include)
    return _api("GET", f"/organizations/{org_slug}/agents", params=params) or []


def get_agent(org_slug: str, agent_id: str) -> dict[str, Any]:
    """Fetch an agent by ID."""
    return _api("GET", f"/organizations/{org_slug}/agents/{agent_id}")


def stop_agent(org_slug: str, agent_id: str) -> None:
    """Stop an agent gracefully."""
    _api("DELETE", f"/organizations/{org_slug}/agents/{agent_id}")
