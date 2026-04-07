"""
CircleCI CI/CD Utilities.

Helpers for triggering and monitoring CircleCI pipelines,
managing workflows, fetching artifacts, and interacting with
the CircleCI API for CI/CD automation.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

CIRCLECI_TOKEN = os.getenv("CIRCLECI_TOKEN", "")
CIRCLECI_API_BASE = "https://circleci.com/api/v2"


def _headers() -> dict[str, str]:
    return {"Circle-Token": CIRCLECI_TOKEN, "Accept": "application/json"}


def _api(
    method: str,
    path: str,
    params: Optional[dict[str, str]] = None,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{CIRCLECI_API_BASE}{path}"
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
        raise CircleCIAPIError(exc.code, exc.read().decode()) from exc


class CircleCIAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"CircleCI API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Pipelines
# --------------------------------------------------------------------------- #

def list_pipelines(
    project_slug: str,
    branch: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    List recent pipelines for a project.

    Args:
        project_slug: Project identifier (e.g. 'gh/my-org/my-repo').
        branch: Filter by branch name.
        limit: Maximum number of pipelines to return.

    Returns:
        List of pipeline objects.
    """
    params: dict[str, str] = {"page-token": "", "limit": str(limit)}
    if branch:
        params["branch"] = branch
    return _api("GET", f"/project/{project_slug}/pipeline", params=params) or []


def trigger_pipeline(
    project_slug: str,
    branch: str,
    parameters: Optional[dict[str, Any]] = None,
    tag: Optional[str] = None,
) -> dict[str, Any]:
    """
    Trigger a new pipeline run.

    Args:
        project_slug: Project identifier.
        branch: Branch to run the pipeline on.
        parameters: Pipeline parameters.
        tag: Git tag to run (triggers tag-based pipeline).

    Returns:
        Triggered pipeline object.
    """
    body: dict[str, Any] = {"branch": branch}
    if tag:
        body["tag"] = tag
    if parameters:
        body["parameters"] = parameters
    return _api(
        "POST",
        f"/project/{project_slug}/pipeline",
        body=body,
    )


def get_pipeline(pipeline_id: str) -> dict[str, Any]:
    """Fetch a pipeline by its ID."""
    return _api("GET", f"/pipeline/{pipeline_id}")


# --------------------------------------------------------------------------- #
# Workflows
# --------------------------------------------------------------------------- #

def list_workflows(pipeline_id: str) -> list[dict[str, Any]]:
    """List workflows for a pipeline."""
    return _api("GET", f"/pipeline/{pipeline_id}/workflow") or []


def get_workflow(workflow_id: str) -> dict[str, Any]:
    """Fetch a workflow by ID."""
    return _api("GET", f"/workflow/{workflow_id}")


def cancel_workflow(workflow_id: str) -> dict[str, Any]:
    """Cancel an in-progress workflow."""
    return _api("POST", f"/workflow/{workflow_id}/cancel")


def approve_workflow_job(workflow_id: str, job_id: str) -> dict[str, Any]:
    """Approve a pending approval job in a workflow."""
    return _api("POST", f"/workflow/{workflow_id}/approve/{job_id}")


# --------------------------------------------------------------------------- #
# Jobs / Steps
# --------------------------------------------------------------------------- #

def list_jobs(workflow_id: str) -> list[dict[str, Any]]:
    """List all jobs in a workflow."""
    return _api("GET", f"/workflow/{workflow_id}/job") or []


def get_job(job_id: str) -> dict[str, Any]:
    """Fetch job details by ID."""
    return _api("GET", f"/job/{job_id}")


def get_job_artifacts(job_id: str) -> list[dict[str, Any]]:
    """Return artifacts produced by a job."""
    return _api("GET", f"/project/all/{job_id}/artifacts") or []


def download_artifact(
    url: str,
    output_path: str,
) -> None:
    """
    Download a single artifact to disk.

    Args:
        url: Artifact download URL from the artifacts list.
        output_path: Local destination file path.
    """
    import pathlib
    req = urllib.request.Request(
        url,
        headers={"Circle-Token": CIRCLECI_TOKEN},
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        pathlib.Path(output_path).write_bytes(resp.read())


# --------------------------------------------------------------------------- #
# User & Insights
# --------------------------------------------------------------------------- #

def me() -> dict[str, Any]:
    """Return information about the authenticated user."""
    return _api("GET", "/me")


def workflow_summary(
    project_slug: str,
    branch: Optional[str] = None,
) -> dict[str, Any]:
    """Return aggregated workflow statistics."""
    path = f"/insights/{project_slug}/workflows"
    params: dict[str, str] = {}
    if branch:
        params["branch"] = branch
    return _api("GET", path, params=params)


def job_summary(
    project_slug: str,
    workflow_name: str,
    branch: Optional[str] = None,
) -> dict[str, Any]:
    """Return job-level statistics for a workflow."""
    params: dict[str, str] = {}
    if branch:
        params["branch"] = branch
    return _api(
        "GET",
        f"/insights/{project_slug}/workflows/{workflow_name}/jobs",
        params=params,
    )


# --------------------------------------------------------------------------- #
# Environment Variables / Settings
# --------------------------------------------------------------------------- #

def list_env_vars(project_slug: str) -> list[dict[str, Any]]:
    """List environment variables for a project (names only, values hidden)."""
    return _api("GET", f"/project/{project_slug}/envvar") or []


def add_env_var(
    project_slug: str,
    name: str,
    value: str,
) -> dict[str, Any]:
    """Add or update an environment variable for a project."""
    return _api(
        "PUT",
        f"/project/{project_slug}/envvar/{name}",
        body={"value": value},
    )


def delete_env_var(project_slug: str, name: str) -> None:
    """Delete an environment variable."""
    _api("DELETE", f"/project/{project_slug}/envvar/{name}")
