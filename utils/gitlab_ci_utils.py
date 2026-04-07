"""
GitLab CI/CD Utilities.

Helpers for triggering GitLab CI pipelines, managing jobs,
downloading artifacts, and interacting with the GitLab API
for pipeline automation.

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

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "")
GITLAB_HOST = os.getenv("GITLAB_HOST", "https://gitlab.com")
GITLAB_API_BASE = f"{GITLAB_HOST}/api/v4"


def _headers() -> dict[str, str]:
    return {
        "PRIVATE-TOKEN": GITLAB_TOKEN,
        "Content-Type": "application/json",
    }


def _api(
    method: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{GITLAB_API_BASE}{path}"
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
        raise GitLabAPIError(exc.code, exc.read().decode()) from exc


class GitLabAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"GitLab API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Projects
# --------------------------------------------------------------------------- #

def get_project(project_id: str) -> dict[str, Any]:
    """Fetch a project by ID or path-encoded namespace/name."""
    return _api("GET", f"/projects/{urllib.parse.quote(project_id, safe='')}")


def list_projects(
    membership: bool = True,
    per_page: int = 50,
) -> list[dict[str, Any]]:
    """List accessible projects."""
    return _api(
        "GET",
        "/projects",
        params={"membership": str(membership), "per_page": str(per_page)},
    ) or []


# --------------------------------------------------------------------------- #
# Pipelines
# --------------------------------------------------------------------------- #

def trigger_pipeline(
    project_id: str,
    ref: str,
    variables: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Trigger a new CI pipeline.

    Args:
        project_id: Project ID or path.
        ref: Git branch or tag to run.
        variables: CI/CD variable overrides.

    Returns:
        Created pipeline object with id, status, web_url.
    """
    body: dict[str, Any] = {"ref": ref}
    if variables:
        body["variables"] = [
            {"key": k, "value": v} for k, v in variables.items()
        ]
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/pipeline",
        body=body,
    )


def list_pipelines(
    project_id: str,
    status: Optional[str] = None,
    ref: Optional[str] = None,
    per_page: int = 20,
) -> list[dict[str, Any]]:
    """List recent pipelines for a project."""
    params: dict[str, Any] = {"per_page": str(per_page)}
    if status:
        params["status"] = status
    if ref:
        params["ref"] = ref
    return _api(
        "GET",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/pipelines",
        params=params,
    ) or []


def get_pipeline(project_id: str, pipeline_id: int) -> dict[str, Any]:
    """Fetch a specific pipeline."""
    return _api(
        "GET",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/pipelines/{pipeline_id}",
    )


def retry_pipeline(project_id: str, pipeline_id: int) -> dict[str, Any]:
    """Retry a failed or canceled pipeline."""
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}"
        f"/pipelines/{pipeline_id}/retry",
    )


def cancel_pipeline(project_id: str, pipeline_id: int) -> dict[str, Any]:
    """Cancel an in-progress pipeline."""
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}"
        f"/pipelines/{pipeline_id}/cancel",
    )


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #

def list_pipeline_jobs(
    project_id: str,
    pipeline_id: int,
    per_page: int = 100,
) -> list[dict[str, Any]]:
    """List all jobs in a pipeline."""
    return _api(
        "GET",
        f"/projects/{urllib.parse.quote(project_id, safe='')}"
        f"/pipelines/{pipeline_id}/jobs",
        params={"per_page": str(per_page)},
    ) or []


def get_job(project_id: str, job_id: int) -> dict[str, Any]:
    """Fetch a single job."""
    return _api(
        "GET",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/jobs/{job_id}",
    )


def retry_job(project_id: str, job_id: int) -> dict[str, Any]:
    """Retry a failed job."""
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/jobs/{job_id}/retry",
    )


def cancel_job(project_id: str, job_id: int) -> dict[str, Any]:
    """Cancel a running job."""
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/jobs/{job_id}/cancel",
    )


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #

def download_job_artifacts(
    project_id: str,
    job_id: int,
    output_path: str,
) -> None:
    """
    Download artifacts for a job.

    Args:
        project_id: Project ID or path.
        job_id: Job ID.
        output_path: Local destination for the zip archive.
    """
    import pathlib
    url = (
        f"{GITLAB_API_BASE}/projects/"
        f"{urllib.parse.quote(project_id, safe='')}"
        f"/jobs/{job_id}/artifacts"
    )
    req = urllib.request.Request(
        url, headers={"PRIVATE-TOKEN": GITLAB_TOKEN}
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        pathlib.Path(output_path).write_bytes(resp.read())


def get_job_trace(project_id: str, job_id: int) -> str:
    """Fetch the raw build log for a job."""
    url = (
        f"{GITLAB_API_BASE}/projects/"
        f"{urllib.parse.quote(project_id, safe='')}"
        f"/jobs/{job_id}/trace"
    )
    req = urllib.request.Request(url, headers={"PRIVATE-TOKEN": GITLAB_TOKEN})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


# --------------------------------------------------------------------------- #
# Variables
# --------------------------------------------------------------------------- #

def list_variables(project_id: str) -> list[dict[str, Any]]:
    """List CI/CD variables for a project."""
    return _api(
        "GET",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/variables",
    ) or []


def create_variable(
    project_id: str,
    key: str,
    value: str,
    masked: bool = False,
    protected: bool = False,
) -> dict[str, Any]:
    """Create or update a CI/CD variable."""
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/variables",
        body={
            "key": key,
            "value": value,
            "masked": masked,
            "protected": protected,
        },
    )


def delete_variable(project_id: str, key: str) -> None:
    """Delete a CI/CD variable."""
    _api(
        "DELETE",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/variables/{key}",
    )


# --------------------------------------------------------------------------- #
# Pipeline Schedules
# --------------------------------------------------------------------------- #

def list_schedules(project_id: str) -> list[dict[str, Any]]:
    """List pipeline schedules for a project."""
    return _api(
        "GET",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/pipeline_schedules",
    ) or []


def create_schedule(
    project_id: str,
    ref: str,
    cron: str,
    description: str = "",
) -> dict[str, Any]:
    """
    Create a pipeline schedule.

    Args:
        project_id: Project ID.
        ref: Target branch/tag.
        cron: Cron expression (e.g. '0 2 * * *').
        description: Schedule description.
    """
    return _api(
        "POST",
        f"/projects/{urllib.parse.quote(project_id, safe='')}/pipeline_schedules",
        body={
            "ref": ref,
            "cron": cron,
            "description": description,
        },
    )
