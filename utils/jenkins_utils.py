"""
Jenkins CI/CD Utilities.

Helpers for interacting with the Jenkins API, triggering builds,
monitoring pipeline status, fetching artifacts, and managing nodes/agents.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import base64
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

JENKINS_URL = os.getenv("JENKINS_URL", "http://localhost:8080")
JENKINS_USER = os.getenv("JENKINS_USER", "")
JENKINS_TOKEN = os.getenv("JENKINS_TOKEN", "")  # Can be API token or personal token


def _auth() -> str:
    creds = f"{JENKINS_USER}:{JENKINS_TOKEN}"
    return "Basic " + base64.b64encode(creds.encode()).decode()


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _api(
    method: str,
    path: str,
    params: Optional[dict[str, str]] = None,
    data: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{JENKINS_URL}{path}"
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}?{qs}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(
        url, data=body, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read()
            if not content:
                return {}
            return json.loads(content)
    except urllib.error.HTTPError as exc:
        raise JenkinsAPIError(exc.code, exc.read().decode()) from exc


class JenkinsAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Jenkins API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Queue
# --------------------------------------------------------------------------- #

def get_queue() -> list[dict[str, Any]]:
    """Return the current Jenkins build queue."""
    return _api("GET", "/queue/api/json") or []


def cancel_queue(item_id: int) -> None:
    """Cancel a queued build by item ID."""
    _api("POST", f"/queue/cancelItem", params={"id": str(item_id)})


# --------------------------------------------------------------------------- #
# Jobs / Projects
# --------------------------------------------------------------------------- #

def list_jobs() -> list[dict[str, Any]]:
    """Return all top-level jobs."""
    data = _api("GET", "/api/json")
    return data.get("jobs", []) if isinstance(data, dict) else []


def get_job(name: str) -> dict[str, Any]:
    """Return job configuration and metadata."""
    return _api("GET", f"/job/{urllib.parse.quote(name)}/api/json")


def create_job(name: str, config_xml: str) -> None:
    """
    Create a new Jenkins job from XML config.

    Args:
        name: Job name.
        config_xml: Full Jenkins job configuration XML.
    """
    data = config_xml.encode()
    url = f"{JENKINS_URL}/createItem?name={urllib.parse.quote(name)}"
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": _auth(),
            "Content-Type": "application/xml",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status not in (200, 201):
                raise JenkinsAPIError(resp.status, "Job creation failed")
    except urllib.error.HTTPError as exc:
        raise JenkinsAPIError(exc.code, exc.read().decode()) from exc


def delete_job(name: str) -> None:
    """Delete a job."""
    _api("POST", f"/job/{urllib.parse.quote(name)}/doDelete")


def build_job(
    name: str,
    parameters: Optional[dict[str, str]] = None,
    token: Optional[str] = None,
) -> int:
    """
    Trigger a build for a job.

    Returns the queue item number.

    Args:
        name: Job name.
        parameters: Build parameters for parameterized jobs.
        token: Optional build trigger token.

    Returns:
        Queue item number.
    """
    job_path = f"/job/{urllib.parse.quote(name)}/build"
    params: dict[str, str] = {}
    if token:
        params["token"] = token
    if parameters:
        # Parameterized build
        job_path = f"/job/{urllib.parse.quote(name)}/buildWithParameters"
        params.update(parameters)
    _api("POST", job_path, params=params)
    # Fetch queue to get item number
    queue = get_queue()
    for item in queue:
        if item.get("task", {}).get("name") == name:
            return item.get("id", 0)
    return 0


def last_build(name: str) -> dict[str, Any]:
    """Return the last build metadata for a job."""
    return _api("GET", f"/job/{urllib.parse.quote(name)}/lastBuild/api/json")


def build_info(name: str, number: int) -> dict[str, Any]:
    """Return metadata for a specific build number."""
    return _api("GET", f"/job/{urllib.parse.quote(name)}/{number}/api/json")


# --------------------------------------------------------------------------- #
# Pipelines
# --------------------------------------------------------------------------- #

def pipeline_runs(name: str) -> list[dict[str, Any]]:
    """Return all pipeline runs for a multibranch or pipeline job."""
    return _api("GET", f"/job/{urllib.parse.quote(name)}/api/json") or []


def get_pipeline_stage(
    name: str,
    run_id: str,
    stage: str,
) -> dict[str, Any]:
    """Return details for a specific pipeline stage."""
    return _api(
        "GET",
        f"/job/{urllib.parse.quote(name)}/{run_id}/wfapi/describe"
    ) or {}


def stop_pipeline(name: str, run_id: str) -> None:
    """Stop an in-progress pipeline."""
    _api("POST", f"/job/{urllib.parse.quote(name)}/{run_id}/stop")


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #

def list_artifacts(name: str, build_number: int) -> list[dict[str, Any]]:
    """Return artifacts for a build."""
    data = _api("GET", f"/job/{urllib.parse.quote(name)}/{build_number}/api/json")
    return data.get("artifacts", []) if isinstance(data, dict) else []


def download_artifact(
    name: str,
    build_number: int,
    artifact_path: str,
    output_path: str,
) -> None:
    """
    Download a build artifact to disk.

    Args:
        name: Job name.
        build_number: Build number.
        artifact_path: Relative path of the artifact within the job.
        output_path: Local destination file path.
    """
    import pathlib
    url = (
        f"{JENKINS_URL}/job/{urllib.parse.quote(name)}/{build_number}"
        f"/artifact/{artifact_path}"
    )
    req = urllib.request.Request(url, headers={"Authorization": _auth()})
    with urllib.request.urlopen(req, timeout=300) as resp:
        content = resp.read()
    pathlib.Path(output_path).write_bytes(content)


# --------------------------------------------------------------------------- #
# Nodes / Agents
# --------------------------------------------------------------------------- #

def list_nodes() -> list[dict[str, Any]]:
    """Return all Jenkins nodes/agents."""
    return _api("GET", "/computer/api/json") or []


def get_node(name: str) -> dict[str, Any]:
    """Return node details."""
    return _api("GET", f"/computer/{urllib.parse.quote(name)}/config.xml")


def set_node_offline(name: str, message: str = "") -> None:
    """Set a node to offline."""
    body: dict[str, str] = {"offlineMessage": message}
    _api("POST", f"/computer/{urllib.parse.quote(name)}/toggleOffline", data=body)


def set_node_online(name: str) -> None:
    """Bring a node back online."""
    set_node_offline(name)


# --------------------------------------------------------------------------- #
# System Information
# --------------------------------------------------------------------------- #

def get_jenkins_version() -> str:
    """Return the Jenkins version string."""
    url = f"{JENKINS_URL}/api/json"
    req = urllib.request.Request(url, headers={"Authorization": _auth()})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("jenkins", "")
    except Exception:
        # Fallback: check the version endpoint
        req2 = urllib.request.Request(
            f"{JENKINS_URL}/login",
            headers={"Authorization": _auth()},
            method="HEAD",
        )
        return req2.get_header("X-Jenkins", "unknown")
