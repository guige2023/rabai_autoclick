"""
Vercel Deployment Utilities.

Helpers for deploying to Vercel, managing projects, environment variables,
aliases, and monitoring deployments via the Vercel API.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import time
import urllib.request
import urllib.error
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

VERCEL_TOKEN = os.getenv("VERCEL_TOKEN", "")
VERCEL_TEAM_ID = os.getenv("VERCEL_TEAM_ID", "")
VERCEL_API_BASE = "https://api.vercel.com/v6"


def _headers() -> dict[str, str]:
    """Return common HTTP headers for Vercel API requests."""
    headers = {"Authorization": f"Bearer {VERCEL_TOKEN}"}
    if VERCEL_TEAM_ID:
        headers["X-Vercel-Team-Id"] = VERCEL_TEAM_ID
    return headers


def _request(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Make an authenticated request to the Vercel API."""
    url = f"{VERCEL_API_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode()
        raise VercelAPIError(exc.code, body_text) from exc


class VercelAPIError(Exception):
    """Raised when a Vercel API call fails."""

    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Vercel API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Deployment
# --------------------------------------------------------------------------- #

def deploy(
    project: str,
    directory: str = ".",
    token: Optional[str] = None,
    team_id: Optional[str] = None,
    target: Optional[str] = None,
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Trigger a new deployment for a Vercel project.

    Args:
        project: Vercel project name.
        directory: Local directory to deploy (default: current dir).
        token: Override Vercel token.
        team_id: Override team ID.
        target: Deployment environment (production, preview, or staging).
        metadata: Key-value annotations attached to the deployment.

    Returns:
        Deployment object from the API.
    """
    t = token or VERCEL_TOKEN
    headers = {"Authorization": f"Bearer {t}"}
    if team_id or VERCEL_TEAM_ID:
        headers["X-Vercel-Team-Id"] = (team_id or VERCEL_TEAM_ID)

    body: dict[str, Any] = {
        "project": project,
        "target": target,
    }
    if metadata:
        body["meta"] = metadata

    req = urllib.request.Request(
        f"{VERCEL_API_BASE}/deployments",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            return result
    except urllib.error.HTTPError as exc:
        raise VercelAPIError(exc.code, exc.read().decode()) from exc


def get_deployment(deployment_id: str) -> dict[str, Any]:
    """Fetch a single deployment by ID."""
    return _request("GET", f"/deployments/{deployment_id}")


def list_deployments(
    project: str,
    limit: int = 20,
    state: Optional[str] = None,
) -> list[dict[str, Any]]:
    """List recent deployments for a project."""
    params: dict[str, str] = {"limit": str(limit), "project": project}
    if state:
        params["state"] = state
    data = _request("GET", "/deployments", params=params)
    return data.get("deployments", [])


def wait_for_deployment(
    deployment_id: str,
    timeout: int = 300,
    poll_interval: int = 5,
) -> dict[str, Any]:
    """Poll until a deployment reaches READY or ERROR state."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        dep = get_deployment(deployment_id)
        state = dep.get("readyState", "")
        if state == "READY":
            return dep
        if state == "ERROR":
            raise VercelAPIError(502, f"Deployment {deployment_id} failed")
        time.sleep(poll_interval)
    raise TimeoutError(f"Deployment {deployment_id} did not complete within {timeout}s")


def cancel_deployment(deployment_id: str) -> dict[str, Any]:
    """Cancel an in-progress deployment."""
    return _request("PATCH", f"/deployments/{deployment_id}", body={"cancel": True})


# --------------------------------------------------------------------------- #
# Environment Variables
# --------------------------------------------------------------------------- #

def get_env_vars(project: str) -> list[dict[str, Any]]:
    """Retrieve all environment variables for a project."""
    return _request("GET", f"/projects/{project}/env") or []


def set_env_var(
    project: str,
    key: str,
    value: str,
    env: str = "production",
    secret: bool = True,
) -> dict[str, Any]:
    """
    Create or update an environment variable.

    Args:
        project: Project name.
        key: Variable name.
        value: Variable value (plain text or secret reference).
        env: Target environment (production, preview, development, or all).
        secret: If True, the value is treated as a secret reference.
    """
    return _request(
        "POST",
        f"/projects/{project}/env",
        body={
            "key": key,
            "value": value,
            "target": [env] if env != "all" else ["production", "preview", "development"],
            "type": "SECRET" if secret else "PLAINTEXT",
        },
    )


def delete_env_var(project: str, key: str, env: str = "production") -> None:
    """Delete an environment variable by key."""
    _request(
        "DELETE",
        f"/projects/{project}/env/{key}",
        params={"target": env},
    )


# --------------------------------------------------------------------------- #
# Aliases / Domains
# --------------------------------------------------------------------------- #

def assign_alias(deployment_id: str, alias: str) -> dict[str, Any]:
    """Assign a domain alias to a deployment."""
    return _request(
        "POST",
        f"/deployments/{deployment_id}/aliases",
        body={"alias": alias},
    )


def list_domains(project: str) -> list[dict[str, Any]]:
    """List all domains configured for a project."""
    return _request("GET", f"/projects/{project}/domains") or []


# --------------------------------------------------------------------------- #
# Secrets
# --------------------------------------------------------------------------- #

def list_secrets() -> list[dict[str, Any]]:
    """List all secrets in the authenticated account/team."""
    return _request("GET", "/secrets") or []


def create_secret(name: str, value: str) -> dict[str, Any]:
    """Create a new secret."""
    return _request(
        "POST",
        "/secrets",
        body={"name": name, "value": value},
    )


def delete_secret(name: str) -> None:
    """Delete a secret by name."""
    _request("DELETE", f"/secrets/{name}")
