"""
Netlify Deployment Utilities.

Helpers for building, deploying, and managing sites on Netlify via
the Netlify API and the Netlify CLI workflow.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import time
import urllib.request
import urllib.error
import subprocess
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

NETLIFY_TOKEN = os.getenv("NETLIFY_TOKEN", "")
NETLIFY_SITE_ID = os.getenv("NETLIFY_SITE_ID", "")
NETLIFY_API_BASE = "https://api.netlify.com/api/v1"


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {NETLIFY_TOKEN}"}


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    url = f"{NETLIFY_API_BASE}{path}"
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
        raise NetlifyAPIError(exc.code, exc.read().decode()) from exc


class NetlifyAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Netlify API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Site Management
# --------------------------------------------------------------------------- #

def list_sites(limit: int = 100) -> list[dict[str, Any]]:
    """Return all Netlify sites for the account."""
    return _api("GET", "/sites", params={"per_page": str(limit)}) or []


def get_site(site_id: Optional[str] = None) -> dict[str, Any]:
    sid = site_id or NETLIFY_SITE_ID
    if not sid:
        raise ValueError("NETLIFY_SITE_ID not set and no site_id provided")
    return _api("GET", f"/sites/{sid}")


def create_site(
    name: Optional[str] = None,
    custom_domain: Optional[str] = None,
) -> dict[str, Any]:
    """Create a new Netlify site."""
    body: dict[str, Any] = {}
    if name:
        body["name"] = name
    if custom_domain:
        body["custom_domain"] = custom_domain
    return _api("POST", "/sites", body=body)


def delete_site(site_id: str) -> None:
    """Permanently delete a site."""
    _api("DELETE", f"/sites/{site_id}")


# --------------------------------------------------------------------------- #
# Deployments
# --------------------------------------------------------------------------- #

def create_deploy(
    site_id: Optional[str] = None,
    branch: Optional[str] = None,
) -> dict[str, Any]:
    """Create a new deploy record, returning an upload URL."""
    sid = site_id or NETLIFY_SITE_ID
    body: dict[str, Any] = {}
    if branch:
        body["branch"] = branch
    return _api("POST", f"/sites/{sid}/deploys", body=body)


def upload_file(
    deploy_id: str,
    file_path: str,
    site_id: Optional[str] = None,
) -> dict[str, Any]:
    """Upload a single file to a deploy."""
    sid = site_id or NETLIFY_SITE_ID
    with open(file_path, "rb") as f:
        data = f.read()
    from pathlib import Path
    filename = Path(file_path).name
    headers = dict(_headers())
    headers["Content-Type"] = "application/octet-stream"
    headers["Content-Length"] = str(len(data))
    req = urllib.request.Request(
        f"{NETLIFY_API_BASE}/deploys/{deploy_id}/files/{filename}",
        data=data,
        headers=headers,
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise NetlifyAPIError(exc.code, exc.read().decode()) from exc


def deploy_directory(
    directory: str,
    site_id: Optional[str] = None,
    message: Optional[str] = None,
) -> dict[str, Any]:
    """
    Upload an entire directory to a new deploy and trigger the deploy.

    Args:
        directory: Local path to the build output directory.
        site_id: Netlify site ID.
        message: Deploy commit message.

    Returns:
        The deploy object.
    """
    from pathlib import Path
    site = create_deploy(site_id=site_id)
    deploy_id = site["id"]
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise ValueError(f"Directory not found: {directory}")
    for file_path in dir_path.rglob("*"):
        if file_path.is_file():
            rel = str(file_path.relative_to(dir_path))
            upload_file(deploy_id, str(file_path), site_id=site_id)
    # Signal deploy is ready
    body: dict[str, Any] = {"state": "ready"}
    if message:
        body["title"] = message
    return _api("POST", f"/deploys/{deploy_id}", body=body)


def wait_for_deploy(
    deploy_id: str,
    timeout: int = 300,
    poll_interval: int = 5,
) -> dict[str, Any]:
    """Poll until a deploy is complete or failed."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        dep = _api("GET", f"/deploys/{deploy_id}")
        state = dep.get("state", "")
        if state in ("ready", "error", "failed"):
            return dep
        time.sleep(poll_interval)
    raise TimeoutError(f"Deploy {deploy_id} did not complete within {timeout}s")


# --------------------------------------------------------------------------- #
# Build Hooks
# --------------------------------------------------------------------------- #

def create_build_hook(site_id: str, name: str, branch: str) -> dict[str, Any]:
    """Create a build hook for triggering deploys via URL."""
    return _api(
        "POST",
        f"/sites/{site_id}/build_hooks",
        body={"title": name, "branch": branch},
    )


def trigger_build_hook(hook_url: str) -> None:
    """Trigger a Netlify build by calling a build hook URL."""
    req = urllib.request.Request(hook_url, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status not in (200, 201, 204):
            raise NetlifyAPIError(resp.status, f"Build hook returned {resp.status}")


# --------------------------------------------------------------------------- #
# Environment Variables
# --------------------------------------------------------------------------- #

def get_env_vars(site_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Fetch all environment variables for a site."""
    sid = site_id or NETLIFY_SITE_ID
    return _api("GET", f"/sites/{sid}/env") or []


def set_env_var(
    site_id: str,
    key: str,
    value: str,
    is_secret: bool = True,
) -> dict[str, Any]:
    """Create or update a site environment variable."""
    return _api(
        "PUT",
        f"/sites/{site_id}/env/{key}",
        body={"value": value, "context": "all"},
    )


# --------------------------------------------------------------------------- #
# CLI Helpers
# --------------------------------------------------------------------------- #

def run_netlify_cli(
    args: list[str],
    cwd: str = ".",
    token: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """
    Run the Netlify CLI with the given arguments.

    Passes NETLIFY_TOKEN as NETLIFY_AUTH_TOKEN for non-interactive use.
    """
    env = dict(os.environ)
    t = token or NETLIFY_TOKEN
    if t:
        env["NETLIFY_AUTH_TOKEN"] = t
    return subprocess.run(
        ["netlify"] + args,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )
