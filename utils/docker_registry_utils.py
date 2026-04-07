"""
Docker & OCI Registry Utilities.

Helpers for interacting with Docker registries, managing images,
tags, manifests, and blobs, and performing authentication with
container registries (Docker Hub, GHCR, ECR, GCR, ACR, etc.).

Author: rabai_autoclick
License: MIT
"""

import os
import json
import base64
import urllib.request
import urllib.error
import hashlib
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

REGISTRY_URL = os.getenv("REGISTRY_URL", "https://index.docker.io")
REGISTRY_USER = os.getenv("REGISTRY_USER", "")
REGISTRY_TOKEN = os.getenv("REGISTRY_TOKEN", "")
REGISTRY_PASSWORD = os.getenv("REGISTRY_PASSWORD", "")


# --------------------------------------------------------------------------- #
# Auth Helpers
# --------------------------------------------------------------------------- #

def basic_auth(user: str = "", password: str = "") -> str:
    """Return a Basic auth header value."""
    creds = f"{user}:{password}"
    return "Basic " + base64.b64encode(creds.encode()).decode()


def bearer_token(auth_header: str) -> str:
    """Extract the bearer token from a WWW-Authenticate header."""
    # e.g. Bearer realm="https://auth.docker.io/token",service="registry.docker.io"
    params = {}
    for part in auth_header.replace("Bearer ", "").split(","):
        k, _, v = part.partition("=")
        params[k.strip().strip('"')] = v.strip().strip('"')
    return params.get("token", "")


def get_dockerhub_token(scope: str = "repository:library/*:pull") -> str:
    """Obtain a short-lived Docker Hub bearer token."""
    realm = "https://auth.docker.io/token"
    params = {
        "service": "registry.docker.io",
        "scope": scope,
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{realm}?{qs}"
    headers: dict[str, str] = {}
    if REGISTRY_USER and REGISTRY_PASSWORD:
        headers["Authorization"] = basic_auth(REGISTRY_USER, REGISTRY_PASSWORD)
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return data.get("token", "")
    except urllib.error.HTTPError as exc:
        raise RegistryAuthError(exc.code, exc.read().decode()) from exc


class RegistryAuthError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        super().__init__(f"Registry auth error {status}: {body}")


class RegistryAPIError(Exception):
    def __init__(self, status: int, path: str, body: str) -> None:
        self.status = status
        self.path = path
        self.body = body
        super().__init__(f"Registry API error {status} at {path}: {body}")


# --------------------------------------------------------------------------- #
# HTTP Helpers
# --------------------------------------------------------------------------- #

def _request(
    method: str,
    url: str,
    headers: Optional[dict[str, str]] = None,
    data: Optional[bytes] = None,
    auth: Optional[str] = None,
) -> urllib.request.Request:
    h = dict(headers or {})
    if auth:
        h["Authorization"] = auth
    return urllib.request.Request(url, data=data, headers=h, method=method)


def _do(
    req: urllib.request.Request,
    expected: Optional[list[int]] = None,
) -> tuple[int, bytes]:
    expected = expected or [200]
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return (resp.status, resp.read())
    except urllib.error.HTTPError as exc:
        if expected and exc.code in expected:
            return (exc.code, exc.read())
        raise RegistryAPIError(exc.code, req.full_url, exc.read().decode())


# --------------------------------------------------------------------------- #
# Manifests
# --------------------------------------------------------------------------- #

def get_manifest(
    image: str,
    tag: str,
    registry: Optional[str] = None,
    auth: Optional[str] = None,
    media_type: str = "application/vnd.docker.distribution.manifest.v2+json",
) -> dict[str, Any]:
    """
    Fetch a manifest for an image tag.

    Args:
        image: Image name (e.g. 'nginx', 'myuser/myrepo').
        tag: Tag or digest reference.
        registry: Registry base URL.
        auth: Auth header string.
        media_type: Accept media type for the manifest.

    Returns:
        Manifest JSON dict.
    """
    reg = registry or REGISTRY_URL
    url = f"{reg}/v2/{image}/manifests/{tag}"
    h = {"Accept": media_type}
    req = _request("GET", url, headers=h, auth=auth)
    status, body = _do(req)
    return json.loads(body)


def put_manifest(
    image: str,
    tag: str,
    manifest: dict[str, Any],
    registry: Optional[str] = None,
    auth: Optional[str] = None,
) -> None:
    """Upload a manifest for an image."""
    reg = registry or REGISTRY_URL
    url = f"{reg}/v2/{image}/manifests/{tag}"
    data = json.dumps(manifest).encode()
    h = {
        "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
    }
    req = _request("PUT", url, headers=h, data=data, auth=auth)
    _do(req, expected=[200, 201, 202])


# --------------------------------------------------------------------------- #
# Blobs (Layers)
# --------------------------------------------------------------------------- #

def upload_blob(
    image: str,
    digest: str,
    content: bytes,
    registry: Optional[str] = None,
    auth: Optional[str] = None,
) -> None:
    """
    Upload a blob (layer) to the registry.

    Uses the layered approach: initiate → chunk upload → complete.
    """
    reg = registry or REGISTRY_URL
    size = len(content)
    # Initiate upload
    url = f"{reg}/v2/{image}/blobs/uploads/"
    req = _request("POST", url, auth=auth)
    status, _ = _do(req, expected=[202])
    loc = ""
    # Get location from header
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            loc = r.headers.get("Location", "")
    except Exception:
        pass
    if not loc:
        raise RegistryAPIError(500, url, "No upload location returned")
    # Upload content
    upload_url = f"{loc}&digest={digest}"
    req = _request("PUT", upload_url, headers={"Content-Length": str(size)}, data=content, auth=auth)
    _do(req, expected=[201, 202])


def get_blob_digest(content: bytes) -> str:
    """Compute the registry-compatible sha256 digest for blob content."""
    h = hashlib.sha256(content).hexdigest()
    return f"sha256:{h}"


def check_blob_exists(
    image: str,
    digest: str,
    registry: Optional[str] = None,
    auth: Optional[str] = None,
) -> bool:
    """Check whether a blob is already present in the registry."""
    reg = registry or REGISTRY_URL
    url = f"{reg}/v2/{image}/blobs/{digest}"
    req = _request("HEAD", url, auth=auth)
    try:
        status, _ = _do(req, expected=[200, 201])
        return status == 200
    except RegistryAPIError:
        return False


# --------------------------------------------------------------------------- #
# Tags
# --------------------------------------------------------------------------- #

def list_tags(
    image: str,
    registry: Optional[str] = None,
    auth: Optional[str] = None,
    limit: int = 100,
) -> list[str]:
    """List tags for an image repository."""
    reg = registry or REGISTRY_URL
    url = f"{reg}/v2/{image}/tags/list?n={limit}"
    req = _request("GET", url, auth=auth)
    _, body = _do(req)
    data = json.loads(body)
    return data.get("tags", [])


# --------------------------------------------------------------------------- #
# Catalog (Repository Listing)
# --------------------------------------------------------------------------- #

def catalog(
    registry: Optional[str] = None,
    auth: Optional[str] = None,
    n: int = 100,
) -> list[str]:
    """List all image repositories in a registry (requires admin auth)."""
    reg = registry or REGISTRY_URL
    url = f"{reg}/v2/_catalog?n={n}"
    req = _request("GET", url, auth=auth)
    _, body = _do(req)
    data = json.loads(body)
    return data.get("repositories", [])


# --------------------------------------------------------------------------- #
# Docker Hub Specific
# --------------------------------------------------------------------------- #

def get_dockerhub_repo_info(namespace: str, repo: str) -> dict[str, Any]:
    """Fetch Docker Hub repository metadata."""
    url = f"https://hub.docker.com/v2/repositories/{namespace}/{repo}/"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise RegistryAPIError(exc.code, url, exc.read().decode()) from exc
