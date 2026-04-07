"""
Sentry Error Tracking Utilities.

Helpers for reporting errors, breadcrumbs, and context to Sentry,
managing projects, releases, and querying issues via the Sentry API.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SENTRY_DSN = os.getenv("SENTRY_DSN", "")
SENTRY_TOKEN = os.getenv("SENTRY_TOKEN", "")
SENTRY_ORG = os.getenv("SENTRY_ORG", "")
SENTRY_API_BASE = "https://sentry.io/api/0"


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if SENTRY_TOKEN:
        headers["Authorization"] = f"Bearer {SENTRY_TOKEN}"
    return headers


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{SENTRY_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 204:
                return {}
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise SentryAPIError(exc.code, exc.read().decode()) from exc


class SentryAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Sentry API error {status}: {body}")


# --------------------------------------------------------------------------- #
# DSN Reporting ( Raven-style client )
# --------------------------------------------------------------------------- #

def parse_dsn(dsn: Optional[str] = None) -> dict[str, str]:
    """Parse a Sentry DSN into its components."""
    d = dsn or SENTRY_DSN
    if not d:
        raise ValueError("No SENTRY_DSN provided")
    # DSN format: https://<key>@sentry.io/<org>/<project>
    import re
    match = re.match(
        r"^https?://([^@]+)@([^/]+)/([^/]+)/([^/]+)$",
        d,
    )
    if not match:
        raise ValueError(f"Invalid DSN format: {d}")
    key, host, org, project = match.groups()
    return {
        "key": key,
        "host": host,
        "org": org,
        "project": project,
        "url": f"https://{host}",
    }


def capture_event(
    event: dict[str, Any],
    dsn: Optional[str] = None,
) -> Optional[str]:
    """
    Report a pre-built event dict to Sentry via the store API.

    Returns the event ID if successful.
    """
    d = parse_dsn(dsn)
    url = f"https://{d['host']}/api/{d['project']}/store/"
    headers = {
        "User-Agent": "rabai-sentry-utils/1.0",
        "X-Sentry-Auth": _build_auth_header(d["key"], d["host"]),
        "Content-Type": "application/json",
    }
    data = json.dumps(event).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            return result.get("id")
    except urllib.error.HTTPError:
        return None


def _build_auth_header(key: str, host: str) -> str:
    """Build the Sentry Raven auth header."""
    import time
    timestamp = int(time.time())
    fields = [
        ("sentry_version", "7"),
        ("sentry_client", "rabai-sentry-utils/1.0"),
        ("sentry_timestamp", str(timestamp)),
        ("sentry_key", key),
    ]
    parts = [f"{k}={v}" for k, v in fields]
    return f"Sentry {', '.join(parts)}"


def capture_exception(
    exc: Exception,
    message: str = "",
    level: str = "error",
    tags: Optional[dict[str, str]] = None,
    dsn: Optional[str] = None,
) -> Optional[str]:
    """Capture an exception with optional tags and message."""
    import traceback
    exc_type = type(exc).__name__
    tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    now = datetime.now(timezone.utc)
    event = {
        "event_id": _generate_event_id(),
        "timestamp": now.isoformat(),
        "level": level,
        "platform": "python",
        "logger": "root",
        "exception": {
            "values": [
                {
                    "type": exc_type,
                    "value": str(exc),
                    "module": exc.__class__.__module__,
                    "stacktrace": {
                        "frames": [
                            {
                                "abs_path": f"line {i}",
                                "filename": "unknown",
                                "function": line.strip(),
                                "lineno": i + 1,
                            }
                            for i, line in enumerate(tb)
                        ]
                    },
                }
            ]
        },
        "message": message or f"{exc_type}: {exc}",
        "tags": (tags or {}),
        "extra": {"github_actions": os.getenv("GITHUB_ACTIONS", "")},
    }
    return capture_event(event, dsn=dsn)


def _generate_event_id() -> str:
    """Generate a random 32-char hex event ID."""
    import secrets
    return secrets.token_hex(16)


# --------------------------------------------------------------------------- #
# Issues / Projects Management
# --------------------------------------------------------------------------- #

def list_projects() -> list[dict[str, Any]]:
    """Return all Sentry projects."""
    if not SENTRY_ORG:
        raise ValueError("SENTRY_ORG not set")
    return _api("GET", f"/organizations/{SENTRY_ORG}/projects/") or []


def list_issues(
    query: str = "is:unresolved",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Search for issues matching a Sentry query."""
    if not SENTRY_ORG:
        raise ValueError("SENTRY_ORG not set")
    return _api(
        "GET",
        f"/organizations/{SENTRY_ORG}/issues/",
        body={"query": query, "limit": limit},
    ) or []


def resolve_issue(issue_id: str) -> dict[str, Any]:
    """Mark an issue as resolved."""
    return _api(
        "PUT",
        f"/organizations/{SENTRY_ORG}/issues/{issue_id}/",
        body={"status": "resolved"},
    )


# --------------------------------------------------------------------------- #
# Releases
# --------------------------------------------------------------------------- #

def create_release(
    version: str,
    projects: list[str],
    repo: Optional[str] = None,
    commits: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Create a Sentry release.

    Args:
        version: Release version string (e.g. git SHA or semver).
        projects: List of project slugs.
        repo: Repository name for commit linking.
        commits: Optional list of commit dicts.
    """
    if not SENTRY_ORG:
        raise ValueError("SENTRY_ORG not set")
    body: dict[str, Any] = {
        "version": version,
        "projects": projects,
    }
    if repo:
        body["refs"] = [{"repository": repo, "commit": version}]
    if commits:
        body["commits"] = commits
    return _api(
        "POST",
        f"/organizations/{SENTRY_ORG}/releases/",
        body=body,
    )


def add_release_file(
    release_version: str,
    project_slug: str,
    file_path: str,
    content: bytes,
) -> dict[str, Any]:
    """Upload a file (e.g. a sourcemap) to a release."""
    if not SENTRY_ORG:
        raise ValueError("SENTRY_ORG not set")
    from pathlib import Path
    filename = Path(file_path).name
    url = (
        f"/organizations/{SENTRY_ORG}/releases/{release_version}/"
        f"files/{project_slug}/"
    )
    headers = dict(_headers())
    headers["Content-Type"] = "application/octet-stream"
    req = urllib.request.Request(
        f"{SENTRY_API_BASE}{url}",
        data=content,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise SentryAPIError(exc.code, exc.read().decode()) from exc
