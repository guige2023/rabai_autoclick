"""
OpsGenie Alert Management Utilities.

Helpers for creating, acknowledging, and resolving OpsGenie alerts,
managing on-call schedules, and routing notifications to teams.

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

OPSGENIE_API_KEY = os.getenv("OPSGENIE_API_KEY", "")
OPSGENIE_TEAM = os.getenv("OPSGENIE_TEAM", "")
OPSGENIE_API_BASE = "https://api.opsgenie.com/v2"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"GenieKey {OPSGENIE_API_KEY}",
        "Content-Type": "application/json",
    }


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{OPSGENIE_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise OpsGenieAPIError(exc.code, exc.read().decode()) from exc


class OpsGenieAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"OpsGenie API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Alerts
# --------------------------------------------------------------------------- #

def create_alert(
    message: str,
    description: str = "",
    priority: str = "P3",
    tags: Optional[list[str]] = None,
    entity: Optional[str] = None,
    user: Optional[str] = None,
    team: Optional[str] = None,
    actions: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Create a new OpsGenie alert.

    Args:
        message: Short alert summary (required).
        description: Long description/details.
        priority: P1 (critical) through P5 (info).
        tags: List of tags for the alert.
        entity: Entity name associated with the alert.
        user: Owner of the alert.
        team: Team to route the alert to.
        actions: List of available actions on the alert.

    Returns:
        Created alert with ID and tiny ID.
    """
    payload: dict[str, Any] = {
        "message": message,
        "priority": priority,
    }
    if description:
        payload["description"] = description
    if tags:
        payload["tags"] = tags
    if entity:
        payload["entity"] = entity
    if user:
        payload["user"] = user
    if team:
        payload["team"] = team
    if actions:
        payload["actions"] = actions
    return _api("POST", "/alerts", body=payload)


def list_alerts(
    status: str = "open",
    limit: int = 100,
    sort: str = "createdAt",
    order: str = "desc",
) -> list[dict[str, Any]]:
    """List alerts filtered by status."""
    params = {
        "limit": str(limit),
        "sort": sort,
        "order": order,
    }
    data = _api("GET", f"/alerts?{urllib.parse.urlencode(params)}&query=status:{status}")
    return data.get("data", [])


def get_alert(alert_id: str) -> dict[str, Any]:
    """Fetch an alert by its ID or tiny ID."""
    return _api("GET", f"/alerts/{alert_id}")


def acknowledge_alert(alert_id: str, note: str = "") -> dict[str, Any]:
    """Acknowledge an alert."""
    body: dict[str, Any] = {}
    if note:
        body["note"] = note
    return _api("POST", f"/alerts/{alert_id}/acknowledge", body=body)


def close_alert(alert_id: str, note: str = "") -> dict[str, Any]:
    """Close (resolve) an alert."""
    body: dict[str, Any] = {}
    if note:
        body["note"] = note
    return _api("POST", f"/alerts/{alert_id}/close", body=body)


def add_note(alert_id: str, note: str) -> dict[str, Any]:
    """Add a note to an existing alert."""
    return _api("POST", f"/alerts/{alert_id}/notes", body={"note": note})


def attach_file(alert_id: str, file_path: str) -> dict[str, Any]:
    """Attach a file to an alert."""
    import urllib.parse
    url = f"{OPSGENIE_API_BASE}/attachments"
    with open(file_path, "rb") as f:
        import email.mime.multipart
        msg = email.mime.multipart.MIMEMultipart()
        msg["Authorization"] = f"GenieKey {OPSGENIE_API_KEY}"
        msg.attach(file_path, "application/octet-stream")
        # Use multipart form directly
        import email.encoders
        from email.mime.base import MIMEBase
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
        email.encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(file_path)}")
        req = urllib.request.Request(
            f"{url}/{alert_id}",
            data=part.as_bytes(),
            headers={"Authorization": f"GenieKey {OPSGENIE_API_KEY}"},
            method="POST",
        )
        # Fallback: simple note-based attachment proxy
        return {"status": "unavailable", "note": "File attachment requires multipart upload"}


# --------------------------------------------------------------------------- #
# Teams
# --------------------------------------------------------------------------- #

def list_teams() -> list[dict[str, Any]]:
    """Return all OpsGenie teams."""
    data = _api("GET", "/teams")
    return data.get("data", [])


def get_team(team_id: str) -> dict[str, Any]:
    """Fetch a team by ID."""
    return _api("GET", f"/teams/{team_id}")


# --------------------------------------------------------------------------- #
# On-Call
# --------------------------------------------------------------------------- #

def get_oncall(team_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Get the current on-call participant(s)."""
    if team_id:
        return _api("GET", f"/on-call/recipients?identifierType=id&identifier={team_id}")
    return _api("GET", "/on-call/recipients")


def who_is_oncall(team_id: str) -> Optional[str]:
    """Return the name of the current on-call for a team."""
    data = _api("GET", f"/on-call/recipients?identifierType=id&identifier={team_id}")
    recipients = data.get("data", [])
    if recipients:
        return recipients[0].get("username", "")
    return None


# --------------------------------------------------------------------------- #
# Schedules
# --------------------------------------------------------------------------- #

def list_schedules() -> list[dict[str, Any]]:
    """List all on-call schedules."""
    data = _api("GET", "/schedules")
    return data.get("data", [])


def get_schedule_timeline(
    schedule_id: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    """Get the timeline for a schedule within a date range."""
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
    }
    return _api("GET", f"/schedules/{schedule_id}/timeline", params=params)


import urllib.parse
