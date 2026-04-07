"""
PagerDuty Incident Management Utilities.

Helpers for creating, managing, and resolving PagerDuty incidents,
escalating alerts, adding responders, and querying on-call schedules.

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

PAGERDUTY_TOKEN = os.getenv("PAGERDUTY_TOKEN", "")
PAGERDUTY_API_BASE = "https://api.pagerduty.com"
PAGERDUTY_ROUTING_KEY = os.getenv("PAGERDUTY_ROUTING_KEY", "")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Token token={PAGERDUTY_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.pagerduty+json;version=2",
    }


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{PAGERDUTY_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise PagerDutyAPIError(exc.code, exc.read().decode()) from exc


class PagerDutyAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"PagerDuty API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Incidents
# --------------------------------------------------------------------------- #

def create_incident(
    title: str,
    service_id: str,
    urgency: str = "high",
    dedup_key: Optional[str] = None,
    body: Optional[str] = None,
    assignee_ids: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Create a new PagerDuty incident.

    Args:
        title: Incident title/summary.
        service_id: PagerDuty service ID.
        urgency: 'high' or 'low'.
        dedup_key: Optional deduplication key for correlation.
        body: Markdown incident body.
        assignee_ids: List of user IDs to assign.

    Returns:
        Created incident object.
    """
    payload: dict[str, Any] = {
        "incident": {
            "type": "incident",
            "title": title,
            "service": {"id": service_id, "type": "service_reference"},
            "urgency": urgency,
        }
    }
    if dedup_key:
        payload["incident"]["dedup_key"] = dedup_key
    if body:
        payload["incident"]["body"] = {"type": "incident_body", "details": body}
    if assignee_ids:
        payload["incident"]["assignments"] = [
            {"assignee": {"id": uid, "type": "user_reference"}}
            for uid in assignee_ids
        ]
    return _api("POST", "/incidents", body=payload)


def list_incidents(
    statuses: Optional[list[str]] = None,
    urgency: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List incidents filtered by status and/or urgency."""
    params = {"limit": str(limit)}
    if statuses:
        params["statuses[]"] = ",".join(statuses)
    if urgency:
        params["urgency"] = urgency
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    data = _api("GET", f"/incidents?{qs}")
    return data.get("incidents", [])


def get_incident(incident_id: str) -> dict[str, Any]:
    """Fetch a single incident by ID."""
    return _api("GET", f"/incidents/{incident_id}")


def update_incident(
    incident_id: str,
    status: Optional[str] = None,
    urgency: Optional[str] = None,
    title: Optional[str] = None,
) -> dict[str, Any]:
    """Update an incident's status, urgency, or title."""
    changes: dict[str, Any] = {}
    if status:
        changes["status"] = status
    if urgency:
        changes["urgency"] = urgency
    if title:
        changes["title"] = title
    return _api("PUT", f"/incidents/{incident_id}", body={"incident": changes})


def resolve_incident(incident_id: str) -> dict[str, Any]:
    """Mark an incident as resolved."""
    return update_incident(incident_id, status="resolved")


def add_note(incident_id: str, note: str) -> dict[str, Any]:
    """Add a note to an incident."""
    return _api(
        "POST",
        f"/incidents/{incident_id}/notes",
        body={"note": {"content": note}},
    )


# --------------------------------------------------------------------------- #
# Escalation & Responders
# --------------------------------------------------------------------------- #

def add_responder(incident_id: str, user_id: str) -> dict[str, Any]:
    """Add a responder to an incident."""
    return _api(
        "POST",
        f"/incidents/{incident_id}/responders",
        body={"responder": {"user": {"id": user_id, "type": "user"}}},
    )


def escalate_incident(incident_id: str) -> dict[str, Any]:
    """Re-escalate an incident based on its escalation policy."""
    return _api(
        "PUT",
        f"/incidents/{incident_id}",
        body={"incident": {"escalation_policy": None}},
    )


# --------------------------------------------------------------------------- #
# Services
# --------------------------------------------------------------------------- #

def list_services(limit: int = 100) -> list[dict[str, Any]]:
    """List all PagerDuty services."""
    data = _api("GET", f"/services?limit={limit}")
    return data.get("services", [])


def get_service(service_id: str) -> dict[str, Any]:
    """Fetch a service by ID."""
    return _api("GET", f"/services/{service_id}")


# --------------------------------------------------------------------------- #
# On-Call Schedules
# --------------------------------------------------------------------------- #

def get_on_calls(
    schedule_id: Optional[str] = None,
    user_id: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """
    Get current on-call assignments.

    Args:
        schedule_id: Filter by schedule.
        user_id: Filter by user.
        since: Start of the time range (UTC).
        until: End of the time range (UTC).

    Returns:
        List of on-call objects.
    """
    params: dict[str, str] = {}
    if since:
        params["since"] = since.isoformat()
    if until:
        params["until"] = until.isoformat()
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    if schedule_id:
        data = _api("GET", f"/oncalls?schedule_ids[]={schedule_id}&{qs}")
    elif user_id:
        data = _api("GET", f"/oncalls?user_ids[]={user_id}&{qs}")
    else:
        data = _api("GET", f"/oncalls?{qs}")
    return data.get("oncalls", [])


def who_is_oncall(schedule_id: str) -> Optional[str]:
    """Return the user ID currently on-call for a schedule."""
    calls = get_on_calls(schedule_id=schedule_id)
    if calls:
        return calls[0].get("user", {}).get("id")
    return None


# --------------------------------------------------------------------------- #
# Events API v2 (Lightweight PagerDuty Events)
# --------------------------------------------------------------------------- #

def send_event(
    routing_key: Optional[str] = None,
    event_action: str = "trigger",
    dedup_key: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Send a lightweight event via the PagerDuty Events API v2.

    This is different from the REST API — it uses the Events API endpoint
    which does not require a token.

    Args:
        routing_key: Integration key (defaults to PAGERDUTY_ROUTING_KEY).
        event_action: 'trigger', 'acknowledge', or 'resolve'.
        dedup_key: Deduplication key.
        payload: Event payload dict with 'summary', 'source', 'severity', etc.

    Returns:
        Parsed API response.
    """
    import urllib.parse
    key = routing_key or PAGERDUTY_ROUTING_KEY
    body = {
        "routing_key": key,
        "event_action": event_action,
        "dedup_key": dedup_key,
    }
    if payload:
        body["payload"] = payload
    url = "https://events.pagerduty.com/v2/enqueue"
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise PagerDutyAPIError(exc.code, exc.read().decode()) from exc
