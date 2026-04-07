"""
StatusPage.io Incident & Component Utilities.

Helpers for creating and managing Statuspage.io incidents, components,
page metrics, and subscriber notifications via the Statuspage API.

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

STATUSPAGE_API_KEY = os.getenv("STATUSPAGE_API_KEY", "")
STATUSPAGE_PAGE_ID = os.getenv("STATUSPAGE_PAGE_ID", "")
STATUSPAGE_API_BASE = "https://api.statuspage.io/v1"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"OAuth {STATUSPAGE_API_KEY}",
        "Content-Type": "application/json",
    }


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{STATUSPAGE_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise StatusPageAPIError(exc.code, exc.read().decode()) from exc


class StatusPageAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"StatusPage API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Components
# --------------------------------------------------------------------------- #

def list_components() -> list[dict[str, Any]]:
    """Return all components for the page."""
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/components") or []


def get_component(component_id: str) -> dict[str, Any]:
    """Fetch a component by ID."""
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/components/{component_id}")


def update_component_status(
    component_id: str,
    status: str,
) -> dict[str, Any]:
    """
    Update a component's operational status.

    Args:
        component_id: Component ID.
        status: One of 'operational', 'degraded_performance',
                'partial_outage', 'major_outage'.
    """
    return _api(
        "PATCH",
        f"/pages/{STATUSPAGE_PAGE_ID}/components/{component_id}",
        body={"component": {"status": status}},
    )


# --------------------------------------------------------------------------- #
# Incidents
# --------------------------------------------------------------------------- #

def list_incidents(
    unresolved_only: bool = False,
) -> list[dict[str, Any]]:
    """
    List incidents for the page.

    Args:
        unresolved_only: If True, return only active incidents.
    """
    if unresolved_only:
        return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/incidents/unresolved") or []
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/incidents") or []


def get_incident(incident_id: str) -> dict[str, Any]:
    """Fetch an incident by ID."""
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/incidents/{incident_id}")


def create_incident(
    name: str,
    body: str,
    status: str = "investigating",
    impact_override: Optional[str] = None,
    component_ids: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Create a new incident.

    Args:
        name: Incident name/title.
        body: Detailed incident description (markdown supported).
        status: 'investigating', 'identified', 'monitoring', 'resolved'.
        impact_override: Override component impact
            ('none', 'partial', 'major').
        component_ids: Components affected by this incident.

    Returns:
        Created incident object.
    """
    payload: dict[str, Any] = {
        "incident": {
            "name": name,
            "body": body,
            "status": status,
        }
    }
    if impact_override:
        payload["incident"]["impact_override"] = impact_override
    if component_ids:
        payload["incident"]["component_ids"] = component_ids
    return _api("POST", f"/pages/{STATUSPAGE_PAGE_ID}/incidents", body=payload)


def update_incident(
    incident_id: str,
    status: Optional[str] = None,
    body: Optional[str] = None,
) -> dict[str, Any]:
    """Update an existing incident's status or body."""
    changes: dict[str, Any] = {}
    if status:
        changes["status"] = status
    if body:
        changes["body"] = body
    return _api(
        "PATCH",
        f"/pages/{STATUSPAGE_PAGE_ID}/incidents/{incident_id}",
        body={"incident": changes},
    )


def resolve_incident(incident_id: str, body: str) -> dict[str, Any]:
    """Mark an incident as resolved with a final message."""
    return update_incident(incident_id, status="resolved", body=body)


def create_incident_update(
    incident_id: str,
    status: str,
    body: str,
    deliver_notification: bool = True,
) -> dict[str, Any]:
    """
    Post an update to an active incident.

    Args:
        incident_id: Target incident ID.
        status: 'investigating', 'identified', 'monitoring', 'resolved'.
        body: Update message (markdown supported).
        deliver_notification: If True, notify subscribers.

    Returns:
        Created incident update.
    """
    return _api(
        "POST",
        f"/pages/{STATUSPAGE_PAGE_ID}/incidents/{incident_id}/updates",
        body={
            "incident_update": {
                "status": status,
                "body": body,
                "deliver_notification": deliver_notification,
            }
        },
    )


# --------------------------------------------------------------------------- #
# Subscribers / Notifications
# --------------------------------------------------------------------------- #

def list_subscribers() -> list[dict[str, Any]]:
    """List all subscribers to the status page."""
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/subscribers") or []


def subscribe_email(email: str) -> dict[str, Any]:
    """Subscribe an email address to the status page."""
    return _api(
        "POST",
        f"/pages/{STATUSPAGE_PAGE_ID}/subscribers",
        body={"subscriber": {"email": email}},
    )


def unsubscribe(subscriber_id: str) -> None:
    """Unsubscribe a subscriber by ID."""
    _api("DELETE", f"/pages/{STATUSPAGE_PAGE_ID}/subscribers/{subscriber_id}")


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #

def get_page_metrics() -> dict[str, Any]:
    """Return uptime and response time metrics for the page."""
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/metrics")


# --------------------------------------------------------------------------- #
# Scheduled Maintenance
# --------------------------------------------------------------------------- #

def list_scheduled_maintenances() -> list[dict[str, Any]]:
    """List all scheduled maintenance windows."""
    return _api("GET", f"/pages/{STATUSPAGE_PAGE_ID}/scheduled-maintenances") or []


def create_scheduled_maintenance(
    name: str,
    body: str,
    scheduled_at: datetime,
    resolved_at: datetime,
    component_ids: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Create a scheduled maintenance window."""
    payload: dict[str, Any] = {
        "scheduled_maintenance": {
            "name": name,
            "body": body,
            "scheduled_for": scheduled_at.isoformat(),
            "scheduled_until": resolved_at.isoformat(),
        }
    }
    if component_ids:
        payload["scheduled_maintenance"]["component_ids"] = component_ids
    return _api(
        "POST",
        f"/pages/{STATUSPAGE_PAGE_ID}/scheduled-maintenances",
        body=payload,
    )
