"""
Grafana Dashboard & Alert Utilities.

Helpers for creating dashboards, managing panels, handling alerts,
and querying the Grafana API for observability workflows.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN", "")
GRAFANA_ORG_ID = os.getenv("GRAFANA_ORG_ID", "1")
GRAFANA_API_BASE = f"{GRAFANA_URL}/api"


def _headers(org: Optional[str] = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {GRAFANA_TOKEN}",
        "Content-Type": "application/json",
    }
    if org:
        headers["X-Grafana-Org-Id"] = org
    return headers


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
    org: Optional[str] = None,
) -> dict[str, Any]:
    url = f"{GRAFANA_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(org), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise GrafanaAPIError(exc.code, exc.read().decode()) from exc


class GrafanaAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Grafana API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Dashboards
# --------------------------------------------------------------------------- #

def get_dashboard(uid: str) -> dict[str, Any]:
    """Fetch a dashboard by its UID."""
    return _api("GET", f"/dashboards/uid/{uid}")


def create_dashboard(
    title: str,
    panels: Optional[list[dict[str, Any]]] = None,
    uid: Optional[str] = None,
    tags: Optional[list[str]] = None,
    folder_uid: Optional[str] = None,
) -> dict[str, Any]:
    """
    Create a new Grafana dashboard.

    Args:
        title: Dashboard title.
        panels: List of panel configuration objects.
        uid: Optional stable UID.
        tags: Optional list of tags.
        folder_uid: Folder to place the dashboard in.

    Returns:
        Created dashboard metadata including uid.
    """
    dashboard: dict[str, Any] = {
        "title": title,
        "tags": tags or [],
        "timezone": "browser",
        "schemaVersion": 38,
        "version": 0,
        "refresh": "30s",
        "panels": panels or [],
    }
    if uid:
        dashboard["uid"] = uid
    payload: dict[str, Any] = {
        "dashboard": dashboard,
        "message": "Created by grafana_utils",
        "overwrite": True,
    }
    if folder_uid:
        payload["folderUid"] = folder_uid
    return _api("POST", "/dashboards/db", body=payload)


def update_dashboard(
    uid: str,
    revisions: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Update an existing dashboard (full replacement)."""
    current = get_dashboard(uid)
    dashboard = current.get("dashboard", {})
    dashboard["version"] = current.get("dashboard", {}).get("version", 0) + 1
    if revisions:
        dashboard.update(revisions)
    return _api(
        "POST",
        "/dashboards/db",
        body={"dashboard": dashboard, "overwrite": True},
    )


# --------------------------------------------------------------------------- #
# Panels
# --------------------------------------------------------------------------- #

def make_graph_panel(
    title: str,
    targets: list[dict[str, Any]],
    x_axis: str = "time",
) -> dict[str, Any]:
    """
    Build a basic graph panel configuration.

    Args:
        title: Panel title.
        targets: List of Prometheus/Loki query targets.
        x_axis: X-axis mode (time, series, histogram).

    Returns:
        Panel dict suitable for use in create_dashboard panels list.
    """
    return {
        "title": title,
        "type": "graph",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
        "id": 1,
        "targets": targets,
        "xaxis": {"mode": x_axis},
        "lines": True,
        "fill": 1,
        "linewidth": 1,
    }


def make_stat_panel(
    title: str,
    targets: list[dict[str, Any]],
    color_mode: str = "value",
) -> dict[str, Any]:
    """Build a stat panel configuration."""
    return {
        "title": title,
        "type": "stat",
        "gridPos": {"h": 6, "w": 6, "x": 0, "y": 0},
        "id": 2,
        "targets": targets,
        "options": {"colorMode": color_mode, "graphMode": "area"},
    }


def make_timeseries_panel(
    title: str,
    targets: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a timeseries panel configuration."""
    return {
        "title": title,
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
        "id": 3,
        "targets": targets,
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
    }


# --------------------------------------------------------------------------- #
# Alerts
# --------------------------------------------------------------------------- #

def create_alert_rule(
    name: str,
    condition: str,
    expr: str,
    for_duration: str = "5m",
    severity: str = "warning",
    labels: Optional[dict[str, str]] = None,
    annotations: Optional[dict[str, str]] = None,
    folder_uid: str = "default",
    group: str = "default",
) -> dict[str, Any]:
    """
    Create a Grafana alert rule.

    Args:
        name: Rule name.
        condition: The condition expression ID (e.g. "C").
        expr: Prometheus query expression.
        for_duration: How long condition must be true before firing.
        severity: Alert severity (info, warning, critical).
        labels: Extra labels attached to the alert.
        annotations: Extra annotations (summary, description).
        folder_uid: Folder UID for the rule group.
        group: Rule group name.

    Returns:
        The created alert rule.
    """
    rule = {
        "title": name,
        "condition": condition,
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": 600, "to": 0},
                "datasourceUid": "prometheus",
                "model": {"expr": expr, "refId": "A"},
            },
            {
                "refId": "B",
                "relativeTimeRange": {"from": 600, "to": 0},
                "datasourceUid": "__expr__",
                "model": {
                    "conditions": [
                        {
                            "evaluator": {"params": [], "type": "gt"},
                            "operator": {"type": "and"},
                            "query": {"params": ["B"]},
                            "reducer": {"params": [], "type": "last"},
                            "type": "query",
                        }
                    ],
                    "datasource": {"type": "__expr__", "uid": "__expr__"},
                    "expression": "A",
                    "reducer": "last",
                    "refId": "B",
                    "type": "reduce",
                },
            },
        ],
        "execErrState": "Error",
        "folderUID": folder_uid,
        "for": for_duration,
        "labels": (labels or {}),
        "annotations": (annotations or {"summary": name}),
        "ruleGroup": group,
        "noDataState": "NoData",
    }
    return _api("POST", "/v1/provisioning/alert-rules", body=rule)


def list_alert_rules() -> list[dict[str, Any]]:
    """List all alert rules."""
    return _api("GET", "/v1/provisioning/alert-rules") or []


def pause_alert_rule(uid: str) -> None:
    """Pause an alert rule by UID."""
    _api("POST", f"/v1/provisioning/alert-rules/{uid}/pause", body={"paused": True})


def resume_alert_rule(uid: str) -> None:
    """Resume a paused alert rule."""
    _api("POST", f"/v1/provisioning/alert-rules/{uid}/pause", body={"paused": False})


# --------------------------------------------------------------------------- #
# Health / Status
# --------------------------------------------------------------------------- #

def check_health() -> dict[str, Any]:
    """Check Grafana instance health."""
    return _api("GET", "/health")


def get_instance_stats() -> dict[str, Any]:
    """Return basic instance statistics."""
    return _api("GET", "/api/frontend/settings")
