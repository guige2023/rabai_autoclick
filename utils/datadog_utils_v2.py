"""
Datadog APM & Monitoring Utilities (v2).

Extended helpers for the Datadog API: metrics submission, dashboards,
monitors, SLOs, logs ingestion, and synthetic testing management.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
import time
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DD_API_KEY = os.getenv("DD_API_KEY", "")
DD_APP_KEY = os.getenv("DD_APP_KEY", "")
DD_SITE = os.getenv("DD_SITE", "datadoghq.com")
DD_API_BASE = f"https://api.{DD_SITE}/api/v1"
DD_METRICS_BASE = f"https://api.{DD_SITE}/api/v2/series"


def _api_headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "DD-API-KEY": DD_API_KEY,
        "DD-APPLICATION-KEY": DD_APP_KEY,
    }


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{DD_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_api_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise DatadogAPIError(exc.code, exc.read().decode()) from exc


class DatadogAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Datadog API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Metrics Submission (Series API v2)
# --------------------------------------------------------------------------- #

def submit_metrics(
    series: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Submit metrics using the Datadog Series API v2.

    Each series dict should have:
        - metric: str
        - type: 'count', 'gauge', 'rate', or 'distribution'
        - points: list of (timestamp, value) tuples
        - tags: list of 'key:value' strings
        - resource: optional resource name

    Args:
        series: List of metric series.

    Returns:
        API response dict.
    """
    payload = {
        "series": [
            {
                "metric": s["metric"],
                "type": s.get("type", "gauge"),
                "points": s["points"],
                "tags": s.get("tags", []),
                "resources": (
                    [{"name": s["resource"], "type": "host"}]
                    if s.get("resource")
                    else []
                ),
            }
            for s in series
        ]
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        DD_METRICS_BASE,
        data=data,
        headers={
            "Content-Type": "application/json",
            "DD-API-KEY": DD_API_KEY,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise DatadogAPIError(exc.code, exc.read().decode()) from exc


def gauge(
    metric: str,
    value: float,
    tags: Optional[list[str]] = None,
    timestamp: Optional[int] = None,
) -> dict[str, Any]:
    """Submit a single gauge metric."""
    ts = timestamp or int(time.time())
    return submit_metrics([
        {"metric": metric, "type": "gauge", "points": [(ts, value)], "tags": tags or []}
    ])


def counter(
    metric: str,
    value: float,
    tags: Optional[list[str]] = None,
    timestamp: Optional[int] = None,
) -> dict[str, Any]:
    """Submit a single count metric."""
    ts = timestamp or int(time.time())
    return submit_metrics([
        {"metric": metric, "type": "count", "points": [(ts, value)], "tags": tags or []}
    ])


# --------------------------------------------------------------------------- #
# Dashboards
# --------------------------------------------------------------------------- #

def list_dashboards() -> list[dict[str, Any]]:
    """List all dashboards."""
    data = _api("GET", "/dashboard")
    return data.get("dashboards", [])


def get_dashboard(dashboard_id: str) -> dict[str, Any]:
    """Fetch a dashboard by ID."""
    return _api("GET", f"/dashboard/{dashboard_id}")


def create_dashboard(
    title: str,
    widgets: list[dict[str, Any]],
    layout_type: str = "ordered",
    tags: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Create a new dashboard with widgets.

    Args:
        title: Dashboard title.
        widgets: List of widget definitions.
        layout_type: 'ordered' or 'free'.
        tags: Dashboard tags.

    Returns:
        Created dashboard with ID.
    """
    body: dict[str, Any] = {
        "title": title,
        "widgets": widgets,
        "layout_type": layout_type,
    }
    if tags:
        body["tags"] = tags
    return _api("POST", "/dashboard", body=body)


# --------------------------------------------------------------------------- #
# Monitors
# --------------------------------------------------------------------------- #

def list_monitors(
    query: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """List monitors, optionally filtered by query or tags."""
    params: dict[str, str] = {}
    if query:
        params["query"] = query
    if tags:
        params["tags"] = ",".join(tags)
    qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    return _api("GET", f"/monitor{qs}") or []


def create_monitor(
    name: str,
    type_: str,
    query: str,
    message: str,
    tags: Optional[list[str]] = None,
    priority: Optional[int] = None,
    options: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Create a new monitor.

    Args:
        name: Monitor name.
        type_: Monitor type ('metric alert', 'service check', 'event alert', etc.).
        query: Monitor query.
        message: Alert notification message.
        tags: Tags for the monitor.
        priority: Priority 1-5 (1=critical).
        options: Monitor options (thresholds, renotify, etc.).

    Returns:
        Created monitor object.
    """
    body: dict[str, Any] = {
        "name": name,
        "type": type_,
        "query": query,
        "message": message,
    }
    if tags:
        body["tags"] = tags
    if priority:
        body["priority"] = priority
    if options:
        body["options"] = options
    return _api("POST", "/monitor", body=body)


def update_monitor(
    monitor_id: str,
    **changes: Any,
) -> dict[str, Any]:
    """Update monitor fields."""
    return _api("PUT", f"/monitor/{monitor_id}", body=changes)


def delete_monitor(monitor_id: str) -> dict[str, Any]:
    """Delete a monitor."""
    return _api("DELETE", f"/monitor/{monitor_id}")


def mute_monitor(monitor_id: str, scope: Optional[str] = None) -> dict[str, Any]:
    """Mute a monitor, optionally scoped to a specific group."""
    body: dict[str, Any] = {}
    if scope:
        body["scope"] = scope
    return _api("POST", f"/monitor/{monitor_id}/mute", body=body if body else None)


# --------------------------------------------------------------------------- #
# Service Level Objectives (SLOs)
# --------------------------------------------------------------------------- #

def list_slos() -> list[dict[str, Any]]:
    """List all SLOs."""
    return _api("GET", "/slo") or []


def create_slo(
    name: str,
    targets: list[dict[str, Any]],
    query_type: str = "metric",
    description: str = "",
    tags: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Create a Service Level Objective.

    Args:
        name: SLO name.
        targets: List of target dicts with 'sli', 'target', 'window'.
        query_type: 'metric' or 'monitor'.
        description: SLO description.
        tags: Tags.

    Returns:
        Created SLO object.
    """
    body: dict[str, Any] = {
        "name": name,
        "type": "slo",
        "query": {"type": query_type, "targets": targets},
        "description": description,
    }
    if tags:
        body["tags"] = tags
    return _api("POST", "/slo", body=body)


# --------------------------------------------------------------------------- #
# Logs
# --------------------------------------------------------------------------- #

def submit_logs(
    logs: list[dict[str, Any]],
    ddtags: Optional[str] = None,
) -> dict[str, Any]:
    """
    Ingest logs into Datadog.

    Args:
        logs: List of log entries (message, status, tags, etc.).
        ddtags: Comma-separated tags for all logs.

    Returns:
        Ingestion status.
    """
    import email.mime.multipart
    lines = [json.dumps(log) for log in logs]
    payload = "\n".join(lines)
    url = f"https://http-intake.logs.{DD_SITE}/v1/input"
    headers = {"DD-API-KEY": DD_API_KEY}
    if ddtags:
        headers["DD-TAGS"] = ddtags
    req = urllib.request.Request(
        url,
        data=payload.encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return {"status": resp.status}
    except urllib.error.HTTPError as exc:
        raise DatadogAPIError(exc.code, exc.read().decode()) from exc


def log_event(
    title: str,
    message: str,
    priority: str = "normal",
    tags: Optional[list[str]] = None,
    alert_type: str = "info",
) -> dict[str, Any]:
    """
    Post an event to Datadog's event stream.

    Args:
        title: Event title.
        message: Event body.
        priority: 'low' or 'normal'.
        tags: Event tags.
        alert_type: 'error', 'warning', 'info', or 'success'.

    Returns:
        API response with event ID.
    """
    body: dict[str, Any] = {
        "title": title,
        "text": message,
        "priority": priority,
        "alert_type": alert_type,
    }
    if tags:
        body["tags"] = tags
    return _api("POST", "/events", body=body)
