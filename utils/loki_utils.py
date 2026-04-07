"""
Grafana Loki Log Aggregation Utilities.

Helpers for querying Loki's LogQL API, managing label values,
computing log-based metrics, and integrating with Grafana dashboards.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100")
LOKI_USER = os.getenv("LOKI_USER", "")
LOKI_PASSWORD = os.getenv("LOKI_PASSWORD", "")
LOKI_API_BASE = f"{LOKI_URL}/loki/api/v1"


def _auth_headers() -> dict[str, str]:
    import base64
    h: dict[str, str] = {}
    if LOKI_USER and LOKI_PASSWORD:
        creds = base64.b64encode(
            f"{LOKI_USER}:{LOKI_PASSWORD}".encode()
        ).decode()
        h["Authorization"] = f"Basic {creds}"
    return h


def _request(
    method: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{LOKI_API_BASE}{path}"
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}?{qs}"
    data = json.dumps(body).encode() if body else None
    headers = {**_auth_headers(), "Content-Type": "application/json"}
    req = urllib.request.Request(
        url, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise LokiAPIError(exc.code, exc.read().decode()) from exc


class LokiAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Loki API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Labels & Series
# --------------------------------------------------------------------------- #

def list_labels(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> list[str]:
    """
    Return all label names known to Loki.

    Args:
        start: Start time (UTC).
        end: End time (UTC).

    Returns:
        List of label name strings.
    """
    params: dict[str, str] = {}
    if start:
        params["start"] = str(int(start.timestamp())) + "ns"
    if end:
        params["end"] = str(int(end.timestamp())) + "ns"
    data = _request("GET", "/labels", params=params)
    return data.get("data", [])


def label_values(
    label: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> list[str]:
    """
    Return all values for a given label name.

    Args:
        label: Label name (e.g. 'app', 'namespace').
        start: Start time (UTC).
        end: End time (UTC).

    Returns:
        List of label value strings.
    """
    params: dict[str, str] = {}
    if start:
        params["start"] = str(int(start.timestamp())) + "ns"
    if end:
        params["end"] = str(int(end.timestamp())) + "ns"
    data = _request("GET", f"/label/{label}/values", params=params)
    return data.get("data", [])


def series(
    match: list[str],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """
    Return series (label sets) matching a set of label matchers.

    Args:
        match: List of label matcher strings (e.g. ['{app="nginx"}']).
        start: Start time (UTC).
        end: End time (UTC).

    Returns:
        List of series dicts with label sets.
    """
    params: dict[str, Any] = {"match[]": match}
    if start:
        params["start"] = str(int(start.timestamp())) + "ns"
    if end:
        params["end"] = str(int(end.timestamp())) + "ns"
    data = _request("GET", "/series", params=params)
    return data.get("data", [])


# --------------------------------------------------------------------------- #
# Log Queries
# --------------------------------------------------------------------------- #

def query_logs(
    logql: str,
    limit: int = 100,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    direction: str = "backward",
) -> list[dict[str, Any]]:
    """
    Execute a LogQL query and return matching log lines.

    Args:
        logql: LogQL query expression.
        limit: Maximum number of log lines to return.
        start: Start time (UTC).
        end: End time (UTC).
        direction: 'forward' or 'backward'.

    Returns:
        List of log stream entries with timestamp, labels, and content.
    """
    params: dict[str, Any] = {
        "query": logql,
        "limit": str(limit),
        "direction": direction,
    }
    if start:
        params["start"] = str(int(start.timestamp())) + "ns"
    if end:
        params["end"] = str(int(end.timestamp())) + "ns"
    data = _request("GET", "/query", params=params)
    streams = data.get("data", {}).get("result", [])
    results: list[dict[str, Any]] = []
    for stream in streams:
        for ts, line in stream.get("values", []):
            results.append({
                "timestamp": ts,
                "stream": stream.get("stream", {}),
                "line": line,
            })
    return results


def query_range_logs(
    logql: str,
    start: datetime,
    end: datetime,
    limit_per_stream: int = 100,
    step: str = "15s",
) -> list[dict[str, Any]]:
    """
    Execute a LogQL range query over a time window.

    Args:
        logql: LogQL query expression.
        start: Start time (UTC).
        end: End time (UTC).
        limit_per_stream: Max lines per stream.
        step: Query resolution step.

    Returns:
        List of log stream entries.
    """
    params: dict[str, Any] = {
        "query": logql,
        "limit": str(limit_per_stream),
        "start": str(int(start.timestamp())) + "ns",
        "end": str(int(end.timestamp())) + "ns",
        "step": step,
    }
    data = _request("GET", "/query_range", params=params)
    streams = data.get("data", {}).get("result", [])
    results: list[dict[str, Any]] = []
    for stream in streams:
        for ts, line in stream.get("values", []):
            results.append({
                "timestamp": ts,
                "stream": stream.get("stream", {}),
                "line": line,
            })
    return results


# --------------------------------------------------------------------------- #
# Metric Queries (LogQL)
# --------------------------------------------------------------------------- #

def query_metric(
    logql: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> dict[str, Any]:
    """
    Execute a LogQL metric query (rate, count_over_time, etc.).

    Args:
        logql: LogQL metric expression.
        start: Start time (UTC).
        end: End time (UTC).

    Returns:
        Query result with metric sample data.
    """
    params: dict[str, Any] = {"query": logql}
    if start:
        params["time"] = str(int(start.timestamp()))
    if end:
        params["end"] = str(int(end.timestamp()))
    return _request("GET", "/query", params=params)


def query_metric_range(
    logql: str,
    start: datetime,
    end: datetime,
    step: str = "15s",
) -> dict[str, Any]:
    """
    Execute a range metric query.

    Args:
        logql: LogQL metric expression.
        start: Start time (UTC).
        end: End time (UTC).
        step: Query resolution.

    Returns:
        Range query result with time series samples.
    """
    params: dict[str, Any] = {
        "query": logql,
        "start": str(int(start.timestamp())) + "ns",
        "end": str(int(end.timestamp())) + "ns",
        "step": step,
    }
    return _request("GET", "/query_range", params=params)


# --------------------------------------------------------------------------- #
# LogQL Helpers
# --------------------------------------------------------------------------- #

def rate(expr: str) -> str:
    """Wrap an expression in a rate() counter aggregation."""
    return f'rate({expr}[5m])'


def count_over_time(expr: str) -> str:
    """Count log entries over a time window."""
    return f'count_over_time({expr}[5m])'


def bytes_rate(expr: str) -> str:
    """Compute bytes/s rate for log volume."""
    return f'bytes_rate({expr}[5m])'


def label_matchers(labels: dict[str, str]) -> str:
    """Build a LogQL label matcher string from a dict."""
    parts = [f'{k}="{v}"' for k, v in labels.items()]
    return "{" + ",".join(parts) + "}"


def json_filter(field: str, op: str, value: Any) -> str:
    """Build a LogQL JSON expression filter."""
    return f'json | {field} {op} "{value}"'


def line_filter(pattern: str) -> str:
    """Build a LogQL line contains filter."""
    return f'|~ "{pattern}"'


# --------------------------------------------------------------------------- #
# Push (Write) API
# --------------------------------------------------------------------------- #

def push_logs(
    streams: list[dict[str, Any]],
    tenant_id: Optional[str] = None,
) -> bool:
    """
    Push log entries to Loki.

    Args:
        streams: List of log stream dicts with 'stream' (labels) and 'values'.
        tenant_id: Tenant ID for multi-tenant Loki.

    Returns:
        True if push succeeded.
    """
    url = f"{LOKI_URL}/loki/api/v1/push"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if tenant_id:
        headers["X-Scope-OrgID"] = tenant_id
    data = json.dumps({"streams": streams}).encode()
    req = urllib.request.Request(
        url, data=data, headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status in (200, 204)
    except urllib.error.HTTPError as exc:
        raise LokiAPIError(exc.code, exc.read().decode()) from exc
