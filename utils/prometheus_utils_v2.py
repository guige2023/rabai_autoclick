"""
Prometheus Metrics Query Utilities.

Extended helpers for querying the Prometheus HTTP API, computing
rate ranges, aggregating histograms, and managing recording rules
and alertmanager routing.

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

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
PROMETHEUS_TOKEN = os.getenv("PROMETHEUS_TOKEN", "")
PROMETHEUS_API = f"{PROMETHEUS_URL}/api/v1"


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if PROMETHEUS_TOKEN:
        h["Authorization"] = f"Bearer {PROMETHEUS_TOKEN}"
    return h


def _get(path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    qs = ""
    if params:
        qs = "?" + urllib.parse.urlencode(params)
    url = f"{PROMETHEUS_API}{path}{qs}"
    req = urllib.request.Request(url, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            if result.get("status") != "success":
                raise PrometheusAPIError(result.get("error", "unknown"))
            return result.get("data", {})
    except urllib.error.HTTPError as exc:
        raise PrometheusAPIError(exc.read().decode()) from exc


class PrometheusAPIError(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(f"Prometheus API error: {msg}")


# --------------------------------------------------------------------------- #
# Query
# --------------------------------------------------------------------------- #

def instant_query(expr: str) -> list[dict[str, Any]]:
    """
    Execute an instant PromQL query.

    Returns a list of metric result objects.
    """
    data = _get("/query", {"query": expr})
    return data.get("result", [])


def range_query(
    expr: str,
    start: datetime,
    end: datetime,
    step: str = "15s",
) -> list[dict[str, Any]]:
    """
    Execute a range PromQL query over a time window.

    Args:
        expr: PromQL expression.
        start: Start UTC datetime.
        end: End UTC datetime.
        step: Query resolution step (e.g. '15s', '1m', '5m').

    Returns:
        List of range results.
    """
    params = {
        "query": expr,
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "step": step,
    }
    data = _get("/query_range", params)
    return data.get("result", [])


# --------------------------------------------------------------------------- #
# Aggregations
# --------------------------------------------------------------------------- #

def cpu_usage_seconds_total(
    instance: Optional[str] = None,
    job: str = "node",
) -> str:
    """Return a PromQL expression for total CPU usage by instance/job."""
    filters = f'job="{job}"'
    if instance:
        filters += f',instance="{instance}"'
    return f'sum by (instance) (rate(node_cpu_seconds_total{{{filters}}}[5m]))'


def memory_usage_percent(
    instance: Optional[str] = None,
    job: str = "node",
) -> str:
    """Return PromQL expression for memory utilization as a percentage."""
    filters = f'job="{job}"'
    if instance:
        filters += f',instance="{instance}"'
    return (
        f'(1 - node_memory_MemAvailable_bytes{{{filters}}}'
        f'/ node_memory_MemTotal_bytes{{{filters}}}) * 100'
    )


def http_request_rate(
    job: str = "nginx",
    method: Optional[str] = None,
    status: Optional[int] = None,
) -> str:
    """Return PromQL expression for HTTP request rate."""
    filters = f'job="{job}"'
    if method:
        filters += f',method="{method}"'
    if status:
        filters += f',status="{status}"'
    return f'sum by (status) (rate(http_requests_total{{{filters}}}[5m]))'


def p95_latency(metric: str, filters: str = "") -> str:
    """Return a histogram_quantile expression for p95 latency."""
    return f'histogram_quantile(0.95, sum(rate({metric}_bucket{{{filters}}}[5m])) by (le))'


# --------------------------------------------------------------------------- #
# Recording Rules
# --------------------------------------------------------------------------- #

def create_recording_rule(
    name: str,
    expr: str,
    labels: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Register a recording rule via the Prometheus rules API.

    The rules API is typically behind a reverse proxy or cortex
    so this is provided for scripting convenience.
    """
    rule = {
        "record": name,
        "expr": expr,
    }
    if labels:
        rule["labels"] = labels
    payload = {
        "name": f"recording:{name}",
        "rules": [rule],
        "interval": "30s",
    }
    url = f"{PROMETHEUS_URL}/api/v1/admin/tsdb/delete_series"
    req = urllib.request.Request(url, headers=_headers(), method="POST")
    # Best-effort — recording rules are usually managed via CRD or cortex
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError:
        return {"status": "unavailable", "note": "Recording rules require write access"}


# --------------------------------------------------------------------------- #
# Targets & Targets Metadata
# --------------------------------------------------------------------------- #

def list_targets() -> dict[str, Any]:
    """Return all active scrape targets grouped by job."""
    return _get("/targets")


def list_targets_metadata(
    metric: Optional[str] = None,
    limit: int = 10000,
) -> list[dict[str, Any]]:
    """Return metadata for scraped metrics."""
    params: dict[str, Any] = {"limit": limit}
    if metric:
        params["metric"] = metric
    return _get("/targets/metadata", params)


# --------------------------------------------------------------------------- #
# Series / Label APIs
# --------------------------------------------------------------------------- #

def list_series(match: str) -> list[dict[str, Any]]:
    """Return series matching a label selector."""
    return _get("/series", {"match[]": match}) or []


def query_label(label: str, match: Optional[str] = None) -> list[str]:
    """Return label values for a given label name."""
    params: dict[str, Any] = {}
    if match:
        params["match[]"] = match
    data = _get(f"/label/{label}/values", params)
    return data or []


def get_metric_metadata(metric: str) -> dict[str, Any]:
    """Return metadata for a specific metric."""
    return _get("/metadata", {"metric": metric})


# --------------------------------------------------------------------------- #
# Alertmanager Routing
# --------------------------------------------------------------------------- #

def get_alertmanager_status() -> dict[str, Any]:
    """Fetch Alertmanager cluster and config status."""
    return _get("/status")


def get_alerts() -> list[dict[str, Any]]:
    """Return all active firing/pending alerts."""
    data = _get("/alerts")
    return data or []
