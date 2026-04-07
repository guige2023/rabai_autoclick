"""
Datadog monitoring utilities for metrics, traces, and dashboards.

Provides metric submission, APM trace creation, dashboard management,
SLO tracking, and monitor configuration.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class DatadogConfig:
    """Configuration for Datadog API."""
    api_key: str = ""
    app_key: str = ""
    site: str = "datadoghq.com"  # datadoghq.com, datadoghq.eu, us3.datadoghq.com
    api_url: str = ""
    metrics_url: str = ""

    def __post_init__(self) -> None:
        if not self.api_url:
            self.api_url = f"https://api.{self.site}"
        if not self.metrics_url:
            self.metrics_url = f"https://api.{self.site}"


class MetricPoint:
    """A single metric data point."""

    def __init__(self, value: float, timestamp: Optional[float] = None) -> None:
        self.value = value
        self.timestamp = timestamp or time.time()


@dataclass
class DogStatsDConfig:
    """Configuration for DogStatsD client."""
    host: str = "localhost"
    port: int = 8125
    max_buffer_size: int = 100
    timeout: float = 1.0


class DatadogMetrics:
    """Submit metrics to Datadog."""

    def __init__(self, config: Optional[DatadogConfig] = None) -> None:
        self.config = config or DatadogConfig()
        self._metrics_buffer: list[dict[str, Any]] = []

    def gauge(
        self,
        metric: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
        hostname: Optional[str] = None,
    ) -> None:
        """Submit a gauge metric."""
        point = {"value": value, "timestamp": time.time()}
        self._submit_metric(metric, "gauge", [point], tags, hostname)

    def count(
        self,
        metric: str,
        value: float = 1,
        tags: Optional[dict[str, str]] = None,
        hostname: Optional[str] = None,
    ) -> None:
        """Submit a count metric."""
        point = {"value": value, "timestamp": time.time()}
        self._submit_metric(metric, "count", [point], tags, hostname)

    def histogram(
        self,
        metric: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
        hostname: Optional[str] = None,
    ) -> None:
        """Submit a histogram metric."""
        point = {"value": value, "timestamp": time.time()}
        self._submit_metric(metric, "histogram", [point], tags, hostname)

    def distribution(
        self,
        metric: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
        hostname: Optional[str] = None,
    ) -> None:
        """Submit a distribution metric."""
        point = {"value": value, "timestamp": time.time()}
        self._submit_metric(metric, "distribution", [point], tags, hostname)

    def _submit_metric(
        self,
        metric: str,
        metric_type: str,
        points: list[dict[str, float]],
        tags: Optional[dict[str, str]],
        hostname: Optional[str],
    ) -> None:
        """Submit a metric to Datadog API."""
        series = {
            "metric": metric,
            "type": metric_type,
            "points": [[p["timestamp"], p["value"]] for p in points],
            "tags": [f"{k}:{v}" for k, v in (tags or {}).items()],
        }
        if hostname:
            series["hostname"] = hostname

        self._metrics_buffer.append(series)
        if len(self._metrics_buffer) >= 100:
            self.flush()

    def flush(self) -> bool:
        """Flush buffered metrics to Datadog."""
        if not self._metrics_buffer:
            return True
        payload = {"series": self._metrics_buffer}
        try:
            response = httpx.post(
                f"{self.config.api_url}/api/v2/series",
                json=payload,
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            self._metrics_buffer.clear()
            return response.status_code in (200, 201, 202)
        except Exception as e:
            logger.error("Failed to submit metrics: %s", e)
            return False


class DatadogTraces:
    """APM trace utilities for Datadog."""

    def __init__(self, config: Optional[DatadogConfig] = None) -> None:
        self.config = config or DatadogConfig()
        self._traces: list[list[dict[str, Any]]] = []

    def create_span(
        self,
        name: str,
        service: str,
        resource: str,
        span_type: str = "web",
        error: bool = False,
        tags: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Create a trace span."""
        trace_id = int(time.time() * 1e6) % (2 ** 64)
        span_id = int(time.time() * 1e6) % (2 ** 64)
        return {
            "trace_id": trace_id,
            "span_id": span_id,
            "name": name,
            "service": service,
            "resource": resource,
            "type": span_type,
            "error": error,
            "tags": tags or {},
            "start": time.time(),
        }

    def finish_span(self, span: dict[str, Any]) -> dict[str, Any]:
        """Finalize a span with duration."""
        span["duration"] = int((time.time() - span.pop("start")) * 1e6)
        return span

    def submit_traces(self, traces: list[list[dict[str, Any]]]) -> bool:
        """Submit traces to Datadog APM."""
        payload = {"traces": traces}
        try:
            response = httpx.post(
                f"{self.config.api_url}/api/v0.3/traces",
                json=payload,
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            return response.status_code in (200, 201, 202)
        except Exception as e:
            logger.error("Failed to submit traces: %s", e)
            return False


class DatadogMonitors:
    """Manage Datadog monitors."""

    def __init__(self, config: Optional[DatadogConfig] = None) -> None:
        self.config = config or DatadogConfig()

    def create_monitor(
        self,
        name: str,
        query: str,
        monitor_type: str = "metric alert",
        tags: Optional[list[str]] = None,
        message: str = "",
    ) -> Optional[dict[str, Any]]:
        """Create a new monitor."""
        payload = {
            "name": name,
            "type": monitor_type,
            "query": query,
            "message": message or f"Alert: {name}",
            "tags": tags or [],
        }
        try:
            response = httpx.post(
                f"{self.config.api_url}/api/v1/monitor",
                json=payload,
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "DD-APPLICATION-KEY": self.config.app_key,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
            if response.status_code in (200, 201):
                return response.json()
            logger.error("Failed to create monitor: %s", response.text)
        except Exception as e:
            logger.error("Failed to create monitor: %s", e)
        return None

    def list_monitors(self, tags_filter: Optional[list[str]] = None) -> list[dict[str, Any]]:
        """List all monitors."""
        params = {}
        if tags_filter:
            params["monitor_tags"] = ",".join(tags_filter)
        try:
            response = httpx.get(
                f"{self.config.api_url}/api/v1/monitor",
                params=params,
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "DD-APPLICATION-KEY": self.config.app_key,
                },
                timeout=30,
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error("Failed to list monitors: %s", e)
        return []

    def delete_monitor(self, monitor_id: int) -> bool:
        """Delete a monitor by ID."""
        try:
            response = httpx.delete(
                f"{self.config.api_url}/api/v1/monitor/{monitor_id}",
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "DD-APPLICATION-KEY": self.config.app_key,
                },
                timeout=30,
            )
            return response.status_code in (200, 204)
        except Exception as e:
            logger.error("Failed to delete monitor: %s", e)
            return False


class DatadogDashboards:
    """Manage Datadog dashboards."""

    def __init__(self, config: Optional[DatadogConfig] = None) -> None:
        self.config = config or DatadogConfig()

    def create_dashboard(
        self,
        title: str,
        widgets: list[dict[str, Any]],
        layout_type: str = "ordered",
    ) -> Optional[dict[str, Any]]:
        """Create a new dashboard."""
        payload = {
            "title": title,
            "layout_type": layout_type,
            "widgets": widgets,
        }
        try:
            response = httpx.post(
                f"{self.config.api_url}/api/v1/dashboard",
                json=payload,
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "DD-APPLICATION-KEY": self.config.app_key,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
            if response.status_code in (200, 201):
                return response.json()
        except Exception as e:
            logger.error("Failed to create dashboard: %s", e)
        return None

    def list_dashboards(self) -> list[dict[str, Any]]:
        """List all dashboards."""
        try:
            response = httpx.get(
                f"{self.config.api_url}/api/v1/dashboard",
                headers={
                    "DD-API-KEY": self.config.api_key,
                    "DD-APPLICATION-KEY": self.config.app_key,
                },
                timeout=30,
            )
            if response.status_code == 200:
                return response.json().get("dashboards", [])
        except Exception as e:
            logger.error("Failed to list dashboards: %s", e)
        return []


class SLOTracker:
    """Track Service Level Objectives."""

    def __init__(self, dd_client: DatadogMetrics) -> None:
        self._client = dd_client
        self._slo_metrics: dict[str, list[bool]] = {}

    def record_request(self, slo_name: str, success: bool, tags: Optional[dict[str, str]] = None) -> None:
        """Record a request outcome for SLO tracking."""
        if slo_name not in self._slo_metrics:
            self._slo_metrics[slo_name] = []
        self._slo_metrics[slo_name].append(success)

        metric_name = f"slo.{slo_name}.good"
        self._client.gauge(metric_name, 1 if success else 0, tags=tags)

    def calculate_slo(self, slo_name: str, window_seconds: int = 3600) -> Optional[float]:
        """Calculate SLO percentage for the given window."""
        if slo_name not in self._slo_metrics:
            return None
        results = self._slo_metrics[slo_name]
        if not results:
            return None
        good = sum(1 for r in results if r)
        return (good / len(results)) * 100
