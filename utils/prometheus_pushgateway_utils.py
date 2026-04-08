"""
Prometheus Pushgateway client utilities.

Provides metrics pushing to Prometheus Pushgateway.
"""

from __future__ import annotations

import time
from typing import Literal


MetricType = Literal["counter", "gauge", "histogram", "summary"]


class PushgatewayClient:
    """Client for pushing metrics to Prometheus Pushgateway."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9091,
        job: str = "default_job",
    ):
        self.base_url = f"http://{host}:{port}"
        self.job = job

    def _escape_label(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def push_metric(
        self,
        metric_name: str,
        value: float,
        metric_type: MetricType = "gauge",
        labels: dict[str, str] | None = None,
        grouping: dict[str, str] | None = None,
    ) -> bool:
        """
        Push a single metric.

        Args:
            metric_name: Metric name
            value: Metric value
            metric_type: Prometheus metric type
            labels: Additional labels
            grouping: Pushgateway grouping labels

        Returns:
            True on success
        """
        import urllib.request
        import urllib.parse

        labels = labels or {}
        label_str = ",".join(f'{k}="{self._escape_label(v)}"' for k, v in labels.items())
        body = f"{metric_name}{{{label_str}}} {value}\n"

        group_str = ""
        if grouping:
            group_str = ";" + ";".join(f'{k}="{self._escape_label(v)}"' for k, v in grouping.items())

        url = f"{self.base_url}/metrics/job/{self.job}{group_str}"
        req = urllib.request.Request(
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
        )
        try:
            urllib.request.urlopen(req, timeout=5)
            return True
        except OSError:
            return False

    def push_counter(
        self,
        name: str,
        value: float = 1,
        labels: dict[str, str] | None = None,
        grouping: dict[str, str] | None = None,
    ) -> bool:
        return self.push_metric(name, value, "counter", labels, grouping)

    def push_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
        grouping: dict[str, str] | None = None,
    ) -> bool:
        return self.push_metric(name, value, "gauge", labels, grouping)

    def push_histogram(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
        grouping: dict[str, str] | None = None,
    ) -> bool:
        return self.push_metric(name, value, "histogram", labels, grouping)

    def push_summary(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
        grouping: dict[str, str] | None = None,
    ) -> bool:
        return self.push_metric(name, value, "summary", labels, grouping)

    def push_metrics_batch(
        self,
        metrics: list[tuple[str, float, MetricType, dict[str, str] | None]],
        grouping: dict[str, str] | None = None,
    ) -> bool:
        """
        Push multiple metrics at once.

        Args:
            metrics: List of (name, value, type, labels)
            grouping: Pushgateway grouping

        Returns:
            True on success
        """
        import urllib.request

        lines = []
        for name, value, mtype, labels in metrics:
            labels = labels or {}
            label_str = ",".join(f'{k}="{self._escape_label(v)}"' for k, v in labels.items())
            lines.append(f"{name}{{{label_str}}} {value}")

        body = "\n".join(lines) + "\n"
        group_str = ""
        if grouping:
            group_str = ";" + ";".join(f'{k}="{self._escape_label(v)}"' for k, v in grouping.items())

        url = f"{self.base_url}/metrics/job/{self.job}{group_str}"
        req = urllib.request.Request(
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            return True
        except OSError:
            return False

    def delete_job(self, grouping: dict[str, str] | None = None) -> bool:
        """
        Delete all metrics for a job.

        Args:
            grouping: Grouping labels to match

        Returns:
            True on success
        """
        import urllib.request

        group_str = ""
        if grouping:
            group_str = "?" + "&".join(
                f"g={urllib.parse.quote(f'{k}={v}')}" for k, v in grouping.items()
            )
        url = f"{self.base_url}/metrics/job/{self.job}{group_str}"
        req = urllib.request.Request(url, method="DELETE")
        try:
            urllib.request.urlopen(req, timeout=5)
            return True
        except OSError:
            return False


def gauge_from_timing(
    name: str,
    duration_ms: float,
    client: PushgatewayClient | None = None,
) -> bool:
    """Helper to push a timing value as a gauge."""
    if client is None:
        client = PushgatewayClient()
    return client.push_gauge(name, duration_ms)
