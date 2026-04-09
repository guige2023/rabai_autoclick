"""Metrics dashboard action for collecting and visualizing metrics.

Provides metric registration, aggregation, time-series storage,
and dashboard data generation.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricValue:
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class Metric:
    name: str
    metric_type: MetricType
    description: str = ""
    unit: str = ""
    values: list[MetricValue] = field(default_factory=list)


class MetricsDashboardAction:
    """Collect and aggregate metrics for dashboards.

    Args:
        max_data_points: Maximum data points per metric.
        retention_period: Data retention period in seconds.
    """

    def __init__(
        self,
        max_data_points: int = 1000,
        retention_period: float = 3600.0,
    ) -> None:
        self._metrics: dict[str, Metric] = {}
        self._max_data_points = max_data_points
        self._retention_period = retention_period
        self._export_handlers: list[Callable[[dict], None]] = []

    def register_metric(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
    ) -> bool:
        """Register a new metric.

        Args:
            name: Metric name.
            metric_type: Type of metric.
            description: Metric description.
            unit: Metric unit.

        Returns:
            True if metric was registered.
        """
        if name in self._metrics:
            logger.warning(f"Metric already registered: {name}")
            return False

        self._metrics[name] = Metric(
            name=name,
            metric_type=metric_type,
            description=description,
            unit=unit,
        )
        logger.debug(f"Registered metric: {name}")
        return True

    def record(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
        timestamp: Optional[float] = None,
    ) -> bool:
        """Record a metric value.

        Args:
            name: Metric name.
            value: Metric value.
            labels: Optional metric labels.
            timestamp: Optional timestamp.

        Returns:
            True if value was recorded.
        """
        metric = self._metrics.get(name)
        if not metric:
            self.register_metric(name, MetricType.GAUGE)

        metric = self._metrics[name]
        metric_value = MetricValue(
            value=value,
            timestamp=timestamp or time.time(),
            labels=labels or {},
        )

        metric.values.append(metric_value)

        if len(metric.values) > self._max_data_points:
            metric.values.pop(0)

        self._cleanup_old_values(name)
        return True

    def increment(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
        delta: float = 1.0,
    ) -> bool:
        """Increment a counter metric.

        Args:
            name: Metric name.
            labels: Optional labels.
            delta: Increment amount.

        Returns:
            True if incremented.
        """
        metric = self._metrics.get(name)
        if metric and metric.metric_type != MetricType.COUNTER:
            logger.warning(f"Metric {name} is not a counter")

        current_value = 0.0
        if metric and metric.values:
            current_value = metric.values[-1].value

        return self.record(name, current_value + delta, labels)

    def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> bool:
        """Set a gauge metric value.

        Args:
            name: Metric name.
            value: Gauge value.
            labels: Optional labels.

        Returns:
            True if set.
        """
        return self.record(name, value, labels)

    def _cleanup_old_values(self, name: str) -> None:
        """Remove values outside retention period.

        Args:
            name: Metric name.
        """
        metric = self._metrics.get(name)
        if not metric:
            return

        cutoff = time.time() - self._retention_period
        metric.values = [v for v in metric.values if v.timestamp >= cutoff]

    def get_metric(self, name: str) -> Optional[Metric]:
        """Get a metric by name.

        Args:
            name: Metric name.

        Returns:
            Metric object or None.
        """
        return self._metrics.get(name)

    def get_current_value(self, name: str) -> Optional[float]:
        """Get the most recent value for a metric.

        Args:
            name: Metric name.

        Returns:
            Most recent value or None.
        """
        metric = self._metrics.get(name)
        if metric and metric.values:
            return metric.values[-1].value
        return None

    def get_values(
        self,
        name: str,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> list[MetricValue]:
        """Get metric values within a time range.

        Args:
            name: Metric name.
            start_time: Start timestamp.
            end_time: End timestamp.

        Returns:
            List of metric values.
        """
        metric = self._metrics.get(name)
        if not metric:
            return []

        values = metric.values
        if start_time:
            values = [v for v in values if v.timestamp >= start_time]
        if end_time:
            values = [v for v in values if v.timestamp <= end_time]

        return values

    def get_aggregated_value(
        self,
        name: str,
        operation: str = "mean",
        window_seconds: Optional[float] = None,
    ) -> Optional[float]:
        """Get an aggregated value for a metric.

        Args:
            name: Metric name.
            operation: Aggregation operation ('mean', 'sum', 'min', 'max').
            window_seconds: Time window in seconds.

        Returns:
            Aggregated value or None.
        """
        values = self.get_values(name)
        if not values:
            return None

        if window_seconds:
            cutoff = time.time() - window_seconds
            values = [v for v in values if v.timestamp >= cutoff]

        if not values:
            return None

        numeric_values = [v.value for v in values]

        if operation == "mean":
            return sum(numeric_values) / len(numeric_values)
        elif operation == "sum":
            return sum(numeric_values)
        elif operation == "min":
            return min(numeric_values)
        elif operation == "max":
            return max(numeric_values)

        return None

    def get_all_metrics(self) -> list[str]:
        """Get list of all registered metric names.

        Returns:
            List of metric names.
        """
        return list(self._metrics.keys())

    def export(self) -> dict[str, Any]:
        """Export all metrics in standard format.

        Returns:
            Dictionary with all metric data.
        """
        export_data = {}
        for name, metric in self._metrics.items():
            export_data[name] = {
                "type": metric.metric_type.value,
                "description": metric.description,
                "unit": metric.unit,
                "current_value": metric.values[-1].value if metric.values else None,
                "values": [
                    {"value": v.value, "timestamp": v.timestamp, "labels": v.labels}
                    for v in metric.values[-100:]
                ],
            }
        return export_data

    def register_export_handler(self, handler: Callable[[dict], None]) -> None:
        """Register a handler for metric exports.

        Args:
            handler: Callback function for exporting.
        """
        self._export_handlers.append(handler)

    def trigger_export(self) -> None:
        """Trigger export to all registered handlers."""
        data = self.export()
        for handler in self._export_handlers:
            try:
                handler(data)
            except Exception as e:
                logger.error(f"Export handler error: {e}")

    def clear_metric(self, name: str) -> bool:
        """Clear all values for a metric.

        Args:
            name: Metric name.

        Returns:
            True if metric was found and cleared.
        """
        metric = self._metrics.get(name)
        if metric:
            metric.values.clear()
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Get metrics dashboard statistics.

        Returns:
            Dictionary with stats.
        """
        total_data_points = sum(len(m.values) for m in self._metrics.values())
        by_type: dict[str, int] = {}
        for metric in self._metrics.values():
            type_name = metric.metric_type.value
            by_type[type_name] = by_type.get(type_name, 0) + 1

        return {
            "total_metrics": len(self._metrics),
            "total_data_points": total_data_points,
            "by_type": by_type,
            "max_data_points_per_metric": self._max_data_points,
            "retention_period": self._retention_period,
        }
