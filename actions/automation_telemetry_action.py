"""
Automation Telemetry Action Module.

Collects and reports telemetry data from automation workflows,
including custom metrics, traces, and operational insights.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import logging
import json

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of telemetry metrics."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    SUMMARY = auto()


@dataclass
class TelemetryPoint:
    """A single telemetry data point."""
    name: str
    metric_type: MetricType
    value: float
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)
    unit: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "type": self.metric_type.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags,
            "unit": self.unit,
        }


@dataclass
class TraceSpan:
    """A trace span for distributed tracing."""
    name: str
    trace_id: str
    span_id: str
    parent_id: Optional[str]
    start_time: datetime
    end_time: Optional[datetime] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "OK"

    def duration_ms(self) -> Optional[float]:
        """Calculate span duration in milliseconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add an event to this span."""
        self.events.append({
            "name": name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "attributes": attributes or {},
        })


@dataclass
class TelemetryBatch:
    """A batch of telemetry data for sending."""
    points: List[TelemetryPoint] = field(default_factory=list)
    spans: List[TraceSpan] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def size(self) -> int:
        """Get total number of telemetry items."""
        return len(self.points) + len(self.spans)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "points": [p.to_dict() for p in self.points],
            "spans": [
                {
                    "name": s.name,
                    "trace_id": s.trace_id,
                    "span_id": s.span_id,
                    "duration_ms": s.duration_ms(),
                    "status": s.status,
                    "attributes": s.attributes,
                }
                for s in self.spans
            ],
            "metadata": self.metadata,
        }


class TelemetryExporter:
    """Base class for telemetry exporters."""

    def __init__(self, endpoint: str, api_key: Optional[str] = None):
        """
        Initialize exporter.

        Args:
            endpoint: Telemetry backend endpoint.
            api_key: Optional API key for authentication.
        """
        self.endpoint = endpoint
        self.api_key = api_key

    def export(self, batch: TelemetryBatch) -> bool:
        """
        Export a telemetry batch.

        Args:
            batch: Batch to export.

        Returns:
            True if export succeeded.
        """
        raise NotImplementedError


class ConsoleExporter(TelemetryExporter):
    """Console exporter for debugging."""

    def export(self, batch: TelemetryBatch) -> bool:
        """Export to console."""
        logger.info(f"Telemetry: {batch.size()} items")
        for point in batch.points:
            logger.info(f"  Metric: {point.name}={point.value} {point.unit}")
        for span in batch.spans:
            logger.info(f"  Trace: {span.name} ({span.duration_ms()}ms)")
        return True


class AutomationTelemetryAction:
    """
    Collects and exports telemetry from automation workflows.

    This action instruments automation code to collect metrics, traces,
    and operational data, exporting to configurable backends.

    Example:
        >>> telemetry = AutomationTelemetryAction()
        >>> telemetry.increment_counter("automation.executions", tags={"env": "prod"})
        >>> with telemetry.trace("data_processing") as span:
        ...     process_data()
        >>> telemetry.flush()
    """

    def __init__(
        self,
        service_name: str = "automation",
        exporters: Optional[List[TelemetryExporter]] = None,
        flush_interval_seconds: float = 60.0,
        max_batch_size: int = 1000,
    ):
        """
        Initialize the Automation Telemetry.

        Args:
            service_name: Name of the service for telemetry context.
            exporters: List of telemetry exporters.
            flush_interval_seconds: Auto-flush interval.
            max_batch_size: Maximum batch size before auto-flush.
        """
        self.service_name = service_name
        self.exporters = exporters or [ConsoleExporter("console")]
        self.flush_interval = flush_interval_seconds
        self.max_batch_size = max_batch_size

        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
        self._spans: Dict[str, TraceSpan] = {}
        self._batch = TelemetryBatch()
        self._last_flush = datetime.now(timezone.utc)

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name.
            value: Value to increment by.
            tags: Optional tags.
        """
        self._counters[name] = self._counters.get(name, 0) + value

        point = TelemetryPoint(
            name=name,
            metric_type=MetricType.COUNTER,
            value=self._counters[name],
            timestamp=datetime.now(timezone.utc),
            tags=tags or {},
        )
        self._batch.points.append(point)
        self._maybe_flush()

    def set_gauge(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name.
            value: Gauge value.
            tags: Optional tags.
        """
        self._gauges[name] = value

        point = TelemetryPoint(
            name=name,
            metric_type=MetricType.GAUGE,
            value=value,
            timestamp=datetime.now(timezone.utc),
            tags=tags or {},
        )
        self._batch.points.append(point)
        self._maybe_flush()

    def record_histogram(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a histogram value.

        Args:
            name: Metric name.
            value: Histogram value.
            tags: Optional tags.
        """
        if name not in self._histograms:
            self._histograms[name] = []
        self._histograms[name].append(value)

        point = TelemetryPoint(
            name=name,
            metric_type=MetricType.HISTOGRAM,
            value=value,
            timestamp=datetime.now(timezone.utc),
            tags=tags or {},
        )
        self._batch.points.append(point)
        self._maybe_flush()

    def start_span(
        self,
        name: str,
        trace_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> str:
        """
        Start a new trace span.

        Args:
            name: Span name.
            trace_id: Optional trace ID for distributed tracing.
            parent_id: Optional parent span ID.

        Returns:
            Span ID.
        """
        import uuid

        span_id = str(uuid.uuid4())[:16]
        if trace_id is None:
            trace_id = str(uuid.uuid4())

        span = TraceSpan(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_id=parent_id,
            start_time=datetime.now(timezone.utc),
        )
        self._spans[span_id] = span
        return span_id

    def end_span(
        self,
        span_id: str,
        status: str = "OK",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        End a trace span.

        Args:
            span_id: ID of span to end.
            status: Span status.
            attributes: Optional final attributes.
        """
        if span_id not in self._spans:
            return

        span = self._spans[span_id]
        span.end_time = datetime.now(timezone.utc)
        span.status = status
        if attributes:
            span.attributes.update(attributes)

        self._batch.spans.append(span)
        self._maybe_flush()

    def trace(self, name: str):
        """
        Context manager for tracing a code block.

        Args:
            name: Span name.

        Yields:
            TraceSpan wrapper.
        """
        return TraceContext(self, name)

    def _maybe_flush(self) -> None:
        """Flush if batch is full or interval elapsed."""
        if self._batch.size() >= self.max_batch_size:
            self.flush()

    def flush(self) -> bool:
        """
        Flush all pending telemetry to exporters.

        Returns:
            True if all exports succeeded.
        """
        if self._batch.size() == 0:
            return True

        self._batch.metadata = {
            "service": self.service_name,
            "flushed_at": datetime.now(timezone.utc).isoformat(),
        }

        success = True
        for exporter in self.exporters:
            try:
                if not exporter.export(self._batch):
                    success = False
            except Exception as e:
                logger.error(f"Telemetry export failed: {e}")
                success = False

        self._batch = TelemetryBatch()
        self._last_flush = datetime.now(timezone.utc)
        return success

    def get_histogram_stats(self, name: str) -> Dict[str, float]:
        """Get statistics for a histogram."""
        if name not in self._histograms:
            return {}

        values = self._histograms[name]
        if not values:
            return {}

        sorted_values = sorted(values)
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "p50": sorted_values[len(values) // 2],
            "p95": sorted_values[int(len(values) * 0.95)],
            "p99": sorted_values[int(len(values) * 0.99)],
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get telemetry summary."""
        return {
            "service": self.service_name,
            "batch_size": self._batch.size(),
            "counters": self._counters.copy(),
            "gauges": self._gauges.copy(),
            "histograms": {
                name: self.get_histogram_stats(name)
                for name in self._histograms
            },
            "active_spans": len(self._spans),
            "last_flush": self._last_flush.isoformat(),
        }


class TraceContext:
    """Context manager wrapper for tracing."""

    def __init__(self, telemetry: AutomationTelemetryAction, name: str):
        """Initialize trace context."""
        self.telemetry = telemetry
        self.name = name
        self.span_id: Optional[str] = None

    def __enter__(self) -> "TraceContext":
        """Start the span."""
        self.span_id = self.telemetry.start_span(self.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End the span."""
        status = "OK" if exc_type is None else "ERROR"
        attrs = {"error.type": exc_type.__name__} if exc_type else None
        self.telemetry.end_span(self.span_id, status=status, attributes=attrs)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add event to current span."""
        if self.span_id and self.span_id in self.telemetry._spans:
            self.telemetry._spans[self.span_id].add_event(name, attributes)


def create_telemetry_action(
    service_name: str = "automation",
    **kwargs,
) -> AutomationTelemetryAction:
    """Factory function to create an AutomationTelemetryAction."""
    return AutomationTelemetryAction(service_name=service_name, **kwargs)
