"""
OpenTelemetry utilities for distributed tracing and observability.

Provides tracer setup, span management, metrics export, baggage
propagation, and auto-instrumentation helpers.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SpanKind(Enum):
    """OpenTelemetry span kinds."""
    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


@dataclass
class SpanConfig:
    """Configuration for creating spans."""
    name: str
    kind: SpanKind = SpanKind.INTERNAL
    attributes: dict[str, Any] = field(default_factory=dict)
    start_time: Optional[float] = None
    parent: Optional[Any] = None


@dataclass
class TraceConfig:
    """Configuration for OpenTelemetry tracing."""
    service_name: str = "unknown-service"
    service_version: str = "1.0.0"
    exporter: str = "otlp"  # otlp, jaeger, zipkin, console
    endpoint: str = "http://localhost:4317"
    headers: dict[str, str] = field(default_factory=dict)
    sample_rate: float = 1.0
    max_export_batch_size: int = 512
    export_timeout_ms: int = 30000


@dataclass
class MetricConfig:
    """Configuration for OpenTelemetry metrics."""
    service_name: str = "unknown-service"
    exporter: str = "otlp"
    endpoint: str = "http://localhost:4317"
    export_interval_ms: int = 10000


@dataclass
class Span:
    """Represents an OpenTelemetry span."""
    name: str
    trace_id: str
    span_id: str
    kind: SpanKind
    start_time: float
    end_time: Optional[float] = None
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    status: str = "OK"

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Optional[dict[str, Any]] = None) -> None:
        self.events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {},
        })

    def set_status(self, status: str, message: str = "") -> None:
        self.status = status
        if message:
            self.attributes["status.message"] = message

    def end(self, end_time: Optional[float] = None) -> None:
        self.end_time = end_time or time.time()

    @property
    def duration_ms(self) -> float:
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time) * 1000


class OpenTelemetryTracer:
    """OpenTelemetry tracer with span management."""

    def __init__(self, config: Optional[TraceConfig] = None) -> None:
        self.config = config or TraceConfig()
        self._tracer: Any = None
        self._spans: list[Span] = []
        self._root_span: Optional[Span] = None

    def _generate_ids(self) -> tuple[str, str]:
        """Generate trace and span IDs."""
        import random
        trace_id = format(random.getrandbits(128), "032x")
        span_id = format(random.getrandbits(64), "016x")
        return trace_id, span_id

    def start_span(self, config: SpanConfig) -> Span:
        """Start a new span."""
        trace_id, span_id = self._generate_ids()
        span = Span(
            name=config.name,
            trace_id=trace_id,
            span_id=span_id,
            kind=config.kind,
            start_time=config.start_time or time.time(),
            attributes=dict(config.attributes),
        )
        self._spans.append(span)
        if not self._root_span:
            self._root_span = span
        logger.debug("Started span: %s [%s]", span.name, span_id[:8])
        return span

    def end_span(self, span: Span) -> None:
        """End a span."""
        span.end()
        logger.debug("Ended span: %s (%.2fms)", span.name, span.duration_ms)

    def get_traces(self) -> list[Span]:
        """Get all recorded spans."""
        return self._spans

    def clear(self) -> None:
        """Clear all recorded spans."""
        self._spans.clear()
        self._root_span = None

    def export(self) -> dict[str, Any]:
        """Export traces as a dictionary."""
        return {
            "service_name": self.config.service_name,
            "spans": [
                {
                    "name": s.name,
                    "trace_id": s.trace_id,
                    "span_id": s.span_id,
                    "kind": s.kind.name,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "duration_ms": s.duration_ms,
                    "attributes": s.attributes,
                    "events": s.events,
                    "status": s.status,
                }
                for s in self._spans
            ],
        }


class TracerContext:
    """Context manager for creating spans."""

    def __init__(self, tracer: OpenTelemetryTracer, config: SpanConfig) -> None:
        self.tracer = tracer
        self.config = config
        self._span: Optional[Span] = None

    def __enter__(self) -> Span:
        self._span = self.tracer.start_span(self.config)
        return self._span

    def __exit__(self, *args: Any) -> None:
        if self._span:
            self._span.end()


class MetricsCollector:
    """Collects and exports OpenTelemetry metrics."""

    def __init__(self, config: Optional[MetricConfig] = None) -> None:
        self.config = config or MetricConfig()
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}

    def counter(self, name: str, value: float = 1, attributes: Optional[dict[str, str]] = None) -> None:
        """Record a counter metric."""
        key = self._make_key(name, attributes or {})
        self._counters[key] = self._counters.get(key, 0) + value

    def gauge(self, name: str, value: float, attributes: Optional[dict[str, str]] = None) -> None:
        """Record a gauge metric."""
        key = self._make_key(name, attributes or {})
        self._gauges[key] = value

    def histogram(self, name: str, value: float, attributes: Optional[dict[str, str]] = None) -> None:
        """Record a histogram observation."""
        key = self._make_key(name, attributes or {})
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)

    def _make_key(self, name: str, labels: dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def export(self) -> dict[str, Any]:
        """Export metrics as a dictionary."""
        return {
            "service_name": self.config.service_name,
            "counters": self._counters,
            "gauges": self._gauges,
            "histograms": {
                k: {
                    "count": len(v),
                    "sum": sum(v),
                    "min": min(v),
                    "max": max(v),
                    "avg": sum(v) / len(v),
                }
                for k, v in self._histograms.items()
            },
        }


class BaggageManager:
    """Manages OpenTelemetry baggage for context propagation."""

    def __init__(self) -> None:
        self._baggage: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        """Set a baggage item."""
        self._baggage[key] = value
        logger.debug("Baggage set: %s=%s", key, value)

    def get(self, key: str) -> Optional[str]:
        """Get a baggage item."""
        return self._baggage.get(key)

    def clear(self) -> None:
        """Clear all baggage."""
        self._baggage.clear()

    def items(self) -> dict[str, str]:
        """Get all baggage items."""
        return dict(self._baggage)

    def inject(self, carrier: dict[str, str]) -> None:
        """Inject baggage into a carrier (e.g., HTTP headers)."""
        for key, value in self._baggage.items():
            carrier[f"baggage-{key}"] = value

    def extract(self, carrier: dict[str, str]) -> None:
        """Extract baggage from a carrier."""
        for header_key, value in carrier.items():
            if header_key.startswith("baggage-"):
                key = header_key[len("baggage-"):]
                self.set(key, value)


def create_span_decorator(tracer: OpenTelemetryTracer, name: str, kind: SpanKind = SpanKind.INTERNAL) -> Callable:
    """Decorator to create a span around a function."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            config = SpanConfig(name=f"{name}.{func.__name__}", kind=kind)
            with TracerContext(tracer, config):
                return func(*args, **kwargs)
        return wrapper
    return decorator
