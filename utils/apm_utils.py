"""Application Performance Monitoring (APM) utilities: tracing, spans, and metrics."""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "Span",
    "Tracer",
    "APMClient",
    "trace",
    "Counter",
    "Histogram",
    "Gauge",
]


@dataclass
class Span:
    """Represents a single trace span."""

    name: str
    trace_id: str
    span_id: str
    parent_id: str | None
    service: str
    start_time: float
    end_time: float | None = None
    tags: dict[str, Any] = field(default_factory=dict)
    error: bool = False

    @property
    def duration_ms(self) -> float:
        """Calculate span duration in milliseconds."""
        if self.end_time is None:
            return time.perf_counter() * 1000 - self.start_time * 1000
        return (self.end_time - self.start_time) * 1000


class Tracer:
    """In-memory tracer for collecting spans."""

    def __init__(self, service_name: str = "unknown") -> None:
        self.service_name = service_name
        self._spans: list[Span] = []
        self._lock = __import__("threading").Lock()

    def start_span(
        self,
        name: str,
        parent_id: str | None = None,
        tags: dict[str, Any] | None = None,
    ) -> Span:
        """Start a new span."""
        span = Span(
            name=name,
            trace_id=uuid.uuid4().hex,
            span_id=uuid.uuid4().hex[:16],
            parent_id=parent_id,
            service=self.service_name,
            start_time=time.time(),
            tags=tags or {},
        )
        with self._lock:
            self._spans.append(span)
        return span

    def end_span(self, span: Span, error: bool = False) -> None:
        """End a span."""
        span.end_time = time.time()
        span.error = error

    @contextmanager
    def trace(
        self,
        name: str,
        parent_id: str | None = None,
        tags: dict[str, Any] | None = None,
    ):
        """Context manager for tracing a block of code."""
        span = self.start_span(name, parent_id, tags)
        try:
            yield span
        except Exception as e:
            span.error = True
            span.tags["error.message"] = str(e)
            raise
        finally:
            self.end_span(span)

    def get_spans(self) -> list[Span]:
        """Get all collected spans."""
        with self._lock:
            return list(self._spans)

    def clear(self) -> None:
        """Clear all spans."""
        with self._lock:
            self._spans.clear()

    def to_dict(self) -> dict[str, Any]:
        """Export spans as a dict."""
        return {
            "service": self.service_name,
            "spans": [
                {
                    "name": s.name,
                    "trace_id": s.trace_id,
                    "span_id": s.span_id,
                    "parent_id": s.parent_id,
                    "duration_ms": s.duration_ms,
                    "tags": s.tags,
                    "error": s.error,
                }
                for s in self._spans
            ],
        }


class Counter:
    """Simple thread-safe counter metric."""

    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        self.name = name
        self._labels = labels or {}
        self._value = 0
        self._lock = __import__("threading").Lock()

    def inc(self, amount: float = 1.0) -> None:
        """Increment counter."""
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0


class Gauge:
    """Simple thread-safe gauge metric."""

    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        self.name = name
        self._labels = labels or {}
        self._value = 0.0
        self._lock = __import__("threading").Lock()

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value -= amount

    @property
    def value(self) -> float:
        with self._lock:
            return self._value


class Histogram:
    """Simple histogram metric for tracking distributions."""

    def __init__(
        self,
        name: str,
        buckets: list[float] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self._labels = labels or {}
        self._buckets = buckets or [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self._counts: dict[float, int] = defaultdict(int)
        self._sum = 0.0
        self._count = 0
        self._lock = __import__("threading").Lock()

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._sum += value
            self._count += 1
            for bound in self._buckets:
                if value <= bound:
                    self._counts[bound] += 1
                    break

    def get_stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "count": self._count,
                "sum": self._sum,
                "buckets": dict(self._counts),
            }


class APMClient:
    """APM client managing metrics collection and export."""

    def __init__(self, service_name: str = "app") -> None:
        self.service_name = service_name
        self.tracer = Tracer(service_name)
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}
        self._lock = __import__("threading").Lock()

    def counter(self, name: str, labels: dict[str, str] | None = None) -> Counter:
        with self._lock:
            key = f"{name}:{sorted(labels.items())}" if labels else name
            if key not in self._counters:
                self._counters[key] = Counter(name, labels)
            return self._counters[key]

    def gauge(self, name: str, labels: dict[str, str] | None = None) -> Gauge:
        with self._lock:
            key = f"{name}:{sorted(labels.items())}" if labels else name
            if key not in self._gauges:
                self._gauges[key] = Gauge(name, labels)
            return self._gauges[key]

    def histogram(
        self,
        name: str,
        buckets: list[float] | None = None,
        labels: dict[str, str] | None = None,
    ) -> Histogram:
        with self._lock:
            key = f"{name}:{sorted(labels.items())}" if labels else name
            if key not in self._histograms:
                self._histograms[key] = Histogram(name, buckets, labels)
            return self._histograms[key]

    def export(self) -> dict[str, Any]:
        """Export all metrics."""
        return {
            "service": self.service_name,
            "timestamp": time.time(),
            "traces": self.tracer.to_dict(),
            "counters": {k: c.value for k, c in self._counters.items()},
            "gauges": {k: g.value for k, g in self._gauges.items()},
            "histograms": {k: h.get_stats() for k, h in self._histograms.items()},
        }


@contextmanager
def trace(tracer: Tracer, name: str, tags: dict[str, Any] | None = None):
    """Convenience context manager for tracing."""
    with tracer.trace(name, tags=tags) as span:
        yield span
