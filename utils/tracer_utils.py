"""Tracer utilities: distributed tracing with span management and context propagation."""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "Span",
    "Tracer",
    "TraceContext",
    "current_span",
    "span",
]


@dataclass
class Span:
    """A distributed trace span."""

    name: str
    trace_id: str
    span_id: str
    parent_id: str | None = None
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    tags: dict[str, Any] = field(default_factory=dict)
    logs: list[tuple[float, str, Any]] = field(default_factory=list)

    @property
    def duration_ms(self) -> float:
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000

    def set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def log(self, message: str, value: Any = None) -> None:
        self.logs.append((time.time(), message, value))

    def finish(self) -> None:
        self.end_time = time.time()


_thread_local = threading.local()


@dataclass
class TraceContext:
    """Trace context for propagating across threads/services."""

    trace_id: str
    span_id: str
    parent_id: str | None = None

    @classmethod
    def current(cls) -> "TraceContext | None":
        span = getattr(_thread_local, "current_span", None)
        if span:
            return cls(span.trace_id, span.span_id, span.parent_id)
        return None


class Tracer:
    """Distributed tracer for collecting trace data."""

    def __init__(self, service_name: str = "unknown") -> None:
        self.service_name = service_name
        self._spans: list[Span] = []
        self._lock = threading.Lock()

    def start_span(
        self,
        name: str,
        parent: Span | None = None,
        tags: dict[str, Any] | None = None,
    ) -> Span:
        """Start a new span."""
        parent_id = parent.span_id if parent else (TraceContext.current().span_id if TraceContext.current() else None)
        trace_id = parent.trace_id if parent else uuid.uuid4().hex[:16]

        span = Span(
            name=name,
            trace_id=trace_id,
            span_id=uuid.uuid4().hex[:8],
            parent_id=parent_id,
            tags=tags or {},
        )
        with self._lock:
            self._spans.append(span)
        return span

    def finish_span(self, span: Span) -> None:
        span.finish()
        _thread_local.current_span = None

    def __enter__(self, name: str, parent: Span | None = None):
        span = self.start_span(name, parent)
        _thread_local.current_span = span
        return span

    def __exit__(self, *args: Any) -> None:
        span = getattr(_thread_local, "current_span", None)
        if span:
            self.finish_span(span)

    def get_traces(self) -> list[dict[str, Any]]:
        """Export all spans as dict."""
        with self._lock:
            return [
                {
                    "service": self.service_name,
                    "name": s.name,
                    "trace_id": s.trace_id,
                    "span_id": s.span_id,
                    "parent_id": s.parent_id,
                    "duration_ms": s.duration_ms,
                    "tags": s.tags,
                    "logs": s.logs,
                }
                for s in self._spans
            ]


def current_span() -> Span | None:
    """Get the current active span."""
    return getattr(_thread_local, "current_span", None)


def span(tracer: Tracer, name: str):
    """Context manager for creating a span."""
    return tracer.start_span(name)
