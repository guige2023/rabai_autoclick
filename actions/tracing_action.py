"""
Distributed tracing module for action workflows.

Provides distributed tracing capabilities using OpenTelemetry-style spans.
Supports trace propagation, span hierarchies, and exporters.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class SpanStatus(Enum):
    """Span status codes."""
    OK = "ok"
    ERROR = "error"
    UNSET = "unset"


class SpanKind(Enum):
    """Span kind types."""
    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


@dataclass
class SpanContext:
    """Context for a distributed trace span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    trace_flags: int = 1
    trace_state: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "trace_flags": self.trace_flags,
            "trace_state": self.trace_state,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SpanContext:
        return cls(
            trace_id=data["trace_id"],
            span_id=data["span_id"],
            parent_span_id=data.get("parent_span_id"),
            trace_flags=data.get("trace_flags", 1),
            trace_state=data.get("trace_state", {}),
        )


@dataclass
class Span:
    """A trace span representing a unit of work."""
    name: str
    context: SpanContext
    kind: SpanKind = SpanKind.INTERNAL
    status: SpanStatus = SpanStatus.UNSET
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    attributes: dict = field(default_factory=dict)
    events: list = field(default_factory=list)
    links: list = field(default_factory=list)
    error_message: Optional[str] = None

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a span attribute."""
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Optional[dict] = None) -> None:
        """Add an event to the span."""
        self.events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {},
        })

    def record_exception(self, exception: Exception) -> None:
        """Record an exception in the span."""
        self.status = SpanStatus.ERROR
        self.error_message = str(exception)
        self.add_event("exception", {
            "type": type(exception).__name__,
            "message": str(exception),
        })

    def end(self, status: Optional[SpanStatus] = None) -> None:
        """End the span."""
        self.end_time = time.time()
        if status:
            self.status = status

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "context": self.context.to_dict(),
            "kind": self.kind.value,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "attributes": self.attributes,
            "events": self.events,
            "links": self.links,
            "error_message": self.error_message,
            "duration_ms": (
                (self.end_time - self.start_time) * 1000
                if self.end_time else None
            ),
        }


class Tracer:
    """
    Distributed tracer for action workflows.

    Provides span creation, context propagation, and export capabilities.
    """

    def __init__(self, service_name: str, exporter: Optional[TraceExporter] = None):
        self.service_name = service_name
        self.exporter = exporter
        self._spans: list[Span] = []
        self._current_span: Optional[Span] = None

    def start_span(
        self,
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
        parent_context: Optional[SpanContext] = None,
        attributes: Optional[dict] = None,
    ) -> Span:
        """Start a new span."""
        trace_id = (
            parent_context.trace_id
            if parent_context
            else uuid.uuid4().hex[:32]
        )
        span_id = uuid.uuid4().hex[:16]
        parent_span_id = (
            parent_context.span_id
            if parent_context
            else None
        )

        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
        )

        span = Span(
            name=name,
            context=context,
            kind=kind,
            attributes=attributes or {},
        )

        self._spans.append(span)
        self._current_span = span

        return span

    def end_span(self, span: Span, status: Optional[SpanStatus] = None) -> None:
        """End a span and export it."""
        span.end(status)
        if self.exporter:
            self.exporter.export([span])
        if self._current_span == span:
            self._current_span = None

    def trace(
        self,
        name: Optional[str] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[dict] = None,
    ) -> Callable:
        """Decorator to trace a function."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                span_name = name or f"{func.__module__}.{func.__qualname__}"
                span = self.start_span(span_name, kind, attributes=attributes)
                try:
                    result = func(*args, **kwargs)
                    self.end_span(span, SpanStatus.OK)
                    return result
                except Exception as e:
                    span.record_exception(e)
                    self.end_span(span, SpanStatus.ERROR)
                    raise
            return wrapper
        return decorator

    def inject_context(self, span: Span) -> dict:
        """Inject span context for propagation (e.g., HTTP headers)."""
        return {
            "trace_id": span.context.trace_id,
            "span_id": span.context.span_id,
        }

    def extract_context(self, carrier: dict) -> Optional[SpanContext]:
        """Extract span context from a carrier (e.g., HTTP headers)."""
        trace_id = carrier.get("trace_id")
        span_id = carrier.get("span_id")
        if not trace_id or not span_id:
            return None
        return SpanContext(
            trace_id=trace_id,
            span_id=span_id,
        )

    def get_all_spans(self) -> list[dict]:
        """Get all completed spans as dictionaries."""
        return [s.to_dict() for s in self._spans if s.end_time is not None]

    def clear(self) -> None:
        """Clear all recorded spans."""
        self._spans.clear()
        self._current_span = None


class TraceExporter:
    """Base class for trace exporters."""

    def export(self, spans: list[Span]) -> None:
        """Export spans to a backend."""
        raise NotImplementedError


class ConsoleExporter(TraceExporter):
    """Exports spans to the console."""

    def export(self, spans: list[Span]) -> None:
        for span in spans:
            print(f"[TRACE] {span.name} - {span.context.trace_id[:8]} - {span.status.value}")


class CollectorExporter(TraceExporter):
    """Exports spans to an OpenTelemetry collector."""

    def __init__(self, endpoint: str, api_key: Optional[str] = None):
        self.endpoint = endpoint
        self.api_key = api_key

    def export(self, spans: list[Span]) -> None:
        import requests
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "spans": [s.to_dict() for s in spans],
            "service": "rabai_autoclick",
        }

        try:
            requests.post(
                self.endpoint,
                json=payload,
                headers=headers,
                timeout=5,
            )
        except Exception as e:
            print(f"Failed to export spans: {e}")


def create_tracer(
    service_name: str,
    exporter_type: str = "console",
    endpoint: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Tracer:
    """Create a configured tracer instance."""
    exporter = None
    if exporter_type == "console":
        exporter = ConsoleExporter()
    elif exporter_type == "collector" and endpoint:
        exporter = CollectorExporter(endpoint, api_key)

    return Tracer(service_name, exporter)
