"""Distributed tracing utilities: spans, trace context propagation, and exporters."""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

__all__ = [
    "SpanStatus",
    "Span",
    "Tracer",
    "TraceContext",
    "TracingExporter",
    "InMemoryExporter",
]


class SpanStatus(Enum):
    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass
class Span:
    """A single trace span."""
    name: str
    trace_id: str
    span_id: str
    parent_id: str | None
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    status: SpanStatus = SpanStatus.UNSET
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    service_name: str = ""

    def end(self, status: SpanStatus = SpanStatus.OK) -> None:
        self.end_time = time.time()
        self.status = status

    @property
    def duration_ms(self) -> float:
        if self.end_time is None:
            return time.time() - self.start_time
        return (self.end_time - self.start_time) * 1000

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        self.events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {},
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "status": self.status.value,
            "attributes": self.attributes,
            "events": self.events,
            "service_name": self.service_name,
        }


@dataclass
class TraceContext:
    """Trace context for propagation across process boundaries."""
    trace_id: str
    span_id: str
    baggage: dict[str, str] = field(default_factory=dict)

    def to_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "x-trace-id": self.trace_id,
            "x-span-id": self.span_id,
        }
        for k, v in self.baggage.items():
            headers[f"x-baggage-{k}"] = v
        return headers

    @classmethod
    def from_headers(cls, headers: dict[str, str]) -> "TraceContext":
        trace_id = headers.get("x-trace-id", str(uuid.uuid4()))
        span_id = headers.get("x-span-id", str(uuid.uuid4()))
        baggage = {
            k.replace("x-baggage-", ""): v
            for k, v in headers.items()
            if k.startswith("x-baggage-")
        }
        return cls(trace_id=trace_id, span_id=span_id, baggage=baggage)


class TracingExporter:
    """Interface for trace exporters."""

    def export(self, spans: list[Span]) -> None:
        raise NotImplementedError


class InMemoryExporter(TracingExporter):
    """In-memory trace exporter for testing/debugging."""

    def __init__(self, max_spans: int = 10000) -> None:
        self._spans: list[Span] = []
        self._max_spans = max_spans
        self._lock = threading.Lock()

    def export(self, spans: list[Span]) -> None:
        with self._lock:
            self._spans.extend(spans)
            if len(self._spans) > self._max_spans:
                self._spans = self._spans[-self._max_spans:]

    def get_spans(
        self,
        trace_id: str | None = None,
        service_name: str | None = None,
    ) -> list[Span]:
        with self._lock:
            result = self._spans
            if trace_id:
                result = [s for s in result if s.trace_id == trace_id]
            if service_name:
                result = [s for s in result if s.service_name == service_name]
            return list(result)

    def clear(self) -> None:
        with self._lock:
            self._spans.clear()

    def summary(self) -> dict[str, Any]:
        with self._lock:
            by_service: dict[str, int] = defaultdict(int)
            by_status: dict[str, int] = defaultdict(int)
            for s in self._spans:
                by_service[s.service_name] += 1
                by_status[s.status.value] += 1
            return {
                "total_spans": len(self._spans),
                "by_service": dict(by_service),
                "by_status": dict(by_status),
            }


class Tracer:
    """Distributed tracer with span management and export."""

    def __init__(
        self,
        service_name: str = "",
        exporter: TracingExporter | None = None,
    ) -> None:
        self.service_name = service_name
        self.exporter = exporter or InMemoryExporter()
        self._active_spans: dict[str, Span] = {}
        self._lock = threading.Lock()
        self._context_var = threading.local()

    def start_span(
        self,
        name: str,
        context: TraceContext | None = None,
        parent_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Span:
        if context is None:
            ctx = getattr(self._context_var, "context", None)
            if ctx is None:
                trace_id = str(uuid.uuid4())
            else:
                trace_id = ctx.trace_id
        else:
            trace_id = context.trace_id

        parent_id = parent_id or (context.span_id if context else None)
        span_id = str(uuid.uuid4())

        span = Span(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_id=parent_id,
            service_name=self.service_name,
            attributes=attributes or {},
        )
        with self._lock:
            self._active_spans[span.span_id] = span
        return span

    def end_span(self, span: Span, status: SpanStatus = SpanStatus.OK) -> None:
        span.end(status)
        with self._lock:
            self._active_spans.pop(span.span_id, None)
        self.exporter.export([span])

    def current_context(self) -> TraceContext | None:
        return getattr(self._context_var, "context", None)

    def inject_context(self, span: Span) -> TraceContext:
        ctx = TraceContext(trace_id=span.trace_id, span_id=span.span_id)
        self._context_var.context = ctx
        return ctx

    def with_span(self, name: str, attributes: dict[str, Any] | None = None):
        """Context manager for creating a span."""
        class SpanContext:
            span: Span | None = None

        state = SpanContext()

        class SpanManager:
            def __enter__(s) -> Span:
                state.span = self.start_span(name, attributes=attributes)
                return state.span

            def __exit__(s, *args: Any) -> None:
                if state.span:
                    self.end_span(state.span)

        return SpanManager()

    def trace(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ):
        return self.with_span(name, attributes)
