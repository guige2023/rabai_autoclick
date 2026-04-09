"""
API Tracing Action Module.

Distributed tracing support for API requests, including span management,
trace propagation, and performance instrumentation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Context variable for the current trace
_current_trace: ContextVar[Optional["TraceContext"]] = ContextVar(
    "current_trace", default=None
)


class SpanKind(Enum):
    """Span kind types."""
    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class SpanStatus(Enum):
    """Span status codes."""
    OK = "ok"
    ERROR = "error"
    UNSET = "unset"


@dataclass
class Span:
    """A single trace span."""
    name: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    kind: SpanKind = SpanKind.INTERNAL
    status: SpanStatus = SpanStatus.UNSET
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration_ms: float = 0.0
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class TraceContext:
    """Context for the current trace."""
    trace_id: str
    spans: List[Span] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def current_span(self) -> Optional[Span]:
        """Get the most recent span."""
        return self.spans[-1] if self.spans else None


@dataclass
class TraceReport:
    """Complete trace report."""
    trace_id: str
    spans: List[Span]
    total_duration_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class SpanBuilder:
    """Builder for creating spans."""

    def __init__(
        self,
        tracer: "APITracingAction",
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
    ) -> None:
        self.tracer = tracer
        self.name = name
        self.kind = kind
        self._attributes: Dict[str, Any] = {}
        self._parent_span_id: Optional[str] = None

    def with_attribute(self, key: str, value: Any) -> "SpanBuilder":
        """Add an attribute to the span."""
        self._attributes[key] = value
        return self

    def with_parent(self, parent_span_id: str) -> "SpanBuilder":
        """Set the parent span ID."""
        self._parent_span_id = parent_span_id
        return self

    def start(self) -> Span:
        """Start the span."""
        return self.tracer.start_span(
            name=self.name,
            kind=self.kind,
            attributes=self._attributes,
            parent_span_id=self._parent_span_id,
        )


class APITracingAction:
    """
    Distributed tracing for API requests.

    Manages trace contexts, spans, and trace propagation across
    service boundaries.

    Example:
        tracer = APITracingAction(service_name="user-api")

        # Start a trace
        with tracer.start_trace(operation_name="get_user") as trace:
            with tracer.start_span("db-query") as span:
                result = db.query(user_id)
                span.add_attribute("db.statement", "SELECT ...")

            with tracer.start_span("serialize") as span:
                output = json.dumps(result)
    """

    def __init__(
        self,
        service_name: str,
        sample_rate: float = 1.0,
        max_spans_per_trace: int = 1000,
    ) -> None:
        self.service_name = service_name
        self.sample_rate = sample_rate
        self.max_spans_per_trace = max_spans_per_trace
        self._traces: Dict[str, TraceContext] = {}
        self._enabled = True

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable tracing."""
        self._enabled = enabled

    def generate_trace_id(self) -> str:
        """Generate a unique trace ID."""
        return uuid.uuid4().hex[:16]

    def generate_span_id(self) -> str:
        """Generate a unique span ID."""
        return uuid.uuid4().hex[:8]

    def start_trace(
        self,
        operation_name: str,
        trace_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TraceContext:
        """Start a new trace context."""
        if not self._enabled:
            return TraceContext(trace_id="disabled", metadata={})

        trace_id = trace_id or self.generate_trace_id()
        ctx = TraceContext(
            trace_id=trace_id,
            metadata=metadata or {},
        )
        self._traces[trace_id] = ctx
        _current_trace.set(ctx)

        # Create root span
        root = self._create_span(
            name=operation_name,
            trace_id=trace_id,
            kind=SpanKind.SERVER,
        )
        ctx.spans.append(root)
        logger.debug(f"Started trace {trace_id} for {operation_name}")
        return ctx

    def end_trace(self, trace: TraceContext) -> TraceReport:
        """End a trace and generate a report."""
        if not self._enabled or trace.trace_id == "disabled":
            return TraceReport(
                trace_id="disabled",
                spans=[],
                total_duration_ms=0.0,
            )

        total_duration = 0.0
        for span in trace.spans:
            if span.end_time:
                span.duration_ms = (span.end_time - span.start_time) * 1000
                total_duration += span.duration_ms

        report = TraceReport(
            trace_id=trace.trace_id,
            spans=trace.spans,
            total_duration_ms=total_duration,
            metadata=trace.metadata,
        )

        if trace.trace_id in self._traces:
            del self._traces[trace.trace_id]

        _current_trace.set(None)
        logger.debug(f"Ended trace {trace.trace_id}, duration={total_duration:.2f}ms")
        return report

    def _create_span(
        self,
        name: str,
        trace_id: str,
        span_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Span:
        """Create a new span."""
        ctx = self._traces.get(trace_id)
        if ctx and len(ctx.spans) >= self.max_spans_per_trace:
            raise RuntimeError(f"Max spans per trace reached: {self.max_spans_per_trace}")

        return Span(
            name=name,
            trace_id=trace_id,
            span_id=span_id or self.generate_span_id(),
            parent_span_id=parent_span_id,
            kind=kind,
            attributes=attributes or {},
        )

    def start_span(
        self,
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        parent_span_id: Optional[str] = None,
    ) -> Span:
        """Start a new span in the current trace."""
        if not self._enabled:
            return Span(name=name, trace_id="disabled", span_id="disabled")

        ctx = _current_trace.get()
        if not ctx:
            # Create a new trace if none exists
            ctx = self.start_trace(operation_name=name)
            return ctx.spans[0]

        # Determine parent span
        if parent_span_id is None and ctx.current_span():
            parent_span_id = ctx.current_span().span_id

        span = self._create_span(
            name=name,
            trace_id=ctx.trace_id,
            parent_span_id=parent_span_id,
            kind=kind,
            attributes=attributes,
        )
        ctx.spans.append(span)
        return span

    def end_span(self, span: Span) -> None:
        """End a span."""
        if span.trace_id == "disabled":
            return
        span.end_time = time.time()

    def add_span_event(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add an event to the current span."""
        ctx = _current_trace.get()
        if not ctx or not ctx.current_span():
            return
        ctx.current_span().events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {},
        })

    def add_span_error(self, error: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Record an error in the current span."""
        ctx = _current_trace.get()
        if not ctx or not ctx.current_span():
            return
        span = ctx.current_span()
        span.errors.append(error)
        span.status = SpanStatus.ERROR
        if attributes:
            span.attributes.update(attributes)

    def inject_trace_context(
        self,
        carrier: Dict[str, str],
    ) -> Dict[str, str]:
        """Inject trace context into a carrier (e.g., HTTP headers)."""
        ctx = _current_trace.get()
        if not ctx:
            return carrier

        carrier["x-trace-id"] = ctx.trace_id
        current = ctx.current_span()
        if current:
            carrier["x-span-id"] = current.span_id

        return carrier

    def extract_trace_context(
        self,
        carrier: Dict[str, str],
    ) -> Optional[TraceContext]:
        """Extract trace context from a carrier."""
        trace_id = carrier.get("x-trace-id")
        if not trace_id:
            return None

        ctx = self.start_trace(operation_name="extracted", trace_id=trace_id)
        return ctx

    def get_current_trace(self) -> Optional[TraceContext]:
        """Get the current trace context."""
        return _current_trace.get()

    def __enter__(self) -> "APITracingAction":
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        ctx = _current_trace.get()
        if ctx:
            self.end_trace(ctx)

    class SpanContext:
        """Context manager for spans."""

        def __init__(self, tracer: "APITracingAction", name: str, **kwargs: Any) -> None:
            self.tracer = tracer
            self.name = name
            self.kwargs = kwargs
            self.span: Optional[Span] = None

        def __enter__(self) -> Span:
            self.span = self.tracer.start_span(self.name, **self.kwargs)
            return self.span

        def __exit__(self, *args: Any) -> None:
            if self.span:
                self.tracer.end_span(self.span)

    def span(self, name: str, **kwargs: Any) -> SpanContext:
        """Create a span context manager."""
        return self.SpanContext(self, name, **kwargs)

    class TraceContextManager:
        """Context manager for traces."""

        def __init__(self, tracer: "APITracingAction", operation_name: str, **kwargs: Any) -> None:
            self.tracer = tracer
            self.operation_name = operation_name
            self.kwargs = kwargs
            self.ctx: Optional[TraceContext] = None

        def __enter__(self) -> TraceContext:
            self.ctx = self.tracer.start_trace(self.operation_name, **self.kwargs)
            return self.ctx

        def __exit__(self, *args: Any) -> None:
            if self.ctx:
                self.tracer.end_trace(self.ctx)

    def trace(self, operation_name: str, **kwargs: Any) -> TraceContextManager:
        """Create a trace context manager."""
        return self.TraceContextManager(self, operation_name, **kwargs)
