"""
API Distributed Tracing Action Module.

Provides distributed tracing for API requests across
services with span management and trace propagation.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SpanStatus(Enum):
    """Span status values."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass
class Span:
    """Represents a trace span."""

    name: str
    trace_id: str
    span_id: str = field(default_factory=lambda: str(uuid.uuid4())[:16])
    parent_id: Optional[str] = None
    service_name: str = ""
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    duration_ms: float = 0.0
    status: SpanStatus = SpanStatus.UNSET
    tags: dict[str, Any] = field(default_factory=dict)
    logs: list[dict[str, Any]] = field(default_factory=list)
    annotations: dict[str, str] = field(default_factory=dict)


@dataclass
class TraceContext:
    """Trace context for propagation."""

    trace_id: str
    span_id: str
    sampled: bool = True
    baggage: dict[str, str] = field(default_factory=dict)


class APIDistributedTracingAction:
    """
    Manages distributed tracing for API requests.

    Features:
    - Trace and span creation
    - Context propagation (W3C Trace Context)
    - Sampling support
    - Span annotations and tags

    Example:
        tracer = APIDistributedTracingAction()
        with tracer.start_span("api_call") as span:
            result = await call_service()
            span.set_tag("status", 200)
    """

    def __init__(
        self,
        service_name: str = "unknown",
        sampling_rate: float = 1.0,
        max_spans_per_trace: int = 1000,
    ) -> None:
        """
        Initialize tracing action.

        Args:
            service_name: Name of this service.
            sampling_rate: Sampling rate (0.0-1.0).
            max_spans_per_trace: Maximum spans per trace.
        """
        self.service_name = service_name
        self.sampling_rate = sampling_rate
        self.max_spans_per_trace = max_spans_per_trace
        self._traces: dict[str, list[Span]] = {}
        self._active_spans: dict[str, Span] = {}
        self._stats = {
            "total_traces": 0,
            "total_spans": 0,
            "sampled_traces": 0,
            "dropped_traces": 0,
        }

    def start_trace(self, trace_id: Optional[str] = None) -> str:
        """
        Start a new trace.

        Args:
            trace_id: Optional trace ID.

        Returns:
            Trace ID.
        """
        tid = trace_id or self._generate_trace_id()

        if tid not in self._traces:
            self._traces[tid] = []

        self._stats["total_traces"] += 1
        return tid

    def start_span(
        self,
        name: str,
        trace_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        service_name: Optional[str] = None,
    ) -> Span:
        """
        Start a new span.

        Args:
            name: Span name.
            trace_id: Trace ID.
            parent_id: Parent span ID.
            service_name: Service name.

        Returns:
            Created Span.
        """
        tid = trace_id or self._generate_trace_id()
        if tid not in self._traces:
            self._traces[tid] = []

        if len(self._traces[tid]) >= self.max_spans_per_trace:
            logger.warning(f"Max spans reached for trace {tid}")
            return Span(name=name, trace_id=tid)

        span = Span(
            name=name,
            trace_id=tid,
            parent_id=parent_id,
            service_name=service_name or self.service_name,
        )

        self._traces[tid].append(span)
        self._active_spans[f"{tid}:{span.span_id}"] = span
        self._stats["total_spans"] += 1

        logger.debug(f"Started span: {span.span_id} in trace {tid}")
        return span

    def end_span(self, span: Span) -> None:
        """
        End a span.

        Args:
            span: Span to end.
        """
        span.end_time = time.time()
        span.duration_ms = (span.end_time - span.start_time) * 1000

        key = f"{span.trace_id}:{span.span_id}"
        if key in self._active_spans:
            del self._active_spans[key]

        logger.debug(f"Ended span: {span.span_id} ({span.duration_ms:.2f}ms)")

    def add_span_tag(
        self,
        span: Span,
        key: str,
        value: Any,
    ) -> None:
        """
        Add a tag to a span.

        Args:
            span: Target span.
            key: Tag key.
            value: Tag value.
        """
        span.tags[key] = value

    def add_span_log(
        self,
        span: Span,
        message: str,
        timestamp: Optional[float] = None,
        **attributes: Any,
    ) -> None:
        """
        Add a log event to a span.

        Args:
            span: Target span.
            message: Log message.
            timestamp: Optional timestamp.
            **attributes: Additional attributes.
        """
        span.logs.append({
            "timestamp": timestamp or time.time(),
            "message": message,
            **attributes,
        })

    def set_span_status(
        self,
        span: Span,
        status: SpanStatus,
    ) -> None:
        """
        Set span status.

        Args:
            span: Target span.
            status: Span status.
        """
        span.status = status

    def inject_context(
        self,
        trace_id: str,
        span_id: str,
        baggage: Optional[dict[str, str]] = None,
    ) -> dict[str, str]:
        """
        Inject trace context for propagation.

        Args:
            trace_id: Trace ID.
            span_id: Span ID.
            baggage: Optional baggage items.

        Returns:
            Headers dictionary for propagation.
        """
        return {
            "traceparent": f"00-{trace_id}-{span_id}-01",
            "tracestate": "",
        }

    def extract_context(self, headers: dict[str, str]) -> Optional[TraceContext]:
        """
        Extract trace context from headers.

        Args:
            headers: Headers dictionary.

        Returns:
            TraceContext or None.
        """
        traceparent = headers.get("traceparent", "")
        if not traceparent:
            return None

        parts = traceparent.split("-")
        if len(parts) < 4:
            return None

        return TraceContext(
            trace_id=parts[1],
            span_id=parts[2],
            sampled=parts[3] == "01",
        )

    def get_trace(self, trace_id: str) -> list[Span]:
        """
        Get all spans for a trace.

        Args:
            trace_id: Trace ID.

        Returns:
            List of spans.
        """
        return self._traces.get(trace_id, [])

    def get_stats(self) -> dict[str, Any]:
        """
        Get tracing statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            **self._stats,
            "active_traces": len(self._traces),
            "active_spans": len(self._active_spans),
        }

    def _generate_trace_id(self) -> str:
        """Generate a trace ID."""
        return str(uuid.uuid4()).replace("-", "")[:32]
