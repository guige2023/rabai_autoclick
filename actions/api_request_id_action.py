"""
API request tracing and correlation ID management.

This module provides distributed tracing capabilities with request ID
propagation across service boundaries.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict
import threading
import contextvars

logger = logging.getLogger(__name__)

# Context variable for request ID
_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")
_parent_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("parent_id", default="")


class TraceStatus(Enum):
    """Trace span status."""
    UNSTARTED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()
    CANCELLED = auto()


@dataclass
class Span:
    """Represents a single trace span."""
    trace_id: str
    span_id: str
    operation_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration: Optional[float] = None
    status: TraceStatus = TraceStatus.UNSTARTED
    parent_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    service_name: str = "unknown"
    error: Optional[Exception] = None

    def start(self) -> None:
        """Mark span as started."""
        self.status = TraceStatus.RUNNING
        self.start_time = time.time()

    def end(self, status: TraceStatus = TraceStatus.SUCCESS) -> None:
        """Mark span as ended."""
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        self.status = status

    def set_tag(self, key: str, value: str) -> None:
        """Set a tag on the span."""
        self.tags[key] = str(value)

    def set_tags(self, **kwargs) -> None:
        """Set multiple tags."""
        self.tags.update({k: str(v) for k, v in kwargs.items()})

    def log_event(self, event: str, **attrs) -> None:
        """Log an event on the span."""
        self.logs.append({
            "timestamp": time.time(),
            "event": event,
            **attrs,
        })

    def record_error(self, error: Exception) -> None:
        """Record an error on the span."""
        self.error = error
        self.status = TraceStatus.ERROR
        self.log_event(
            "error",
            error_type=type(error).__name__,
            error_message=str(error),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert span to dictionary."""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "operation_name": self.operation_name,
            "service_name": self.service_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "status": self.status.name,
            "tags": self.tags,
            "logs": self.logs,
        }


class Tracer:
    """
    Distributed tracing tracer.

    Features:
    - Span creation and management
    - Context propagation
    - Request ID generation
    - Automatic parent-child relationships
    - Logging and tagging
    - Export to various formats

    Example:
        >>> tracer = Tracer(service_name="user-service")
        >>> with tracer.start_span("get_user") as span:
        ...     span.set_tag("user_id", user_id)
        ...     result = fetch_user_from_db(user_id)
        ...     span.set_tag("db_hits", 1)
    """

    def __init__(
        self,
        service_name: str,
        generate_request_id: bool = True,
        max_spans: int = 10000,
    ):
        """
        Initialize tracer.

        Args:
            service_name: Name of this service
            generate_request_id: Whether to generate request IDs
            max_spans: Maximum number of spans to retain
        """
        self.service_name = service_name
        self.generate_request_id = generate_request_id
        self.max_spans = max_spans

        self._active_spans: Dict[str, Span] = {}
        self._completed_spans: List[Span] = []
        self._lock = threading.RLock()
        self._stats = {
            "spans_created": 0,
            "spans_completed": 0,
            "spans_errored": 0,
        }

        logger.info(f"Tracer initialized for service: {service_name}")

    def generate_trace_id(self) -> str:
        """Generate a new trace ID."""
        return str(uuid.uuid4()).replace("-", "")[:16]

    def generate_span_id(self) -> str:
        """Generate a new span ID."""
        return str(uuid.uuid4()).replace("-", "")[:8]

    def start_span(
        self,
        operation_name: str,
        trace_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        service_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Span:
        """
        Start a new span.

        Args:
            operation_name: Name of the operation
            trace_id: Optional trace ID (generates if not provided)
            parent_id: Optional parent span ID
            service_name: Service name override
            tags: Initial tags

        Returns:
            Created Span
        """
        if trace_id is None:
            trace_id = _request_id_var.get()
            if not trace_id and self.generate_request_id:
                trace_id = self.generate_trace_id()

        if parent_id is None:
            parent_id = _parent_id_var.get()

        span_id = self.generate_span_id()

        span = Span(
            trace_id=trace_id,
            span_id=span_id,
            operation_name=operation_name,
            parent_id=parent_id if parent_id else None,
            service_name=service_name or self.service_name,
        )

        if tags:
            span.set_tags(**tags)

        with self._lock:
            self._active_spans[span_id] = span
            self._stats["spans_created"] += 1

        span.start()
        logger.debug(
            f"Span started: {span_id} (trace={trace_id}, parent={parent_id})"
        )
        return span

    def end_span(self, span: Span, status: TraceStatus = TraceStatus.SUCCESS) -> None:
        """
        End a span.

        Args:
            span: Span to end
            status: Final status
        """
        span.end(status)

        with self._lock:
            self._active_spans.pop(span.span_id, None)
            self._completed_spans.append(span)
            self._stats["spans_completed"] += 1

            if status == TraceStatus.ERROR:
                self._stats["spans_errored"] += 1

            while len(self._completed_spans) > self.max_spans:
                self._completed_spans.pop(0)

        logger.debug(
            f"Span ended: {span.span_id} (duration={span.duration:.3f}s, status={status.name})"
        )

    def get_active_spans(self) -> List[Span]:
        """Get all currently active spans."""
        with self._lock:
            return list(self._active_spans.values())

    def get_trace(self, trace_id: str) -> List[Span]:
        """Get all spans for a trace."""
        with self._lock:
            return [
                span for span in self._completed_spans
                if span.trace_id == trace_id
            ]

    def get_stats(self) -> Dict[str, Any]:
        """Get tracer statistics."""
        with self._lock:
            return {
                "service_name": self.service_name,
                "active_spans": len(self._active_spans),
                "completed_spans": len(self._completed_spans),
                **self._stats,
            }

    def export_zipkin(self) -> List[Dict[str, Any]]:
        """Export spans in Zipkin format."""
        spans = []
        with self._lock:
            for span in self._completed_spans:
                zipkin_span = {
                    "traceId": span.trace_id,
                    "id": span.span_id,
                    "name": span.operation_name,
                    "duration": int((span.duration or 0) * 1000000),
                    "timestamp": int(span.start_time * 1000000),
                    "localEndpoint": {
                        "serviceName": span.service_name,
                    },
                    "tags": span.tags,
                }
                if span.parent_id:
                    zipkin_span["parentId"] = span.parent_id

                if span.error:
                    zipkin_span["tags"]["error"] = str(span.error)

                spans.append(zipkin_span)
        return spans

    def export_jaeger(self) -> List[Dict[str, Any]]:
        """Export spans in Jaeger format."""
        spans = []
        with self._lock:
            for span in self._completed_spans:
                jaeger_span = {
                    "traceId": span.trace_id,
                    "spanID": span.span_id,
                    "operationName": span.operation_name,
                    "startTime": int(span.start_time * 1000000),
                    "duration": int((span.duration or 0) * 1000000),
                    "tags": [
                        {"key": k, "vStr": v}
                        for k, v in span.tags.items()
                    ],
                    "logs": [
                        {
                            "timestamp": int(log["timestamp"] * 1000000),
                            "fields": [
                                {"key": k, "vStr": str(v)}
                                for k, v in log.items()
                                if k != "timestamp"
                            ],
                        }
                        for log in span.logs
                    ],
                }
                if span.parent_id:
                    jaeger_span["references"] = [
                        {
                            "refType": "CHILD_OF",
                            "traceID": span.trace_id,
                            "spanID": span.parent_id,
                        }
                    ]
                spans.append(jaeger_span)
        return spans


class TracerContext:
    """Context manager for spans."""

    def __init__(
        self,
        tracer: Tracer,
        operation_name: str,
        trace_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        **kwargs,
    ):
        self.tracer = tracer
        self.operation_name = operation_name
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.kwargs = kwargs
        self._span: Optional[Span] = None

    def __enter__(self) -> Span:
        self._span = self.tracer.start_span(
            self.operation_name,
            self.trace_id,
            self.parent_id,
            **self.kwargs,
        )
        return self._span

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._span:
            if exc_val:
                self._span.record_error(exc_val)
                self.tracer.end_span(self._span, TraceStatus.ERROR)
            else:
                self.tracer.end_span(self._span, TraceStatus.SUCCESS)
        return False


class RequestIDMiddleware:
    """
    Middleware for request ID handling.

    Example:
        >>> middleware = RequestIDMiddleware(tracer)
        >>> result = middleware.trace(my_function, request)
    """

    def __init__(self, tracer: Tracer):
        self.tracer = tracer

    def trace(
        self,
        func: Callable[..., Any],
        request_id: Optional[str] = None,
        **kwargs,
    ) -> Any:
        """Trace a function call with request context."""
        if request_id:
            _request_id_var.set(request_id)

        trace_id = _request_id_var.get()
        parent_id = _parent_id_var.get()

        with self.tracer.start_span(
            func.__name__,
            trace_id=trace_id,
            parent_id=parent_id,
        ) as span:
            span.set_tag("function", func.__name__)
            try:
                result = func(**kwargs)
                return result
            except Exception as e:
                span.record_error(e)
                raise

    def inject_context(
        self,
        headers: Dict[str, str],
    ) -> Dict[str, str]:
        """Inject trace context into headers."""
        trace_id = _request_id_var.get()
        span_id = self.tracer.generate_span_id()

        headers["X-Request-ID"] = trace_id
        headers["X-Trace-ID"] = trace_id
        headers["X-Parent-ID"] = span_id

        return headers

    def extract_context(
        self,
        headers: Dict[str, str],
    ) -> tuple[str, str]:
        """Extract trace context from headers."""
        trace_id = headers.get("X-Request-ID") or headers.get("X-Trace-ID", "")
        parent_id = headers.get("X-Parent-ID", "")

        _request_id_var.set(trace_id)
        _parent_id_var.set(parent_id)

        return trace_id, parent_id
