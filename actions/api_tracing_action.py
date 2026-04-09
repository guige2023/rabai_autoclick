"""
API Tracing Action Module

Distributed request tracing with trace context propagation.
Span management, timing instrumentation, and correlation IDs.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


trace_context: ContextVar[Optional["TraceContext"]] = ContextVar("trace_context", default=None)


class SpanKind(Enum):
    """Kind of span for categorization."""
    
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    INTERNAL = "internal"


class SpanStatus(Enum):
    """Status of a span."""
    
    OK = "ok"
    ERROR = "error"
    UNSET = "unset"


@dataclass
class Span:
    """Represents a single tracing span."""
    
    name: str
    trace_id: str
    span_id: str
    parent_id: Optional[str] = None
    kind: SpanKind = SpanKind.INTERNAL
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    status: SpanStatus = SpanStatus.UNSET
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[Dict[str, Any]] = None
    
    def finish(self, status: SpanStatus = SpanStatus.OK) -> None:
        """Finish the span."""
        self.end_time = time.time()
        self.status = status
    
    def set_attribute(self, key: str, value: Any) -> None:
        """Set a span attribute."""
        self.attributes[key] = value
    
    def add_event(self, name: str, attributes: Optional[Dict] = None) -> None:
        """Add an event to the span."""
        self.events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {}
        })
    
    def record_error(self, error: Exception, message: Optional[str] = None) -> None:
        """Record an error in the span."""
        self.error = {
            "type": type(error).__name__,
            "message": message or str(error),
            "timestamp": time.time()
        }
        self.status = SpanStatus.ERROR
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert span to dictionary."""
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "kind": self.kind.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": (
                (self.end_time - self.start_time) * 1000
                if self.end_time else None
            ),
            "status": self.status.value,
            "attributes": self.attributes,
            "events": self.events,
            "error": self.error
        }


@dataclass
class TraceContext:
    """Context for distributed tracing."""
    
    trace_id: str
    span_id: str
    parent_id: Optional[str] = None
    baggage: Dict[str, str] = field(default_factory=dict)
    
    @classmethod
    def new(cls) -> "TraceContext":
        """Create a new trace context."""
        return cls(
            trace_id=uuid.uuid4().hex[:16],
            span_id=uuid.uuid4().hex[:8]
        )
    
    @classmethod
    def from_headers(cls, headers: Dict[str, str]) -> Optional["TraceContext"]:
        """Extract trace context from headers."""
        trace_id = headers.get("X-Trace-ID") or headers.get("traceparent", "").split("-")[0] or None
        span_id = headers.get("X-Span-ID") or uuid.uuid4().hex[:8]
        parent_id = headers.get("X-Parent-Span-ID")
        
        if not trace_id:
            return None
        
        return cls(trace_id=trace_id, span_id=span_id, parent_id=parent_id)
    
    def to_headers(self) -> Dict[str, str]:
        """Convert trace context to headers."""
        return {
            "X-Trace-ID": self.trace_id,
            "X-Span-ID": self.span_id,
            "X-Parent-Span-ID": self.parent_id or ""
        }
    
    def child(self) -> "TraceContext":
        """Create a child context."""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=uuid.uuid4().hex[:8],
            parent_id=self.span_id,
            baggage=dict(self.baggage)
        )


class Tracer:
    """Main tracing instrumenter."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self._spans: List[Span] = []
        self._ exporters: List[Callable] = []
        self._current_span: Optional[Span] = None
    
    def add_exporter(self, exporter: Callable) -> None:
        """Add a span exporter."""
        self._exporters.append(exporter)
    
    def start_span(
        self,
        name: str,
        context: Optional[TraceContext] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[Dict] = None
    ) -> Span:
        """Start a new span."""
        if context is None:
            existing = trace_context.get()
            if existing:
                context = existing.child()
            else:
                context = TraceContext.new()
        
        span = Span(
            name=name,
            trace_id=context.trace_id,
            span_id=context.span_id,
            parent_id=context.parent_id,
            kind=kind
        )
        
        if attributes:
            span.attributes.update(attributes)
        
        span.set_attribute("service.name", self.service_name)
        
        self._spans.append(span)
        self._current_span = span
        
        return span
    
    def end_span(self, span: Span, status: SpanStatus = SpanStatus.OK) -> None:
        """End a span."""
        span.finish(status)
        
        for exporter in self._exporters:
            try:
                exporter(span)
            except Exception as e:
                logger.error(f"Span export failed: {e}")
        
        if self._current_span == span:
            self._current_span = None
    
    def get_current_span(self) -> Optional[Span]:
        """Get the current active span."""
        return self._current_span
    
    def clear(self) -> None:
        """Clear all recorded spans."""
        self._spans.clear()


class APITracingAction:
    """
    Main API tracing action handler.
    
    Provides request tracing with automatic context propagation,
    timing instrumentation, and error recording.
    """
    
    def __init__(self, service_name: str = "api-service"):
        self.service_name = service_name
        self.tracer = Tracer(service_name)
        self._middleware: List[Callable] = []
        self._processors: List[Callable] = []
    
    def add_middleware(self, func: Callable) -> None:
        """Add tracing middleware."""
        self._middleware.append(func)
    
    def add_processor(self, processor: Callable[[Span], None]) -> None:
        """Add a span processor."""
        self._processors.append(processor)
    
    async def process_request(
        self,
        request: Dict,
        context: Optional[TraceContext] = None
    ) -> Dict[str, Any]:
        """Process request with tracing."""
        headers = request.get("headers", {})
        
        if context is None:
            context = TraceContext.from_headers(headers) or TraceContext.new()
        
        token = trace_context.set(context)
        
        try:
            span = self.tracer.start_span(
                name=f"{request.get('method', 'GET')} {request.get('path', '/')}",
                context=context,
                kind=SpanKind.SERVER,
                attributes={
                    "http.method": request.get("method", "GET"),
                    "http.url": request.get("url", ""),
                    "http.target": request.get("path", "/")
                }
            )
            
            for mw in self._middleware:
                await mw(request, span)
            
            return {
                "request": request,
                "context": context,
                "span": span
            }
        
        finally:
            trace_context.reset(token)
    
    async def process_response(
        self,
        response: Dict,
        span: Span,
        context: TraceContext
    ) -> Dict[str, Any]:
        """Process response with tracing."""
        status_code = response.get("status_code", 200)
        span.set_attribute("http.status_code", status_code)
        
        if status_code >= 400:
            span.status = SpanStatus.ERROR
        
        for processor in self._processors:
            try:
                processor(span)
            except Exception as e:
                logger.error(f"Span processor failed: {e}")
        
        self.tracer.end_span(span, span.status)
        
        response_headers = response.get("headers", {})
        response_headers.update(context.to_headers())
        
        return {
            "response": response,
            "headers": response_headers
        }
    
    def record_exception(self, span: Span, error: Exception) -> None:
        """Record an exception in the current span."""
        span.record_error(error)
    
    def get_trace(self) -> List[Dict[str, Any]]:
        """Get all recorded spans as dictionaries."""
        return [span.to_dict() for span in self.tracer._spans]
    
    def clear_trace(self) -> None:
        """Clear recorded trace data."""
        self.tracer.clear()
