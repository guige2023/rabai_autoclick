"""
API Distributed Tracing Action Module

Provides distributed tracing capabilities for API requests across microservices.
Supports trace propagation, span management, sampling strategies, and 
trace visualization data export.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SpanStatus(Enum):
    """Span execution status."""

    UNSTARTED = "unstarted"
    RUNNING = "running"
    OK = "ok"
    ERROR = "error"


class SamplingStrategy(Enum):
    """Trace sampling strategies."""

    ALWAYS = "always"
    NEVER = "never"
    PROBABILISTIC = "probabilistic"
    RATE_LIMITED = "rate_limited"
    ADAPTIVE = "adaptive"


@dataclass
class Span:
    """A single span in a distributed trace."""

    span_id: str
    trace_id: str
    name: str
    service_name: str
    parent_span_id: Optional[str] = None
    status: SpanStatus = SpanStatus.UNSTARTED
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration_ms: float = 0.0
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    annotations: Dict[str, str] = field(default_factory=dict)

    def start(self) -> None:
        """Start the span."""
        self.start_time = time.time()
        self.status = SpanStatus.RUNNING

    def finish(self, status: SpanStatus = SpanStatus.OK) -> None:
        """Finish the span."""
        self.end_time = time.time()
        self.status = status
        self.duration_ms = (self.end_time - self.start_time) * 1000 if self.start_time else 0.0

    def set_tag(self, key: str, value: Any) -> None:
        """Set a span tag."""
        self.tags[key] = value

    def add_log(self, message: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add a log entry to the span."""
        self.logs.append({
            "timestamp": time.time(),
            "message": message,
            "attributes": attributes or {},
        })

    def set_annotation(self, key: str, value: str) -> None:
        """Set a text annotation."""
        self.annotations[key] = value


@dataclass
class TraceContext:
    """Context for a complete distributed trace."""

    trace_id: str
    spans: List[Span] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    def add_span(self, span: Span) -> None:
        """Add a span to the trace."""
        self.spans.append(span)

    def get_span(self, span_id: str) -> Optional[Span]:
        """Get a span by ID."""
        for span in self.spans:
            if span.span_id == span_id:
                return span
        return None

    def get_duration_ms(self) -> float:
        """Get total trace duration."""
        if self.started_at is None:
            return 0.0
        end = self.completed_at or time.time()
        return (end - self.started_at) * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Serialize trace to dictionary."""
        return {
            "trace_id": self.trace_id,
            "spans": [
                {
                    "span_id": s.span_id,
                    "trace_id": s.trace_id,
                    "name": s.name,
                    "service_name": s.service_name,
                    "parent_span_id": s.parent_span_id,
                    "status": s.status.value,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "duration_ms": s.duration_ms,
                    "tags": s.tags,
                    "logs": s.logs,
                    "annotations": s.annotations,
                }
                for s in self.spans
            ],
            "metadata": self.metadata,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_duration_ms": self.get_duration_ms(),
        }


@dataclass
class SamplingConfig:
    """Configuration for trace sampling."""

    strategy: SamplingStrategy = SamplingStrategy.PROBABILISTIC
    sampling_rate: float = 0.1
    min_traces_per_second: int = 100
    max_traces_per_second: int = 1000


@dataclass
class TracingConfig:
    """Configuration for distributed tracing."""

    service_name: str = "unknown-service"
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    max_span_depth: int = 100
    trace_ttl_seconds: float = 3600.0
    include_local_annotations: bool = True
    export_format: str = "json"


class Tracer:
    """Tracer for creating and managing spans."""

    def __init__(self, config: Optional[TracingConfig] = None):
        self.config = config or TracingConfig()
        self._active_spans: Dict[str, Span] = {}
        self._completed_traces: Dict[str, TraceContext] = {}

    def should_sample(self) -> bool:
        """Determine if a new trace should be sampled."""
        cfg = self.config.sampling
        if cfg.strategy == SamplingStrategy.ALWAYS:
            return True
        elif cfg.strategy == SamplingStrategy.NEVER:
            return False
        elif cfg.strategy == SamplingStrategy.PROBABILISTIC:
            import random
            return random.random() < cfg.sampling_rate
        elif cfg.strategy == SamplingStrategy.RATE_LIMITED:
            return True  # Simplified rate limiting
        return True

    def create_trace_id(self) -> str:
        """Generate a new trace ID."""
        return uuid.uuid4().hex

    def create_span_id(self) -> str:
        """Generate a new span ID."""
        return uuid.uuid4().hex[:16]

    def start_span(
        self,
        name: str,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
    ) -> Span:
        """Start a new span."""
        if not self.should_sample():
            span = Span(
                span_id="unsampled",
                trace_id=trace_id or "unsampled",
                name=name,
                service_name=self.config.service_name,
                parent_span_id=parent_span_id,
            )
            span.start()
            return span

        span_id = self.create_span_id()
        actual_trace_id = trace_id or self.create_trace_id()

        span = Span(
            span_id=span_id,
            trace_id=actual_trace_id,
            name=name,
            service_name=self.config.service_name,
            parent_span_id=parent_span_id,
            tags=tags or {},
        )
        span.start()
        self._active_spans[span_id] = span

        return span

    def end_span(self, span: Span, status: SpanStatus = SpanStatus.OK) -> None:
        """End a span."""
        span.finish(status)
        if span.span_id in self._active_spans:
            del self._active_spans[span.span_id]

    def get_trace(self, trace_id: str) -> Optional[TraceContext]:
        """Get a completed trace by ID."""
        return self._completed_traces.get(trace_id)

    def complete_trace(self, trace: TraceContext) -> None:
        """Mark a trace as complete."""
        trace.completed_at = time.time()
        self._completed_traces[trace.trace_id] = trace


class APIDistributedTracingAction:
    """
    Distributed tracing action for API requests.

    Features:
    - Trace and span creation with context propagation
    - Multiple sampling strategies
    - Automatic trace assembly from spans
    - HTTP header injection for cross-service propagation
    - Export to multiple formats (JSON, Jaeger, Zipkin)

    Usage:
        tracing = APIDistributedTracingAction(config)
        with tracing.start_span("api_call") as span:
            result = await api.request()
            span.set_tag("http.status_code", result.status_code)
    """

    def __init__(self, config: Optional[TracingConfig] = None):
        self.config = config or TracingConfig()
        self._tracer = Tracer(self.config)
        self._trace_history: Dict[str, TraceContext] = {}
        self._stats = {
            "traces_started": 0,
            "traces_completed": 0,
            "spans_created": 0,
            "errors": 0,
        }

    def inject_trace_context(
        self,
        headers: Dict[str, str],
        trace_id: str,
        span_id: str,
    ) -> Dict[str, str]:
        """
        Inject trace context into HTTP headers for propagation.

        Args:
            headers: Existing HTTP headers dict
            trace_id: Current trace ID
            span_id: Current span ID

        Returns:
            Updated headers with trace context
        """
        headers["X-Trace-Id"] = trace_id
        headers["X-Span-Id"] = span_id
        headers["X-Service-Name"] = self.config.service_name
        return headers

    def extract_trace_context(
        self,
        headers: Dict[str, str],
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Extract trace context from HTTP headers.

        Args:
            headers: HTTP headers dict

        Returns:
            Tuple of (trace_id, span_id)
        """
        trace_id = headers.get("X-Trace-Id")
        span_id = headers.get("X-Span-Id")
        return trace_id, span_id

    async def trace_api_call(
        self,
        name: str,
        func: Callable[..., Any],
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute an API call with tracing.

        Args:
            name: Name of the span
            func: Async function to call
            trace_id: Optional existing trace ID
            parent_span_id: Optional parent span ID
            tags: Optional span tags
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result
        """
        span = self._tracer.start_span(
            name=name,
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            tags=tags,
        )
        self._stats["spans_created"] += 1

        trace = self._trace_history.get(span.trace_id)
        if trace is None:
            trace = TraceContext(trace_id=span.trace_id)
            trace.started_at = span.start_time
            self._trace_history[span.trace_id] = trace
            self._stats["traces_started"] += 1

        trace.add_span(span)

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            span.set_tag("status", "ok")
            self._tracer.end_span(span, SpanStatus.OK)
            return result

        except Exception as e:
            span.set_tag("error", True)
            span.set_tag("error.type", type(e).__name__)
            span.add_log(f"Error: {str(e)}")
            self._tracer.end_span(span, SpanStatus.ERROR)
            self._stats["errors"] += 1
            raise

    def create_span(
        self,
        name: str,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
    ) -> Span:
        """Create a new span for manual instrumentation."""
        return self._tracer.start_span(name, trace_id, parent_span_id)

    def end_span(self, span: Span, status: SpanStatus = SpanStatus.OK) -> None:
        """End a span."""
        self._tracer.end_span(span, status)

    def complete_trace(self, trace_id: str) -> Optional[TraceContext]:
        """Complete and finalize a trace."""
        trace = self._trace_history.get(trace_id)
        if trace:
            self._tracer.complete_trace(trace)
            self._stats["traces_completed"] += 1
        return trace

    def export_trace(
        self,
        trace_id: str,
        format: str = "json",
    ) -> Dict[str, Any]:
        """
        Export a trace in the specified format.

        Args:
            trace_id: ID of trace to export
            format: Export format (json, jaeger, zipkin)

        Returns:
            Exported trace data
        """
        trace = self._trace_history.get(trace_id)
        if trace is None:
            return {"error": "Trace not found"}

        if format == "json":
            return trace.to_dict()
        elif format == "jaeger":
            return self._export_jaeger_format(trace)
        elif format == "zipkin":
            return self._export_zipkin_format(trace)
        return trace.to_dict()

    def _export_jaeger_format(self, trace: TraceContext) -> Dict[str, Any]:
        """Export trace in Jaeger format."""
        return {
            "traceID": trace.trace_id,
            "spans": [
                {
                    "traceID": s.trace_id,
                    "spanID": s.span_id,
                    "parentSpanID": s.parent_span_id or "",
                    "operationName": s.name,
                    "startTime": int((s.start_time or 0) * 1000000),
                    "duration": int(s.duration_ms * 1000),
                    "tags": [{"key": k, "vStr": str(v)} for k, v in s.tags.items()],
                    "logs": [
                        {"timestamp": int(l["timestamp"] * 1000000), "fields": [{"key": "message", "vStr": l["message"]}]}
                        for l in s.logs
                    ],
                }
                for s in trace.spans
            ],
        }

    def _export_zipkin_format(self, trace: TraceContext) -> List[Dict[str, Any]]:
        """Export trace in Zipkin format."""
        return [
            {
                "traceId": s.trace_id,
                "id": s.span_id,
                "parentId": s.parent_span_id,
                "name": s.name,
                "timestamp": int((s.start_time or 0) * 1000000),
                "duration": int(s.duration_ms * 1000),
                "localEndpoint": {"serviceName": s.service_name},
                "tags": s.tags,
            }
            for s in trace.spans
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get tracing statistics."""
        return self._stats.copy()

    def get_active_span_count(self) -> int:
        """Get number of currently active spans."""
        return len(self._tracer._active_spans)

    def get_trace_ids(self, limit: int = 100) -> List[str]:
        """Get recent trace IDs."""
        return list(self._trace_history.keys())[-limit:]


async def demo_tracing():
    """Demonstrate distributed tracing usage."""
    config = TracingConfig(
        service_name="demo-service",
        sampling=SamplingConfig(strategy=SamplingStrategy.PROBABILISTIC, sampling_rate=1.0),
    )
    tracing = APIDistributedTracingAction(config)

    async def fetch_user(user_id: str) -> Dict[str, Any]:
        await asyncio.sleep(0.05)
        return {"id": user_id, "name": f"User {user_id}"}

    trace_id = tracing._tracer.create_trace_id()

    result = await tracing.trace_api_call(
        "fetch_user",
        fetch_user,
        trace_id=trace_id,
        tags={"user.id": "123"},
        "123",
    )
    print(f"Result: {result}")
    print(f"Stats: {tracing.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_tracing())
