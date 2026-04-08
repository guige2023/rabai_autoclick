"""Distributed API Tracing Action.

Implements distributed tracing for API requests across service boundaries.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import time
import uuid


@dataclass
class TraceSpan:
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "ok"

    def finish(self) -> None:
        self.end_time = time.time()

    def add_tag(self, key: str, value: str) -> None:
        self.tags[key] = value

    def add_log(self, event: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self.logs.append({
            "timestamp": time.time(),
            "event": event,
            "metadata": metadata or {},
        })

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": (self.end_time - self.start_time) * 1000 if self.end_time else None,
            "tags": self.tags,
            "logs": self.logs,
            "status": self.status,
        }


class APITraceAction:
    """Distributed tracing for API requests."""

    def __init__(self, service_name: str = "unknown") -> None:
        self.service_name = service_name
        self.spans: Dict[str, TraceSpan] = {}
        self._current_span: Optional[TraceSpan] = None

    def start_span(
        self,
        operation_name: str,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> TraceSpan:
        span_id = uuid.uuid4().hex[:16]
        span = TraceSpan(
            trace_id=trace_id or uuid.uuid4().hex,
            span_id=span_id,
            parent_span_id=parent_span_id,
            operation_name=operation_name,
        )
        if tags:
            for k, v in tags.items():
                span.add_tag(k, v)
        span.add_tag("service.name", self.service_name)
        self.spans[span_id] = span
        self._current_span = span
        return span

    def end_span(self, span: TraceSpan) -> None:
        span.finish()
        self._current_span = None

    def get_current_span(self) -> Optional[TraceSpan]:
        return self._current_span

    def inject_context(self) -> Dict[str, str]:
        span = self._current_span
        if not span:
            return {}
        return {
            "X-Trace-Id": span.trace_id,
            "X-Span-Id": span.span_id,
        }

    def extract_context(self, headers: Dict[str, str]) -> Optional[Dict[str, Optional[str]]]:
        return {
            "trace_id": headers.get("X-Trace-Id"),
            "parent_span_id": headers.get("X-Span-Id"),
        }

    def get_trace_tree(self, trace_id: str) -> List[TraceSpan]:
        return [s for s in self.spans.values() if s.trace_id == trace_id]

    def export_trace(self, trace_id: str) -> List[Dict[str, Any]]:
        return [s.to_dict() for s in self.get_trace_tree(trace_id)]
