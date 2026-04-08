"""API Trace Action Module.

Implements distributed tracing for API requests
with span management and trace context propagation.
"""

from __future__ import annotations

import sys
import os
import time
import uuid
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Span:
    """A trace span."""
    span_id: str
    trace_id: str
    name: str
    start_time: float
    end_time: Optional[float] = None
    parent_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)


class APITraceAction(BaseAction):
    """
    Distributed tracing for API requests.

    Creates and manages trace spans for request
    tracking and performance analysis.

    Example:
        tracer = APITraceAction()
        result = tracer.execute(ctx, {"action": "start_span", "name": "db_query"})
    """
    action_type = "api_trace"
    display_name = "API链路追踪"
    description = "API分布式链路追踪"

    def __init__(self) -> None:
        super().__init__()
        self._active_spans: Dict[str, Span] = {}
        self._completed_spans: List[Span] = []
        self._trace_context: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "start_span":
                return self._start_span(params)
            elif action == "end_span":
                return self._end_span(params)
            elif action == "add_tag":
                return self._add_tag(params)
            elif action == "add_log":
                return self._add_log(params)
            elif action == "get_trace":
                return self._get_trace(params)
            elif action == "inject_context":
                return self._inject_context(params)
            elif action == "extract_context":
                return self._extract_context(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Trace error: {str(e)}")

    def _start_span(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "span")
        trace_id = params.get("trace_id")
        parent_id = params.get("parent_id")

        if not trace_id:
            trace_id = str(uuid.uuid4()).replace("-", "")[:16]

        span_id = str(uuid.uuid4()).replace("-", "")[:8]

        span = Span(span_id=span_id, trace_id=trace_id, name=name, start_time=time.time(), parent_id=parent_id)
        self._active_spans[span_id] = span

        return ActionResult(success=True, message=f"Started span: {name}", data={"span_id": span_id, "trace_id": trace_id})

    def _end_span(self, params: Dict[str, Any]) -> ActionResult:
        span_id = params.get("span_id", "")

        if span_id not in self._active_spans:
            return ActionResult(success=False, message=f"Span not found: {span_id}")

        span = self._active_spans[span_id]
        span.end_time = time.time()

        self._completed_spans.append(span)
        del self._active_spans[span_id]

        duration_ms = (span.end_time - span.start_time) * 1000

        return ActionResult(success=True, message=f"Completed span: {span.name}", data={"span_id": span_id, "duration_ms": duration_ms})

    def _add_tag(self, params: Dict[str, Any]) -> ActionResult:
        span_id = params.get("span_id", "")
        key = params.get("key", "")
        value = params.get("value", "")

        if span_id not in self._active_spans:
            return ActionResult(success=False, message=f"Span not found: {span_id}")

        self._active_spans[span_id].tags[key] = str(value)

        return ActionResult(success=True, message=f"Tag added: {key}")

    def _add_log(self, params: Dict[str, Any]) -> ActionResult:
        span_id = params.get("span_id", "")
        message = params.get("message", "")

        if span_id not in self._active_spans:
            return ActionResult(success=False, message=f"Span not found: {span_id}")

        self._active_spans[span_id].logs.append({"timestamp": time.time(), "message": message})

        return ActionResult(success=True, message="Log added")

    def _get_trace(self, params: Dict[str, Any]) -> ActionResult:
        trace_id = params.get("trace_id", "")

        all_spans = list(self._active_spans.values()) + self._completed_spans
        trace_spans = [s for s in all_spans if s.trace_id == trace_id]

        if not trace_spans:
            return ActionResult(success=False, message=f"Trace not found: {trace_id}")

        return ActionResult(success=True, data={"trace_id": trace_id, "spans": [{"span_id": s.span_id, "name": s.name, "duration_ms": (s.end_time - s.start_time) * 1000 if s.end_time else None} for s in trace_spans]})

    def _inject_context(self, params: Dict[str, Any]) -> ActionResult:
        trace_id = params.get("trace_id", "")
        span_id = params.get("span_id", "")

        headers = {"X-Trace-Id": trace_id, "X-Span-Id": span_id}

        return ActionResult(success=True, data={"headers": headers})

    def _extract_context(self, params: Dict[str, Any]) -> ActionResult:
        headers = params.get("headers", {})

        trace_id = headers.get("X-Trace-Id", "")
        span_id = headers.get("X-Span-Id", "")

        return ActionResult(success=True, data={"trace_id": trace_id, "span_id": span_id})
