"""API Trace Action Module. Distributed tracing for API requests."""
import sys, os, time, uuid, json
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class TraceSpan:
    name: str; span_id: str; parent_id: Optional[str]
    start_time_ms: float; end_time_ms: float; duration_ms: float
    metadata: dict = field(default_factory=dict)
    tags: dict = field(default_factory=dict)
    annotations: list = field(default_factory=list)

@dataclass
class TraceResult:
    trace_id: str; root_span: TraceSpan; total_spans: int
    total_duration_ms: float; spans_by_name: dict = field(default_factory=dict)

class APITraceAction(BaseAction):
    action_type = "api_trace"; display_name = "API链路追踪"
    description = "追踪API请求调用链路"
    def __init__(self) -> None: super().__init__()
    def _new_span_id(self) -> str: return uuid.uuid4().hex[:16]
    def execute(self, context: Any, params: dict) -> ActionResult:
        url = params.get("url")
        if not url: return ActionResult(success=False, message="URL is required")
        method = params.get("method", "GET").upper()
        headers = dict(params.get("headers", {}))
        body = params.get("body")
        span_names = params.get("trace_spans", ["api_request"])
        annotations = params.get("annotations", [])
        propagate = params.get("propagate", True)
        trace_id = uuid.uuid4().hex
        if propagate: headers["X-Trace-ID"] = trace_id
        root_start = time.perf_counter()
        import urllib.request
        prep_start = time.perf_counter()
        serialized = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=serialized, headers=headers, method=method)
        prep_dur = (time.perf_counter() - prep_start) * 1000
        spans = [TraceSpan("preparation", self._new_span_id(), None,
                          prep_start*1000, (prep_start+prep_dur/1000)*1000, prep_dur,
                          {"url": url, "method": method})]
        parent_id = spans[0].span_id
        for i, name in enumerate(span_names):
            s_start = time.perf_counter()
            ann = annotations[i] if i < len(annotations) else None
            time.sleep(0.001)
            s_dur = (time.perf_counter() - s_start) * 1000
            spans.append(TraceSpan(name, self._new_span_id(), parent_id,
                                   s_start*1000, (s_start+s_dur/1000)*1000, s_dur,
                                   annotations=[ann] if ann else []))
        net_start = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                _ = response.read()
            net_dur = (time.perf_counter() - net_start) * 1000
        except Exception as e:
            net_dur = (time.perf_counter() - net_start) * 1000
            spans.append(TraceSpan("network_request", self._new_span_id(), parent_id,
                                   net_start*1000, (net_start+net_dur/1000)*1000, net_dur,
                                   {"error": str(e)}))
            total_dur = (time.perf_counter() - root_start) * 1000
            return ActionResult(success=False, message=f"Trace failed: {e}",
                               data={"trace_id": trace_id, "spans": [vars(s) for s in spans]})
        spans.append(TraceSpan("network_request", self._new_span_id(), parent_id,
                               net_start*1000, (net_start+net_dur/1000)*1000, net_dur,
                               {"status": "success"}))
        total_dur = (time.perf_counter() - root_start) * 1000
        by_name = {}
        for s in spans: by_name.setdefault(s.name, []).append(s.duration_ms)
        result = TraceResult(trace_id=trace_id, root_span=spans[0], total_spans=len(spans),
                            total_duration_ms=total_dur, spans_by_name=by_name)
        return ActionResult(success=True, message=f"Trace: {len(spans)} spans, {total_dur:.2f}ms",
                          data=result)
