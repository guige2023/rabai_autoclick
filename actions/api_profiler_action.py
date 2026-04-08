"""API Profiler Action Module. Profiles API request performance."""
import sys, os, time, json
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class ProfilerStage:
    name: str; duration_ms: float; percentage: float

@dataclass
class ProfilerResult:
    total_duration_ms: float; stages: list; request_size_bytes: int = 0
    response_size_bytes: int = 0; parsing_duration_ms: float = 0.0
    serialization_duration_ms: float = 0.0; network_duration_ms: float = 0.0
    bottlenecks: list = field(default_factory=list)

class APIProfilerAction(BaseAction):
    action_type = "api_profiler"; display_name = "API性能分析"
    description = "分析API请求各阶段耗时"
    def __init__(self) -> None: super().__init__()
    def execute(self, context: Any, params: dict) -> ActionResult:
        url = params.get("url")
        if not url: return ActionResult(success=False, message="URL is required")
        method = params.get("method", "GET").upper()
        headers = dict(params.get("headers", {}))
        body = params.get("body")
        total_start = time.perf_counter()
        ser_start = time.perf_counter()
        serialized = json.dumps(body).encode() if body else None
        ser_dur = (time.perf_counter() - ser_start) * 1000
        import urllib.request
        req_start = time.perf_counter()
        req = urllib.request.Request(url, data=serialized, headers=headers, method=method)
        req_dur = (time.perf_counter() - req_start) * 1000
        net_start = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = response.read()
            net_dur = (time.perf_counter() - net_start) * 1000
        except Exception as e:
            return ActionResult(success=False, message=f"Request failed: {e}")
        parse_start = time.perf_counter()
        try: parsed = json.loads(response_data) if response_data else {}
        except: parsed = response_data.decode(errors="replace") if response_data else ""
        parse_dur = (time.perf_counter() - parse_start) * 1000
        total_dur = (time.perf_counter() - total_start) * 1000
        def pct(d): return (d / total_dur * 100) if total_dur > 0 else 0.0
        stages = [ProfilerStage("serialization", ser_dur, pct(ser_dur)),
                  ProfilerStage("request_construction", req_dur, pct(req_dur)),
                  ProfilerStage("network", net_dur, pct(net_dur)),
                  ProfilerStage("parsing", parse_dur, pct(parse_dur))]
        bottlenecks = [f"High {s.name} overhead: {s.percentage:.1f}%" for s in stages if s.percentage > 40]
        result = ProfilerResult(total_duration_ms=total_dur, stages=stages,
            request_size_bytes=len(serialized) if serialized else 0,
            response_size_bytes=len(response_data),
            parsing_duration_ms=parse_dur, serialization_duration_ms=ser_dur,
            network_duration_ms=net_dur, bottlenecks=bottlenecks)
        return ActionResult(success=True, message=f"Profiled: {total_dur:.2f}ms total", data=result)
