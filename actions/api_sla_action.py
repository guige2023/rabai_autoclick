"""API SLA Action Module. Monitors API SLA compliance."""
import sys, os, time
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class SLATarget:
    metric: str; threshold: float; window_hours: int

@dataclass
class SLAReport:
    window_start: float; window_end: float; total_requests: int
    successful_requests: int; failed_requests: int; availability: float
    avg_latency_ms: float; p99_latency_ms: float; error_rate: float
    sla_targets: list; violations: list; compliant: bool

class APISLAAction(BaseAction):
    action_type = "api_sla"; display_name = "API SLA监控"
    description = "监控API服务等级协议"
    def __init__(self) -> None:
        super().__init__(); self._metrics_history = []; self._window_seconds = 3600
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "check")
        url = params.get("url")
        avail_target = params.get("availability_target", 99.9)
        latency_target = params.get("latency_p99_target", 500.0)
        error_target = params.get("error_rate_target", 0.1)
        window_hours = params.get("window_hours", 1)
        self._window_seconds = window_hours * 3600
        if mode == "record":
            metric = params.get("record_metric", {})
            self._metrics_history.append({"timestamp": time.time(),
                "success": metric.get("success", True),
                "latency_ms": metric.get("latency_ms", 0.0),
                "error": metric.get("error", False)})
            cutoff = time.time() - self._window_seconds
            self._metrics_history = [m for m in self._metrics_history if m["timestamp"] >= cutoff]
            return ActionResult(success=True, message=f"Recorded. Total in window: {len(self._metrics_history)}")
        if url:
            import urllib.request
            start = time.time()
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=30) as response:
                    latency_ms = (time.time() - start) * 1000
                    compliant = 200 <= response.status < 400
            except:
                latency_ms = (time.time() - start) * 1000; compliant = False
            return ActionResult(success=True, message=f"SLA check: {'OK' if compliant else 'FAIL'} ({latency_ms:.1f}ms)", data={"url": url, "latency_ms": latency_ms, "compliant": compliant})
        if not self._metrics_history:
            return ActionResult(success=False, message="No metrics recorded")
        cutoff = time.time() - self._window_seconds
        window = [m for m in self._metrics_history if m["timestamp"] >= cutoff]
        if not window: return ActionResult(success=False, message="No metrics in window")
        total = len(window); successful = sum(1 for m in window if m.get("success", True))
        failed = total - successful; latencies = sorted([m.get("latency_ms", 0) for m in window])
        errors = sum(1 for m in window if m.get("error", False))
        availability = successful / total * 100 if total > 0 else 0.0
        avg_lat = sum(latencies) / len(latencies) if latencies else 0.0
        p99_idx = min(int(len(latencies)*0.99), len(latencies)-1)
        p99_lat = latencies[p99_idx] if latencies else 0.0
        error_rate = errors / total * 100 if total > 0 else 0.0
        violations = []
        if availability < avail_target: violations.append(f"Availability {availability:.2f}% < {avail_target}%")
        if p99_lat > latency_target: violations.append(f"P99 {p99_lat:.1f}ms > {latency_target}ms")
        if error_rate > error_target: violations.append(f"Error rate {error_rate:.2f}% > {error_target}%")
        targets = [SLATarget("availability", avail_target, window_hours),
                   SLATarget("latency_p99", latency_target, window_hours),
                   SLATarget("error_rate", error_target, window_hours)]
        report = SLAReport(window_start=window[0]["timestamp"], window_end=window[-1]["timestamp"],
                          total_requests=total, successful_requests=successful,
                          failed_requests=failed, availability=availability,
                          avg_latency_ms=avg_lat, p99_latency_ms=p99_lat,
                          error_rate=error_rate, sla_targets=targets,
                          violations=violations, compliant=len(violations)==0)
        return ActionResult(success=True, message=f"SLA {'compliant' if report.compliant else 'VIOLATION'}", data=report)
