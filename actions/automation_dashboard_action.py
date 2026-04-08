"""Automation Dashboard Action Module. Creates real-time automation dashboards."""
import sys, os, time, threading
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class MetricSnapshot:
    name: str; value: float; unit: str; timestamp: float; tags: dict = field(default_factory=dict)

@dataclass
class DashboardData:
    timestamp: float; metrics: list; workflow_count: int; active_workflows: int
    completed_today: int; failed_today: int; avg_duration_ms: float; uptime_seconds: float

class AutomationDashboardAction(BaseAction):
    action_type = "automation_dashboard"; display_name = "自动化仪表板"
    description = "生成自动化仪表板"
    def __init__(self) -> None:
        super().__init__(); self._lock = threading.Lock()
        self._metrics = []; self._workflows = []; self._start_time = time.time(); self._max = 1000
    def record_metric(self, name: str, value: float, unit: str = "", tags: dict = None) -> None:
        with self._lock:
            self._metrics.append(MetricSnapshot(name=name, value=value, unit=unit, timestamp=time.time(), tags=tags or {}))
            if len(self._metrics) > self._max: self._metrics = self._metrics[-self._max:]
    def record_workflow(self, name: str, status: str, duration_ms: float) -> None:
        with self._lock:
            self._workflows.append({"name": name, "status": status, "duration_ms": duration_ms, "timestamp": time.time()})
            cutoff = time.time() - 86400
            self._workflows = [w for w in self._workflows if w["timestamp"] >= cutoff]
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "generate")
        if mode == "record_metric":
            self.record_metric(params.get("name","metric"), params.get("value",0), params.get("unit",""), params.get("tags",{}))
            return ActionResult(success=True, message="Metric recorded")
        if mode == "record_workflow":
            self.record_workflow(params.get("workflow_name","workflow"), params.get("status","success"), params.get("duration_ms",0))
            return ActionResult(success=True, message="Workflow recorded")
        now = time.time(); cutoff = now - 86400
        with self._lock:
            recent_m = list(self._metrics)
            recent_w = [w for w in self._workflows if w["timestamp"] >= cutoff]
        completed = sum(1 for w in recent_w if w["status"]=="success")
        failed = sum(1 for w in recent_w if w["status"]=="failed")
        durations = [w["duration_ms"] for w in recent_w]
        avg_dur = sum(durations)/len(durations) if durations else 0
        data = DashboardData(timestamp=now, metrics=recent_m[-50:], workflow_count=len(recent_w),
                            active_workflows=sum(1 for w in recent_w if w["status"]=="running"),
                            completed_today=completed, failed_today=failed,
                            avg_duration_ms=avg_dur, uptime_seconds=now-self._start_time)
        output_fmt = params.get("output_format","json")
        if output_fmt == "html":
            html = f"<html><body><h1>Dashboard</h1><p>Active: {data.active_workflows}, Completed: {completed}, Failed: {failed}</p></body></html>"
            return ActionResult(success=True, message=f"Dashboard generated", data={"html": html})
        return ActionResult(success=True, message=f"Dashboard: {completed} done, {failed} failed", data=vars(data))
