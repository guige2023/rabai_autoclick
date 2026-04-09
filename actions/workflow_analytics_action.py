"""
Workflow Analytics and Performance Metrics Module.

Tracks execution time, success rates, bottlenecks, and provides
dashboards for workflow performance analysis.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class MetricType(Enum):
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()


@dataclass
class MetricPoint:
    value: float
    timestamp: float
    labels: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)


@dataclass
class WorkflowExecution:
    execution_id: str
    workflow_name: str
    started_at: float
    completed_at: Optional[float] = None
    status: str = "running"
    stage_durations: Dict[str, float] = field(default_factory=dict)
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StageMetrics:
    stage_name: str
    call_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0
    p50_duration_ms: float = 0.0
    p95_duration_ms: float = 0.0
    p99_duration_ms: float = 0.0


class MetricsCollector:
    """Collects and aggregates metrics."""

    def __init__(self, retention_seconds: int = 3600):
        self.retention_seconds = retention_seconds
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, float] = {}
        self._last_cleanup: float = time.time()

    def increment(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._make_key(name, labels)
        self._counters[key] += value
        self._maybe_cleanup()

    def gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._make_key(name, labels)
        self._gauges[key] = value

    def histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._make_key(name, labels)
        self._histograms[key].append(value)
        self._maybe_cleanup()

    def timer_start(self, name: str) -> str:
        timer_id = f"{name}:{time.time()}"
        self._timers[timer_id] = time.time()
        return timer_id

    def timer_stop(self, timer_id: str) -> Optional[float]:
        if timer_id not in self._timers:
            return None
        duration = (time.time() - self._timers.pop(timer_id)) * 1000
        name = timer_id.rsplit(":", 1)[0]
        self.histogram(name, duration)
        return duration

    def _make_key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def _maybe_cleanup(self) -> None:
        if time.time() - self._last_cleanup > self.retention_seconds:
            self._cleanup()

    def _cleanup(self) -> None:
        cutoff = time.time() - self.retention_seconds
        for key in list(self._histograms.keys()):
            self._histograms[key] = [v for v in self._histograms[key] if v >= cutoff]
            if not self._histograms[key]:
                del self._histograms[key]
        self._last_cleanup = time.time()

    def get_percentile(self, values: List[float], p: float) -> float:
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        idx = int(len(sorted_vals) * p / 100)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    def compute_histogram_stats(self, key: str) -> Dict[str, float]:
        values = self._histograms.get(key, [])
        if not values:
            return {}
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "p50": self.get_percentile(values, 50),
            "p95": self.get_percentile(values, 95),
            "p99": self.get_percentile(values, 99),
        }


class WorkflowAnalytics:
    """
    Analytics and metrics tracking for workflow executions.
    """

    def __init__(self):
        self.metrics = MetricsCollector()
        self._executions: Dict[str, WorkflowExecution] = {}
        self._stage_metrics: Dict[str, Dict[str, StageMetrics]] = defaultdict(dict)
        self._workflow_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_runs": 0, "successes": 0, "failures": 0, "avg_duration_ms": 0
        })

    def start_execution(
        self, workflow_name: str, execution_id: Optional[str] = None
    ) -> str:
        import uuid
        eid = execution_id or str(uuid.uuid4())[:8]
        exec_record = WorkflowExecution(
            execution_id=eid,
            workflow_name=workflow_name,
            started_at=time.time(),
        )
        self._executions[eid] = exec_record
        self.metrics.increment(f"workflow.running", labels={"workflow": workflow_name})
        logger.debug("Started tracking execution %s for '%s'", eid, workflow_name)
        return eid

    def start_stage(self, execution_id: str, stage_name: str) -> str:
        timer_id = f"{execution_id}:{stage_name}:{time.time()}"
        self._timers[timer_id] = time.time()
        return timer_id

    _timers: Dict[str, float] = {}

    def end_stage(
        self, execution_id: str, stage_name: str, status: str = "success"
    ) -> None:
        if execution_id not in self._executions:
            return
        exec_record = self._executions[execution_id]
        now = time.time()

        timer_key = next((k for k in self._timers if k.startswith(f"{execution_id}:{stage_name}:")), None)
        if timer_key:
            duration = (now - self._timers.pop(timer_key)) * 1000
            exec_record.stage_durations[stage_name] = duration
            self._update_stage_metrics(exec_record.workflow_name, stage_name, duration, status)

    def _update_stage_metrics(
        self, workflow_name: str, stage_name: str, duration_ms: float, status: str
    ) -> None:
        if workflow_name not in self._stage_metrics:
            self._stage_metrics[workflow_name] = {}
        if stage_name not in self._stage_metrics[workflow_name]:
            self._stage_metrics[workflow_name][stage_name] = StageMetrics(stage_name=stage_name)

        sm = self._stage_metrics[workflow_name][stage_name]
        sm.call_count += 1
        if status != "success":
            sm.error_count += 1
        sm.total_duration_ms += duration_ms
        sm.min_duration_ms = min(sm.min_duration_ms, duration_ms)
        sm.max_duration_ms = max(sm.max_duration_ms, duration_ms)

        if "durations" not in self.__dict__:
            self._durations = {}
        if workflow_name not in self._durations:
            self._durations[workflow_name] = {}
        if stage_name not in self._durations[workflow_name]:
            self._durations[workflow_name][stage_name] = []
        self._durations[workflow_name][stage_name].append(duration_ms)

        durations = self._durations[workflow_name][stage_name]
        sm.p50_duration_ms = self.metrics.get_percentile(durations, 50)
        sm.p95_duration_ms = self.metrics.get_percentile(durations, 95)
        sm.p99_duration_ms = self.metrics.get_percentile(durations, 99)

    def end_execution(
        self,
        execution_id: str,
        status: str = "success",
        error: Optional[str] = None,
    ) -> None:
        if execution_id not in self._executions:
            return
        exec_record = self._executions[execution_id]
        exec_record.completed_at = time.time()
        exec_record.status = status
        exec_record.error = error

        duration = (exec_record.completed_at - exec_record.started_at) * 1000
        self.metrics.histogram("workflow.duration_ms", duration, {"workflow": exec_record.workflow_name})
        self.metrics.increment(f"workflow.{status}")
        self.metrics.increment(f"workflow.running", labels={"workflow": exec_record.workflow_name}, value=-1)

        ws = self._workflow_stats[exec_record.workflow_name]
        ws["total_runs"] += 1
        if status == "success":
            ws["successes"] += 1
        else:
            ws["failures"] += 1
        ws["avg_duration_ms"] = (ws["avg_duration_ms"] * (ws["total_runs"] - 1) + duration) / ws["total_runs"]

    def get_workflow_summary(self, workflow_name: str) -> Dict[str, Any]:
        ws = self._workflow_stats.get(workflow_name, {})
        stage_data = self._stage_metrics.get(workflow_name, {})
        return {
            "workflow": workflow_name,
            "total_runs": ws.get("total_runs", 0),
            "successes": ws.get("successes", 0),
            "failures": ws.get("failures", 0),
            "success_rate": ws.get("successes", 0) / max(ws.get("total_runs", 1), 1),
            "avg_duration_ms": ws.get("avg_duration_ms", 0),
            "stages": {
                name: {
                    "call_count": sm.call_count,
                    "error_count": sm.error_count,
                    "avg_duration_ms": sm.total_duration_ms / max(sm.call_count, 1),
                    "min_duration_ms": sm.min_duration_ms if sm.min_duration_ms != float("inf") else 0,
                    "max_duration_ms": sm.max_duration_ms,
                    "p50_duration_ms": sm.p50_duration_ms,
                    "p95_duration_ms": sm.p95_duration_ms,
                }
                for name, sm in stage_data.items()
            },
        }

    def get_all_summaries(self) -> Dict[str, Dict[str, Any]]:
        return {name: self.get_workflow_summary(name) for name in self._workflow_stats}

    def get_bottlenecks(self, workflow_name: str, top_k: int = 5) -> List[Tuple[str, float]]:
        summary = self.get_workflow_summary(workflow_name)
        stages = summary.get("stages", {})
        bottlenecks = [(name, data["avg_duration_ms"]) for name, data in stages.items()]
        bottlenecks.sort(key=lambda x: x[1], reverse=True)
        return bottlenecks[:top_k]

    def export_json(self) -> str:
        return json.dumps({
            "workflows": self.get_all_summaries(),
            "generated_at": datetime.utcnow().isoformat(),
        }, indent=2)
