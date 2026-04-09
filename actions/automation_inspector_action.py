"""Automation Inspector Action Module.

Provides runtime inspection of automation state, step execution,
variables, and performance metrics for debugging automation flows.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class InspectionPoint:
    point_id: str
    step_id: str
    timestamp: float
    state: str
    variables: Dict[str, Any]
    performance_ms: float


@dataclass
class StepMetrics:
    step_id: str
    execution_count: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0
    failure_count: int = 0
    last_execution: Optional[float] = None
    last_success: Optional[float] = None


class AutomationInspectorAction:
    """Runtime inspection and debugging for automation flows."""

    def __init__(self) -> None:
        self._enabled = True
        self._breakpoints: Set[str] = set()
        self._watches: Dict[str, Callable] = {}
        self._step_metrics: Dict[str, StepMetrics] = {}
        self._inspection_log: List[InspectionPoint] = []
        self._variable_snapshots: Dict[str, Dict[str, Any]] = {}
        self._listeners: Dict[str, List[Callable]] = {
            "breakpoint_hit": [],
            "watch_triggered": [],
            "step_complete": [],
        }
        self._max_log_size = 10000

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def set_breakpoint(self, step_id: str) -> None:
        self._breakpoints.add(step_id)
        logger.info(f"Breakpoint set at {step_id}")

    def remove_breakpoint(self, step_id: str) -> None:
        self._breakpoints.discard(step_id)

    def clear_breakpoints(self) -> None:
        self._breakpoints.clear()

    def is_breakpoint(self, step_id: str) -> bool:
        return step_id in self._breakpoints

    def watch(
        self,
        watch_id: str,
        condition_fn: Callable[[Dict[str, Any]], bool],
    ) -> None:
        self._watches[watch_id] = condition_fn

    def unwatch(self, watch_id: str) -> bool:
        return self._watches.pop(watch_id, None) is not None

    def inspect_step(
        self,
        step_id: str,
        state: str,
        variables: Dict[str, Any],
        performance_ms: float,
    ) -> Optional[str]:
        if not self._enabled:
            return None
        point = InspectionPoint(
            point_id=f"point_{len(self._inspection_log)}",
            step_id=step_id,
            timestamp=time.time(),
            state=state,
            variables=dict(variables),
            performance_ms=performance_ms,
        )
        self._inspection_log.append(point)
        if len(self._inspection_log) > self._max_log_size:
            self._inspection_log = self._inspection_log[-self._max_log_size // 2 :]
        self._update_metrics(step_id, performance_ms, success=(state == "success"))
        self._check_watches(variables)
        if step_id in self._breakpoints:
            self._notify("breakpoint_hit", point)
            return point.point_id
        return None

    def _update_metrics(
        self,
        step_id: str,
        duration_ms: float,
        success: bool,
    ) -> None:
        if step_id not in self._step_metrics:
            self._step_metrics[step_id] = StepMetrics(step_id=step_id)
        m = self._step_metrics[step_id]
        m.execution_count += 1
        m.total_duration_ms += duration_ms
        m.min_duration_ms = min(m.min_duration_ms, duration_ms)
        m.max_duration_ms = max(m.max_duration_ms, duration_ms)
        m.last_execution = time.time()
        if success:
            m.last_success = time.time()
        else:
            m.failure_count += 1

    def _check_watches(self, variables: Dict[str, Any]) -> None:
        for watch_id, condition_fn in self._watches.items():
            try:
                if condition_fn(variables):
                    self._notify("watch_triggered", {"watch_id": watch_id, "variables": variables})
            except Exception as e:
                logger.error(f"Watch condition failed for {watch_id}: {e}")

    def get_step_metrics(self, step_id: Optional[str] = None) -> Dict[str, Any]:
        if step_id:
            m = self._step_metrics.get(step_id)
            if not m:
                return {}
            return self._metrics_to_dict(m)
        return {sid: self._metrics_to_dict(m) for sid, m in self._step_metrics.items()}

    def _metrics_to_dict(self, m: StepMetrics) -> Dict[str, Any]:
        return {
            "step_id": m.step_id,
            "execution_count": m.execution_count,
            "avg_duration_ms": m.total_duration_ms / m.execution_count if m.execution_count > 0 else 0,
            "min_duration_ms": m.min_duration_ms if m.min_duration_ms != float("inf") else 0,
            "max_duration_ms": m.max_duration_ms,
            "failure_count": m.failure_count,
            "failure_rate": m.failure_count / m.execution_count if m.execution_count > 0 else 0,
            "last_execution": m.last_execution,
            "last_success": m.last_success,
        }

    def get_recent_inspections(
        self,
        count: int = 100,
        step_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        log = self._inspection_log
        if step_id:
            log = [p for p in log if p.step_id == step_id]
        return [
            {
                "point_id": p.point_id,
                "step_id": p.step_id,
                "timestamp": p.timestamp,
                "state": p.state,
                "performance_ms": p.performance_ms,
                "variable_count": len(p.variables),
            }
            for p in log[-count:]
        ]

    def get_slowest_steps(self, limit: int = 10) -> List[Dict[str, Any]]:
        sorted_metrics = sorted(
            self._step_metrics.values(),
            key=lambda m: m.total_duration_ms / m.execution_count if m.execution_count > 0 else 0,
            reverse=True,
        )
        return [self._metrics_to_dict(m) for m in sorted_metrics[:limit]]

    def get_failed_steps(self) -> List[Dict[str, Any]]:
        return [
            self._metrics_to_dict(m)
            for m in self._step_metrics.values()
            if m.failure_count > 0
        ]

    def add_listener(self, event: str, callback: Callable) -> None:
        if event in self._listeners:
            self._listeners[event].append(callback)

    def _notify(self, event: str, data: Any) -> None:
        for cb in self._listeners.get(event, []):
            try:
                cb(data)
            except Exception as e:
                logger.error(f"Inspector listener error for {event}: {e}")

    def take_variable_snapshot(self, name: str, variables: Dict[str, Any]) -> None:
        self._variable_snapshots[name] = dict(variables)

    def get_variable_snapshot(self, name: str) -> Optional[Dict[str, Any]]:
        return self._variable_snapshots.get(name)

    def diff_snapshots(
        self,
        snap_a: str,
        snap_b: str,
    ) -> Dict[str, Any]:
        a = self._variable_snapshots.get(snap_a, {})
        b = self._variable_snapshots.get(snap_b, {})
        all_keys = set(a.keys()) | set(b.keys())
        diff = {"added": [], "removed": [], "changed": []}
        for key in all_keys:
            if key not in a:
                diff["added"].append(key)
            elif key not in b:
                diff["removed"].append(key)
            elif a[key] != b[key]:
                diff["changed"].append({"key": key, "old": a[key], "new": b[key]})
        return diff
