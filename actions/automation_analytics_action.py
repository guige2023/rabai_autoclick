"""Automation Analytics Action.

Collects and analyzes metrics from automation executions.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import statistics


@dataclass
class ExecutionMetrics:
    execution_id: str
    start_time: float
    end_time: Optional[float] = None
    duration_ms: float = 0.0
    steps_completed: int = 0
    steps_failed: int = 0
    error_message: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)

    def finish(self) -> None:
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000

    def success_rate(self) -> float:
        total = self.steps_completed + self.steps_failed
        return self.steps_completed / total if total > 0 else 0.0


class AutomationAnalyticsAction:
    """Tracks and analyzes automation execution metrics."""

    def __init__(self) -> None:
        self.metrics: List[ExecutionMetrics] = []
        self._active: Optional[ExecutionMetrics] = None

    def start_execution(self, execution_id: str, tags: Optional[Dict[str, str]] = None) -> None:
        self._active = ExecutionMetrics(
            execution_id=execution_id,
            start_time=time.time(),
            tags=tags or {},
        )

    def end_execution(self, error: Optional[str] = None) -> Optional[ExecutionMetrics]:
        if self._active:
            self._active.error_message = error
            self._active.finish()
            self.metrics.append(self._active)
            result = self._active
            self._active = None
            return result
        return None

    def record_step(self, success: bool = True) -> None:
        if self._active:
            if success:
                self._active.steps_completed += 1
            else:
                self._active.steps_failed += 1

    def get_summary(
        self,
        since: Optional[datetime] = None,
        tag_filter: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        since_ts = since.timestamp() if since else 0
        filtered = [
            m for m in self.metrics
            if m.start_time >= since_ts
            and (not tag_filter or all(m.tags.get(k) == v for k, v in tag_filter.items()))
        ]
        if not filtered:
            return {"count": 0, "success_rate": 0.0, "avg_duration_ms": 0.0}
        durations = [m.duration_ms for m in filtered if m.duration_ms > 0]
        return {
            "count": len(filtered),
            "success_rate": statistics.mean(m.success_rate() for m in filtered),
            "avg_duration_ms": statistics.mean(durations) if durations else 0,
            "min_duration_ms": min(durations) if durations else 0,
            "max_duration_ms": max(durations) if durations else 0,
            "total_steps": sum(m.steps_completed for m in filtered),
            "total_errors": sum(m.steps_failed for m in filtered),
        }

    def get_trend(self, window_hours: int = 24) -> List[Dict[str, Any]]:
        now = time.time()
        window_sec = window_hours * 3600
        buckets: Dict[int, List[ExecutionMetrics]] = {}
        for m in self.metrics:
            if now - m.start_time <= window_sec:
                bucket = int(m.start_time / 3600) * 3600
                buckets.setdefault(bucket, []).append(m)
        return [
            {
                "timestamp": ts,
                "count": len(ms),
                "success_rate": statistics.mean(x.success_rate() for x in ms) if ms else 0,
                "avg_duration_ms": statistics.mean(x.duration_ms for x in ms if x.duration_ms > 0) if ms else 0,
            }
            for ts, ms in sorted(buckets.items())
        ]
