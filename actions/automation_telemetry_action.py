"""Automation Telemetry Action.

Collects and reports telemetry data for automation workflows including
execution traces, performance metrics, and resource usage.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class TelemetryLevel(Enum):
    """Telemetry verbosity levels."""
    MINIMAL = "minimal"
    STANDARD = "standard"
    VERBOSE = "verbose"
    DEBUG = "debug"


class EventType(Enum):
    """Types of telemetry events."""
    WORKFLOW_START = "workflow_start"
    WORKFLOW_COMPLETE = "workflow_complete"
    WORKFLOW_FAIL = "workflow_fail"
    STEP_START = "step_start"
    STEP_COMPLETE = "step_complete"
    STEP_FAIL = "step_fail"
    METRIC_SAMPLE = "metric_sample"
    RESOURCE_USAGE = "resource_usage"


@dataclass
class TelemetryEvent:
    """A single telemetry event."""
    timestamp: float
    event_type: EventType
    workflow_id: str
    step_name: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    duration_ms: Optional[float] = None
    level: TelemetryLevel = TelemetryLevel.STANDARD


@dataclass
class TelemetrySpan:
    """A timing span for measuring duration."""
    name: str
    workflow_id: str
    start_time: float
    end_time: Optional[float] = None
    tags: Dict[str, str] = field(default_factory=dict)
    annotations: List[str] = field(default_factory=list)

    def finish(self) -> None:
        """Mark the span as finished."""
        self.end_time = time.time()

    @property
    def duration_ms(self) -> float:
        """Get span duration in milliseconds."""
        if self.end_time is None:
            return (time.time() - self.start_time) * 1000
        return (self.end_time - self.start_time) * 1000


@dataclass
class WorkflowTelemetry:
    """Aggregated telemetry for a workflow."""
    workflow_id: str
    workflow_name: str
    started_at: float
    completed_at: Optional[float] = None
    status: str = "running"
    total_steps: int = 0
    completed_steps: int = 0
    failed_steps: int = 0
    total_duration_ms: float = 0.0
    spans: List[TelemetrySpan] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)


class AutomationTelemetryAction:
    """Collects and manages automation workflow telemetry."""

    def __init__(self, level: TelemetryLevel = TelemetryLevel.STANDARD) -> None:
        self.level = level
        self._events: List[TelemetryEvent] = []
        self._active_spans: Dict[str, TelemetrySpan] = {}
        self._workflows: Dict[str, WorkflowTelemetry] = {}
        self._metrics: Dict[str, List[float]] = defaultdict(list)
        self._max_events = 10000

    def record_event(
        self,
        event_type: EventType,
        workflow_id: str,
        step_name: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
    ) -> None:
        """Record a telemetry event."""
        if self.level == TelemetryLevel.MINIMAL and event_type not in [
            EventType.WORKFLOW_COMPLETE,
            EventType.WORKFLOW_FAIL,
        ]:
            return

        event = TelemetryEvent(
            timestamp=time.time(),
            event_type=event_type,
            workflow_id=workflow_id,
            step_name=step_name,
            data=data or {},
            duration_ms=duration_ms,
            level=self.level,
        )
        self._events.append(event)

        if len(self._events) > self._max_events:
            self._events = self._events[-self._max_events // 2:]

        self._update_workflow_status(event)

    def start_span(
        self,
        name: str,
        workflow_id: str,
        tags: Optional[Dict[str, str]] = None,
    ) -> TelemetrySpan:
        """Start a new timing span."""
        span = TelemetrySpan(
            name=name,
            workflow_id=workflow_id,
            start_time=time.time(),
            tags=tags or {},
        )
        self._active_spans[f"{workflow_id}:{name}"] = span
        return span

    def end_span(self, span: TelemetrySpan) -> float:
        """End a timing span and return duration."""
        span.finish()
        key = f"{span.workflow_id}:{span.name}"
        if key in self._active_spans:
            del self._active_spans[key]
        return span.duration_ms

    def record_metric(self, metric_name: str, value: float) -> None:
        """Record a numeric metric value."""
        self._metrics[metric_name].append(value)
        if len(self._metrics[metric_name]) > 1000:
            self._metrics[metric_name] = self._metrics[metric_name][-500:]

    def start_workflow(
        self,
        workflow_id: str,
        workflow_name: str,
        total_steps: int,
    ) -> None:
        """Mark the start of a workflow."""
        self._workflows[workflow_id] = WorkflowTelemetry(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            started_at=time.time(),
            total_steps=total_steps,
        )
        self.record_event(
            EventType.WORKFLOW_START,
            workflow_id,
            data={"total_steps": total_steps, "name": workflow_name},
        )

    def complete_workflow(
        self,
        workflow_id: str,
        status: str = "success",
    ) -> None:
        """Mark a workflow as complete."""
        if workflow_id in self._workflows:
            wf = self._workflows[workflow_id]
            wf.completed_at = time.time()
            wf.status = status
            wf.total_duration_ms = (wf.completed_at - wf.started_at) * 1000
        self.record_event(
            EventType.WORKFLOW_COMPLETE,
            workflow_id,
            data={"status": status},
            duration_ms=wf.total_duration_ms if workflow_id in self._workflows else None,
        )

    def _update_workflow_status(self, event: TelemetryEvent) -> None:
        """Update workflow status based on events."""
        if event.workflow_id not in self._workflows:
            return
        wf = self._workflows[event.workflow_id]

        if event.event_type == EventType.STEP_COMPLETE:
            wf.completed_steps += 1
        elif event.event_type == EventType.STEP_FAIL:
            wf.failed_steps += 1

    def get_workflow_telemetry(self, workflow_id: str) -> Optional[WorkflowTelemetry]:
        """Get telemetry for a specific workflow."""
        return self._workflows.get(workflow_id)

    def get_metric_stats(self, metric_name: str) -> Dict[str, float]:
        """Get statistics for a recorded metric."""
        values = self._metrics.get(metric_name, [])
        if not values:
            return {}
        sorted_values = sorted(values)
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "p50": sorted_values[len(values) // 2],
            "p95": sorted_values[int(len(values) * 0.95)],
            "p99": sorted_values[int(len(values) * 0.99)],
        }

    def get_all_metrics_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all recorded metrics."""
        return {name: self.get_metric_stats(name) for name in self._metrics}

    def export_events(self) -> List[Dict[str, Any]]:
        """Export all events as serializable dicts."""
        return [
            {
                "timestamp": e.timestamp,
                "event_type": e.event_type.value,
                "workflow_id": e.workflow_id,
                "step_name": e.step_name,
                "data": e.data,
                "duration_ms": e.duration_ms,
            }
            for e in self._events
        ]

    def clear(self) -> None:
        """Clear all telemetry data."""
        self._events.clear()
        self._active_spans.clear()
        self._workflows.clear()
        self._metrics.clear()
