"""
Automation Process Mining Module.

Analyzes event logs to discover, validate, and improve
processes. Extracts process models, identifies bottlenecks,
and provides recommendations for automation.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ProcessState(Enum):
    """Process states."""
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class ProcessEvent:
    """Single event in a process trace."""
    case_id: str
    activity: str
    timestamp: float
    resource: Optional[str] = None
    duration_ms: float = 0.0
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessTrace:
    """Complete trace of a process case."""
    case_id: str
    events: list[ProcessEvent] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    state: ProcessState = ProcessState.ACTIVE
    total_duration_ms: float = 0.0


@dataclass
class ProcessMetrics:
    """Process performance metrics."""
    case_count: int = 0
    completed_count: int = 0
    failed_count: int = 0
    avg_duration_ms: float = 0.0
    min_duration_ms: float = 0.0
    max_duration_ms: float = 0.0
    throughput_per_hour: float = 0.0


@dataclass
class ActivityMetrics:
    """Metrics for a single activity."""
    activity: str
    count: int = 0
    avg_duration_ms: float = 0.0
    min_duration_ms: float = 0.0
    max_duration_ms: float = 0.0
    resources: set[str] = field(default_factory=set)
    successors: dict[str, int] = field(default_factory=dict)


@dataclass
class Bottleneck:
    """Identified process bottleneck."""
    activity: str
    avg_wait_time_ms: float
    cases_affected: int
    percentage_of_cases: float
    recommendation: str


class ProcessMiner:
    """
    Process mining and analysis.

    Analyzes event logs to discover process models,
    calculate metrics, and identify bottlenecks.

    Example:
        miner = ProcessMiner()
        miner.load_events(events)
        model = miner.discover_process()
        bottlenecks = miner.find_bottlenecks()
    """

    def __init__(self) -> None:
        self._traces: dict[str, ProcessTrace] = {}
        self._activities: dict[str, ActivityMetrics] = {}
        self._sequences: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._start_activities: dict[str, int] = defaultdict(int)
        self._end_activities: dict[str, int] = defaultdict(int)

    def load_events(self, events: list[ProcessEvent]) -> None:
        """
        Load events for process mining.

        Args:
            events: List of process events
        """
        for event in events:
            self._add_event(event)

    def _add_event(self, event: ProcessEvent) -> None:
        """Add a single event to the mining data."""
        if event.case_id not in self._traces:
            self._traces[event.case_id] = ProcessTrace(
                case_id=event.case_id,
                start_time=event.timestamp,
                state=ProcessState.ACTIVE
            )
            self._start_activities[event.activity] += 1

        trace = self._traces[event.case_id]
        trace.events.append(event)

        if len(trace.events) > 1:
            prev = trace.events[-2]
            self._sequences[prev.activity][event.activity] += 1

        self._update_activity_metrics(event)

    def _update_activity_metrics(self, event: ProcessEvent) -> None:
        """Update activity-level metrics."""
        if event.activity not in self._activities:
            self._activities[event.activity] = ActivityMetrics(
                activity=event.activity
            )

        metrics = self._activities[event.activity]
        metrics.count += 1

        if event.duration_ms > 0:
            if metrics.avg_duration_ms == 0:
                metrics.avg_duration_ms = event.duration_ms
            else:
                metrics.avg_duration_ms = (
                    metrics.avg_duration_ms * (metrics.count - 1) + event.duration_ms
                ) / metrics.count

        metrics.min_duration_ms = (
            min(metrics.min_duration_ms, event.duration_ms)
            if metrics.min_duration_ms > 0 else event.duration_ms
        )
        metrics.max_duration_ms = max(metrics.max_duration_ms, event.duration_ms)

        if event.resource:
            metrics.resources.add(event.resource)

    def discover_process(self) -> dict[str, Any]:
        """
        Discover process model from event logs.

        Returns:
            Process model with activities and transitions
        """
        process_flow: dict[str, list[str]] = {}

        for from_activity, transitions in self._sequences.items():
            process_flow[from_activity] = []
            for to_activity in sorted(transitions.keys(), key=lambda x: transitions[x], reverse=True):
                count = transitions[to_activity]
                process_flow[from_activity].append(f"{to_activity}({count})")

        return {
            "activities": list(self._activities.keys()),
            "flow": dict(process_flow),
            "start_activities": dict(self._start_activities),
            "end_activities": dict(self._end_activities),
            "total_cases": len(self._traces)
        }

    def calculate_metrics(self) -> ProcessMetrics:
        """Calculate overall process metrics."""
        completed = [t for t in self._traces.values() if t.state == ProcessState.COMPLETED]

        if not self._traces:
            return ProcessMetrics()

        durations = [t.total_duration_ms for t in self._traces.values() if t.total_duration_ms > 0]
        durations.sort()

        start_time = min(t.start_time for t in self._traces.values())
        end_time = max((t.end_time for t in self._traces.values()), default=time.time())
        time_span_hours = max((end_time - start_time) / 3600, 1)

        return ProcessMetrics(
            case_count=len(self._traces),
            completed_count=len([t for t in self._traces.values() if t.state == ProcessState.COMPLETED]),
            failed_count=len([t for t in self._traces.values() if t.state == ProcessState.FAILED]),
            avg_duration_ms=sum(durations) / len(durations) if durations else 0,
            min_duration_ms=durations[0] if durations else 0,
            max_duration_ms=durations[-1] if durations else 0,
            throughput_per_hour=len(self._traces) / time_span_hours
        )

    def get_activity_metrics(self, activity: str) -> Optional[ActivityMetrics]:
        """Get metrics for a specific activity."""
        return self._activities.get(activity)

    def get_activity_durations(self, activity: str) -> dict[str, float]:
        """Get duration stats for an activity."""
        metrics = self._activities.get(activity)
        if not metrics:
            return {}
        return {
            "avg_ms": metrics.avg_duration_ms,
            "min_ms": metrics.min_duration_ms,
            "max_ms": metrics.max_duration_ms
        }

    def find_bottlenecks(
        self,
        wait_time_threshold_ms: float = 60000,
        affected_threshold_pct: float = 0.1
    ) -> list[Bottleneck]:
        """
        Identify process bottlenecks.

        Args:
            wait_time_threshold_ms: Minimum wait time to consider bottleneck
            affected_threshold_pct: Minimum percentage of cases affected

        Returns:
            List of identified bottlenecks with recommendations
        """
        bottlenecks: list[Bottleneck] = []
        total_cases = len(self._traces)

        for activity, metrics in self._activities.items():
            if metrics.count < 5:
                continue

            avg_wait = metrics.avg_duration_ms / metrics.count if metrics.count > 0 else 0

            if avg_wait < wait_time_threshold_ms:
                continue

            cases_affected = sum(
                1 for t in self._traces.values()
                if any(e.activity == activity for e in t.events)
            )
            pct_cases = cases_affected / total_cases if total_cases > 0 else 0

            if pct_cases < affected_threshold_pct:
                continue

            recommendations = []
            if metrics.resources:
                recommendations.append("Consider adding resources to this activity")
            if metrics.max_duration_ms > metrics.avg_duration_ms * 2:
                recommendations.append("High variance detected - investigate edge cases")
            recommendations.append("Evaluate for automation or parallel processing")

            bottlenecks.append(Bottleneck(
                activity=activity,
                avg_wait_time_ms=avg_wait,
                cases_affected=cases_affected,
                percentage_of_cases=pct_cases,
                recommendation="; ".join(recommendations)
            ))

        return sorted(bottlenecks, key=lambda b: b.avg_wait_time_ms, reverse=True)

    def get_case_duration_distribution(self) -> dict[str, int]:
        """Get distribution of case durations."""
        buckets = {
            "< 1min": 0,
            "1-5min": 0,
            "5-15min": 0,
            "15-60min": 0,
            "1-4hr": 0,
            "4-24hr": 0,
            "> 24hr": 0
        }

        for trace in self._traces.values():
            d_ms = trace.total_duration_ms
            d_min = d_ms / 60000

            if d_min < 1:
                buckets["< 1min"] += 1
            elif d_min < 5:
                buckets["1-5min"] += 1
            elif d_min < 15:
                buckets["5-15min"] += 1
            elif d_min < 60:
                buckets["15-60min"] += 1
            elif d_min < 240:
                buckets["1-4hr"] += 1
            elif d_min < 1440:
                buckets["4-24hr"] += 1
            else:
                buckets["> 24hr"] += 1

        return buckets

    def get_activity_frequencies(self) -> dict[str, int]:
        """Get frequency of each activity."""
        return {k: v.count for k, v in self._activities.items()}

    def get_common_sequences(self, min_occurrences: int = 5) -> list[tuple[str, str, int]]:
        """Get common activity sequences."""
        sequences = []
        for from_activity, transitions in self._sequences.items():
            for to_activity, count in transitions.items():
                if count >= min_occurrences:
                    sequences.append((from_activity, to_activity, count))
        return sorted(sequences, key=lambda x: x[2], reverse=True)
