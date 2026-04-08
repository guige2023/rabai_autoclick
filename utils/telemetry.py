"""
Telemetry Collector Utility

Collects and exports telemetry data from automation runs.
Tracks performance metrics, action counts, and success rates.

Example:
    >>> telemetry = TelemetryCollector()
    >>> telemetry.record_action("click", duration=0.05, success=True)
    >>> telemetry.record_metric("elements_found", 5)
    >>> telemetry.export_json("/tmp/telemetry.json")
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Optional


@dataclass
class ActionRecord:
    """A single action record."""
    action: str
    timestamp: float
    duration: float
    success: bool
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricRecord:
    """A single metric record."""
    name: str
    value: float
    timestamp: float
    unit: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunSummary:
    """Summary of an automation run."""
    run_id: str
    start_time: float
    end_time: float
    total_actions: int
    successful_actions: int
    failed_actions: int
    total_duration: float
    actions_per_minute: float


class TelemetryCollector:
    """
    Collects telemetry data during automation runs.

    Thread-safe for concurrent action recording.
    """

    def __init__(self, run_id: Optional[str] = None) -> None:
        self.run_id = run_id or f"run_{int(time.time())}"
        self._start_time = time.time()
        self._actions: list[ActionRecord] = []
        self._metrics: list[MetricRecord] = []
        self._lock = threading.Lock()
        self._custom_events: list[dict[str, Any]] = []

    def record_action(
        self,
        action: str,
        duration: float,
        success: bool,
        error: Optional[str] = None,
        **metadata: Any,
    ) -> None:
        """
        Record an action execution.

        Args:
            action: Action name/identifier.
            duration: Time taken in seconds.
            success: Whether action succeeded.
            error: Error message if failed.
            **metadata: Additional metadata.
        """
        with self._lock:
            self._actions.append(ActionRecord(
                action=action,
                timestamp=time.time(),
                duration=duration,
                success=success,
                error=error,
                metadata=metadata,
            ))

    def record_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
        **metadata: Any,
    ) -> None:
        """
        Record a metric value.

        Args:
            name: Metric name.
            value: Metric value.
            unit: Optional unit string.
            **metadata: Additional metadata.
        """
        with self._lock:
            self._metrics.append(MetricRecord(
                name=name,
                value=value,
                timestamp=time.time(),
                unit=unit,
                metadata=metadata,
            ))

    def record_event(
        self,
        event_name: str,
        **data: Any,
    ) -> None:
        """
        Record a custom event.

        Args:
            event_name: Event identifier.
            **data: Event data.
        """
        with self._lock:
            self._custom_events.append({
                "event": event_name,
                "timestamp": time.time(),
                "data": data,
            })

    def get_summary(self) -> RunSummary:
        """Generate a summary of the run."""
        with self._lock:
            total = len(self._actions)
            successful = sum(1 for a in self._actions if a.success)
            failed = total - successful
            end_time = time.time()
            duration = end_time - self._start_time
            actions_per_minute = (successful / duration * 60) if duration > 0 else 0

            return RunSummary(
                run_id=self.run_id,
                start_time=self._start_time,
                end_time=end_time,
                total_actions=total,
                successful_actions=successful,
                failed_actions=failed,
                total_duration=duration,
                actions_per_minute=actions_per_minute,
            )

    def export_json(self, path: str) -> None:
        """
        Export all telemetry data to JSON file.

        Args:
            path: Output file path.
        """
        with self._lock:
            summary = self.get_summary()
            data = {
                "run_id": self.run_id,
                "start_time": datetime.fromtimestamp(self._start_time).isoformat(),
                "end_time": datetime.fromtimestamp(summary.end_time).isoformat(),
                "summary": asdict(summary),
                "actions": [asdict(a) for a in self._actions],
                "metrics": [asdict(m) for m in self._metrics],
                "events": list(self._custom_events),
            }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def export_csv(self, actions_path: str, metrics_path: str) -> None:
        """
        Export telemetry data to CSV files.

        Args:
            actions_path: Path for actions CSV.
            metrics_path: Path for metrics CSV.
        """
        import csv

        with self._lock:
            # Actions CSV
            with open(actions_path, "w", newline="") as f:
                if self._actions:
                    writer = csv.DictWriter(f, fieldnames=ActionRecord.__dataclass_fields__.keys())
                    writer.writeheader()
                    for action in self._actions:
                        writer.writerow(asdict(action))

            # Metrics CSV
            with open(metrics_path, "w", newline="") as f:
                if self._metrics:
                    writer = csv.DictWriter(f, fieldnames=MetricRecord.__dataclass_fields__.keys())
                    writer.writeheader()
                    for metric in self._metrics:
                        writer.writerow(asdict(metric))

    def get_action_stats(self) -> dict[str, Any]:
        """Get statistics about recorded actions."""
        with self._lock:
            if not self._actions:
                return {}

            from collections import Counter
            action_counts = Counter(a.action for a in self._actions)
            durations = {}
            for action in action_counts:
                action_durations = [a.duration for a in self._actions if a.action == action]
                if action_durations:
                    durations[action] = {
                        "count": len(action_durations),
                        "avg": sum(action_durations) / len(action_durations),
                        "min": min(action_durations),
                        "max": max(action_durations),
                    }

            return {
                "total_actions": len(self._actions),
                "unique_actions": len(action_counts),
                "action_counts": dict(action_counts),
                "duration_stats": durations,
            }

    def clear(self) -> None:
        """Clear all recorded data."""
        with self._lock:
            self._actions.clear()
            self._metrics.clear()
            self._custom_events.clear()


class PerformanceTracker:
    """
    Tracks performance metrics for automation operations.

    Useful for identifying slow operations and bottlenecks.
    """

    def __init__(self) -> None:
        self._timers: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def start_operation(self, name: str) -> float:
        """Start timing an operation. Returns start timestamp."""
        return time.time()

    def end_operation(self, name: str, start_time: float) -> float:
        """
        End timing an operation.

        Args:
            name: Operation name.
            start_time: Start timestamp from start_operation.

        Returns:
            Duration in seconds.
        """
        duration = time.time() - start_time
        with self._lock:
            if name not in self._timers:
                self._timers[name] = []
            self._timers[name].append(duration)

            # Limit stored durations
            if len(self._timers[name]) > 1000:
                self._timers[name] = self._timers[name][-1000:]

        return duration

    def get_stats(self, name: str) -> Optional[dict[str, float]]:
        """Get statistics for an operation."""
        with self._lock:
            durations = self._timers.get(name)
            if not durations:
                return None

            sorted_d = sorted(durations)
            return {
                "count": len(durations),
                "avg": sum(durations) / len(durations),
                "min": min(durations),
                "max": max(durations),
                "p50": sorted_d[len(sorted_d) // 2],
                "p95": sorted_d[int(len(sorted_d) * 0.95)] if len(sorted_d) > 1 else sorted_d[0],
                "p99": sorted_d[int(len(sorted_d) * 0.99)] if len(sorted_d) > 1 else sorted_d[0],
            }

    def get_all_stats(self) -> dict[str, dict[str, float]]:
        """Get stats for all tracked operations."""
        with self._lock:
            return {name: self.get_stats(name) for name in self._timers}
