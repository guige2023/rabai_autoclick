"""
Automation Profiler Action Module.

Profiles and measures automation workflow performance, tracking execution
times, resource usage, and identifying optimization opportunities.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from contextlib import contextmanager
import time
import logging
import statistics

logger = logging.getLogger(__name__)


class ProfilerLevel(Enum):
    """Granularity levels for profiling."""
    MINIMAL = auto()
    STANDARD = auto()
    DETAILED = auto()
    TRACE = auto()


@dataclass
class ProfilerMetrics:
    """Metrics collected during profiling."""
    total_calls: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0
    mean_duration_ms: float = 0.0
    median_duration_ms: float = 0.0
    stddev_ms: float = 0.0
    error_count: int = 0
    success_count: int = 0

    def update(self, duration_ms: float, success: bool = True) -> None:
        """Update metrics with a new measurement."""
        self.total_calls += 1
        self.total_duration_ms += duration_ms
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)

        if success:
            self.success_count += 1
        else:
            self.error_count += 1

        durations = []
        if self.total_calls > 0:
            self.mean_duration_ms = self.total_duration_ms / self.total_calls

    def calculate_statistics(self, durations: List[float]) -> None:
        """Calculate detailed statistics."""
        if not durations:
            return

        self.min_duration_ms = min(durations)
        self.max_duration_ms = max(durations)
        self.mean_duration_ms = statistics.mean(durations)
        self.median_duration_ms = statistics.median(durations)

        if len(durations) > 1:
            self.stddev_ms = statistics.stdev(durations)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_calls": self.total_calls,
            "total_duration_ms": self.total_duration_ms,
            "min_duration_ms": self.min_duration_ms if self.total_calls else 0,
            "max_duration_ms": self.max_duration_ms if self.total_calls else 0,
            "mean_duration_ms": self.mean_duration_ms,
            "median_duration_ms": self.median_duration_ms,
            "stddev_ms": self.stddev_ms,
            "error_count": self.error_count,
            "success_count": self.success_count,
            "success_rate": (
                self.success_count / self.total_calls * 100
                if self.total_calls > 0 else 0
            ),
        }


@dataclass
class ProfileSession:
    """A profiling session for an automation."""
    name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    metrics: ProfilerMetrics = field(default_factory=ProfilerMetrics)
    events: List[Dict[str, Any]] = field(default_factory=list)
    checkpoints: Dict[str, float] = field(default_factory=dict)

    def duration_ms(self) -> float:
        """Get session duration in milliseconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return (datetime.now(timezone.utc) - self.start_time).total_seconds() * 1000


@dataclass
class ProfileResult:
    """Final result of a profiling operation."""
    session: ProfileSession
    comparisons: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "session_name": self.session.name,
            "duration_ms": self.session.duration_ms(),
            "metrics": self.session.metrics.to_dict(),
            "event_count": len(self.session.events),
            "comparisons": self.comparisons,
            "recommendations": self.recommendations,
        }


class AutomationProfilerAction:
    """
    Profiles automation workflows to measure performance and identify bottlenecks.

    This action wraps automation execution with profiling instrumentation,
    collecting detailed metrics about execution times, resource usage,
    and performance characteristics.

    Example:
        >>> profiler = AutomationProfilerAction()
        >>> with profiler.profile("data_processing"):
        ...     run_automation()
        >>> result = profiler.get_result("data_processing")
        >>> print(result.session.metrics.mean_duration_ms)
        45.2
    """

    def __init__(self, level: ProfilerLevel = ProfilerLevel.STANDARD):
        """
        Initialize the Automation Profiler.

        Args:
            level: Profiling granularity level.
        """
        self.level = level
        self._sessions: Dict[str, ProfileSession] = {}
        self._active_sessions: Dict[str, float] = {}
        self._durations: Dict[str, List[float]] = {}
        self._current_session: Optional[str] = None

    @contextmanager
    def profile(self, name: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for profiling a block of code.

        Args:
            name: Name for this profiling session.
            metadata: Optional metadata to attach.

        Yields:
            ProfileSession for this profiling session.
        """
        session = ProfileSession(
            name=name,
            start_time=datetime.now(timezone.utc),
        )

        if metadata:
            session.events.append({
                "type": "metadata",
                "data": metadata,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        self._current_session = name
        start = time.perf_counter()

        try:
            yield session
        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            session.metrics.update(duration, success=False)
            session.events.append({
                "type": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            raise
        finally:
            duration = (time.perf_counter() - start) * 1000
            session.end_time = datetime.now(timezone.utc)
            session.metrics.update(duration, success=True)
            self._sessions[name] = session
            self._current_session = None

            if name not in self._durations:
                self._durations[name] = []
            self._durations[name].append(duration)

    def checkpoint(self, name: str, label: str) -> None:
        """
        Record a checkpoint within a profiling session.

        Args:
            name: Session name.
            label: Checkpoint label.
        """
        if name not in self._sessions:
            return

        elapsed = self._sessions[name].duration_ms()
        self._sessions[name].checkpoints[label] = elapsed

        if self.level == ProfilerLevel.DETAILED or self.level == ProfilerLevel.TRACE:
            self._sessions[name].events.append({
                "type": "checkpoint",
                "label": label,
                "elapsed_ms": elapsed,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    def record_event(
        self,
        name: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a custom event during profiling.

        Args:
            name: Session name.
            event_type: Type of event.
            data: Event data.
        """
        if name not in self._sessions:
            return

        self._sessions[name].events.append({
            "type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def start_session(self, name: str) -> None:
        """
        Manually start a profiling session.

        Args:
            name: Session name.
        """
        self._active_sessions[name] = time.perf_counter()
        self._sessions[name] = ProfileSession(
            name=name,
            start_time=datetime.now(timezone.utc),
        )

    def end_session(self, name: str, success: bool = True) -> None:
        """
        Manually end a profiling session.

        Args:
            name: Session name.
            success: Whether execution was successful.
        """
        if name not in self._active_sessions:
            return

        duration = (time.perf_counter() - self._active_sessions[name]) * 1000
        session = self._sessions[name]
        session.end_time = datetime.now(timezone.utc)
        session.metrics.update(duration, success=success)

        if name not in self._durations:
            self._durations[name] = []
        self._durations[name].append(duration)

        del self._active_sessions[name]

    def get_result(self, name: str) -> Optional[ProfileResult]:
        """
        Get profiling result for a session.

        Args:
            name: Session name.

        Returns:
            ProfileResult or None if session not found.
        """
        if name not in self._sessions:
            return None

        session = self._sessions[name]
        session.metrics.calculate_statistics(self._durations.get(name, []))

        return ProfileResult(
            session=session,
            comparisons=self._compare_with_baseline(name),
            recommendations=self._generate_recommendations(name),
        )

    def _compare_with_baseline(self, name: str) -> Dict[str, Any]:
        """Compare session metrics with baseline."""
        if name not in self._durations or len(self._durations[name]) < 2:
            return {}

        durations = self._durations[name]
        recent = durations[-5:] if len(durations) >= 5 else durations[-2:]
        baseline = durations[:5] if len(durations) >= 5 else durations[:-2] if len(durations) > 2 else durations

        if not baseline:
            return {}

        recent_mean = statistics.mean(recent)
        baseline_mean = statistics.mean(baseline)
        change_pct = ((recent_mean - baseline_mean) / baseline_mean * 100) if baseline_mean else 0

        return {
            "recent_mean_ms": recent_mean,
            "baseline_mean_ms": baseline_mean,
            "change_percent": change_pct,
            "trend": "improving" if change_pct < -5 else "degrading" if change_pct > 5 else "stable",
        }

    def _generate_recommendations(self, name: str) -> List[str]:
        """Generate optimization recommendations."""
        recommendations = []

        if name not in self._sessions:
            return recommendations

        session = self._sessions[name]
        metrics = session.metrics

        if metrics.success_rate < 95:
            recommendations.append(
                f"Error rate is {100 - metrics.success_rate:.1f}%. "
                "Consider adding error handling and retry logic."
            )

        if metrics.stddev_ms > metrics.mean_duration_ms * 0.3:
            recommendations.append(
                "High variability detected. Execution time is inconsistent. "
                "Consider caching or pre-computation."
            )

        if metrics.max_duration_ms > metrics.mean_duration_ms * 5:
            recommendations.append(
                "Outliers detected. Some executions take 5x longer than average. "
                "Investigate worst-case scenarios."
            )

        if session.checkpoints:
            checkpoints = list(session.checkpoints.items())
            for i in range(1, len(checkpoints)):
                delta = checkpoints[i][1] - checkpoints[i-1][1]
                if delta > metrics.mean_duration_ms * 0.2:
                    recommendations.append(
                        f"Checkpoint '{checkpoints[i][0]}' shows significant delay. "
                        "This may indicate a bottleneck."
                    )

        return recommendations

    def get_all_results(self) -> List[ProfileResult]:
        """Get results for all profiling sessions."""
        return [
            self.get_result(name)
            for name in self._sessions
            if self.get_result(name) is not None
        ]

    def clear_session(self, name: str) -> None:
        """Clear a profiling session."""
        self._sessions.pop(name, None)
        self._durations.pop(name, None)
        self._active_sessions.pop(name, None)

    def clear_all(self) -> None:
        """Clear all profiling sessions."""
        self._sessions.clear()
        self._durations.clear()
        self._active_sessions.clear()

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all profiling sessions."""
        return {
            "session_count": len(self._sessions),
            "active_sessions": len(self._active_sessions),
            "sessions": {
                name: {
                    "duration_ms": session.duration_ms(),
                    "metrics": session.metrics.to_dict(),
                    "event_count": len(session.events),
                }
                for name, session in self._sessions.items()
            },
        }


def create_profiler_action(level: ProfilerLevel = ProfilerLevel.STANDARD) -> AutomationProfilerAction:
    """Factory function to create an AutomationProfilerAction."""
    return AutomationProfilerAction(level=level)
