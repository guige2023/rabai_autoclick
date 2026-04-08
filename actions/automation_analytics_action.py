"""
Automation Analytics Action Module.

Collects and analyzes automation workflow metrics, performance
 patterns, and provides actionable insights.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import statistics
import logging

logger = logging.getLogger(__name__)


class MetricCategory(Enum):
    """Category of analytics metric."""
    PERFORMANCE = "performance"
    RELIABILITY = "reliability"
    THROUGHPUT = "throughput"
    ERROR = "error"


@dataclass
class AnalyticsMetric:
    """A single analytics metric."""
    name: str
    category: MetricCategory
    value: float
    timestamp: float = field(default_factory=time.time)
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class PerformanceInsight:
    """An actionable insight from analytics."""
    title: str
    description: str
    metric_name: str
    current_value: float
    threshold: float
    recommendation: str
    priority: str = "medium"


@dataclass
class AnalyticsSummary:
    """Summary of analytics data."""
    total_executions: int = 0
    success_rate: float = 0.0
    avg_duration_ms: float = 0.0
    p95_duration_ms: float = 0.0
    error_rate: float = 0.0
    throughput_per_minute: float = 0.0
    insights: list[PerformanceInsight] = field(default_factory=list)


class AutomationAnalyticsAction:
    """
    Analytics and insights engine for automation workflows.

    Collects execution metrics, analyzes patterns, and generates
    actionable insights for optimization.

    Example:
        analytics = AutomationAnalyticsAction()
        analytics.record_execution("scrape", success=True, duration_ms=1500)
        analytics.record_execution("scrape", success=False, error="timeout")
        summary = analytics.get_summary()
    """

    def __init__(
        self,
        window_size: int = 1000,
        insight_threshold: float = 0.8,
    ) -> None:
        self.window_size = window_size
        self.insight_threshold = insight_threshold
        self._metrics: dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self._executions: deque = deque(maxlen=window_size)
        self._error_patterns: dict[str, int] = defaultdict(int)
        self._insight_rules: list[Callable[[AnalyticsSummary], list[PerformanceInsight]]] = []

    def record_execution(
        self,
        workflow_name: str,
        success: bool,
        duration_ms: Optional[float] = None,
        error: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Record a workflow execution."""
        now = time.time()
        execution = {
            "workflow": workflow_name,
            "success": success,
            "duration_ms": duration_ms,
            "error": error,
            "timestamp": now,
            "metadata": metadata or {},
        }
        self._executions.append(execution)

        self._metrics[f"{workflow_name}.executions"].append(
            AnalyticsMetric(
                name=f"{workflow_name}.executions",
                category=MetricCategory.THROUGHPUT,
                value=1,
                timestamp=now,
            )
        )

        if success:
            self._metrics[f"{workflow_name}.success"].append(
                AnalyticsMetric(
                    name=f"{workflow_name}.success",
                    category=MetricCategory.RELIABILITY,
                    value=1,
                    timestamp=now,
                )
            )
        else:
            self._metrics[f"{workflow_name}.failure"].append(
                AnalyticsMetric(
                    name=f"{workflow_name}.failure",
                    category=MetricCategory.ERROR,
                    value=1,
                    timestamp=now,
                )
            )
            if error:
                self._error_patterns[error] += 1

        if duration_ms is not None:
            self._metrics[f"{workflow_name}.duration"].append(
                AnalyticsMetric(
                    name=f"{workflow_name}.duration",
                    category=MetricCategory.PERFORMANCE,
                    value=duration_ms,
                    timestamp=now,
                )
            )

    def record_metric(
        self,
        name: str,
        value: float,
        category: MetricCategory = MetricCategory.PERFORMANCE,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a custom metric."""
        metric = AnalyticsMetric(
            name=name,
            category=category,
            value=value,
            tags=tags or {},
        )
        self._metrics[name].append(metric)

    def get_workflow_stats(
        self,
        workflow_name: str,
        window_seconds: Optional[float] = None,
    ) -> dict[str, Any]:
        """Get statistics for a specific workflow."""
        now = time.time()
        window = window_seconds or 300

        executions = [
            e for e in self._executions
            if e["workflow"] == workflow_name
            and (now - e["timestamp"]) <= window
        ]

        if not executions:
            return {}

        durations = [e["duration_ms"] for e in executions if e["duration_ms"]]
        successes = sum(1 for e in executions if e["success"])
        failures = sum(1 for e in executions if not e["success"])

        stats: dict[str, Any] = {
            "total_executions": len(executions),
            "success_count": successes,
            "failure_count": failures,
            "success_rate": successes / len(executions) if executions else 0,
            "avg_duration_ms": statistics.mean(durations) if durations else 0,
            "min_duration_ms": min(durations) if durations else 0,
            "max_duration_ms": max(durations) if durations else 0,
        }

        if len(durations) >= 10:
            sorted_durations = sorted(durations)
            stats["p50_duration_ms"] = sorted_durations[int(len(durations) * 0.5)]
            stats["p95_duration_ms"] = sorted_durations[int(len(durations) * 0.95)]
            stats["p99_duration_ms"] = sorted_durations[int(len(durations) * 0.99)]

        return stats

    def get_summary(
        self,
        window_seconds: float = 300.0,
    ) -> AnalyticsSummary:
        """Get overall analytics summary."""
        now = time.time()
        window = window_seconds

        recent = [
            e for e in self._executions
            if (now - e["timestamp"]) <= window
        ]

        if not recent:
            return AnalyticsSummary()

        successes = sum(1 for e in recent if e["success"])
        durations = [e["duration_ms"] for e in recent if e["duration_ms"]]

        summary = AnalyticsSummary(
            total_executions=len(recent),
            success_rate=successes / len(recent) if recent else 0,
            avg_duration_ms=statistics.mean(durations) if durations else 0,
            error_rate=1 - (successes / len(recent)) if recent else 0,
            throughput_per_minute=len(recent) / (window / 60.0) if window > 0 else 0,
        )

        if len(durations) >= 10:
            sorted_durations = sorted(durations)
            summary.p95_duration_ms = sorted_durations[int(len(durations) * 0.95)]

        summary.insights = self._generate_insights(summary)

        return summary

    def _generate_insights(
        self,
        summary: AnalyticsSummary,
    ) -> list[PerformanceInsight]:
        """Generate actionable insights from analytics."""
        insights: list[PerformanceInsight] = []

        if summary.success_rate < 0.95:
            insights.append(PerformanceInsight(
                title="Low Success Rate",
                description=f"Success rate is {summary.success_rate:.1%}, below 95% threshold",
                metric_name="success_rate",
                current_value=summary.success_rate,
                threshold=0.95,
                recommendation="Investigate failure patterns and add retry logic",
                priority="high" if summary.success_rate < 0.8 else "medium",
            ))

        if summary.avg_duration_ms > 5000:
            insights.append(PerformanceInsight(
                title="High Average Duration",
                description=f"Average execution time is {summary.avg_duration_ms:.0f}ms",
                metric_name="avg_duration_ms",
                current_value=summary.avg_duration_ms,
                threshold=5000,
                recommendation="Consider optimizing workflow or adding parallelization",
                priority="medium",
            ))

        if self._error_patterns:
            top_error = max(self._error_patterns.items(), key=lambda x: x[1])
            insights.append(PerformanceInsight(
                title=f"Common Error: {top_error[0][:50]}",
                description=f"Error '{top_error[0][:50]}...' occurred {top_error[1]} times",
                metric_name="error_patterns",
                current_value=top_error[1],
                threshold=10,
                recommendation="Address this error pattern to improve reliability",
                priority="high",
            ))

        return insights

    def get_top_errors(self, limit: int = 10) -> list[tuple[str, int]]:
        """Get the most common errors."""
        sorted_errors = sorted(
            self._error_patterns.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        return sorted_errors[:limit]

    def export_metrics(
        self,
        format: str = "json",
    ) -> str:
        """Export metrics in specified format."""
        if format == "json":
            import json
            data = {
                name: [
                    {"value": m.value, "timestamp": m.timestamp, "tags": m.tags}
                    for m in metrics
                ]
                for name, metrics in self._metrics.items()
            }
            return json.dumps(data, indent=2)
        return str(self._metrics)
