"""Analytics Collector Action Module.

Collect and aggregate analytics events with dimensional analysis.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .aggregation_engine_action import AggregationEngine, AggregationConfig, AggregationType


@dataclass
class AnalyticsEvent:
    """Analytics event."""
    event_name: str
    timestamp: float
    dimensions: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    user_id: str | None = None
    session_id: str | None = None


@dataclass
class AnalyticsSummary:
    """Analytics summary for a time period."""
    period_start: float
    period_end: float
    total_events: int
    unique_users: int
    unique_sessions: int
    metrics_summary: dict[str, float]
    top_dimensions: dict[str, dict[str, int]]


class AnalyticsCollector:
    """Collect and aggregate analytics events."""

    def __init__(self) -> None:
        self._events: list[AnalyticsEvent] = []
        self._user_sessions: dict[str, set] = defaultdict(set)
        self._dimension_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._metric_sums: dict[str, float] = defaultdict(float)
        self._metric_counts: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self._flush_interval = 60.0
        self._max_events = 10000

    async def track(
        self,
        event_name: str,
        dimensions: dict[str, str] | None = None,
        metrics: dict[str, float] | None = None,
        user_id: str | None = None,
        session_id: str | None = None
    ) -> AnalyticsEvent:
        """Track an analytics event."""
        event = AnalyticsEvent(
            event_name=event_name,
            timestamp=time.time(),
            dimensions=dimensions or {},
            metrics=metrics or {},
            user_id=user_id,
            session_id=session_id
        )
        async with self._lock:
            self._events.append(event)
            if user_id:
                self._user_sessions["users"].add(user_id)
            if session_id:
                self._user_sessions["sessions"].add(session_id)
            for dim_name, dim_value in (dimensions or {}).items():
                self._dimension_counts[dim_name][dim_value] += 1
            for metric_name, metric_value in (metrics or {}).items():
                self._metric_sums[metric_name] += metric_value
                self._metric_counts[metric_name] += 1
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events // 2:]
        return event

    async def get_summary(self, period_start: float | None = None, period_end: float | None = None) -> AnalyticsSummary:
        """Get analytics summary for a period."""
        async with self._lock:
            events = self._events
            if period_start:
                events = [e for e in events if e.timestamp >= period_start]
            if period_end:
                events = [e for e in events if e.timestamp <= period_end]
            users = {e.user_id for e in events if e.user_id}
            sessions = {e.session_id for e in events if e.session_id}
            metrics_summary = {}
            for name, total in self._metric_sums.items():
                count = self._metric_counts[name]
                metrics_summary[f"{name}_total"] = total
                metrics_summary[f"{name}_avg"] = total / max(count, 1)
            top_dims = {}
            for dim_name, counts in self._dimension_counts.items():
                sorted_items = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
                top_dims[dim_name] = dict(sorted_items)
            return AnalyticsSummary(
                period_start=period_start or 0,
                period_end=period_end or time.time(),
                total_events=len(events),
                unique_users=len(users),
                unique_sessions=len(sessions),
                metrics_summary=metrics_summary,
                top_dimensions=top_dims
            )

    async def aggregate(
        self,
        group_by: list[str],
        metric: str,
        agg_type: AggregationType = AggregationType.SUM
    ) -> list[dict]:
        """Aggregate metrics by dimensions."""
        async with self._lock:
            groups: dict[tuple, list[float]] = defaultdict(list)
            for event in self._events:
                key = tuple(event.dimensions.get(d) for d in group_by)
                if metric in event.metrics:
                    groups[key].append(event.metrics[metric])
            engine = AggregationEngine()
            config = AggregationConfig(field=metric, aggregation_type=agg_type)
            results = []
            for key, values in groups.items():
                group_data = [{metric: v} for v in values]
                agg_result = engine._compute_aggregation(values, agg_type)
                result = dict(zip(group_by, key))
                result[metric] = agg_result
                results.append(result)
            return sorted(results, key=lambda x: x.get(metric, 0), reverse=True)

    async def clear(self) -> None:
        """Clear all collected events."""
        async with self._lock:
            self._events.clear()
            self._user_sessions.clear()
            self._dimension_counts.clear()
            self._metric_sums.clear()
            self._metric_counts.clear()
