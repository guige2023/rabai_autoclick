"""
Accessibility Reliability Utilities

Measure and score the reliability of accessibility queries
over time, tracking query success rates and element availability.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class QueryReliabilityScore:
    """Reliability score for accessibility queries."""
    query_id: str
    success_rate: float  # 0.0 to 1.0
    avg_latency_ms: float
    total_attempts: int
    is_reliable: bool


@dataclass
class QueryAttempt:
    """A single accessibility query attempt."""
    query_id: str
    success: bool
    latency_ms: float
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000)


class AccessibilityReliabilityTracker:
    """Track reliability metrics for accessibility queries."""

    def __init__(
        self,
        reliability_threshold: float = 0.8,
        window_size: int = 50,
    ):
        self.reliability_threshold = reliability_threshold
        self._attempts: dict[str, deque[QueryAttempt]] = {}

    def record(
        self,
        query_id: str,
        success: bool,
        latency_ms: float,
    ) -> QueryReliabilityScore:
        """Record a query attempt and return updated reliability score."""
        if query_id not in self._attempts:
            self._attempts[query_id] = deque(maxlen=50)

        self._attempts[query_id].append(
            QueryAttempt(query_id=query_id, success=success, latency_ms=latency_ms)
        )

        return self.get_score(query_id)

    def get_score(self, query_id: str) -> QueryReliabilityScore:
        """Get the current reliability score for a query."""
        attempts = list(self._attempts.get(query_id, []))
        if not attempts:
            return QueryReliabilityScore(
                query_id=query_id,
                success_rate=0.0,
                avg_latency_ms=0.0,
                total_attempts=0,
                is_reliable=False,
            )

        success_count = sum(1 for a in attempts if a.success)
        total_latency = sum(a.latency_ms for a in attempts)
        success_rate = success_count / len(attempts)
        avg_latency = total_latency / len(attempts)

        return QueryReliabilityScore(
            query_id=query_id,
            success_rate=success_rate,
            avg_latency_ms=avg_latency,
            total_attempts=len(attempts),
            is_reliable=success_rate >= self.reliability_threshold,
        )

    def get_all_scores(self) -> dict[str, QueryReliabilityScore]:
        """Get reliability scores for all tracked queries."""
        return {qid: self.get_score(qid) for qid in self._attempts}
