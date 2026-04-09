"""
Input Batch Profile Utilities

Profile input batches to analyze timing patterns, batch efficiency,
and detect optimization opportunities in automation pipelines.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
import statistics
from dataclasses import dataclass, field
from typing import List, Optional, Dict


@dataclass
class BatchProfile:
    """Profile data for a batch of input events."""
    batch_id: str
    event_count: int
    total_duration_ms: float
    avg_inter_event_ms: float
    median_inter_event_ms: float
    min_inter_event_ms: float
    max_inter_event_ms: float
    std_dev_ms: float
    throughput_hz: float  # events per second
    is_optimizable: bool
    suggestions: List[str] = field(default_factory=list)


class InputBatchProfiler:
    """
    Profile batches of input events to find optimization opportunities.

    Analyzes inter-event timing, throughput, and identifies patterns
    that could be improved (e.g., too much delay between events).
    """

    def __init__(
        self,
        target_throughput_hz: float = 60.0,
        max_inter_event_ms: float = 50.0,
    ):
        self.target_throughput_hz = target_throughput_hz
        self.max_inter_event_ms = max_inter_event_ms
        self._profiles: Dict[str, BatchProfile] = {}

    def profile_batch(
        self,
        batch_id: str,
        event_timestamps_ms: List[float],
    ) -> BatchProfile:
        """
        Profile a batch of events given their timestamps.

        Args:
            batch_id: Identifier for this batch.
            event_timestamps_ms: Chronological list of event timestamps.

        Returns:
            BatchProfile with analysis.
        """
        if len(event_timestamps_ms) < 2:
            return BatchProfile(
                batch_id=batch_id,
                event_count=len(event_timestamps_ms),
                total_duration_ms=0.0,
                avg_inter_event_ms=0.0,
                median_inter_event_ms=0.0,
                min_inter_event_ms=0.0,
                max_inter_event_ms=0.0,
                std_dev_ms=0.0,
                throughput_hz=0.0,
                is_optimizable=False,
                suggestions=["Batch too small to profile"],
            )

        # Compute inter-event intervals
        intervals = [
            event_timestamps_ms[i] - event_timestamps_ms[i - 1]
            for i in range(1, len(event_timestamps_ms))
        ]

        total_duration = event_timestamps_ms[-1] - event_timestamps_ms[0]
        avg_inter = statistics.mean(intervals) if intervals else 0.0
        median_inter = statistics.median(intervals) if intervals else 0.0
        min_inter = min(intervals) if intervals else 0.0
        max_inter = max(intervals) if intervals else 0.0
        std_dev = statistics.stdev(intervals) if len(intervals) > 1 else 0.0
        throughput = (len(event_timestamps_ms) / total_duration * 1000) if total_duration > 0 else 0.0

        suggestions = []
        if avg_inter > self.max_inter_event_ms:
            suggestions.append(f"High avg inter-event time: {avg_inter:.1f}ms")
        if throughput < self.target_throughput_hz * 0.5:
            suggestions.append(f"Low throughput: {throughput:.1f}Hz (target: {self.target_throughput_hz}Hz)")
        if std_dev > avg_inter * 0.5:
            suggestions.append(f"High timing variance: std_dev={std_dev:.1f}ms")

        profile = BatchProfile(
            batch_id=batch_id,
            event_count=len(event_timestamps_ms),
            total_duration_ms=total_duration,
            avg_inter_event_ms=avg_inter,
            median_inter_event_ms=median_inter,
            min_inter_event_ms=min_inter,
            max_inter_event_ms=max_inter,
            std_dev_ms=std_dev,
            throughput_hz=throughput,
            is_optimizable=len(suggestions) > 0,
            suggestions=suggestions,
        )

        self._profiles[batch_id] = profile
        return profile

    def get_profile(self, batch_id: str) -> Optional[BatchProfile]:
        """Get a previously computed profile."""
        return self._profiles.get(batch_id)

    def get_all_profiles(self) -> Dict[str, BatchProfile]:
        """Get all computed profiles."""
        return self._profiles.copy()
