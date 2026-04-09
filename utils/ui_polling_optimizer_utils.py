"""
UI polling optimizer utilities.

This module provides utilities for optimizing UI polling strategies,
including adaptive intervals and smart polling triggers.
"""

from __future__ import annotations

import time
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum, auto


class PollStrategy(Enum):
    """Polling strategy types."""
    FIXED = auto()
    ADAPTIVE = auto()
    EVENT_DRIVEN = auto()
    EXPONENTIAL_BACKOFF = auto()


@dataclass
class PollConfig:
    """Configuration for UI polling."""
    strategy: PollStrategy = PollStrategy.ADAPTIVE
    initial_interval_ms: float = 100.0
    min_interval_ms: float = 50.0
    max_interval_ms: float = 5000.0
    backoff_factor: float = 1.5
    stability_threshold_ms: float = 2000.0
    stability_samples: int = 5


@dataclass
class PollMetrics:
    """Metrics for polling performance."""
    poll_count: int = 0
    change_count: int = 0
    miss_count: int = 0
    avg_latency_ms: float = 0.0
    current_interval_ms: float = 100.0


@dataclass
class PollResult:
    """Result of a polling operation."""
    detected_change: bool
    timestamp: float
    latency_ms: float
    current_interval_ms: float


class AdaptivePollingController:
    """Adaptive controller for UI polling intervals."""

    def __init__(self, config: Optional[PollConfig] = None):
        self.config = config or PollConfig()
        self._metrics = PollMetrics()
        self._last_change_time: float = 0.0
        self._last_poll_time: float = 0.0
        self._change_timestamps: List[float] = []
        self._current_interval: float = self.config.initial_interval_ms

    def compute_next_interval(self) -> float:
        """
        Compute the next polling interval based on strategy.

        Returns:
            Next interval in milliseconds.
        """
        now = time.time()
        elapsed_since_change = (now - self._last_change_time) * 1000.0

        if self.config.strategy == PollStrategy.FIXED:
            return self.config.initial_interval_ms

        elif self.config.strategy == PollStrategy.ADAPTIVE:
            # Increase interval when no changes detected
            if elapsed_since_change > self.config.stability_threshold_ms:
                self._current_interval = min(
                    self.config.max_interval_ms,
                    self._current_interval * self.config.backoff_factor
                )
            else:
                # Keep interval low when changes are frequent
                self._current_interval = max(
                    self.config.min_interval_ms,
                    self._current_interval / self.config.backoff_factor
                )
            return self._current_interval

        elif self.config.strategy == PollStrategy.EXPONENTIAL_BACKOFF:
            return min(
                self.config.max_interval_ms,
                self._current_interval * self.config.backoff_factor
            )

        return self._current_interval

    def record_poll(self, result: PollResult) -> None:
        """
        Record the result of a poll operation.

        Args:
            result: Poll result from the last poll.
        """
        self._metrics.poll_count += 1
        self._last_poll_time = result.timestamp

        if result.detected_change:
            self._change_timestamps.append(result.timestamp)
            self._last_change_time = result.timestamp
            self._metrics.change_count += 1
            self._current_interval = self.config.initial_interval_ms
        else:
            self._metrics.miss_count += 1

        # Keep only recent change timestamps
        cutoff = time.time() - self.config.stability_threshold_ms / 1000.0
        self._change_timestamps = [t for t in self._change_timestamps if t > cutoff]

        # Update average latency
        total = self._metrics.avg_latency_ms * (self._metrics.poll_count - 1)
        self._metrics.avg_latency_ms = (total + result.latency_ms) / self._metrics.poll_count

    def should_poll(self) -> bool:
        """
        Determine if a poll should be performed now.

        Returns:
            True if polling should occur.
        """
        now = time.time()
        elapsed = (now - self._last_poll_time) * 1000.0
        return elapsed >= self._current_interval

    @property
    def metrics(self) -> PollMetrics:
        """Get current polling metrics."""
        return self._metrics

    @property
    def change_frequency(self) -> float:
        """Get current change frequency (changes per second)."""
        if not self._change_timestamps:
            return 0.0
        if len(self._change_timestamps) < 2:
            return 0.0
        time_span = self._change_timestamps[-1] - self._change_timestamps[0]
        if time_span < 0.001:
            return 0.0
        return (len(self._change_timestamps) - 1) / time_span


def estimate_optimal_poll_interval(
    observed_changes: List[float],
    target_catch_rate: float = 0.95,
) -> float:
    """
    Estimate optimal polling interval given observed change patterns.

    Args:
        observed_changes: List of timestamps when changes occurred.
        target_catch_rate: Desired probability of catching a change.

    Returns:
        Recommended polling interval in milliseconds.
    """
    if len(observed_changes) < 2:
        return 100.0  # Default

    # Compute inter-change intervals
    intervals: List[float] = []
    for i in range(1, len(observed_changes)):
        interval_ms = (observed_changes[i] - observed_changes[i - 1]) * 1000.0
        intervals.append(interval_ms)

    if not intervals:
        return 100.0

    # Sort intervals
    intervals.sort()
    median_interval = intervals[len(intervals) // 2]

    # For target catch rate with random polling:
    # P(catch) = 1 - e^(-poll_interval / avg_change_interval)
    # Solve for poll_interval: poll_interval = -avg_change_interval * ln(1 - target)
    import math
    catch_rate = -median_interval * math.log(1.0 - target_catch_rate)

    return max(10.0, min(5000.0, catch_rate))
