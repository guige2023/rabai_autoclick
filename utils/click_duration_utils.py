"""
Click Duration Utilities

Provides utilities for analyzing, computing, and validating click durations
in the context of mouse/touch automation.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ClickDurationResult:
    """Result of a click duration measurement."""
    duration_ms: float
    is_valid: bool
    classification: str  # 'short', 'normal', 'long', 'stuck'
    metadata: dict = field(default_factory=dict)


@dataclass
class ClickDurationThresholds:
    """Configurable thresholds for click duration classification."""
    min_valid_ms: float = 10.0
    max_normal_ms: float = 500.0
    max_long_ms: float = 2000.0
    stuck_threshold_ms: float = 5000.0

    def classify(self, duration_ms: float) -> str:
        """Classify a duration into a category."""
        if duration_ms < self.min_valid_ms:
            return "invalid"
        if duration_ms < 50:
            return "short"
        if duration_ms > self.stuck_threshold_ms:
            return "stuck"
        if duration_ms > self.max_long_ms:
            return "long"
        if duration_ms > self.max_normal_ms:
            return "prolonged"
        return "normal"


class ClickDurationAnalyzer:
    """Analyze click durations for automation quality assessment."""

    def __init__(self, thresholds: Optional[ClickDurationThresholds] = None):
        self.thresholds = thresholds or ClickDurationThresholds()
        self._history: list[float] = []
        self._max_history = 1000

    def record(self, duration_ms: float) -> ClickDurationResult:
        """Record a click duration and return the analysis result."""
        classification = self.thresholds.classify(duration_ms)
        is_valid = classification not in ("invalid", "stuck")

        self._history.append(duration_ms)
        if len(self._history) > self._max_history:
            self._history.pop(0)

        result = ClickDurationResult(
            duration_ms=duration_ms,
            is_valid=is_valid,
            classification=classification,
            metadata=self._compute_metadata(),
        )
        return result

    def _compute_metadata(self) -> dict:
        """Compute statistics from recorded history."""
        if not self._history:
            return {}
        sorted_history = sorted(self._history)
        n = len(sorted_history)
        return {
            "count": n,
            "mean_ms": sum(self._history) / n,
            "median_ms": sorted_history[n // 2],
            "p95_ms": sorted_history[int(n * 0.95)],
            "min_ms": sorted_history[0],
            "max_ms": sorted_history[-1],
        }

    def get_statistics(self) -> dict:
        """Return current statistics from history."""
        return self._compute_metadata()


def measure_click_duration(
    down_handler: Callable[[], float],
    up_handler: Callable[[], float],
) -> float:
    """
    Measure the duration between a mouse-down and mouse-up event.

    Args:
        down_handler: Returns the timestamp (ms) when the button was pressed.
        up_handler: Returns the timestamp (ms) when the button was released.

    Returns:
        Duration in milliseconds.
    """
    down_time = down_handler()
    up_time = up_handler()
    return max(0.0, up_time - down_time)


def is_stuck_click(duration_ms: float, threshold_ms: float = 5000.0) -> bool:
    """Check if a click is stuck based on duration threshold."""
    return duration_ms > threshold_ms


def format_duration_ms(duration_ms: float) -> str:
    """Format a duration in milliseconds as a human-readable string."""
    if duration_ms < 1:
        return f"{duration_ms * 1000:.1f}µs"
    if duration_ms < 1000:
        return f"{duration_ms:.1f}ms"
    return f"{duration_ms / 1000:.2f}s"
