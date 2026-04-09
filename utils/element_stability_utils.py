"""
Element Stability Utilities

Measure the stability of UI elements over time by analyzing
position, size, and attribute changes. Used to detect when
an element has become unreliable for automation.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ElementSnapshot:
    """A snapshot of an element's state at a point in time."""
    x: float
    y: float
    width: float
    height: float
    label: str
    timestamp_ms: float


@dataclass
class StabilityMetrics:
    """Stability metrics for a UI element."""
    position_stability: float  # 0.0 (unstable) to 1.0 (stable)
    size_stability: float
    label_stability: float
    overall_stability: float
    is_stable: bool


class ElementStabilityAnalyzer:
    """Analyze the stability of a UI element over time."""

    def __init__(
        self,
        stability_threshold: float = 0.8,
        window_size: int = 20,
    ):
        self.stability_threshold = stability_threshold
        self._snapshots: deque[ElementSnapshot] = deque(maxlen=window_size)

    def record_snapshot(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        label: str,
        timestamp_ms: float,
    ) -> None:
        """Record a new snapshot of the element's state."""
        self._snapshots.append(
            ElementSnapshot(x=x, y=y, width=width, height=height, label=label, timestamp_ms=timestamp_ms)
        )

    def compute_metrics(self) -> StabilityMetrics:
        """Compute stability metrics from recorded snapshots."""
        if len(self._snapshots) < 2:
            return StabilityMetrics(
                position_stability=0.0,
                size_stability=0.0,
                label_stability=0.0,
                overall_stability=0.0,
                is_stable=False,
            )

        positions = [(s.x, s.y) for s in self._snapshots]
        position_variance = self._variance_2d(positions)
        position_stability = max(0.0, 1.0 - math.sqrt(position_variance) / 100.0)

        sizes = [(s.width, s.height) for s in self._snapshots]
        size_variance = self._variance_2d(sizes)
        size_stability = max(0.0, 1.0 - math.sqrt(size_variance) / 100.0)

        label_changes = sum(
            1 for i in range(1, len(self._snapshots))
            if self._snapshots[i].label != self._snapshots[i - 1].label
        )
        label_stability = max(0.0, 1.0 - label_changes / max(1, len(self._snapshots) - 1))

        overall = (position_stability + size_stability + label_stability) / 3.0

        return StabilityMetrics(
            position_stability=position_stability,
            size_stability=size_stability,
            label_stability=label_stability,
            overall_stability=overall,
            is_stable=overall >= self.stability_threshold,
        )

    @staticmethod
    def _variance_2d(points: list[tuple[float, float]]) -> float:
        """Compute 2D variance of a list of (x, y) points."""
        if not points:
            return 0.0
        mean_x = sum(p[0] for p in points) / len(points)
        mean_y = sum(p[1] for p in points) / len(points)
        return sum((p[0] - mean_x) ** 2 + (p[1] - mean_y) ** 2 for p in points) / len(points)
