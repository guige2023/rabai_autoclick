"""
Touch Filtering Utilities for UI Automation.

This module provides utilities for filtering and cleaning
raw touch input data in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple


@dataclass
class FilteredTouch:
    """A filtered touch point."""
    x: float
    y: float
    pressure: float
    timestamp: float
    original_x: float
    original_y: float
    was_filtered: bool = False


@dataclass
class FilterConfig:
    """Configuration for touch filtering."""
    enabled: bool = True
    smoothing_factor: float = 0.3
    velocity_threshold: float = 100.0
    min_update_interval_ms: float = 5.0
    interpolate_gaps: bool = True
    gap_threshold_ms: float = 50.0


class TouchFilter:
    """Filters and cleans raw touch input data."""

    def __init__(self, config: Optional[FilterConfig] = None) -> None:
        self._config = config or FilterConfig()
        self._last_touch: Optional[FilteredTouch] = None
        self._touch_buffer: List[FilteredTouch] = []
        self._max_buffer_size: int = 10

    def set_config(self, config: FilterConfig) -> None:
        """Update the filter configuration."""
        self._config = config

    def get_config(self) -> FilterConfig:
        """Get the current filter configuration."""
        return self._config

    def process(
        self,
        x: float,
        y: float,
        pressure: float = 0.5,
        timestamp: Optional[float] = None,
    ) -> FilteredTouch:
        """Process a raw touch point and return filtered result."""
        if timestamp is None:
            timestamp = time.time()

        if not self._config.enabled:
            return FilteredTouch(
                x=x, y=y, pressure=pressure, timestamp=timestamp,
                original_x=x, original_y=y, was_filtered=False,
            )

        filtered_x, filtered_y = x, y
        was_filtered = False

        if self._last_touch is not None:
            dt = (timestamp - self._last_touch.timestamp) * 1000.0

            if dt < self._config.min_update_interval_ms:
                return self._last_touch

            dx = x - self._last_touch.x
            dy = y - self._last_touch.y
            distance = math.sqrt(dx * dx + dy * dy)
            velocity = distance / dt if dt > 0 else 0.0

            if velocity > self._config.velocity_threshold:
                max_distance = self._config.velocity_threshold * dt
                if distance > 0:
                    scale = max_distance / distance
                    filtered_x = self._last_touch.x + dx * scale
                    filtered_y = self._last_touch.y + dy * scale
                was_filtered = True
            else:
                factor = self._config.smoothing_factor
                filtered_x = self._last_touch.x * (1 - factor) + x * factor
                filtered_y = self._last_touch.y * (1 - factor) + y * factor

        result = FilteredTouch(
            x=filtered_x,
            y=filtered_y,
            pressure=pressure,
            timestamp=timestamp,
            original_x=x,
            original_y=y,
            was_filtered=was_filtered,
        )

        self._last_touch = result
        self._add_to_buffer(result)

        return result

    def _add_to_buffer(self, touch: FilteredTouch) -> None:
        """Add a touch to the history buffer."""
        self._touch_buffer.append(touch)
        if len(self._touch_buffer) > self._max_buffer_size:
            self._touch_buffer.pop(0)

    def get_last_touch(self) -> Optional[FilteredTouch]:
        """Get the last filtered touch."""
        return self._last_touch

    def get_buffer(self) -> List[FilteredTouch]:
        """Get the touch history buffer."""
        return list(self._touch_buffer)

    def interpolate_gaps(self) -> List[FilteredTouch]:
        """Interpolate gaps in the touch buffer."""
        if not self._config.interpolate_gaps or len(self._touch_buffer) < 2:
            return list(self._touch_buffer)

        result: List[FilteredTouch] = [self._touch_buffer[0]]

        for i in range(1, len(self._touch_buffer)):
            prev = self._touch_buffer[i - 1]
            curr = self._touch_buffer[i]
            dt_ms = (curr.timestamp - prev.timestamp) * 1000.0

            if dt_ms > self._config.gap_threshold_ms:
                steps = int(dt_ms / self._config.min_update_interval_ms)
                for j in range(1, steps):
                    t = j / steps
                    interp = FilteredTouch(
                        x=prev.x + (curr.x - prev.x) * t,
                        y=prev.y + (curr.y - prev.y) * t,
                        pressure=prev.pressure + (curr.pressure - prev.pressure) * t,
                        timestamp=prev.timestamp + (curr.timestamp - prev.timestamp) * t,
                        original_x=prev.x + (curr.x - prev.x) * t,
                        original_y=prev.y + (curr.y - prev.y) * t,
                        was_filtered=True,
                    )
                    result.append(interp)

            result.append(curr)

        return result

    def reset(self) -> None:
        """Reset all filter state."""
        self._last_touch = None
        self._touch_buffer.clear()

    def get_average_velocity(self) -> float:
        """Calculate average touch velocity from the buffer."""
        if len(self._touch_buffer) < 2:
            return 0.0

        total_velocity = 0.0
        count = 0

        for i in range(1, len(self._touch_buffer)):
            p0 = self._touch_buffer[i - 1]
            p1 = self._touch_buffer[i]
            dt = p1.timestamp - p0.timestamp

            if dt > 0:
                dx = p1.x - p0.x
                dy = p1.y - p0.y
                distance = math.sqrt(dx * dx + dy * dy)
                total_velocity += distance / dt
                count += 1

        return total_velocity / count if count > 0 else 0.0


def apply_exponential_smoothing(
    values: List[float],
    alpha: float = 0.3,
) -> List[float]:
    """Apply exponential smoothing to a list of values."""
    if not values:
        return []

    smoothed = [values[0]]
    for i in range(1, len(values)):
        smoothed.append(alpha * values[i] + (1 - alpha) * smoothed[i - 1])

    return smoothed
