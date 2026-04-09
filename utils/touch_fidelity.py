"""
Touch Fidelity Module.

Provides utilities for ensuring high-fidelity touch input processing,
including touch point accuracy, pressure handling, and gesture recognition
refinement.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


logger = logging.getLogger(__name__)


class TouchType(Enum):
    """Types of touch events."""
    DOWN = auto()
    MOVE = auto()
    UP = auto()
    CANCEL = auto()


@dataclass
class TouchPoint:
    """Represents a single touch point."""
    id: int
    x: float
    y: float
    pressure: float = 1.0
    size: float = 1.0
    timestamp: float = field(default_factory=time.time)
    tilt_x: float = 0.0
    tilt_y: float = 0.0
    twist: float = 0.0
    major_axis: float = 1.0
    minor_axis: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def distance_to(self, other: TouchPoint) -> float:
        """
        Calculate distance to another touch point.

        Args:
            other: Other TouchPoint.

        Returns:
            Euclidean distance.
        """
        dx = self.x - other.x
        dy = self.y - other.y
        return (dx * dx + dy * dy) ** 0.5

    def angle_to(self, other: TouchPoint) -> float:
        """
        Calculate angle to another touch point.

        Args:
            other: Other TouchPoint.

        Returns:
            Angle in radians.
        """
        import math
        return math.atan2(other.y - self.y, other.x - self.x)


@dataclass
class TouchEvent:
    """Represents a touch event with multiple touch points."""
    touch_type: TouchType
    points: list[TouchPoint]
    timestamp: float = field(default_factory=time.time)
    duration_ms: float = 0.0
    device_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class TouchFidelityFilter:
    """
    Filters and refines touch input for higher fidelity.

    Applies noise reduction, smoothing, and pressure calibration.
    """

    def __init__(
        self,
        smoothing_factor: float = 0.3,
        noise_threshold: float = 2.0,
        min_pressure: float = 0.01
    ) -> None:
        """
        Initialize the touch fidelity filter.

        Args:
            smoothing_factor: Smoothing strength (0.0 to 1.0).
            noise_threshold: Minimum movement to register (pixels).
            min_pressure: Minimum pressure value.
        """
        self.smoothing_factor = smoothing_factor
        self.noise_threshold = noise_threshold
        self.min_pressure = min_pressure
        self._previous_points: dict[int, TouchPoint] = {}

    def process(self, event: TouchEvent) -> TouchEvent:
        """
        Process a touch event for higher fidelity.

        Args:
            event: Original touch event.

        Returns:
            Refined touch event.
        """
        processed_points: list[TouchPoint] = []

        for point in event.points:
            processed = self._process_point(point)
            processed_points.append(processed)

        return TouchEvent(
            touch_type=event.touch_type,
            points=processed_points,
            timestamp=event.timestamp,
            duration_ms=event.duration_ms,
            device_id=event.device_id,
            metadata=event.metadata
        )

    def _process_point(self, point: TouchPoint) -> TouchPoint:
        """Process a single touch point."""
        processed = TouchPoint(
            id=point.id,
            x=point.x,
            y=point.y,
            pressure=max(self.min_pressure, point.pressure),
            size=point.size,
            timestamp=point.timestamp,
            tilt_x=point.tilt_x,
            tilt_y=point.tilt_y,
            twist=point.twist,
            major_axis=point.major_axis,
            minor_axis=point.minor_axis,
            metadata=point.metadata.copy()
        )

        prev = self._previous_points.get(point.id)
        if prev:
            processed = self._smooth(processed, prev)
            processed = self._apply_noise_filter(processed, prev)

        self._previous_points[point.id] = processed
        return processed

    def _smooth(
        self,
        current: TouchPoint,
        previous: TouchPoint
    ) -> TouchPoint:
        """Apply smoothing to reduce jitter."""
        t = self.smoothing_factor
        current.x = previous.x + t * (current.x - previous.x)
        current.y = previous.y + t * (current.y - previous.y)
        current.pressure = previous.pressure + t * (current.pressure - previous.pressure)
        return current

    def _apply_noise_filter(
        self,
        current: TouchPoint,
        previous: TouchPoint
    ) -> TouchPoint:
        """Filter out noise based on movement threshold."""
        distance = current.distance_to(previous)

        if distance < self.noise_threshold:
            current.x = previous.x
            current.y = previous.y

        return current

    def reset(self) -> None:
        """Reset filter state."""
        self._previous_points.clear()


class PressureCalibrator:
    """
    Calibrates pressure values for consistent touch response.

    Example:
        >>> calibrator = PressureCalibrator()
        >>> calibrator.calibrate(sample_events)
        >>> calibrated = calibrator.calibrate_event(event)
    """

    def __init__(self) -> None:
        """Initialize the pressure calibrator."""
        self._min_raw: float = 0.0
        self._max_raw: float = 1.0
        self._min_calibrated: float = 0.0
        self._max_calibrated: float = 1.0
        self._offset: float = 0.0
        self._scale: float = 1.0
        self._calibrated: bool = False

    def calibrate(
        self,
        sample_events: list[TouchEvent],
        target_min: float = 0.0,
        target_max: float = 1.0
    ) -> None:
        """
        Calibrate pressure using sample events.

        Args:
            sample_events: List of sample touch events.
            target_min: Target minimum pressure.
            target_max: Target maximum pressure.
        """
        all_pressures: list[float] = []

        for event in sample_events:
            for point in event.points:
                all_pressures.append(point.pressure)

        if not all_pressures:
            return

        self._min_raw = min(all_pressures)
        self._max_raw = max(all_pressures)
        self._min_calibrated = target_min
        self._max_calibrated = target_max

        pressure_range = self._max_raw - self._min_raw
        if pressure_range > 0:
            self._scale = (target_max - target_min) / pressure_range
            self._offset = target_min - self._min_raw * self._scale
        else:
            self._scale = 1.0
            self._offset = target_min

        self._calibrated = True
        logger.info(
            f"Pressure calibrated: raw [{self._min_raw:.3f}, {self._max_raw:.3f}] "
            f"-> calibrated [{target_min}, {target_max}]"
        )

    def calibrate_pressure(self, raw_pressure: float) -> float:
        """
        Calibrate a raw pressure value.

        Args:
            raw_pressure: Raw pressure value.

        Returns:
            Calibrated pressure value.
        """
        if not self._calibrated:
            return raw_pressure

        calibrated = raw_pressure * self._scale + self._offset
        return max(self._min_calibrated, min(self._max_calibrated, calibrated))

    def calibrate_event(self, event: TouchEvent) -> TouchEvent:
        """
        Calibrate all points in an event.

        Args:
            event: Original touch event.

        Returns:
            Event with calibrated pressures.
        """
        calibrated_points = []

        for point in event.points:
            calibrated = TouchPoint(
                id=point.id,
                x=point.x,
                y=point.y,
                pressure=self.calibrate_pressure(point.pressure),
                size=point.size,
                timestamp=point.timestamp,
                tilt_x=point.tilt_x,
                tilt_y=point.tilt_y,
                twist=point.twist,
                major_axis=point.major_axis,
                minor_axis=point.minor_axis,
                metadata=point.metadata.copy()
            )
            calibrated_points.append(calibrated)

        return TouchEvent(
            touch_type=event.touch_type,
            points=calibrated_points,
            timestamp=event.timestamp,
            duration_ms=event.duration_ms,
            device_id=event.device_id,
            metadata=event.metadata
        )


class TouchAccuracyAnalyzer:
    """
    Analyzes touch input accuracy and reports metrics.

    Tracks touch precision, accuracy against targets, and drift.
    """

    def __init__(self) -> None:
        """Initialize the accuracy analyzer."""
        self._interactions: list[dict[str, Any]] = []
        self._drift: dict[int, tuple[float, float]] = {}

    def record_interaction(
        self,
        touch_point: TouchPoint,
        target_x: float,
        target_y: float
    ) -> None:
        """
        Record a touch interaction against a target.

        Args:
            touch_point: Actual touch point.
            target_x: Target X coordinate.
            target_y: Target Y coordinate.
        """
        dx = touch_point.x - target_x
        dy = touch_point.y - target_y
        error = (dx * dx + dy * dy) ** 0.5

        self._interactions.append({
            "touch_id": touch_point.id,
            "x": touch_point.x,
            "y": touch_point.y,
            "target_x": target_x,
            "target_y": target_y,
            "error": error,
            "pressure": touch_point.pressure,
            "timestamp": touch_point.timestamp
        })

        self._drift[touch_point.id] = (dx, dy)

    def get_accuracy_stats(self) -> dict[str, float]:
        """
        Get accuracy statistics.

        Returns:
            Dictionary with accuracy metrics.
        """
        if not self._interactions:
            return {
                "mean_error": 0.0,
                "max_error": 0.0,
                "min_error": 0.0,
                "std_dev": 0.0,
                "total_interactions": 0
            }

        errors = [i["error"] for i in self._interactions]
        mean_error = sum(errors) / len(errors)

        variance = sum((e - mean_error) ** 2 for e in errors) / len(errors)
        std_dev = variance ** 0.5

        return {
            "mean_error": mean_error,
            "max_error": max(errors),
            "min_error": min(errors),
            "std_dev": std_dev,
            "total_interactions": len(self._interactions)
        }

    def get_drift(self, touch_id: int) -> tuple[float, float]:
        """
        Get drift for a touch ID.

        Args:
            touch_id: Touch point ID.

        Returns:
            Tuple of (x_drift, y_drift).
        """
        return self._drift.get(touch_id, (0.0, 0.0))

    def clear(self) -> None:
        """Clear all recorded interactions."""
        self._interactions.clear()
        self._drift.clear()


class TouchLatencyCompensator:
    """
    Compensates for touch input latency.

    Predicts touch position based on velocity to reduce perceived latency.
    """

    def __init__(self, lookahead_ms: float = 16.0) -> None:
        """
        Initialize the latency compensator.

        Args:
            lookahead_ms: Milliseconds to look ahead.
        """
        self.lookahead_ms = lookahead_ms
        self._velocities: dict[int, tuple[float, float]] = {}
        self._previous_points: dict[int, TouchPoint] = {}

    def compensate(self, event: TouchEvent) -> TouchEvent:
        """
        Compensate touch event for latency.

        Args:
            event: Original touch event.

        Returns:
            Compensated touch event.
        """
        compensated_points: list[TouchPoint] = []

        for point in event.points:
            compensated = self._compensate_point(point)
            compensated_points.append(compensated)

        return TouchEvent(
            touch_type=event.touch_type,
            points=compensated_points,
            timestamp=event.timestamp,
            duration_ms=event.duration_ms,
            device_id=event.device_id,
            metadata=event.metadata
        )

    def _compensate_point(self, point: TouchPoint) -> TouchPoint:
        """Compensate a single point."""
        prev = self._previous_points.get(point.id)

        if prev:
            dt = point.timestamp - prev.timestamp
            if dt > 0:
                vx = (point.x - prev.x) / dt
                vy = (point.y - prev.y) / dt

                self._velocities[point.id] = (vx, vy)

                lookahead_s = self.lookahead_ms / 1000.0
                compensated = TouchPoint(
                    id=point.id,
                    x=point.x + vx * lookahead_s,
                    y=point.y + vy * lookahead_s,
                    pressure=point.pressure,
                    size=point.size,
                    timestamp=point.timestamp,
                    tilt_x=point.tilt_x,
                    tilt_y=point.tilt_y,
                    twist=point.twist,
                    major_axis=point.major_axis,
                    minor_axis=point.minor_axis,
                    metadata=point.metadata.copy()
                )
                self._previous_points[point.id] = point
                return compensated

        self._previous_points[point.id] = point
        return point

    def reset(self) -> None:
        """Reset compensator state."""
        self._velocities.clear()
        self._previous_points.clear()


@dataclass
class TouchFidelityConfig:
    """Configuration for touch fidelity processing."""
    enable_smoothing: bool = True
    enable_pressure_calibration: bool = True
    enable_latency_compensation: bool = True
    enable_noise_filtering: bool = True
    smoothing_factor: float = 0.3
    noise_threshold: float = 2.0
    lookahead_ms: float = 16.0


class TouchFidelityProcessor:
    """
    High-level touch fidelity processor combining all fidelity utilities.

    Example:
        >>> processor = TouchFidelityProcessor(config)
        >>> high_fidelity = processor.process(touch_event)
    """

    def __init__(self, config: TouchFidelityConfig | None = None) -> None:
        """
        Initialize the processor.

        Args:
            config: Fidelity configuration.
        """
        self.config = config or TouchFidelityConfig()

        self._filter = TouchFidelityFilter(
            smoothing_factor=self.config.smoothing_factor,
            noise_threshold=self.config.noise_threshold
        )

        self._calibrator = PressureCalibrator()
        self._compensator = TouchLatencyCompensator(
            lookahead_ms=self.config.lookahead_ms
        )

    def process(self, event: TouchEvent) -> TouchEvent:
        """
        Process touch event through the fidelity pipeline.

        Args:
            event: Original touch event.

        Returns:
            High-fidelity touch event.
        """
        processed = event

        if self.config.enable_noise_filtering or self.config.enable_smoothing:
            processed = self._filter.process(processed)

        if self.config.enable_pressure_calibration:
            processed = self._calibrator.calibrate_event(processed)

        if self.config.enable_latency_compensation:
            processed = self._compensator.compensate(processed)

        return processed

    def calibrate_with_samples(
        self,
        sample_events: list[TouchEvent]
    ) -> None:
        """
        Calibrate the processor with sample events.

        Args:
            sample_events: Sample touch events for calibration.
        """
        self._calibrator.calibrate(sample_events)
        logger.info("Touch fidelity processor calibrated")

    def reset(self) -> None:
        """Reset all internal state."""
        self._filter.reset()
        self._compensator.reset()
