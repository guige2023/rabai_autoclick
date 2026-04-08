"""Input device calibration utilities for UI automation.

Calibrates mouse, touch, and stylus input devices to improve
automation accuracy by accounting for offset, scaling, and latency.
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class DeviceType(Enum):
    """Types of input devices."""
    MOUSE = auto()
    TOUCH_SCREEN = auto()
    TOUCH_PAD = auto()
    STYLUS = auto()
    TRACKPAD = auto()


class CalibrationStatus(Enum):
    """Calibration status values."""
    UNCALIBRATED = auto()
    IN_PROGRESS = auto()
    CALIBRATED = auto()
    STALE = auto()  # Needs recalibration


@dataclass
class CalibrationPoint:
    """A single calibration point pair.

    Attributes:
        device_x: Raw device X coordinate.
        device_y: Raw device Y coordinate.
        target_x: Expected/actual screen X coordinate.
        target_y: Expected/actual screen Y coordinate.
        timestamp: When this point was captured.
    """
    device_x: float
    device_y: float
    target_x: float
    target_y: float
    timestamp: float = field(default_factory=time.time)

    def offset_x(self) -> float:
        """X offset (error) for this point."""
        return self.target_x - self.device_x

    def offset_y(self) -> float:
        """Y offset (error) for this point."""
        return self.target_y - self.device_y


@dataclass
class CalibrationResult:
    """Result of a calibration operation.

    Attributes:
        offset_x: X offset to apply to raw coordinates.
        offset_y: Y offset to apply to raw coordinates.
        scale_x: X scale factor.
        scale_y: Y scale factor.
        rotation: Rotation correction in degrees.
        confidence: Calibration confidence (0.0-1.0).
        max_error: Maximum observed error after calibration.
        mean_error: Mean error after calibration.
        points_used: Number of calibration points used.
    """
    offset_x: float = 0.0
    offset_y: float = 0.0
    scale_x: float = 1.0
    scale_y: float = 1.0
    rotation: float = 0.0
    confidence: float = 0.0
    max_error: float = 0.0
    mean_error: float = 0.0
    points_used: int = 0

    def apply(self, x: float, y: float) -> tuple[float, float]:
        """Apply calibration to raw coordinates.

        Returns corrected (x, y) tuple.
        """
        # Apply offset
        x = x + self.offset_x
        y = y + self.offset_y
        # Apply scale
        x = x * self.scale_x
        y = y * self.scale_y
        return (x, y)


@dataclass
class DeviceCalibration:
    """Complete calibration data for an input device.

    Attributes:
        device_id: Unique identifier for this device.
        device_type: Type of the device.
        status: Current calibration status.
        result: Current calibration result.
        points: All calibration points collected.
        created_at: When calibration was first created.
        updated_at: When calibration was last updated.
    """
    device_id: str
    device_type: DeviceType
    status: CalibrationStatus = CalibrationStatus.UNCALIBRATED
    result: Optional[CalibrationResult] = None
    points: list[CalibrationPoint] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def is_valid(self) -> bool:
        """Return True if calibration is valid and usable."""
        return (
            self.status == CalibrationStatus.CALIBRATED
            and self.result is not None
            and self.result.confidence >= 0.7
        )


class InputDeviceCalibrator:
    """Calibrates input devices for improved automation accuracy.

    Supports multi-point calibration with offset, scale, and
    rotation correction.
    """

    def __init__(self) -> None:
        """Initialize calibrator with empty state."""
        self._calibrations: dict[str, DeviceCalibration] = {}
        self._calibration_callbacks: list[
            Callable[[str, CalibrationResult], None]
        ] = []

    def create_calibration(
        self,
        device_id: str,
        device_type: DeviceType,
    ) -> DeviceCalibration:
        """Create a new calibration entry for a device."""
        calibration = DeviceCalibration(
            device_id=device_id,
            device_type=device_type,
        )
        self._calibrations[device_id] = calibration
        return calibration

    def get_calibration(self, device_id: str) -> Optional[DeviceCalibration]:
        """Get calibration for a device."""
        return self._calibrations.get(device_id)

    def add_calibration_point(
        self,
        device_id: str,
        device_x: float,
        device_y: float,
        target_x: float,
        target_y: float,
    ) -> bool:
        """Add a calibration point for a device.

        Returns True if point was added successfully.
        """
        calibration = self._calibrations.get(device_id)
        if not calibration:
            return False

        point = CalibrationPoint(
            device_x=device_x,
            device_y=device_y,
            target_x=target_x,
            target_y=target_y,
        )
        calibration.points.append(point)
        calibration.status = CalibrationStatus.IN_PROGRESS
        calibration.updated_at = time.time()
        return True

    def compute_calibration(
        self,
        device_id: str,
        min_points: int = 3,
    ) -> Optional[CalibrationResult]:
        """Compute calibration result from collected points.

        Args:
            device_id: The device to compute calibration for.
            min_points: Minimum calibration points required.

        Returns:
            CalibrationResult if successful, or None if insufficient points.
        """
        calibration = self._calibrations.get(device_id)
        if not calibration or len(calibration.points) < min_points:
            return None

        points = calibration.points

        # Compute offset (mean error)
        offset_x = sum(p.offset_x() for p in points) / len(points)
        offset_y = sum(p.offset_y() for p in points) / len(points)

        # Compute scale using endpoint distances
        device_distances = [
            ((p.device_x - points[0].device_x) ** 2 +
             (p.device_y - points[0].device_y) ** 2) ** 0.5
            for p in points
        ]
        target_distances = [
            ((p.target_x - points[0].target_x) ** 2 +
             (p.target_y - points[0].target_y) ** 2) ** 0.5
            for p in points
        ]

        scale_x = scale_y = 1.0
        if device_distances and target_distances:
            valid_pairs = [
                (d, t) for d, t in zip(device_distances, target_distances)
                if d > 0
            ]
            if valid_pairs:
                ratios = [t / d for d, t in valid_pairs]
                scale_x = sum(ratios) / len(ratios)
                scale_y = sum(ratios) / len(ratios)

        # Compute errors after calibration
        corrected_points: list[tuple[float, float]] = []
        for p in points:
            cx = (p.device_x + offset_x) * scale_x
            cy = (p.device_y + offset_y) * scale_y
            corrected_points.append((cx, cy))

        errors = [
            ((c[0] - p.target_x) ** 2 + (c[1] - p.target_y) ** 2) ** 0.5
            for c, p in zip(corrected_points, points)
        ]

        max_error = max(errors) if errors else 0.0
        mean_error = sum(errors) / len(errors) if errors else 0.0
        confidence = max(0.0, 1.0 - (mean_error / 100.0))

        result = CalibrationResult(
            offset_x=offset_x,
            offset_y=offset_y,
            scale_x=scale_x,
            scale_y=scale_y,
            confidence=confidence,
            max_error=max_error,
            mean_error=mean_error,
            points_used=len(points),
        )

        calibration.result = result
        calibration.status = CalibrationStatus.CALIBRATED
        calibration.updated_at = time.time()

        self._notify_calibration(device_id, result)
        return result

    def apply_calibration(
        self,
        device_id: str,
        x: float,
        y: float,
    ) -> tuple[float, float]:
        """Apply calibration to raw coordinates.

        Returns (corrected_x, corrected_y).
        """
        calibration = self._calibrations.get(device_id)
        if not calibration or not calibration.is_valid():
            return (x, y)
        return calibration.result.apply(x, y)

    def clear_calibration(self, device_id: str) -> bool:
        """Clear calibration for a device."""
        if device_id in self._calibrations:
            del self._calibrations[device_id]
            return True
        return False

    def invalidate_calibration(self, device_id: str) -> bool:
        """Mark a calibration as stale (needs recalibration)."""
        calibration = self._calibrations.get(device_id)
        if not calibration:
            return False
        calibration.status = CalibrationStatus.STALE
        return True

    def on_calibration_complete(
        self,
        callback: Callable[[str, CalibrationResult], None],
    ) -> None:
        """Register a callback for calibration completion."""
        self._calibration_callbacks.append(callback)

    def _notify_calibration(
        self,
        device_id: str,
        result: CalibrationResult,
    ) -> None:
        """Notify all calibration callbacks."""
        for cb in self._calibration_callbacks:
            try:
                cb(device_id, result)
            except Exception:
                pass

    @property
    def calibrated_devices(self) -> list[str]:
        """Return IDs of all calibrated devices."""
        return [
            did for did, c in self._calibrations.items()
            if c.is_valid()
        ]

    @property
    def all_devices(self) -> list[str]:
        """Return IDs of all tracked devices."""
        return list(self._calibrations.keys())


# Utility: quick single-point calibration
def quick_calibrate(
    device_x: float,
    device_y: float,
    target_x: float,
    target_y: float,
) -> CalibrationResult:
    """Quick single-point calibration.

    Returns calibration result with offset only.
    """
    offset_x = target_x - device_x
    offset_y = target_y - device_y
    return CalibrationResult(
        offset_x=offset_x,
        offset_y=offset_y,
        confidence=0.8,
        max_error=max(abs(offset_x), abs(offset_y)),
        mean_error=max(abs(offset_x), abs(offset_y)),
        points_used=1,
    )
