"""
Input normalization and calibration utilities for UI automation.

Provides functions for normalizing input coordinates,
calibrating devices, and handling input transformations.
"""

from __future__ import annotations

import math
from typing import Tuple, Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


Point = Tuple[float, float]
Size = Tuple[float, float]
Rect = Tuple[float, float, float, float]


class InputDevice(Enum):
    """Types of input devices."""
    MOUSE = auto()
    TOUCHPAD = auto()
    TOUCHSCREEN = auto()
    STYLUS = auto()
    TRACKPAD = auto()


@dataclass
class CalibrationProfile:
    """Input device calibration profile."""
    device_type: InputDevice
    name: str
    offset_x: float = 0.0
    offset_y: float = 0.0
    scale_x: float = 1.0
    scale_y: float = 1.0
    rotation: float = 0.0
    aspect_correction: float = 1.0
    acceleration: float = 1.0
    velocity_decay: float = 0.0
    pressure_curve: List[Tuple[float, float]] = field(
        default_factory=lambda: [(0.0, 0.0), (1.0, 1.0)]
    )
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def apply(self, x: float, y: float) -> Tuple[float, float]:
        """Apply calibration to raw coordinates.
        
        Args:
            x: Raw X coordinate
            y: Raw Y coordinate
        
        Returns:
            Calibrated (x, y)
        """
        x = x * self.scale_x + self.offset_x
        y = y * self.scale_y + self.offset_y
        
        if self.rotation != 0:
            x, y = self._rotate(x, y, self.rotation)
        
        x = x * self.aspect_correction
        
        return (x, y)
    
    def _rotate(self, x: float, y: float, angle: float) -> Tuple[float, float]:
        """Rotate point around origin."""
        rad = math.radians(angle)
        cos_a = math.cos(rad)
        sin_a = math.sin(rad)
        
        rx = x * cos_a - y * sin_a
        ry = x * sin_a + y * cos_a
        
        return (rx, ry)
    
    def reverse(self, x: float, y: float) -> Tuple[float, float]:
        """Reverse calibration to get raw coordinates.
        
        Args:
            x: Calibrated X coordinate
            y: Calibrated Y coordinate
        
        Returns:
            Raw (x, y)
        """
        x = x / self.aspect_correction
        
        if self.rotation != 0:
            x, y = self._rotate(x, y, -self.rotation)
        
        x = (x - self.offset_x) / self.scale_x
        y = (y - self.offset_y) / self.scale_y
        
        return (x, y)


class Normalizer:
    """Normalize input coordinates across devices and displays."""
    
    def __init__(
        self,
        screen_size: Size = (1920, 1080),
        native_resolution: Size = (1920, 1080),
        dpi_scale: float = 1.0,
    ) -> None:
        """Initialize normalizer.
        
        Args:
            screen_size: Logical screen size
            native_resolution: Native display resolution
            dpi_scale: DPI scaling factor
        """
        self.screen_size = screen_size
        self.native_resolution = native_resolution
        self.dpi_scale = dpi_scale
        
        self._calibration: Dict[InputDevice, CalibrationProfile] = {}
        self._default_calibration()
    
    def _default_calibration(self) -> None:
        """Set up default calibration profiles."""
        for device_type in InputDevice:
            self._calibration[device_type] = CalibrationProfile(
                device_type=device_type,
                name=f"Default {device_type.name}",
            )
    
    def set_calibration(self, profile: CalibrationProfile) -> None:
        """Set calibration profile for device type.
        
        Args:
            profile: Calibration profile
        """
        self._calibration[profile.device_type] = profile
    
    def normalize(
        self,
        x: float,
        y: float,
        device: InputDevice = InputDevice.MOUSE,
        raw: bool = False,
    ) -> Tuple[int, int]:
        """Normalize coordinates to screen space.
        
        Args:
            x: Input X coordinate
            y: Input Y coordinate
            device: Source device type
            raw: Whether input is raw device coordinates
        
        Returns:
            Normalized (x, y) in screen space
        """
        if raw:
            profile = self._calibration.get(device)
            if profile:
                x, y = profile.apply(x, y)
        
        x = x * self.dpi_scale
        y = y * self.dpi_scale
        
        nx = int(x * self.screen_size[0] / self.native_resolution[0])
        ny = int(y * self.screen_size[1] / self.native_resolution[1])
        
        return (nx, ny)
    
    def denormalize(
        self,
        x: int,
        y: int,
        device: InputDevice = InputDevice.MOUSE,
        raw: bool = False,
    ) -> Tuple[float, float]:
        """Convert screen coordinates to device space.
        
        Args:
            x: Screen X coordinate
            y: Screen Y coordinate
            device: Target device type
            raw: Whether to return raw device coordinates
        
        Returns:
            Device space (x, y)
        """
        x = x * self.native_resolution[0] / self.screen_size[0]
        y = y * self.native_resolution[1] / self.screen_size[1]
        
        x = x / self.dpi_scale
        y = y / self.dpi_scale
        
        if raw:
            profile = self._calibration.get(device)
            if profile:
                x, y = profile.reverse(x, y)
        
        return (x, y)
    
    def scale_for_display(
        self,
        x: float,
        y: float,
        from_display: int,
        to_display: int,
    ) -> Tuple[int, int]:
        """Scale coordinates between displays.
        
        Args:
            x: Source X coordinate
            y: Source Y coordinate
            from_display: Source display index
            to_display: Target display index
        
        Returns:
            Scaled (x, y)
        """
        scale_factor = self.dpi_scale
        
        return (int(x * scale_factor), int(y * scale_factor))


class InputFilter:
    """Filter and smooth input data."""
    
    def __init__(
        self,
        smoothing: float = 0.3,
        velocity_threshold: float = 2.0,
        deadzone: float = 0.0,
    ) -> None:
        """Initialize input filter.
        
        Args:
            smoothing: Smoothing factor (0.0 to 1.0)
            velocity_threshold: Minimum velocity to register
            deadzone: Deadzone radius
        """
        self.smoothing = smoothing
        self.velocity_threshold = velocity_threshold
        self.deadzone = deadzone
        self._last_position: Optional[Point] = None
        self._velocity: Tuple[float, float] = (0.0, 0.0)
        self._last_time: float = 0.0
    
    def filter(self, x: float, y: float, timestamp: float) -> Tuple[float, float, bool]:
        """Filter input coordinates.
        
        Args:
            x: Input X
            y: Input Y
            timestamp: Event timestamp
        
        Returns:
            (filtered_x, filtered_y, is_valid)
        """
        current = (x, y)
        
        if self._last_position is None:
            self._last_position = current
            self._last_time = timestamp
            return (x, y, True)
        
        dt = timestamp - self._last_time
        if dt > 0:
            vx = (x - self._last_position[0]) / dt
            vy = (y - self._last_position[1]) / dt
            self._velocity = (vx, vy)
        
        speed = math.sqrt(self._velocity[0] ** 2 + self._velocity[1] ** 2)
        
        if speed < self.velocity_threshold:
            return (x, y, False)
        
        dx = x - self._last_position[0]
        dy = y - self._last_position[1]
        dist = math.sqrt(dx * dx + dy * dy)
        
        if dist < self.deadzone:
            return (self._last_position[0], self._last_position[1], False)
        
        filtered_x = self._last_position[0] + dx * self.smoothing
        filtered_y = self._last_position[1] + dy * self.smoothing
        
        self._last_position = (filtered_x, filtered_y)
        self._last_time = timestamp
        
        return (filtered_x, filtered_y, True)
    
    def reset(self) -> None:
        """Reset filter state."""
        self._last_position = None
        self._velocity = (0.0, 0.0)
        self._last_time = 0.0


class VelocityTracker:
    """Track input velocity and direction."""
    
    def __init__(
        self,
        history_size: int = 5,
        min_samples: int = 2,
    ) -> None:
        """Initialize velocity tracker.
        
        Args:
            history_size: Number of samples to track
            min_samples: Minimum samples for velocity
        """
        self.history_size = history_size
        self.min_samples = min_samples
        self._samples: List[Tuple[Point, float]] = []
    
    def add_sample(self, position: Point, timestamp: float) -> None:
        """Add position sample.
        
        Args:
            position: (x, y)
            timestamp: Sample timestamp
        """
        self._samples.append((position, timestamp))
        
        if len(self._samples) > self.history_size:
            self._samples.pop(0)
    
    def get_velocity(self) -> Tuple[float, float]:
        """Get current velocity.
        
        Returns:
            (vx, vy) velocity components
        """
        if len(self._samples) < self.min_samples:
            return (0.0, 0.0)
        
        p1 = self._samples[0]
        p2 = self._samples[-1]
        
        dt = p2[1] - p1[1]
        if dt == 0:
            return (0.0, 0.0)
        
        vx = (p2[0][0] - p1[0][0]) / dt
        vy = (p2[0][1] - p1[0][1]) / dt
        
        return (vx, vy)
    
    def get_speed(self) -> float:
        """Get current speed (magnitude of velocity).
        
        Returns:
            Speed in units per second
        """
        vx, vy = self.get_velocity()
        return math.sqrt(vx * vx + vy * vy)
    
    def get_direction(self) -> Optional[float]:
        """Get direction angle in degrees.
        
        Returns:
            Angle in degrees (0 = right, 90 = up) or None
        """
        vx, vy = self.get_velocity()
        
        if self.get_speed() < 0.1:
            return None
        
        return math.degrees(math.atan2(-vy, vx))
    
    def clear(self) -> None:
        """Clear sample history."""
        self._samples.clear()


def normalize_pressure(
    pressure: float,
    min_pressure: float = 0.0,
    max_pressure: float = 1.0,
) -> float:
    """Normalize pressure value to 0.0-1.0 range.
    
    Args:
        pressure: Raw pressure value
        min_pressure: Minimum expected pressure
        max_pressure: Maximum expected pressure
    
    Returns:
        Normalized pressure
    """
    if max_pressure == min_pressure:
        return 1.0
    
    normalized = (pressure - min_pressure) / (max_pressure - min_pressure)
    
    return max(0.0, min(1.0, normalized))


def apply_pressure_curve(
    pressure: float,
    curve: List[Tuple[float, float]],
) -> float:
    """Apply custom pressure curve.
    
    Args:
        pressure: Normalized pressure (0.0 to 1.0)
        curve: List of (input, output) control points
    
    Returns:
        Adjusted pressure
    """
    if not curve:
        return pressure
    
    if pressure <= curve[0][0]:
        return curve[0][1]
    if pressure >= curve[-1][0]:
        return curve[-1][1]
    
    for i in range(len(curve) - 1):
        x1, y1 = curve[i]
        x2, y2 = curve[i + 1]
        
        if x1 <= pressure <= x2:
            t = (pressure - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
    
    return pressure


def transform_coordinate(
    point: Point,
    from_rect: Rect,
    to_rect: Rect,
    preserve_aspect: bool = True,
) -> Point:
    """Transform coordinate from one rectangle to another.
    
    Args:
        point: (x, y) in source coordinates
        from_rect: Source rectangle (x, y, width, height)
        to_rect: Target rectangle (x, y, width, height)
        preserve_aspect: Maintain aspect ratio
    
    Returns:
        Transformed (x, y)
    """
    fx, fy, fw, fh = from_rect
    tx, ty, tw, th = to_rect
    
    x = (point[0] - fx) / fw
    y = (point[1] - fy) / fh
    
    if preserve_aspect:
        from_aspect = fw / fh
        to_aspect = tw / th
        
        if from_aspect > to_aspect:
            scale = tw / fw
            y_offset = (th - fh * scale) / 2
            x = tx + x * tw
            y = ty + y_offset + y * fh * scale
        else:
            scale = th / fh
            x_offset = (tw - fw * scale) / 2
            x = tx + x_offset + x * fw * scale
            y = ty + y * th
    else:
        x = tx + x * tw
        y = ty + y * th
    
    return (x, y)


def clamp_point(
    point: Point,
    bounds: Rect,
) -> Point:
    """Clamp point to bounds rectangle.
    
    Args:
        point: (x, y)
        bounds: (x, y, width, height)
    
    Returns:
        Clamped (x, y)
    """
    bx, by, bw, bh = bounds
    
    x = max(bx, min(point[0], bx + bw))
    y = max(by, min(point[1], by + bh))
    
    return (x, y)


def distance_point_to_rect(point: Point, rect: Rect) -> float:
    """Calculate minimum distance from point to rectangle.
    
    Args:
        point: (x, y)
        rect: (x, y, width, height)
    
    Returns:
        Minimum distance
    """
    px, py = point
    rx, ry, rw, rh = rect
    
    dx = max(rx - px, 0, px - (rx + rw))
    dy = max(ry - py, 0, py - (ry + rh))
    
    return math.sqrt(dx * dx + dy * dy)
