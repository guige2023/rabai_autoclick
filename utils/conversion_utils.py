"""Conversion utilities for unit transformations and format conversions.

This module provides utilities for converting between different units,
coordinate systems, and data formats commonly used in UI automation.
"""

from __future__ import annotations

from typing import Any
from enum import Enum
import base64
import json


class CoordinateSystem(Enum):
    """2D coordinate system types."""
    CARTESIAN = "cartesian"
    POLAR = "polar"
    SCREEN = "screen"


@dataclass
class Point2D:
    """2D point representation.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
    """
    x: float
    y: float


@dataclass
class Size2D:
    """2D size representation.

    Attributes:
        width: Width value.
        height: Height value.
    """
    width: float
    height: float


def dpi_scale(value: float, from_dpi: int, to_dpi: int) -> float:
    """Scale a value from one DPI to another.

    Args:
        value: Value to scale.
        from_dpi: Source DPI.
        to_dpi: Target DPI.

    Returns:
        Scaled value.
    """
    if from_dpi == 0:
        return value
    return value * (to_dpi / from_dpi)


def points_to_pixels(points: float, dpi: int = 72) -> int:
    """Convert points to pixels.

    Args:
        points: Value in points.
        dpi: DPI for conversion (default 72).

    Returns:
        Pixel value.
    """
    return int(points * dpi / 72.0)


def pixels_to_points(pixels: int, dpi: int = 72) -> float:
    """Convert pixels to points.

    Args:
        pixels: Pixel value.
        dpi: DPI for conversion (default 72).

    Returns:
        Point value.
    """
    return pixels * 72.0 / dpi


def screen_to_cartesian(x: float, y: float, screen_height: float) -> Point2D:
    """Convert screen coordinates to Cartesian coordinates.

    Screen coordinates have origin at top-left, Y increases downward.
    Cartesian coordinates have origin at bottom-left, Y increases upward.

    Args:
        x: Screen X coordinate.
        y: Screen Y coordinate.
        screen_height: Total screen height.

    Returns:
        Point2D in Cartesian coordinates.
    """
    return Point2D(x=x, y=screen_height - y)


def cartesian_to_screen(x: float, y: float, screen_height: float) -> Point2D:
    """Convert Cartesian coordinates to screen coordinates.

    Args:
        x: Cartesian X coordinate.
        y: Cartesian Y coordinate.
        screen_height: Total screen height.

    Returns:
        Point2D in screen coordinates.
    """
    return Point2D(x=x, y=screen_height - y)


def cartesian_to_polar(x: float, y: float) -> tuple[float, float]:
    """Convert Cartesian to polar coordinates.

    Args:
        x: X coordinate.
        y: Y coordinate.

    Returns:
        Tuple of (radius, angle_degrees).
    """
    import math
    radius = math.sqrt(x * x + y * y)
    angle = math.degrees(math.atan2(y, x))
    return (radius, angle)


def polar_to_cartesian(radius: float, angle_degrees: float) -> Point2D:
    """Convert polar to Cartesian coordinates.

    Args:
        radius: Radius distance.
        angle_degrees: Angle in degrees.

    Returns:
        Point2D in Cartesian coordinates.
    """
    import math
    angle_rad = math.radians(angle_degrees)
    return Point2D(
        x=radius * math.cos(angle_rad),
        y=radius * math.sin(angle_rad)
    )


def mm_to_inches(mm: float) -> float:
    """Convert millimeters to inches.

    Args:
        mm: Millimeter value.

    Returns:
        Inch value.
    """
    return mm / 25.4


def inches_to_mm(inches: float) -> float:
    """Convert inches to millimeters.

    Args:
        inches: Inch value.

    Returns:
        Millimeter value.
    """
    return inches * 25.4


def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert Celsius to Fahrenheit.

    Args:
        celsius: Temperature in Celsius.

    Returns:
        Temperature in Fahrenheit.
    """
    return celsius * 9.0 / 5.0 + 32.0


def fahrenheit_to_celsius(fahrenheit: float) -> float:
    """Convert Fahrenheit to Celsius.

    Args:
        fahrenheit: Temperature in Fahrenheit.

    Returns:
        Temperature in Celsius.
    """
    return (fahrenheit - 32.0) * 5.0 / 9.0


def degrees_to_radians(degrees: float) -> float:
    """Convert degrees to radians.

    Args:
        degrees: Angle in degrees.

    Returns:
        Angle in radians.
    """
    import math
    return degrees * (math.pi / 180.0)


def radians_to_degrees(radians: float) -> float:
    """Convert radians to degrees.

    Args:
        radians: Angle in radians.

    Returns:
        Angle in degrees.
    """
    import math
    return radians * (180.0 / math.pi)


def bytes_to_human(bytes_val: int) -> str:
    """Convert bytes to human-readable string.

    Args:
        bytes_val: Number of bytes.

    Returns:
        Human-readable string like "1.5 MB".
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(bytes_val)
    unit_idx = 0

    while size >= 1024.0 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1

    if unit_idx == 0:
        return f"{int(size)} {units[unit_idx]}"
    return f"{size:.1f} {units[unit_idx]}"


def human_to_bytes(human_str: str) -> int:
    """Convert human-readable size string to bytes.

    Args:
        human_str: String like "1.5 MB" or "1GB".

    Returns:
        Number of bytes.
    """
    units = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 ** 2,
        "GB": 1024 ** 3,
        "TB": 1024 ** 4,
    }

    human_str = human_str.strip().upper()

    for unit, multiplier in units.items():
        if human_str.endswith(unit):
            try:
                num_str = human_str[:-len(unit)].strip()
                return int(float(num_str) * multiplier)
            except ValueError:
                pass

    try:
        return int(human_str)
    except ValueError:
        return 0


def seconds_to_human(seconds: float) -> str:
    """Convert seconds to human-readable duration.

    Args:
        seconds: Duration in seconds.

    Returns:
        Human-readable string like "1h 30m".
    """
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    if minutes < 60:
        if remaining_seconds == 0:
            return f"{minutes}m"
        return f"{minutes}m {remaining_seconds:.0f}s"

    hours = minutes // 60
    minutes = minutes % 60

    if minutes == 0:
        return f"{hours}h"
    return f"{hours}h {minutes}m"


def encode_base64(data: str | bytes) -> str:
    """Encode data to base64 string.

    Args:
        data: String or bytes to encode.

    Returns:
        Base64 encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b64encode(data).decode("ascii")


def decode_base64(data: str) -> bytes:
    """Decode base64 string to bytes.

    Args:
        data: Base64 encoded string.

    Returns:
        Decoded bytes.
    """
    return base64.b64decode(data)


def encode_json(data: Any) -> str:
    """Encode data to JSON string.

    Args:
        data: Data to encode.

    Returns:
        JSON string.
    """
    return json.dumps(data, ensure_ascii=False)


def decode_json(data: str) -> Any:
    """Decode JSON string to Python object.

    Args:
        data: JSON string.

    Returns:
        Decoded Python object.
    """
    return json.loads(data)


def normalize_path(path: str) -> str:
    """Normalize file path separators.

    Args:
        path: File path with any separator style.

    Returns:
        Normalized path with forward slashes.
    """
    return path.replace("\\", "/")


def join_paths(*parts: str) -> str:
    """Join path parts with normalized separators.

    Args:
        parts: Path components to join.

    Returns:
        Joined path.
    """
    return "/".join(str(p).strip("/") for p in parts if p)
