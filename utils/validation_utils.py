"""Validation utilities for action parameters and automation state.

Provides validation helpers for coordinates, regions, colors,
image files, and other common automation inputs, with clear
error messages and schema validation.

Example:
    >>> from utils.validation_utils import validate_coordinates, validate_region, validate_color
    >>> validate_coordinates((100, 200))
    >>> validate_region((0, 0, 1920, 1080))
    >>> validate_color((255, 0, 0))
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Sequence

__all__ = [
    "ValidationError",
    "validate_coordinates",
    "validate_region",
    "validate_color",
    "validate_image_path",
    "validate_region_against_screen",
    "validate_speed",
    "validate_repeat_count",
    "is_valid_screen_coordinate",
    "is_valid_color",
    "Validator",
]


class ValidationError(ValueError):
    """Raised when validation fails."""

    pass


def validate_coordinates(
    coords: Any,
    name: str = "coordinates",
    allow_none: bool = False,
) -> tuple[float, float]:
    """Validate a (x, y) coordinate pair.

    Args:
        coords: Value to validate.
        name: Name for error messages.
        allow_none: Whether None is acceptable.

    Returns:
        Tuple of (x, y) as floats.

    Raises:
        ValidationError: If validation fails.
    """
    if coords is None:
        if allow_none:
            return None
        raise ValidationError(f"{name} cannot be None")

    if not isinstance(coords, (tuple, list)):
        raise ValidationError(
            f"{name} must be a tuple or list, got {type(coords).__name__}"
        )

    if len(coords) != 2:
        raise ValidationError(
            f"{name} must have exactly 2 elements, got {len(coords)}"
        )

    try:
        x, y = float(coords[0]), float(coords[1])
    except (TypeError, ValueError) as e:
        raise ValidationError(f"{name} elements must be numeric: {e}")

    return (x, y)


def validate_region(
    region: Any,
    name: str = "region",
    allow_none: bool = False,
) -> tuple[float, float, float, float]:
    """Validate a screen region tuple (x, y, width, height).

    Args:
        region: Value to validate.
        name: Name for error messages.
        allow_none: Whether None is acceptable.

    Returns:
        Tuple of (x, y, width, height) as floats.

    Raises:
        ValidationError: If validation fails.
    """
    if region is None:
        if allow_none:
            return None
        raise ValidationError(f"{name} cannot be None")

    if not isinstance(region, (tuple, list)):
        raise ValidationError(
            f"{name} must be a tuple or list, got {type(region).__name__}"
        )

    if len(region) != 4:
        raise ValidationError(
            f"{name} must have exactly 4 elements, got {len(region)}"
        )

    try:
        x, y, w, h = float(region[0]), float(region[1]), float(region[2]), float(region[3])
    except (TypeError, ValueError) as e:
        raise ValidationError(f"{name} elements must be numeric: {e}")

    if w < 0:
        raise ValidationError(f"{name} width must be non-negative, got {w}")
    if h < 0:
        raise ValidationError(f"{name} height must be non-negative, got {h}")

    return (x, y, w, h)


def validate_color(
    color: Any,
    name: str = "color",
    allow_alpha: bool = False,
) -> tuple[int, int, int] | tuple[int, int, int, int]:
    """Validate an RGB or RGBA color tuple.

    Args:
        color: Value to validate (e.g., (255, 0, 0) or (255, 0, 0, 128)).
        name: Name for error messages.
        allow_alpha: Whether to accept a 4-element tuple with alpha.

    Returns:
        Validated color tuple.

    Raises:
        ValidationError: If validation fails.
    """
    if not isinstance(color, (tuple, list)):
        raise ValidationError(
            f"{name} must be a tuple or list, got {type(color).__name__}"
        )

    expected_len = 4 if allow_alpha else 3
    if len(color) not in (3, 4):
        raise ValidationError(
            f"{name} must have {'3 or 4' if allow_alpha else 'exactly 3'} elements"
        )

    for i, component in enumerate(color):
        if not isinstance(component, (int, float)):
            raise ValidationError(
                f"{name}[{i}] must be int/float, got {type(component).__name__}"
            )
        if not 0 <= component <= 255:
            raise ValidationError(
                f"{name}[{i}] must be 0-255, got {component}"
            )

    return tuple(int(c) for c in color)


def validate_image_path(
    path: Any,
    name: str = "image_path",
    must_exist: bool = True,
) -> Path:
    """Validate a path to an image file.

    Args:
        path: Value to validate as an image path.
        name: Name for error messages.
        must_exist: Whether the file must already exist.

    Returns:
        Path object.

    Raises:
        ValidationError: If validation fails.
    """
    if path is None:
        raise ValidationError(f"{name} cannot be None")

    if isinstance(path, Path):
        p = path
    else:
        try:
            p = Path(str(path))
        except Exception as e:
            raise ValidationError(f"{name} is not a valid path: {e}")

    if must_exist:
        if not p.exists():
            raise ValidationError(f"{name} does not exist: {p}")
        if not p.is_file():
            raise ValidationError(f"{name} is not a file: {p}")

    return p


def validate_region_against_screen(
    region: tuple[float, float, float, float],
    screen_bounds: Optional[tuple[float, float, float, float]] = None,
) -> bool:
    """Check that a region is fully within screen bounds.

    Args:
        region: Region as (x, y, width, height).
        screen_bounds: Screen bounds as (x, y, width, height).
            If None, queries the primary display.

    Returns:
        True if the region is entirely within the screen.

    Raises:
        ValidationError: If the region extends beyond the screen.
    """
    if screen_bounds is None:
        try:
            from utils.display_utils import get_primary_display

            primary = get_primary_display()
            if primary is not None:
                screen_bounds = primary.bounds
        except ImportError:
            screen_bounds = (0, 0, 1920, 1080)

    sx, sy, sw, sh = screen_bounds
    rx, ry, rw, rh = region

    if rx < sx:
        raise ValidationError(f"Region x={rx} is below screen left edge {sx}")
    if ry < sy:
        raise ValidationError(f"Region y={ry} is above screen top edge {sy}")
    if rx + rw > sx + sw:
        raise ValidationError(
            f"Region x+w={rx + rw} exceeds screen right edge {sx + sw}"
        )
    if ry + rh > sy + sh:
        raise ValidationError(
            f"Region y+h={ry + rh} exceeds screen bottom edge {sy + sh}"
        )

    return True


def validate_speed(speed: Any) -> float:
    """Validate a playback speed value.

    Args:
        speed: Speed value (1.0 = normal, 2.0 = 2x, etc.).

    Returns:
        Validated speed as float.

    Raises:
        ValidationError: If speed is invalid.
    """
    try:
        s = float(speed)
    except (TypeError, ValueError):
        raise ValidationError(f"speed must be numeric, got {type(speed).__name__}")

    if s <= 0:
        raise ValidationError(f"speed must be positive, got {s}")
    if s > 100:
        raise ValidationError(f"speed {s} is excessively large (max 100)")

    return s


def validate_repeat_count(count: Any) -> int:
    """Validate a repeat count.

    Args:
        count: Number of repetitions.

    Returns:
        Validated count as int.

    Raises:
        ValidationError: If count is invalid.
    """
    try:
        n = int(count)
    except (TypeError, ValueError):
        raise ValidationError(f"repeat count must be int, got {type(count).__name__}")

    if n < 0:
        raise ValidationError(f"repeat count cannot be negative, got {n}")
    if n > 10000:
        raise ValidationError(f"repeat count {n} is excessively large (max 10000)")

    return n


def is_valid_screen_coordinate(x: float, y: float, screen_bounds: Optional[tuple] = None) -> bool:
    """Check if coordinates are within the primary screen.

    Args:
        x: X coordinate.
        y: Y coordinate.
        screen_bounds: Optional screen bounds override.

    Returns:
        True if the point is on-screen.
    """
    try:
        validate_coordinates((x, y))
        validate_region_against_screen((x, y, 1, 1), screen_bounds)
        return True
    except ValidationError:
        return False


def is_valid_color(color: tuple) -> bool:
    """Check if a color tuple is valid RGB.

    Args:
        color: Color tuple to check.

    Returns:
        True if valid.
    """
    try:
        validate_color(color, allow_alpha=False)
        return True
    except ValidationError:
        return False


class Validator:
    """Chainable validation builder.

    Example:
        >>> v = Validator()
        >>> coords = v.validate(coords).is_coordinates().value
        >>> region = v.validate(region).is_region().value
    """

    def __init__(self, value: Any):
        self._value = value
        self._error: Optional[str] = None

    @classmethod
    def validate(cls, value: Any) -> "Validator":
        return cls(value)

    def is_coordinates(self) -> "Validator":
        if self._error:
            return self
        try:
            self._value = validate_coordinates(self._value, allow_none=False)
        except ValidationError as e:
            self._error = str(e)
        return self

    def is_region(self) -> "Validator":
        if self._error:
            return self
        try:
            self._value = validate_region(self._value, allow_none=False)
        except ValidationError as e:
            self._error = str(e)
        return self

    def is_color(self) -> "Validator":
        if self._error:
            return self
        try:
            self._value = validate_color(self._value, allow_alpha=False)
        except ValidationError as e:
            self._error = str(e)
        return self

    def is_image_path(self) -> "Validator":
        if self._error:
            return self
        try:
            self._value = validate_image_path(self._value)
        except ValidationError as e:
            self._error = str(e)
        return self

    def is_positive(self) -> "Validator":
        if self._error:
            return self
        try:
            val = float(self._value)
            if val <= 0:
                self._error = f"value must be positive, got {val}"
        except (TypeError, ValueError) as e:
            self._error = str(e)
        return self

    def in_range(self, min_val: float, max_val: float) -> "Validator":
        if self._error:
            return self
        try:
            val = float(self._value)
            if not (min_val <= val <= max_val):
                self._error = f"value {val} out of range [{min_val}, {max_val}]"
        except (TypeError, ValueError) as e:
            self._error = str(e)
        return self

    @property
    def value(self) -> Any:
        if self._error:
            raise ValidationError(self._error)
        return self._value

    @property
    def is_valid(self) -> bool:
        return self._error is None

    @property
    def error(self) -> Optional[str]:
        return self._error
