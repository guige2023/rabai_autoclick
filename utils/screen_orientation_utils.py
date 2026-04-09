"""
Screen orientation utilities for rotation-aware automation.

This module provides utilities for handling screen orientation changes
and adapting automation actions accordingly.
"""

from __future__ import annotations

import platform
from typing import Tuple, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto


IS_MACOS: bool = platform.system() == 'Darwin'


class Orientation(Enum):
    """Screen orientation states."""
    PORTRAIT = auto()
    PORTRAIT_UPSIDE_DOWN = auto()
    LANDSCAPE_LEFT = auto()
    LANDSCAPE_RIGHT = auto()
    FACE_UP = auto()
    FACE_DOWN = auto()
    UNKNOWN = auto()


class OrientationAngle(Enum):
    """Orientation angles in degrees."""
    ANGLE_0 = 0
    ANGLE_90 = 90
    ANGLE_180 = 180
    ANGLE_270 = 270


@dataclass
class OrientationState:
    """
    Current screen orientation state.

    Attributes:
        orientation: Current orientation enum.
        angle: Rotation angle in degrees.
        is_rotated: Whether screen is rotated from default.
        is_portrait: Whether in portrait mode.
        native_width: Native width in pixels.
        native_height: Native height in pixels.
    """
    orientation: Orientation = Orientation.UNKNOWN
    angle: int = 0
    is_rotated: bool = False
    is_portrait: bool = True
    native_width: int = 0
    native_height: int = 0

    @property
    def effective_width(self) -> int:
        """Effective width considering rotation."""
        if self.is_rotated:
            return self.native_height
        return self.native_width

    @property
    def effective_height(self) -> int:
        """Effective height considering rotation."""
        if self.is_rotated:
            return self.native_width
        return self.native_height

    def effective_resolution(self) -> Tuple[int, int]:
        """Get effective (width, height)."""
        return (self.effective_width, self.effective_height)


@dataclass
class RotationMatrix:
    """
    3x3 rotation matrix for 2D coordinate transformation.

    Standard format:
    [[cos, -sin, tx],
     [sin,  cos, ty],
     [0,    0,   1 ]]
    """
    a: float  # cos(angle)
    b: float  # -sin(angle)
    c: float  # sin(angle)
    d: float  # cos(angle)
    tx: float  # translate x
    ty: float  # translate y

    @classmethod
    def identity(cls) -> RotationMatrix:
        """Create identity matrix."""
        return cls(a=1.0, b=0.0, c=0.0, d=1.0, tx=0.0, ty=0.0)

    @classmethod
    def rotation(cls, angle_degrees: float) -> RotationMatrix:
        """Create rotation matrix."""
        import math
        rad = math.radians(angle_degrees)
        cos_a = math.cos(rad)
        sin_a = math.sin(rad)
        return cls(a=cos_a, b=-sin_a, c=sin_a, d=cos_a, tx=0.0, ty=0.0)

    @classmethod
    def translation(cls, tx: float, ty: float) -> RotationMatrix:
        """Create translation matrix."""
        return cls(a=1.0, b=0.0, c=0.0, d=1.0, tx=tx, ty=ty)

    def transform_point(self, x: int, y: int) -> Tuple[int, int]:
        """Apply matrix transformation to a point."""
        nx = self.a * x + self.b * y + self.tx
        ny = self.c * x + self.d * y + self.ty
        return (int(nx), int(ny))

    def transform_points(self, points: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        """Transform multiple points."""
        return [self.transform_point(x, y) for x, y in points]

    def multiply(self, other: RotationMatrix) -> RotationMatrix:
        """Multiply this matrix with another."""
        return RotationMatrix(
            a=self.a * other.a + self.b * other.c,
            b=self.a * other.b + self.b * other.d,
            c=self.c * other.a + self.d * other.c,
            d=self.c * other.b + self.d * other.d,
            tx=self.a * other.tx + self.b * other.ty + self.tx,
            ty=self.c * other.tx + self.d * other.ty + self.ty,
        )

    def invert(self) -> RotationMatrix:
        """Return the inverse matrix."""
        import math
        det = self.a * self.d - self.b * self.c
        if abs(det) < 1e-10:
            return self.identity()
        inv_det = 1.0 / det
        return RotationMatrix(
            a=self.d * inv_det,
            b=-self.b * inv_det,
            c=-self.c * inv_det,
            d=self.a * inv_det,
            tx=(self.b * self.ty - self.d * self.tx) * inv_det,
            ty=(self.c * self.tx - self.a * self.ty) * inv_det,
        )


def get_current_orientation() -> OrientationState:
    """
    Get the current screen orientation state.

    Returns:
        OrientationState for the current orientation.
    """
    if IS_MACOS:
        return _get_macos_orientation()
    else:
        return _get_fallback_orientation()


def _get_macos_orientation() -> OrientationState:
    """Get screen orientation on macOS."""
    from Cocoa import NSScreen
    screen = NSScreen.mainScreen()
    f = screen.frame()
    w, h = int(f.size.width), int(f.size.height)

    # macOS doesn't have physical device orientation like iOS
    # but we can check if display is rotated
    is_portrait = h > w
    angle = 0 if not is_portrait else 90

    orientation = Orientation.PORTRAIT if is_portrait else Orientation.LANDSCAPE_RIGHT

    return OrientationState(
        orientation=orientation,
        angle=angle,
        is_rotated=False,
        is_portrait=is_portrait,
        native_width=w,
        native_height=h,
    )


def _get_fallback_orientation() -> OrientationState:
    """Get fallback orientation."""
    import pyautogui
    w, h = pyautogui.size()
    is_portrait = h > w
    return OrientationState(
        orientation=Orientation.PORTRAIT if is_portrait else Orientation.LANDSCAPE_RIGHT,
        angle=0,
        is_rotated=False,
        is_portrait=is_portrait,
        native_width=w,
        native_height=h,
    )


def rotate_coordinates_for_orientation(
    x: int, y: int,
    from_orientation: Orientation,
    to_orientation: Orientation,
    width: int, height: int
) -> Tuple[int, int]:
    """
    Rotate coordinates from one orientation to another.

    Args:
        x: Source x coordinate.
        y: Source y coordinate.
        from_orientation: Source orientation.
        to_orientation: Target orientation.
        width: Screen width.
        height: Screen height.

    Returns:
        Tuple of (rotated_x, rotated_y).
    """
    angle_diff = _orientation_angle(to_orientation) - _orientation_angle(from_orientation)
    angle_diff = angle_diff % 360

    if angle_diff == 0:
        return (x, y)
    elif angle_diff == 90:
        return (height - y - 1, x)
    elif angle_diff == 180:
        return (width - x - 1, height - y - 1)
    elif angle_diff == 270:
        return (y, width - x - 1)
    return (x, y)


def _orientation_angle(orientation: Orientation) -> int:
    """Get the rotation angle for an orientation."""
    angle_map = {
        Orientation.PORTRAIT: 0,
        Orientation.LANDSCAPE_RIGHT: 90,
        Orientation.PORTRAIT_UPSIDE_DOWN: 180,
        Orientation.LANDSCAPE_LEFT: 270,
    }
    return angle_map.get(orientation, 0)


def get_rotation_matrix_for_orientation(
    orientation: Orientation,
    width: int, height: int
) -> RotationMatrix:
    """
    Get a rotation matrix for converting coordinates.

    Args:
        orientation: Target orientation.
        width: Screen width.
        height: Screen height.

    Returns:
        RotationMatrix for the orientation.
    """
    angle = _orientation_angle(orientation)

    if angle == 0:
        return RotationMatrix.identity()
    elif angle == 90:
        # Rotate 90 degrees clockwise: swap dimensions
        return RotationMatrix.translation(height, 0).multiply(
            RotationMatrix.rotation(angle)
        )
    elif angle == 180:
        return RotationMatrix.translation(width, height).multiply(
            RotationMatrix.rotation(angle)
        )
    elif angle == 270:
        return RotationMatrix.translation(0, width).multiply(
            RotationMatrix.rotation(angle)
        )
    return RotationMatrix.identity()


def adapt_action_for_orientation(
    action: Callable,
    orientation: Orientation
) -> Callable:
    """
    Wrap an action function to adapt coordinates for orientation.

    Args:
        action: Action function that takes (x, y, ...) coordinates.
        orientation: Target orientation.

    Returns:
        Wrapped action function with automatic coordinate adaptation.
    """
    def wrapped(x: int, y: int, *args, width: int = 1920, height: int = 1080, **kwargs):
        rx, ry = rotate_coordinates_for_orientation(
            x, y, Orientation.PORTRAIT, orientation, width, height
        )
        return action(rx, ry, *args, **kwargs)
    return wrapped
