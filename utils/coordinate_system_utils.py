"""Coordinate system transformation utilities for UI automation.

This module provides utilities for transforming coordinates between
different reference frames, including screen, window, and element-local
coordinate systems.
"""

from __future__ import annotations

from typing import NamedTuple
from dataclasses import dataclass
import math


class ReferenceFrame(NamedTuple):
    """Reference frame definition.

    Attributes:
        origin_x: X coordinate of origin in parent frame.
        origin_y: Y coordinate of origin in parent frame.
        scale_x: X scale factor.
        scale_y: Y scale factor.
        rotation: Rotation in degrees.
        parent: Parent reference frame or None for root.
    """
    origin_x: float
    origin_y: float
    scale_x: float = 1.0
    scale_y: float = 1.0
    rotation: float = 0.0
    parent: ReferenceFrame | None = None


@dataclass
class Transform2D:
    """2D affine transformation.

    Attributes:
        m11: Scale X component.
        m12: Shear Y component.
        m21: Shear X component.
        m22: Scale Y component.
        tx: Translate X component.
        ty: Translate Y component.
    """
    m11: float = 1.0
    m12: float = 0.0
    m21: float = 0.0
    m22: float = 1.0
    tx: float = 0.0
    ty: float = 0.0

    @staticmethod
    def identity() -> Transform2D:
        """Create identity transform."""
        return Transform2D()

    @staticmethod
    def translation(tx: float, ty: float) -> Transform2D:
        """Create translation transform."""
        return Transform2D(tx=tx, ty=ty)

    @staticmethod
    def scaling(sx: float, sy: float) -> Transform2D:
        """Create scaling transform."""
        return Transform2D(m11=sx, m22=sy)

    @staticmethod
    def rotation(angle_degrees: float) -> Transform2D:
        """Create rotation transform."""
        angle_rad = math.radians(angle_degrees)
        cos_a = math.cos(angle_rad)
        sin_a = math.sin(angle_rad)
        return Transform2D(m11=cos_a, m12=-sin_a, m21=sin_a, m22=cos_a)

    def compose(self, other: Transform2D) -> Transform2D:
        """Compose this transform with another.

        Args:
            other: Transform to apply after this one.

        Returns:
            Combined transform.
        """
        return Transform2D(
            m11=self.m11 * other.m11 + self.m12 * other.m21,
            m12=self.m11 * other.m12 + self.m12 * other.m22,
            m21=self.m21 * other.m11 + self.m22 * other.m21,
            m22=self.m21 * other.m12 + self.m22 * other.m22,
            tx=self.m11 * other.tx + self.m12 * other.ty + self.tx,
            ty=self.m21 * other.tx + self.m22 * other.ty + self.ty
        )

    def transform_point(self, x: float, y: float) -> tuple[float, float]:
        """Transform a point.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            Transformed point coordinates.
        """
        return (
            self.m11 * x + self.m12 * y + self.tx,
            self.m21 * x + self.m22 * y + self.ty
        )

    def inverse(self) -> Transform2D:
        """Compute inverse transform.

        Returns:
            Inverse transform.
        """
        det = self.m11 * self.m22 - self.m12 * self.m21

        if abs(det) < 1e-10:
            return Transform2D()

        inv_det = 1.0 / det

        return Transform2D(
            m11=self.m22 * inv_det,
            m12=-self.m12 * inv_det,
            m21=-self.m21 * inv_det,
            m22=self.m11 * inv_det,
            tx=(self.m12 * self.ty - self.m22 * self.tx) * inv_det,
            ty=(self.m21 * self.tx - self.m11 * self.ty) * inv_det
        )


def screen_to_window(
    screen_x: float,
    screen_y: float,
    window_origin_x: float,
    window_origin_y: float
) -> tuple[float, float]:
    """Convert screen coordinates to window coordinates.

    Args:
        screen_x: Screen X coordinate.
        screen_y: Screen Y coordinate.
        window_origin_x: Window origin X in screen coordinates.
        window_origin_y: Window origin Y in screen coordinates.

    Returns:
        Tuple of (window_x, window_y).
    """
    return (screen_x - window_origin_x, screen_y - window_origin_y)


def window_to_screen(
    window_x: float,
    window_y: float,
    window_origin_x: float,
    window_origin_y: float
) -> tuple[float, float]:
    """Convert window coordinates to screen coordinates.

    Args:
        window_x: Window X coordinate.
        window_y: Window Y coordinate.
        window_origin_x: Window origin X in screen coordinates.
        window_origin_y: Window origin Y in screen coordinates.

    Returns:
        Tuple of (screen_x, screen_y).
    """
    return (window_x + window_origin_x, window_y + window_origin_y)


def element_to_window(
    element_x: float,
    element_y: float,
    element_origin_x: float,
    element_origin_y: float
) -> tuple[float, float]:
    """Convert element-local coordinates to window coordinates.

    Args:
        element_x: Element-local X coordinate.
        element_y: Element-local Y coordinate.
        element_origin_x: Element origin X in window coordinates.
        element_origin_y: Element origin Y in window coordinates.

    Returns:
        Tuple of (window_x, window_y).
    """
    return (element_x + element_origin_x, element_y + element_origin_y)


def window_to_element(
    window_x: float,
    window_y: float,
    element_origin_x: float,
    element_origin_y: float
) -> tuple[float, float]:
    """Convert window coordinates to element-local coordinates.

    Args:
        window_x: Window X coordinate.
        window_y: Window Y coordinate.
        element_origin_x: Element origin X in window coordinates.
        element_origin_y: Element origin Y in window coordinates.

    Returns:
        Tuple of (element_x, element_y).
    """
    return (window_x - element_origin_x, window_y - element_origin_y)


def apply_transform(
    x: float,
    y: float,
    transform: Transform2D
) -> tuple[float, float]:
    """Apply transformation to a point.

    Args:
        x: X coordinate.
        y: Y coordinate.
        transform: Transformation to apply.

    Returns:
        Transformed coordinates.
    """
    return transform.transform_point(x, y)


def inverse_transform(
    x: float,
    y: float,
    transform: Transform2D
) -> tuple[float, float]:
    """Apply inverse transformation to a point.

    Args:
        x: X coordinate.
        y: Y coordinate.
        transform: Transformation to invert.

    Returns:
        Original coordinates after inverse transform.
    """
    inv = transform.inverse()
    return inv.transform_point(x, y)


def compose_transforms(transforms: list[Transform2D]) -> Transform2D:
    """Compose multiple transforms into one.

    Args:
        transforms: List of transforms (first to last).

    Returns:
        Combined transform.
    """
    if not transforms:
        return Transform2D.identity()

    result = transforms[0]
    for t in transforms[1:]:
        result = result.compose(t)

    return result


def rotate_point_around_origin(
    x: float,
    y: float,
    angle_degrees: float
) -> tuple[float, float]:
    """Rotate a point around the origin.

    Args:
        x: X coordinate.
        y: Y coordinate.
        angle_degrees: Rotation angle in degrees.

    Returns:
        Rotated coordinates.
    """
    angle_rad = math.radians(angle_degrees)
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)

    return (
        x * cos_a - y * sin_a,
        x * sin_a + y * cos_a
    )


def rotate_point_around_center(
    x: float,
    y: float,
    cx: float,
    cy: float,
    angle_degrees: float
) -> tuple[float, float]:
    """Rotate a point around a center point.

    Args:
        x: X coordinate.
        y: Y coordinate.
        cx: Center X coordinate.
        cy: Center Y coordinate.
        angle_degrees: Rotation angle in degrees.

    Returns:
        Rotated coordinates.
    """
    translated_x = x - cx
    translated_y = y - cy

    rotated_x, rotated_y = rotate_point_around_origin(
        translated_x, translated_y, angle_degrees
    )

    return (rotated_x + cx, rotated_y + cy)


def scale_point_from_origin(
    x: float,
    y: float,
    sx: float,
    sy: float
) -> tuple[float, float]:
    """Scale a point from the origin.

    Args:
        x: X coordinate.
        y: Y coordinate.
        sx: X scale factor.
        sy: Y scale factor.

    Returns:
        Scaled coordinates.
    """
    return (x * sx, y * sy)


def scale_point_from_center(
    x: float,
    y: float,
    cx: float,
    cy: float,
    sx: float,
    sy: float
) -> tuple[float, float]:
    """Scale a point from a center point.

    Args:
        x: X coordinate.
        y: Y coordinate.
        cx: Center X coordinate.
        cy: Center Y coordinate.
        sx: X scale factor.
        sy: Y scale factor.

    Returns:
        Scaled coordinates.
    """
    translated_x = x - cx
    translated_y = y - cy

    scaled_x = translated_x * sx
    scaled_y = translated_y * sy

    return (scaled_x + cx, scaled_y + cy)


def clamp_point(
    x: float,
    y: float,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float
) -> tuple[float, float]:
    """Clamp point coordinates to a bounding box.

    Args:
        x: X coordinate.
        y: Y coordinate.
        min_x: Minimum X bound.
        min_y: Minimum Y bound.
        max_x: Maximum X bound.
        max_y: Maximum Y bound.

    Returns:
        Tuple of (clamped_x, clamped_y).
    """
    return (
        max(min_x, min(max_x, x)),
        max(min_y, min(max_y, y))
    )
