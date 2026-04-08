"""Coordinate normalization and transformation utilities.

This module provides utilities for normalizing and transforming
coordinates between different coordinate systems and reference frames.
"""

from __future__ import annotations

import math
from typing import NamedTuple


class Point(NamedTuple):
    """A 2D point."""
    x: float
    y: float
    
    def distance_to(self, other: "Point") -> float:
        """Calculate Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)
    
    def angle_to(self, other: "Point") -> float:
        """Calculate angle in radians from this point to another."""
        return math.atan2(other.y - self.y, other.x - self.x)
    
    def transform(self, matrix: "TransformMatrix") -> "Point":
        """Apply a transformation matrix to this point."""
        return matrix.apply_to_point(self)


class TransformMatrix:
    """2D affine transformation matrix."""
    
    def __init__(
        self,
        a: float = 1, b: float = 0,
        c: float = 0, d: float = 1,
        tx: float = 0, ty: float = 0,
    ):
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.tx = tx
        self.ty = ty
    
    @classmethod
    def identity(cls) -> "TransformMatrix":
        """Create an identity matrix."""
        return cls()
    
    @classmethod
    def translation(cls, tx: float, ty: float) -> "TransformMatrix":
        """Create a translation matrix."""
        return cls(tx=tx, ty=ty)
    
    @classmethod
    def scaling(cls, sx: float, sy: float) -> "TransformMatrix":
        """Create a scaling matrix."""
        return cls(a=sx, d=sy)
    
    @classmethod
    def rotation(cls, angle: float) -> "TransformMatrix":
        """Create a rotation matrix.
        
        Args:
            angle: Rotation angle in radians.
        """
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        return cls(a=cos_a, b=sin_a, c=-sin_a, d=cos_a)
    
    def apply_to_point(self, point: Point) -> Point:
        """Apply this transformation to a point."""
        x = self.a * point.x + self.c * point.y + self.tx
        y = self.b * point.x + self.d * point.y + self.ty
        return Point(x, y)
    
    def multiply(self, other: "TransformMatrix") -> "TransformMatrix":
        """Multiply this matrix with another."""
        return TransformMatrix(
            a=self.a * other.a + self.b * other.c,
            b=self.a * other.b + self.b * other.d,
            c=self.c * other.a + self.d * other.c,
            d=self.c * other.b + self.d * other.d,
            tx=self.tx * other.a + self.ty * other.c + other.tx,
            ty=self.tx * other.b + self.ty * other.d + other.ty,
        )
    
    def invert(self) -> "TransformMatrix":
        """Return the inverse of this matrix."""
        det = self.a * self.d - self.b * self.c
        if det == 0:
            raise ValueError("Matrix is not invertible")
        inv_det = 1.0 / det
        return TransformMatrix(
            a=self.d * inv_det,
            b=-self.b * inv_det,
            c=-self.c * inv_det,
            d=self.a * inv_det,
            tx=(self.ty * self.c - self.tx * self.d) * inv_det,
            ty=(self.tx * self.b - self.ty * self.a) * inv_det,
        )


def normalize_coordinates(
    x: float,
    y: float,
    source_width: int,
    source_height: int,
    target_width: int,
    target_height: int,
) -> tuple[int, int]:
    """Normalize coordinates from one resolution to another.
    
    Args:
        x: Source X coordinate.
        y: Source Y coordinate.
        source_width: Source width.
        source_height: Source height.
        target_width: Target width.
        target_height: Target height.
    
    Returns:
        Tuple of (normalized_x, normalized_y).
    """
    norm_x = int(x * target_width / source_width)
    norm_y = int(y * target_height / source_height)
    return (norm_x, norm_y)


def denormalize_coordinates(
    norm_x: float,
    norm_y: float,
    source_width: int,
    source_height: int,
    target_width: int,
    target_height: int,
) -> tuple[int, int]:
    """Convert normalized coordinates back to source coordinates.
    
    Args:
        norm_x: Normalized X (0.0 to 1.0).
        norm_y: Normalized Y (0.0 to 1.0).
        source_width: Source width.
        source_height: Source height.
        target_width: Target width.
        target_height: Target height.
    
    Returns:
        Tuple of (x, y) in source coordinates.
    """
    x = int(norm_x * source_width / target_width)
    y = int(norm_y * source_height / target_height)
    return (x, y)


def flip_coordinates(
    x: int,
    y: int,
    width: int,
    height: int,
    horizontal: bool = True,
    vertical: bool = False,
) -> tuple[int, int]:
    """Flip coordinates across an axis.
    
    Args:
        x: Source X coordinate.
        y: Source Y coordinate.
        width: Total width.
        height: Total height.
        horizontal: Flip horizontally.
        vertical: Flip vertically.
    
    Returns:
        Tuple of (flipped_x, flipped_y).
    """
    new_x = width - x - 1 if horizontal else x
    new_y = height - y - 1 if vertical else y
    return (new_x, new_y)


def rotate_coordinates(
    x: int,
    y: int,
    width: int,
    height: int,
    rotation: int,
) -> tuple[int, int]:
    """Rotate coordinates by 90-degree increments.
    
    Args:
        x: Source X coordinate.
        y: Source Y coordinate.
        width: Total width.
        height: Total height.
        rotation: Rotation in degrees (0, 90, 180, 270).
    
    Returns:
        Tuple of (rotated_x, rotated_y).
    """
    rotation = rotation % 360
    
    if rotation == 0:
        return (x, y)
    elif rotation == 90:
        return (height - y - 1, x)
    elif rotation == 180:
        return (width - x - 1, height - y - 1)
    elif rotation == 270:
        return (y, width - x - 1)
    return (x, y)


def clamp_to_bounds(
    x: int,
    y: int,
    min_x: int,
    min_y: int,
    max_x: int,
    max_y: int,
) -> tuple[int, int]:
    """Clamp coordinates to a bounding box.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        min_x: Minimum X.
        min_y: Minimum Y.
        max_x: Maximum X.
        max_y: Maximum Y.
    
    Returns:
        Tuple of (clamped_x, clamped_y).
    """
    return (
        max(min_x, min(max_x, x)),
        max(min_y, min(max_y, y)),
    )


def polar_to_cartesian(
    radius: float,
    angle: float,
    center_x: float = 0,
    center_y: float = 0,
) -> tuple[float, float]:
    """Convert polar coordinates to Cartesian.
    
    Args:
        radius: Distance from center.
        angle: Angle in radians.
        center_x: Center X coordinate.
        center_y: Center Y coordinate.
    
    Returns:
        Tuple of (x, y).
    """
    x = center_x + radius * math.cos(angle)
    y = center_y + radius * math.sin(angle)
    return (x, y)
