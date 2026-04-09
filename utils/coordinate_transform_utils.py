"""
Coordinate system transformation utilities.

Handles conversion between screen coordinates, window coordinates,
element-relative coordinates, and various display scaling factors.

Author: Auto-generated
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


@dataclass
class Point:
    """A 2D point with x and y coordinates."""
    x: float
    y: float
    
    def __add__(self, other: Point) -> Point:
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: Point) -> Point:
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> Point:
        return Point(self.x * scalar, self.y * scalar)
    
    def distance_to(self, other: Point) -> float:
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)
    
    def to_tuple(self) -> tuple[float, float]:
        return (self.x, self.y)


@dataclass
class Rect:
    """A rectangle defined by origin and size."""
    x: float
    y: float
    width: float
    height: float
    
    @property
    def left(self) -> float:
        return self.x
    
    @property
    def top(self) -> float:
        return self.y
    
    @property
    def right(self) -> float:
        return self.x + self.width
    
    @property
    def bottom(self) -> float:
        return self.y + self.height
    
    @property
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def center_x(self) -> float:
        return self.x + self.width / 2
    
    @property
    def center_y(self) -> float:
        return self.y + self.height / 2
    
    def contains_point(self, x: float, y: float) -> bool:
        return self.left <= x <= self.right and self.top <= y <= self.bottom
    
    def contains_point_relative(
        self, x: float, y: float, parent: Rect
    ) -> bool:
        """Check if point is within rect when parent has given bounds."""
        abs_x = parent.x + x
        abs_y = parent.y + y
        return self.contains_point(abs_x, abs_y)
    
    def to_tuple(self) -> tuple[float, float, float, float]:
        return (self.x, self.y, self.width, self.height)


@dataclass
class TransformMatrix:
    """
    2D affine transformation matrix.
    
    Matrix layout:
    [a c tx]
    [b d ty]
    [0 0 1]
    
    Transforms point (x, y) to (a*x + c*y + tx, b*x + d*y + ty)
    """
    a: float = 1.0
    b: float = 0.0
    c: float = 0.0
    d: float = 1.0
    tx: float = 0.0
    ty: float = 0.0
    
    @classmethod
    def identity(cls) -> TransformMatrix:
        """Create identity transformation."""
        return cls(a=1.0, d=1.0)
    
    @classmethod
    def translation(cls, tx: float, ty: float) -> TransformMatrix:
        """Create translation transformation."""
        return cls(tx=tx, ty=ty)
    
    @classmethod
    def scale(cls, sx: float, sy: float) -> TransformMatrix:
        """Create scale transformation."""
        return cls(a=sx, d=sy)
    
    @classmethod
    def rotation(cls, angle_radians: float) -> TransformMatrix:
        """Create rotation transformation."""
        cos_a = math.cos(angle_radians)
        sin_a = math.sin(angle_radians)
        return cls(a=cos_a, b=sin_a, c=-sin_a, d=cos_a)
    
    def transform_point(self, x: float, y: float) -> Point:
        """Apply transformation to a point."""
        return Point(
            self.a * x + self.c * y + self.tx,
            self.b * x + self.d * y + self.ty,
        )
    
    def transform_points(
        self, points: Sequence[tuple[float, float]]
    ) -> list[Point]:
        """Apply transformation to multiple points."""
        return [self.transform_point(x, y) for x, y in points]
    
    def invert(self) -> TransformMatrix:
        """Compute inverse transformation."""
        det = self.a * self.d - self.b * self.c
        if abs(det) < 1e-10:
            raise ValueError("Matrix is not invertible")
        
        inv_det = 1.0 / det
        return TransformMatrix(
            a=self.d * inv_det,
            b=-self.b * inv_det,
            c=-self.c * inv_det,
            d=self.a * inv_det,
            tx=(self.c * self.ty - self.d * self.tx) * inv_det,
            ty=(self.b * self.tx - self.a * self.ty) * inv_det,
        )
    
    def compose(self, other: TransformMatrix) -> TransformMatrix:
        """Compose this transformation with another."""
        return TransformMatrix(
            a=self.a * other.a + self.c * other.b,
            b=self.b * other.a + self.d * other.b,
            c=self.a * other.c + self.c * other.d,
            d=self.b * other.c + self.d * other.d,
            tx=self.a * other.tx + self.c * other.ty + self.tx,
            ty=self.b * other.tx + self.d * other.ty + self.ty,
        )


class CoordinateTransformer:
    """
    Transforms coordinates between different coordinate systems.
    
    Supports:
    - Screen to window coordinates
    - Window to screen coordinates
    - Element-relative to absolute coordinates
    - Display DPI scaling transformations
    """
    
    def __init__(self, screen_bounds: Rect, window_bounds: Rect):
        self._screen_bounds = screen_bounds
        self._window_bounds = window_bounds
    
    @classmethod
    def from_display_info(
        cls,
        screen_width: float,
        screen_height: float,
        window_x: float,
        window_y: float,
        window_width: float,
        window_height: float,
    ) -> CoordinateTransformer:
        """Create transformer from display information."""
        return cls(
            screen_bounds=Rect(0, 0, screen_width, screen_height),
            window_bounds=Rect(window_x, window_y, window_width, window_height),
        )
    
    def screen_to_window(self, x: float, y: float) -> Point:
        """Convert screen coordinates to window coordinates."""
        return Point(
            x - self._window_bounds.x,
            y - self._window_bounds.y,
        )
    
    def window_to_screen(self, x: float, y: float) -> Point:
        """Convert window coordinates to screen coordinates."""
        return Point(
            x + self._window_bounds.x,
            y + self._window_bounds.y,
        )
    
    def element_to_window(
        self,
        element_bounds: Rect,
        local_x: float,
        local_y: float,
    ) -> Point:
        """Convert element-local coordinates to window coordinates."""
        return Point(
            element_bounds.x + local_x,
            element_bounds.y + local_y,
        )
    
    def element_to_screen(
        self,
        element_bounds: Rect,
        local_x: float,
        local_y: float,
    ) -> Point:
        """Convert element-local coordinates to screen coordinates."""
        window_point = self.element_to_window(element_bounds, local_x, local_y)
        return self.window_to_screen(window_point.x, window_point.y)
    
    def normalize_coordinates(
        self, x: float, y: float, source_bounds: Rect
    ) -> Point:
        """
        Normalize coordinates relative to source bounds.
        
        Returns coordinates in range [0, 1].
        """
        return Point(
            (x - source_bounds.x) / source_bounds.width,
            (y - source_bounds.y) / source_bounds.height,
        )
    
    def denormalize_coordinates(
        self, normalized_x: float, normalized_y: float, target_bounds: Rect
    ) -> Point:
        """
        Convert normalized [0, 1] coordinates to absolute coordinates.
        """
        return Point(
            target_bounds.x + normalized_x * target_bounds.width,
            target_bounds.y + normalized_y * target_bounds.height,
        )


def transform_rect(
    matrix: TransformMatrix,
    rect: Rect,
) -> Rect:
    """
    Transform a rectangle using a transformation matrix.
    
    Computes the bounding box of all four corners transformed.
    """
    corners = [
        (rect.x, rect.y),
        (rect.x + rect.width, rect.y),
        (rect.x, rect.y + rect.height),
        (rect.x + rect.width, rect.y + rect.height),
    ]
    
    transformed = matrix.transform_points(corners)
    
    min_x = min(p.x for p in transformed)
    max_x = max(p.x for p in transformed)
    min_y = min(p.y for p in transformed)
    max_y = max(p.y for p in transformed)
    
    return Rect(min_x, min_y, max_x - min_x, max_y - min_y)


def clip_point_to_rect(x: float, y: float, rect: Rect) -> Point:
    """
    Clip a point to be within a rectangle.
    
    Returns the closest point on or within the rectangle.
    """
    clipped_x = max(rect.left, min(rect.right, x))
    clipped_y = max(rect.top, min(rect.bottom, y))
    return Point(clipped_x, clipped_y)


def clip_rect_to_bounds(rect: Rect, bounds: Rect) -> Rect | None:
    """
    Clip a rectangle to be within bounds.
    
    Returns None if the rectangles don't intersect.
    """
    left = max(rect.left, bounds.left)
    top = max(rect.top, bounds.top)
    right = min(rect.right, bounds.right)
    bottom = min(rect.bottom, bounds.bottom)
    
    if left >= right or top >= bottom:
        return None
    
    return Rect(left, top, right - left, bottom - top)
