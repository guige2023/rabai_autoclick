"""
Coordinate Transform Utilities for UI Automation.

This module provides utilities for transforming coordinates between different
coordinate systems used in UI automation (screen, window, element-relative).

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Optional


@dataclass
class Point:
    """Represents a 2D point."""
    x: float
    y: float
    
    def __add__(self, other: 'Point') -> 'Point':
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: 'Point') -> 'Point':
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> 'Point':
        return Point(self.x * scalar, self.y * scalar)
    
    def distance_to(self, other: 'Point') -> float:
        """Calculate Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return (dx * dx + dy * dy) ** 0.5
    
    def midpoint(self, other: 'Point') -> 'Point':
        """Get the midpoint between this point and another."""
        return Point((self.x + other.x) / 2, (self.y + other.y) / 2)


@dataclass
class Rect:
    """Represents a rectangle in 2D space."""
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
    
    def contains_point(self, point: Point) -> bool:
        """Check if a point is inside this rectangle."""
        return (self.left <= point.x <= self.right and 
                self.top <= point.y <= self.bottom)
    
    def intersects(self, other: 'Rect') -> bool:
        """Check if this rectangle intersects with another."""
        return not (self.right < other.left or 
                    self.left > other.right or
                    self.bottom < other.top or 
                    self.top > other.bottom)
    
    def intersection(self, other: 'Rect') -> Optional['Rect']:
        """Get the intersection rectangle with another."""
        if not self.intersects(other):
            return None
        x = max(self.left, other.left)
        y = max(self.top, other.top)
        width = min(self.right, other.right) - x
        height = min(self.bottom, other.bottom) - y
        return Rect(x, y, width, height)


@dataclass
class TransformMatrix:
    """
    Represents a 2D transformation matrix for coordinate transforms.
    
    Matrix layout:
    [a c e]
    [b d f]
    [0 0 1]
    
    Transforms point (x, y) to (a*x + c*y + e, b*x + d*y + f)
    """
    a: float = 1.0
    b: float = 0.0
    c: float = 0.0
    d: float = 1.0
    e: float = 0.0
    f: float = 0.0
    
    @classmethod
    def identity(cls) -> 'TransformMatrix':
        """Create an identity matrix."""
        return cls()
    
    @classmethod
    def translation(cls, tx: float, ty: float) -> 'TransformMatrix':
        """Create a translation matrix."""
        return cls(e=tx, f=ty)
    
    @classmethod
    def scaling(cls, sx: float, sy: float) -> 'TransformMatrix':
        """Create a scaling matrix."""
        return cls(a=sx, d=sy)
    
    @classmethod
    def rotation(cls, angle_degrees: float) -> 'TransformMatrix':
        """Create a rotation matrix."""
        import math
        angle_rad = math.radians(angle_degrees)
        cos_a = math.cos(angle_rad)
        sin_a = math.sin(angle_rad)
        return cls(a=cos_a, b=sin_a, c=-sin_a, d=cos_a)
    
    def transform_point(self, x: float, y: float) -> Tuple[float, float]:
        """Transform a point using this matrix."""
        new_x = self.a * x + self.c * y + self.e
        new_y = self.b * x + self.d * y + self.f
        return (new_x, new_y)
    
    def transform_point_obj(self, point: Point) -> Point:
        """Transform a Point object."""
        new_x, new_y = self.transform_point(point.x, point.y)
        return Point(new_x, new_y)
    
    def multiply(self, other: 'TransformMatrix') -> 'TransformMatrix':
        """Multiply this matrix with another."""
        return TransformMatrix(
            a=self.a * other.a + self.c * other.b,
            b=self.b * other.a + self.d * other.b,
            c=self.a * other.c + self.c * other.d,
            d=self.b * other.c + self.d * other.d,
            e=self.a * other.e + self.c * other.f + self.e,
            f=self.b * other.e + self.d * other.f + self.f
        )
    
    def inverse(self) -> 'TransformMatrix':
        """Get the inverse of this matrix."""
        import math
        det = self.a * self.d - self.b * self.c
        if abs(det) < 1e-10:
            raise ValueError("Matrix is not invertible")
        inv_det = 1.0 / det
        return TransformMatrix(
            a=self.d * inv_det,
            b=-self.b * inv_det,
            c=-self.c * inv_det,
            d=self.a * inv_det,
            e=(self.c * self.f - self.d * self.e) * inv_det,
            f=(self.b * self.e - self.a * self.f) * inv_det
        )


class CoordinateTransformer:
    """
    Handles coordinate transformations between different coordinate spaces.
    
    Supported spaces:
    - Screen: Absolute screen coordinates
    - Window: Coordinates relative to window origin
    - Element: Coordinates relative to element bounds
    """
    
    def __init__(self):
        self._transforms: dict[str, TransformMatrix] = {}
    
    def add_transform(self, name: str, matrix: TransformMatrix) -> None:
        """Add a named transformation."""
        self._transforms[name] = matrix
    
    def screen_to_window(
        self, 
        screen_point: Point, 
        window_offset: Point
    ) -> Point:
        """Convert screen coordinates to window coordinates."""
        matrix = TransformMatrix.translation(-window_offset.x, -window_offset.y)
        return matrix.transform_point_obj(screen_point)
    
    def window_to_screen(
        self, 
        window_point: Point, 
        window_offset: Point
    ) -> Point:
        """Convert window coordinates to screen coordinates."""
        matrix = TransformMatrix.translation(window_offset.x, window_offset.y)
        return matrix.transform_point_obj(window_point)
    
    def element_to_screen(
        self,
        element_point: Point,
        element_rect: Rect,
        window_offset: Point
    ) -> Point:
        """Convert element-relative coordinates to screen coordinates."""
        # Element -> Window
        window_point = Point(
            element_rect.x + element_point.x,
            element_rect.y + element_point.y
        )
        # Window -> Screen
        return self.window_to_screen(window_point, window_offset)
    
    def screen_to_element(
        self,
        screen_point: Point,
        element_rect: Rect,
        window_offset: Point
    ) -> Point:
        """Convert screen coordinates to element-relative coordinates."""
        # Screen -> Window
        window_point = self.screen_to_window(screen_point, window_offset)
        # Window -> Element
        return Point(
            window_point.x - element_rect.x,
            window_point.y - element_rect.y
        )
    
    def transform(
        self,
        point: Point,
        from_space: str,
        to_space: str
    ) -> Point:
        """Transform a point between named coordinate spaces."""
        if from_space == to_space:
            return point
        
        path_key = f"{from_space}_to_{to_space}"
        if path_key in self._transforms:
            return self._transforms[path_key].transform_point_obj(point)
        
        # Try to find a path through identity
        if 'identity' in self._transforms:
            return self._transforms['identity'].transform_point_obj(point)
        
        return point


def normalize_coordinates(
    x: float, 
    y: float, 
    bounds: Rect,
    normalize_to: Tuple[float, float] = (1.0, 1.0)
) -> Tuple[float, float]:
    """
    Normalize coordinates to a 0-1 range within bounds.
    
    Args:
        x: X coordinate
        y: Y coordinate
        bounds: Reference bounds
        normalize_to: Target range (e.g., (1.0, 1.0) for 0-1)
        
    Returns:
        Normalized (x, y) tuple
    """
    norm_x = (x - bounds.x) / bounds.width * normalize_to[0]
    norm_y = (y - bounds.y) / bounds.height * normalize_to[1]
    return (norm_x, norm_y)


def denormalize_coordinates(
    norm_x: float,
    norm_y: float,
    bounds: Rect,
    from_range: Tuple[float, float] = (1.0, 1.0)
) -> Tuple[float, float]:
    """
    Denormalize coordinates from 0-1 range to absolute coordinates.
    
    Args:
        norm_x: Normalized X coordinate
        norm_y: Normalized Y coordinate
        bounds: Target bounds
        from_range: Source range (e.g., (1.0, 1.0) for 0-1)
        
    Returns:
        Absolute (x, y) tuple
    """
    x = norm_x / from_range[0] * bounds.width + bounds.x
    y = norm_y / from_range[1] * bounds.height + bounds.y
    return (x, y)
