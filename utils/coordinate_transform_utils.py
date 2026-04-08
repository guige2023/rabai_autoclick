"""
Coordinate transform utilities for converting between coordinate spaces.

Handles transformations between screen, window, element, and
image coordinate spaces with rotation and scaling support.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class Point:
    """2D point."""
    x: float
    y: float

    def __add__(self, other: Point) -> Point:
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: Point) -> Point:
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> Point:
        return Point(self.x * scalar, self.y * scalar)

    def distance_to(self, other: Point) -> float:
        return math.hypot(self.x - other.x, self.y - other.y)


@dataclass
class Transform2D:
    """2D affine transformation."""
    scale_x: float = 1.0
    scale_y: float = 1.0
    rotation: float = 0.0  # radians
    translate_x: float = 0.0
    translate_y: float = 0.0
    origin_x: float = 0.0
    origin_y: float = 0.0

    def transform_point(self, x: float, y: float) -> tuple[float, float]:
        """Apply transformation to a point."""
        # Translate to origin
        px = x - self.origin_x
        py = y - self.origin_y

        # Scale
        px *= self.scale_x
        py *= self.scale_y

        # Rotate
        if self.rotation != 0:
            cos_r = math.cos(self.rotation)
            sin_r = math.sin(self.rotation)
            rx = px * cos_r - py * sin_r
            ry = px * sin_r + py * cos_r
            px, py = rx, ry

        # Translate back and apply offset
        px += self.origin_x + self.translate_x
        py += self.origin_y + self.translate_y

        return (px, py)

    def inverse(self) -> Transform2D:
        """Get the inverse transformation."""
        cos_r = math.cos(-self.rotation)
        sin_r = math.sin(-self.rotation)

        # Reverse translation
        tx = -self.translate_x
        ty = -self.translate_y

        # Reverse scale
        sx = 1.0 / self.scale_x if self.scale_x != 0 else 1.0
        sy = 1.0 / self.scale_y if self.scale_y != 0 else 1.0

        return Transform2D(
            scale_x=sx,
            scale_y=sy,
            rotation=-self.rotation,
            translate_x=tx,
            translate_y=ty,
            origin_x=self.origin_x,
            origin_y=self.origin_y,
        )


class CoordinateTransformer:
    """Transforms coordinates between different spaces."""

    def __init__(self):
        self._transforms: dict[str, Transform2D] = {}
        self._parent_chain: list[str] = []

    def add_space(self, name: str, transform: Optional[Transform2D] = None) -> None:
        """Register a coordinate space."""
        self._transforms[name] = transform or Transform2D()

    def set_transform(self, name: str, transform: Transform2D) -> None:
        self._transforms[name] = transform

    def set_parent_chain(self, chain: list[str]) -> None:
        """Set parent chain for coordinate space hierarchy.

        Example: ["screen", "window", "element"]
        """
        self._parent_chain = chain

    def transform(
        self,
        x: float,
        y: float,
        from_space: str,
        to_space: str,
    ) -> tuple[float, float]:
        """Transform a point from one coordinate space to another."""
        if from_space == to_space:
            return (x, y)

        # Find path in parent chain
        try:
            from_idx = self._parent_chain.index(from_space)
            to_idx = self._parent_chain.index(to_space)
        except ValueError:
            return self._transform_direct(x, y, from_space, to_space)

        if from_idx < to_idx:
            # Moving down (parent to child) - apply inverse transforms
            for i in range(from_idx, to_idx):
                t = self._transforms.get(self._parent_chain[i + 1])
                if t:
                    x, y = t.inverse().transform_point(x, y)
        else:
            # Moving up (child to parent) - apply transforms
            for i in range(from_idx, to_idx, -1):
                t = self._transforms.get(self._parent_chain[i])
                if t:
                    x, y = t.transform_point(x, y)

        return (x, y)

    def _transform_direct(
        self,
        x: float,
        y: float,
        from_space: str,
        to_space: str,
    ) -> tuple[float, float]:
        """Direct transformation between two spaces."""
        t_from = self._transforms.get(from_space, Transform2D())
        t_to = self._transforms.get(to_space, Transform2D())

        # Compose: to^-1 o from
        composed = Transform2D(
            scale_x=t_from.scale_x / (t_to.scale_x if t_to.scale_x != 0 else 1),
            scale_y=t_from.scale_y / (t_to.scale_y if t_to.scale_y != 0 else 1),
            rotation=t_from.rotation - t_to.rotation,
            translate_x=t_from.translate_x - t_to.translate_x,
            translate_y=t_from.translate_y - t_to.translate_y,
        )
        return composed.transform_point(x, y)

    def screen_to_image(
        self,
        x: float,
        y: float,
        screen_scale: float = 1.0,
    ) -> tuple[float, float]:
        """Convert screen coordinates to image coordinates."""
        return (x * screen_scale, y * screen_scale)

    def image_to_screen(
        self,
        x: float,
        y: float,
        screen_scale: float = 1.0,
    ) -> tuple[float, float]:
        """Convert image coordinates to screen coordinates."""
        return (x / screen_scale if screen_scale != 0 else x,
                y / screen_scale if screen_scale != 0 else y)


def transform_bounds(
    x: float, y: float,
    width: float, height: float,
    transform: Transform2D,
) -> tuple[float, float, float, float]:
    """Transform a bounding box with a 2D transform."""
    x1, y1 = transform.transform_point(x, y)
    x2, y2 = transform.transform_point(x + width, y + height)
    return (x1, y1, x2 - x1, y2 - y1)


__all__ = ["Point", "Transform2D", "CoordinateTransformer", "transform_bounds"]
