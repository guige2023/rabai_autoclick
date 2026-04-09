"""Transform matrix and affine transformation utilities.

Provides 2D/3D transformation matrices, affine transforms,
rotation, scaling, translation, and composition operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, List, Optional, Sequence
from enum import Enum, auto
import math


class TransformType(Enum):
    """Types of geometric transforms."""
    IDENTITY = auto()
    TRANSLATE = auto()
    SCALE = auto()
    ROTATE = auto()
    SHEAR = auto()
    REFLECT = auto()
    COMPOSITE = auto()


@dataclass
class Transform2D:
    """2D affine transformation matrix [a b c; d e f; 0 0 1].

    The transformation is applied as: [x' y'] = [x y 1] @ matrix
    """
    a: float = 1.0  # scale x
    b: float = 0.0  # shear y
    c: float = 0.0  # translate x
    d: float = 0.0  # shear x
    e: float = 1.0  # scale y
    f: float = 0.0  # translate y

    @classmethod
    def identity(cls) -> Transform2D:
        """Create identity transform."""
        return cls(a=1.0, b=0.0, c=0.0, d=0.0, e=1.0, f=0.0)

    @classmethod
    def translation(cls, tx: float, ty: float) -> Transform2D:
        """Create translation transform."""
        return cls(a=1.0, b=0.0, c=tx, d=0.0, e=1.0, f=ty)

    @classmethod
    def scaling(cls, sx: float, sy: Optional[float] = None) -> Transform2D:
        """Create scaling transform."""
        sy = sy if sy is not None else sx
        return cls(a=sx, b=0.0, c=0.0, d=0.0, e=sy, f=0.0)

    @classmethod
    def rotation(cls, angle_rad: float) -> Transform2D:
        """Create rotation transform around origin."""
        cos_a = math.cos(angle_rad)
        sin_a = math.sin(angle_rad)
        return cls(a=cos_a, b=sin_a, c=0.0, d=-sin_a, e=cos_a, f=0.0)

    @classmethod
    def rotation_around_point(
        cls, angle_rad: float, px: float, py: float
    ) -> Transform2D:
        """Create rotation transform around a specific point."""
        t1 = cls.translation(-px, -py)
        t2 = cls.rotation(angle_rad)
        t3 = cls.translation(px, py)
        return t1.compose(t2).compose(t3)

    @classmethod
    def shear(cls, shx: float = 0.0, shy: float = 0.0) -> Transform2D:
        """Create shear transform."""
        return cls(a=1.0, b=shy, c=0.0, d=shx, e=1.0, f=0.0)

    @classmethod
    def reflection(cls, axis: str) -> Transform2D:
        """Create reflection across axis ('x', 'y', or 'origin')."""
        if axis == "x":
            return cls(a=1.0, b=0.0, c=0.0, d=0.0, e=-1.0, f=0.0)
        elif axis == "y":
            return cls(a=-1.0, b=0.0, c=0.0, d=0.0, e=1.0, f=0.0)
        else:  # origin
            return cls(a=-1.0, b=0.0, c=0.0, d=0.0, e=-1.0, f=0.0)

    def transform_point(self, x: float, y: float) -> Tuple[float, float]:
        """Apply transform to a single point."""
        new_x = self.a * x + self.b * y + self.c
        new_y = self.d * x + self.e * y + self.f
        return (new_x, new_y)

    def transform_points(
        self, points: Sequence[Tuple[float, float]]
    ) -> List[Tuple[float, float]]:
        """Apply transform to multiple points."""
        return [self.transform_point(x, y) for x, y in points]

    def compose(self, other: Transform2D) -> Transform2D:
        """Compose this transform with another (self @ other)."""
        return Transform2D(
            a=self.a * other.a + self.b * other.d,
            b=self.a * other.b + self.b * other.e,
            c=self.a * other.c + self.b * other.f + self.c,
            d=self.d * other.a + self.e * other.d,
            e=self.d * other.b + self.e * other.e,
            f=self.d * other.c + self.e * other.f + self.f,
        )

    def inverse(self) -> Transform2D:
        """Compute inverse transformation."""
        det = self.a * self.e - self.b * self.d
        if abs(det) < 1e-10:
            raise ValueError("Transform is not invertible")
        inv_det = 1.0 / det
        return Transform2D(
            a=inv_det * self.e,
            b=inv_det * -self.b,
            c=inv_det * (self.b * self.f - self.c * self.e),
            d=inv_det * -self.d,
            e=inv_det * self.a,
            f=inv_det * (self.c * self.d - self.a * self.f),
        )

    def determinant(self) -> float:
        """Get determinant of transformation matrix."""
        return self.a * self.e - self.b * self.d

    def is_identity(self, tol: float = 1e-9) -> bool:
        """Check if transform is identity (within tolerance)."""
        return (
            abs(self.a - 1.0) < tol
            and abs(self.b) < tol
            and abs(self.c) < tol
            and abs(self.d) < tol
            and abs(self.e - 1.0) < tol
            and abs(self.f) < tol
        )

    def to_matrix(self) -> Tuple[float, float, float, float, float, float]:
        """Get flat tuple representation."""
        return (self.a, self.b, self.c, self.d, self.e, self.f)

    def __repr__(self) -> str:
        return (
            f"Transform2D(a={self.a:.4f}, b={self.b:.4f}, c={self.c:.4f}, "
            f"d={self.d:.4f}, e={self.e:.4f}, f={self.f:.4f})"
        )


@dataclass
class BoundingBox:
    """Axis-aligned bounding box in 2D."""
    x_min: float
    y_min: float
    x_max: float
    y_max: float

    @classmethod
    def from_points(
        cls, points: Sequence[Tuple[float, float]]
    ) -> BoundingBox:
        """Create bounding box from a set of points."""
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        return cls(x_min=min(xs), y_min=min(ys), x_max=max(xs), y_max=max(ys))

    @classmethod
    def from_center_size(
        cls, cx: float, cy: float, width: float, height: float
    ) -> BoundingBox:
        """Create bounding box from center and dimensions."""
        hw, hh = width / 2.0, height / 2.0
        return cls(
            x_min=cx - hw, y_min=cy - hh,
            x_max=cx + hw, y_max=cy + hh
        )

    def transform(self, transform: Transform2D) -> BoundingBox:
        """Transform this bounding box."""
        corners = [
            (self.x_min, self.y_min),
            (self.x_max, self.y_min),
            (self.x_min, self.y_max),
            (self.x_max, self.y_max),
        ]
        transformed = transform.transform_points(corners)
        xs = [p[0] for p in transformed]
        ys = [p[1] for p in transformed]
        return BoundingBox(x_min=min(xs), y_min=min(ys),
                           x_max=max(xs), y_max=max(ys))

    @property
    def width(self) -> float:
        return self.x_max - self.x_min

    @property
    def height(self) -> float:
        return self.y_max - self.y_min

    @property
    def center(self) -> Tuple[float, float]:
        return ((self.x_min + self.x_max) / 2.0,
                (self.y_min + self.y_max) / 2.0)

    def contains_point(self, x: float, y: float) -> bool:
        """Check if point is inside bounding box."""
        return self.x_min <= x <= self.x_max and self.y_min <= y <= self.y_max

    def intersects(self, other: BoundingBox) -> bool:
        """Check if two bounding boxes intersect."""
        return not (
            self.x_max < other.x_min or self.x_min > other.x_max
            or self.y_max < other.y_min or self.y_min > other.y_max
        )

    def union(self, other: BoundingBox) -> BoundingBox:
        """Get union of two bounding boxes."""
        return BoundingBox(
            x_min=min(self.x_min, other.x_min),
            y_min=min(self.y_min, other.y_min),
            x_max=max(self.x_max, other.x_max),
            y_max=max(self.y_max, other.y_max),
        )

    def expand(self, margin: float) -> BoundingBox:
        """Expand bounding box by margin."""
        return BoundingBox(
            x_min=self.x_min - margin,
            y_min=self.y_min - margin,
            x_max=self.x_max + margin,
            y_max=self.y_max + margin,
        )
