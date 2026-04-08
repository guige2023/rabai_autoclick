"""Transform matrix utilities for coordinate transformations."""

from typing import Tuple, List, Optional
import math


Matrix3x3 = Tuple[float, float, float, float, float, float, float, float, float]


def identity() -> Matrix3x3:
    """Create 3x3 identity matrix."""
    return (1, 0, 0, 0, 1, 0, 0, 0, 1)


def translation(tx: float, ty: float) -> Matrix3x3:
    """Create translation matrix.
    
    Args:
        tx: Translation in x.
        ty: Translation in y.
    
    Returns:
        3x3 translation matrix.
    """
    return (1, 0, 0, 0, 1, 0, tx, ty, 1)


def rotation(angle: float) -> Matrix3x3:
    """Create rotation matrix.
    
    Args:
        angle: Rotation angle in radians.
    
    Returns:
        3x3 rotation matrix.
    """
    c = math.cos(angle)
    s = math.sin(angle)
    return (c, s, 0, -s, c, 0, 0, 0, 1)


def scaling(sx: float, sy: float) -> Matrix3x3:
    """Create scaling matrix.
    
    Args:
        sx: Scale in x.
        sy: Scale in y.
    
    Returns:
        3x3 scaling matrix.
    """
    return (sx, 0, 0, 0, sy, 0, 0, 0, 1)


def multiply(m1: Matrix3x3, m2: Matrix3x3) -> Matrix3x3:
    """Multiply two 3x3 matrices.
    
    Args:
        m1, m2: Input matrices.
    
    Returns:
        Result matrix.
    """
    a, b, c, d, e, f, g, h, i = m1
    j, k, l, m, n, o, p, q, r = m2
    return (
        a * j + b * m + c * p,
        a * k + b * n + c * q,
        a * l + b * o + c * r,
        d * j + e * m + f * p,
        d * k + e * n + f * q,
        d * l + e * o + f * r,
        g * j + h * m + i * p,
        g * k + h * n + i * q,
        g * l + h * o + i * r,
    )


def transform_point(m: Matrix3x3, x: float, y: float) -> Tuple[float, float]:
    """Transform a point using matrix.
    
    Args:
        m: Transform matrix.
        x, y: Input point.
    
    Returns:
        Transformed (x, y).
    """
    a, b, c, d, e, f, g, h, i = m
    return (a * x + b * y + c, d * x + e * y + f)


def transform_points(m: Matrix3x3, points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """Transform multiple points.
    
    Args:
        m: Transform matrix.
        points: List of (x, y) points.
    
    Returns:
        List of transformed points.
    """
    return [transform_point(m, x, y) for x, y in points]


def inverse(m: Matrix3x3) -> Optional[Matrix3x3]:
    """Compute inverse of 3x3 matrix.
    
    Args:
        m: Input matrix.
    
    Returns:
        Inverse matrix or None if not invertible.
    """
    a, b, c, d, e, f, g, h, i = m
    det = a * (e * i - f * h) - b * (d * i - f * g) + c * (d * h - e * g)
    if abs(det) < 1e-10:
        return None
    inv_det = 1.0 / det
    return (
        (e * i - f * h) * inv_det,
        (c * h - b * i) * inv_det,
        (b * f - c * e) * inv_det,
        (f * g - d * i) * inv_det,
        (a * i - c * g) * inv_det,
        (c * d - a * f) * inv_det,
        (d * h - e * g) * inv_det,
        (b * g - a * h) * inv_det,
        (a * e - b * d) * inv_det,
    )


def compose(*transforms: Matrix3x3) -> Matrix3x3:
    """Compose multiple transforms (right to left).
    
    Args:
        *transforms: Transform matrices.
    
    Returns:
        Combined transform matrix.
    """
    result = identity()
    for t in transforms:
        result = multiply(t, result)
    return result
