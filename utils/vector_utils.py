"""Vector math utilities for RabAI AutoClick.

Provides:
- 2D and 3D vector operations
- Vector transformations
- Angle and distance utilities
- Vector field operations
"""

from typing import List, Tuple, Optional
import math


# Type aliases
Vec2 = Tuple[float, float]
Vec3 = Tuple[float, float, float]


def vec2(x: float = 0.0, y: float = 0.0) -> Vec2:
    """Create 2D vector."""
    return (x, y)


def vec3(x: float = 0.0, y: float = 0.0, z: float = 0.0) -> Vec3:
    """Create 3D vector."""
    return (x, y, z)


def add(v1: Vec2, v2: Vec2) -> Vec2:
    """Add two 2D vectors."""
    return (v1[0] + v2[0], v1[1] + v2[1])


def sub(v1: Vec2, v2: Vec2) -> Vec2:
    """Subtract vectors."""
    return (v1[0] - v2[0], v1[1] - v2[1])


def mul(v: Vec2, s: float) -> Vec2:
    """Multiply vector by scalar."""
    return (v[0] * s, v[1] * s)


def dot(v1: Vec2, v2: Vec2) -> float:
    """Dot product of two vectors."""
    return v1[0] * v2[0] + v1[1] * v2[1]


def cross_2d(v1: Vec2, v2: Vec2) -> float:
    """2D cross product (returns scalar z-component)."""
    return v1[0] * v2[1] - v1[1] * v2[0]


def cross_3d(v1: Vec3, v2: Vec3) -> Vec3:
    """3D cross product."""
    return (
        v1[1] * v2[2] - v1[2] * v2[1],
        v1[2] * v2[0] - v1[0] * v2[2],
        v1[0] * v2[1] - v1[1] * v2[0],
    )


def length(v: Vec2) -> float:
    """Vector length (magnitude)."""
    return math.sqrt(v[0] * v[0] + v[1] * v[1])


def length_3d(v: Vec3) -> float:
    """3D vector length."""
    return math.sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2])


def normalize(v: Vec2) -> Vec2:
    """Normalize to unit length."""
    mag = length(v)
    if mag < 1e-10:
        return (0.0, 0.0)
    return (v[0] / mag, v[1] / mag)


def normalize_3d(v: Vec3) -> Vec3:
    """Normalize 3D vector."""
    mag = length_3d(v)
    if mag < 1e-10:
        return (0.0, 0.0, 0.0)
    return (v[0] / mag, v[1] / mag, v[2] / mag)


def distance(p1: Vec2, p2: Vec2) -> float:
    """Euclidean distance between two points."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    return math.sqrt(dx * dx + dy * dy)


def distance_3d(p1: Vec3, p2: Vec3) -> float:
    """3D Euclidean distance."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    dz = p2[2] - p1[2]
    return math.sqrt(dx * dx + dy * dy + dz * dz)


def manhattan_distance(p1: Vec2, p2: Vec2) -> float:
    """Manhattan (L1) distance."""
    return abs(p2[0] - p1[0]) + abs(p2[1] - p1[1])


def angle(v: Vec2) -> float:
    """Angle of vector from positive X-axis (radians)."""
    return math.atan2(v[1], v[0])


def angle_between(v1: Vec2, v2: Vec2) -> float:
    """Angle between two vectors (radians)."""
    d = dot(v1, v2) / (length(v1) * length(v2)) if length(v1) > 0 and length(v2) > 0 else 0
    d = max(-1.0, min(1.0, d))
    return math.acos(d)


def rotate(v: Vec2, angle: float) -> Vec2:
    """Rotate vector by angle (radians)."""
    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    return (
        v[0] * cos_a - v[1] * sin_a,
        v[0] * sin_a + v[1] * cos_a,
    )


def rotate_around(p: Vec2, center: Vec2, angle: float) -> Vec2:
    """Rotate point around center."""
    translated = (p[0] - center[0], p[1] - center[1])
    rotated = rotate(translated, angle)
    return (rotated[0] + center[0], rotated[1] + center[1])


def perpendicular(v: Vec2) -> Vec2:
    """Get perpendicular vector (90 degrees counter-clockwise)."""
    return (-v[1], v[0])


def project(v: Vec2, onto: Vec2) -> Vec2:
    """Project vector onto another vector."""
    onto_len_sq = dot(onto, onto)
    if onto_len_sq < 1e-10:
        return (0.0, 0.0)
    scalar = dot(v, onto) / onto_len_sq
    return mul(onto, scalar)


def reflect(v: Vec2, normal: Vec2) -> Vec2:
    """Reflect vector across normal."""
    n = normalize(normal)
    return sub(v, mul(n, 2 * dot(v, n)))


def lerp(v1: Vec2, v2: Vec2, t: float) -> Vec2:
    """Linear interpolation between vectors."""
    return (
        v1[0] + (v2[0] - v1[0]) * t,
        v1[1] + (v2[1] - v1[1]) * t,
    )


def lerp_3d(v1: Vec3, v2: Vec3, t: float) -> Vec3:
    """3D linear interpolation."""
    return (
        v1[0] + (v2[0] - v1[0]) * t,
        v1[1] + (v2[1] - v1[1]) * t,
        v1[2] + (v2[2] - v1[2]) * t,
    )


def slerp(v1: Vec2, v2: Vec2, t: float) -> Vec2:
    """Spherical linear interpolation (for unit vectors)."""
    ang = angle_between(v1, v2)
    if ang < 1e-10:
        return v1
    sin_ang = math.sin(ang)
    a = math.sin((1 - t) * ang) / sin_ang
    b = math.sin(t * ang) / sin_ang
    return (a * v1[0] + b * v2[0], a * v1[1] + b * v2[1])


def clamp(v: Vec2, min_val: float, max_val: float) -> Vec2:
    """Clamp vector components."""
    return (
        max(min_val, min(max_val, v[0])),
        max(min_val, min(max_val, v[1])),
    )


def clamp_length(v: Vec2, max_len: float) -> Vec2:
    """Clamp vector length to maximum."""
    mag = length(v)
    if mag <= max_len:
        return v
    return mul(normalize(v), max_len)


def closest_point_on_segment(
    p: Vec2,
    a: Vec2,
    b: Vec2,
) -> Vec2:
    """Find closest point on line segment AB to point P."""
    ab = sub(b, a)
    ab_len_sq = dot(ab, ab)
    if ab_len_sq < 1e-10:
        return a
    t = dot(sub(p, a), ab) / ab_len_sq
    t = max(0.0, min(1.0, t))
    return add(a, mul(ab, t))


def point_to_segment_distance(
    p: Vec2,
    a: Vec2,
    b: Vec2,
) -> float:
    """Compute distance from point to line segment."""
    closest = closest_point_on_segment(p, a, b)
    return distance(p, closest)


def signed_angle(from_v: Vec2, to_v: Vec2) -> float:
    """Signed angle from one vector to another (radians)."""
    cross = cross_2d(from_v, to_v)
    ang = angle_between(from_v, to_v)
    return -ang if cross < 0 else ang


def triangle_area(a: Vec2, b: Vec2, c: Vec2) -> float:
    """Compute triangle area using cross product."""
    return abs(cross_2d(sub(b, a), sub(c, a))) / 2


def polygon_centroid(vertices: List[Vec2]) -> Vec2:
    """Compute centroid of polygon."""
    if not vertices:
        return (0.0, 0.0)
    cx = sum(v[0] for v in vertices) / len(vertices)
    cy = sum(v[1] for v in vertices) / len(vertices)
    return (cx, cy)
