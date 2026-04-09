"""Shape morphing utilities for RabAI AutoClick.

Provides:
- Point set morphing between shapes
- Shape interpolation
- Warping transformations
- Shape blending
"""

from typing import List, Tuple, Callable, Optional
import math


def resample_points(
    points: List[Tuple[float, float]],
    num_points: int,
) -> List[Tuple[float, float]]:
    """Resample a shape to fixed number of points.

    Args:
        points: Input shape points.
        num_points: Desired number of points.

    Returns:
        Resampled points.
    """
    if len(points) < 2:
        return points[:]

    # Compute cumulative chord lengths
    dists = [0.0]
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        dists.append(dists[-1] + math.sqrt(dx * dx + dy * dy))

    total = dists[-1]
    if total == 0:
        return [points[0]] * num_points

    result: List[Tuple[float, float]] = []
    for i in range(num_points):
        target = total * i / (num_points - 1)
        lo, hi = 0, len(dists) - 1
        while lo < hi - 1:
            mid = (lo + hi) // 2
            if dists[mid] <= target:
                lo = mid
            else:
                hi = mid
        seg_len = dists[hi] - dists[lo]
        frac = (target - dists[lo]) / seg_len if seg_len > 0 else 0.0
        x = points[lo][0] + frac * (points[hi][0] - points[lo][0])
        y = points[lo][1] + frac * (points[hi][1] - points[lo][1])
        result.append((x, y))

    return result


def morph_shapes(
    shape_a: List[Tuple[float, float]],
    shape_b: List[Tuple[float, float]],
    t: float,
) -> List[Tuple[float, float]]:
    """Morph between two shapes.

    Args:
        shape_a: Start shape points.
        shape_b: End shape points.
        t: Interpolation parameter (0 = shape_a, 1 = shape_b).

    Returns:
        Interpolated shape.
    """
    n = max(len(shape_a), len(shape_b))
    a = resample_points(shape_a, n)
    b = resample_points(shape_b, n)
    return [
        (
            a[i][0] + (b[i][0] - a[i][0]) * t,
            a[i][1] + (b[i][1] - a[i][1]) * t,
        )
        for i in range(n)
    ]


def centroid(points: List[Tuple[float, float]]) -> Tuple[float, float]:
    """Compute centroid of a point set."""
    if not points:
        return (0.0, 0.0)
    cx = sum(p[0] for p in points) / len(points)
    cy = sum(p[1] for p in points) / len(points)
    return (cx, cy)


def translate_shape(
    points: List[Tuple[float, float]],
    dx: float,
    dy: float,
) -> List[Tuple[float, float]]:
    """Translate shape by (dx, dy)."""
    return [(p[0] + dx, p[1] + dy) for p in points]


def scale_shape(
    points: List[Tuple[float, float]],
    sx: float,
    sy: Optional[float] = None,
    origin: Optional[Tuple[float, float]] = None,
) -> List[Tuple[float, float]]:
    """Scale shape around origin."""
    if sy is None:
        sy = sx
    if origin is None:
        origin = centroid(points)
    return [
        (
            origin[0] + (p[0] - origin[0]) * sx,
            origin[1] + (p[1] - origin[1]) * sy,
        )
        for p in points
    ]


def rotate_shape(
    points: List[Tuple[float, float]],
    angle: float,  # radians
    origin: Optional[Tuple[float, float]] = None,
) -> List[Tuple[float, float]]:
    """Rotate shape around origin."""
    if origin is None:
        origin = centroid(points)
    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    return [
        (
            origin[0] + (p[0] - origin[0]) * cos_a - (p[1] - origin[1]) * sin_a,
            origin[1] + (p[0] - origin[0]) * sin_a + (p[1] - origin[1]) * cos_a,
        )
        for p in points
    ]


def bounding_box(
    points: List[Tuple[float, float]],
) -> Tuple[float, float, float, float]:
    """Get bounding box (min_x, min_y, max_x, max_y)."""
    if not points:
        return (0.0, 0.0, 0.0, 0.0)
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return (min(xs), min(ys), max(xs), max(ys))


def normalize_shape(
    points: List[Tuple[float, float]],
    target_size: float = 1.0,
) -> List[Tuple[float, float]]:
    """Normalize shape to unit bounding box."""
    min_x, min_y, max_x, max_y = bounding_box(points)
    w = max_x - min_x
    h = max_y - min_y
    scale = max(w, h)
    if scale < 1e-10:
        return points[:]
    cx = (min_x + max_x) / 2
    cy = (min_y + max_y) / 2
    return [
        (
            (p[0] - cx) / scale * target_size,
            (p[1] - cy) / scale * target_size,
        )
        for p in points
    ]


def shape_distance(
    shape_a: List[Tuple[float, float]],
    shape_b: List[Tuple[float, float]],
) -> float:
    """Compute average point-to-point distance between two shapes.

    Args:
        shape_a: First shape.
        shape_b: Second shape.

    Returns:
        Average distance.
    """
    n = max(len(shape_a), len(shape_b))
    a = resample_points(shape_a, n)
    b = resample_points(shape_b, n)
    return sum(
        math.sqrt((a[i][0] - b[i][0]) ** 2 + (a[i][1] - b[i][1]) ** 2)
        for i in range(n)
    ) / n


def shape_area(points: List[Tuple[float, float]]) -> float:
    """Compute polygon area (shoelace formula)."""
    n = len(points)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    return abs(area) / 2.0


def shape_perimeter(points: List[Tuple[float, float]]) -> float:
    """Compute polygon perimeter."""
    n = len(points)
    if n < 2:
        return 0.0
    perimeter = 0.0
    for i in range(n):
        j = (i + 1) % n
        dx = points[j][0] - points[i][0]
        dy = points[j][1] - points[i][1]
        perimeter += math.sqrt(dx * dx + dy * dy)
    return perimeter


def smooth_shape(
    points: List[Tuple[float, float]],
    iterations: int = 1,
) -> List[Tuple[float, float]]:
    """Smooth shape using neighbor averaging.

    Args:
        points: Input shape points.
        iterations: Number of smoothing passes.

    Returns:
        Smoothed shape.
    """
    if len(points) < 3:
        return points[:]

    result = points[:]
    for _ in range(iterations):
        smoothed: List[Tuple[float, float]] = []
        n = len(result)
        for i in range(n):
            prev_i = (i - 1) % n
            next_i = (i + 1) % n
            x = (result[prev_i][0] + result[i][0] * 2 + result[next_i][0]) / 4
            y = (result[prev_i][1] + result[i][1] * 2 + result[next_i][1]) / 4
            smoothed.append((x, y))
        result = smoothed

    return result


def morph_frames(
    frames: List[List[Tuple[float, float]]],
    t: float,
) -> List[Tuple[float, float]]:
    """Morph through multiple shape frames.

    Args:
        frames: Sequence of shapes.
        t: Time parameter (0 to len(frames)-1).

    Returns:
        Interpolated shape.
    """
    if not frames:
        return []
    if len(frames) == 1:
        return frames[0]

    num_frames = len(frames)
    index = t * (num_frames - 1)
    i = int(math.floor(index))
    frac = index - i

    if i >= num_frames - 1:
        return frames[-1]

    return morph_shapes(frames[i], frames[i + 1], frac)
