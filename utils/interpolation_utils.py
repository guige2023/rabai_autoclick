"""Interpolation utilities for RabAI AutoClick.

Provides:
- Various interpolation methods (linear, cubic, hermite, etc.)
- Multi-dimensional interpolation
- Step interpolation variants
- 2D bilinear and bicubic interpolation
"""

from typing import List, Tuple, Callable, Optional, Dict
import math


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation."""
    return a + (b - a) * t


def inv_lerp(a: float, b: float, v: float) -> float:
    """Inverse linear interpolation (find t such that lerp(a,b,t) = v)."""
    if abs(b - a) < 1e-10:
        return 0.0
    return (v - a) / (b - a)


def remap(
    value: float,
    in_min: float,
    in_max: float,
    out_min: float,
    out_max: float,
) -> float:
    """Remap value from input range to output range."""
    t = inv_lerp(in_min, in_max, value)
    return lerp(out_min, out_max, t)


def step(t: float) -> float:
    """Step function: 0 if t < 0.5, 1 otherwise."""
    return 0.0 if t < 0.5 else 1.0


def smoothstep(t: float) -> float:
    """Smoothstep (Perlin ease)."""
    t = max(0.0, min(1.0, t))
    return t * t * (3 - 2 * t)


def smootherstep(t: float) -> float:
    """Smootherstep (Ken Perlin's improved version)."""
    t = max(0.0, min(1.0, t))
    return t * t * t * (t * (t * 6 - 15) + 10)


def ease_in_quad(t: float) -> float:
    """Quadratic ease in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease out."""
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease in-out."""
    return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Cubic ease in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease out."""
    return (t - 1) ** 3 + 1


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease in-out."""
    return 4 * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2


def ease_in_out_sine(t: float) -> float:
    """Sine ease in-out."""
    return -(math.cos(math.pi * t) - 1) / 2


def ease_in_elastic(t: float) -> float:
    """Elastic ease in."""
    if t == 0 or t == 1:
        return t
    p = 0.3
    return -2 ** (10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / p)


def ease_out_elastic(t: float) -> float:
    """Elastic ease out."""
    if t == 0 or t == 1:
        return t
    p = 0.3
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / p) + 1


def ease_out_bounce(t: float) -> float:
    """Bounce ease out."""
    n1 = 7.5625
    d1 = 2.75
    if t < 1 / d1:
        return n1 * t * t
    elif t < 2 / d1:
        t -= 1.5 / d1
        return n1 * t * t + 0.75
    elif t < 2.5 / d1:
        t -= 2.25 / d1
        return n1 * t * t + 0.9375
    else:
        t -= 2.625 / d1
        return n1 * t * t + 0.984375


def hermite_interpolate(
    p0: float, p1: float,
    m0: float, m1: float,
    t: float,
) -> float:
    """Hermite cubic interpolation with tangent control.

    Args:
        p0: Start value.
        p1: End value.
        m0: Tangent at start.
        m1: Tangent at end.
        t: Parameter [0, 1].

    Returns:
        Interpolated value.
    """
    t2 = t * t
    t3 = t2 * t
    h00 = 2 * t3 - 3 * t2 + 1
    h10 = t3 - 2 * t2 + t
    h01 = -2 * t3 + 3 * t2
    h11 = t3 - t2
    return h00 * p0 + h10 * m0 + h01 * p1 + h11 * m1


def cubic_interpolate(
    points: List[float],
    t: float,
) -> float:
    """Cubic interpolation from 4 control points.

    Args:
        points: 4 values [p0, p1, p2, p3].
        t: Parameter [0, 1].

    Returns:
        Interpolated value.
    """
    if len(points) < 4:
        return lerp(points[0], points[-1], t) if points else 0.0
    p0, p1, p2, p3 = points[0], points[1], points[2], points[3]
    a0 = -p0 / 2 + 3 * p1 / 2 - 3 * p2 / 2 + p3 / 2
    a1 = p0 - 5 * p1 / 2 + 2 * p2 - p3 / 2
    a2 = -p0 / 2 + p2 / 2
    a3 = p1
    t2 = t * t
    return a0 * t * t2 + a1 * t2 + a2 * t + a3


def bilinear_interpolate(
    q00: float, q10: float,
    q01: float, q11: float,
    tx: float, ty: float,
) -> float:
    """Bilinear interpolation on a 2x2 grid.

    Args:
        q00: Top-left value.
        q10: Top-right value.
        q01: Bottom-left value.
        q11: Bottom-right value.
        tx: X parameter [0, 1].
        ty: Y parameter [0, 1].

    Returns:
        Interpolated value.
    """
    x0 = lerp(q00, q10, tx)
    x1 = lerp(q01, q11, tx)
    return lerp(x0, x1, ty)


def bicubic_interpolate(
    grid: List[List[float]],
    tx: float,
    ty: float,
) -> float:
    """Bicubic interpolation on a 4x4 grid.

    Args:
        grid: 4x4 values (rows of columns).
        tx, ty: Parameters [0, 1].

    Returns:
        Interpolated value.
    """
    if len(grid) < 4 or len(grid[0]) < 4:
        return bilinear_interpolate(
            grid[0][0], grid[0][1] if len(grid[0]) > 1 else grid[0][0],
            grid[1][0] if len(grid) > 1 else grid[0][0],
            grid[1][1] if len(grid) > 1 and len(grid[0]) > 1 else grid[0][0],
            tx, ty
        )

    # Apply cubic interpolation row-wise, then column-wise
    row_vals: List[float] = []
    for row in grid[:4]:
        row_vals.append(cubic_interpolate(row[:4], tx))

    return cubic_interpolate(row_vals, ty)


def nearest_interpolate(
    points: List[float],
    t: float,
) -> float:
    """Nearest-neighbor interpolation."""
    if not points:
        return 0.0
    n = len(points)
    idx = max(0, min(n - 1, int(t * n)))
    return points[idx]


EASING_FUNCTIONS: Dict[str, Callable[[float], float]] = {
    "linear": lambda t: t,
    "step": step,
    "smoothstep": smoothstep,
    "smootherstep": smootherstep,
    "ease_in_quad": ease_in_quad,
    "ease_out_quad": ease_out_quad,
    "ease_in_out_quad": ease_in_out_quad,
    "ease_in_cubic": ease_in_cubic,
    "ease_out_cubic": ease_out_cubic,
    "ease_in_out_cubic": ease_in_out_cubic,
    "ease_in_out_sine": ease_in_out_sine,
    "ease_in_elastic": ease_in_elastic,
    "ease_out_elastic": ease_out_elastic,
    "ease_out_bounce": ease_out_bounce,
}


def interpolate_values(
    values: List[float],
    t: float,
    easing: str = "linear",
) -> float:
    """Interpolate between values with easing.

    Args:
        values: List of values to interpolate through.
        t: Overall parameter [0, 1].
        easing: Easing function name.

    Returns:
        Interpolated value.
    """
    if len(values) < 2:
        return values[0] if values else 0.0

    n = len(values)
    scaled_t = t * (n - 1)
    index = int(scaled_t)
    frac = scaled_t - index

    easing_fn = EASING_FUNCTIONS.get(easing, lerp)
    frac = easing_fn(frac)

    i0 = min(index, n - 1)
    i1 = min(index + 1, n - 1)

    return lerp(values[i0], values[i1], frac)


def multistep_interpolate(
    values: List[float],
    t: float,
    steps: int = 5,
) -> float:
    """Multi-step interpolation (quantizes output).

    Args:
        values: Values to interpolate.
        t: Parameter [0, 1].
        steps: Number of discrete output levels.

    Returns:
        Quantized interpolated value.
    """
    if not values:
        return 0.0
    scaled = interpolate_values(values, t, "linear")
    return round(scaled * steps) / steps
