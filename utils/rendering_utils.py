"""Rendering utilities for RabAI AutoClick.

Provides:
- 2D rendering primitives
- Simple software renderer
- Z-buffer operations
- Scanline rasterization
"""

from typing import List, Tuple, Optional, Callable
import math


RGB = Tuple[int, int, int]
RGBA = Tuple[int, int, int, int]


def blend_pixel(
    src: RGBA,
    dst: RGBA,
) -> RGBA:
    """Alpha blend src onto dst."""
    sa = src[3] / 255.0
    da = dst[3] / 255.0
    oa = sa + da * (1 - sa)
    if oa < 1e-10:
        return (0, 0, 0, 0)
    r = int((src[0] * sa + dst[0] * da * (1 - sa)) / oa)
    g = int((src[1] * sa + dst[1] * da * (1 - sa)) / oa)
    b = int((src[2] * sa + dst[2] * da * (1 - sa)) / oa)
    return (r, g, b, int(oa * 255))


def draw_line_dda(
    image: List[List[RGBA]],
    x0: int, y0: int,
    x1: int, y1: int,
    color: RGBA,
) -> None:
    """Draw line using DDA (Digital Differential Analyzer).

    Args:
        image: Output image.
        x0, y0: Start point.
        x1, y1: End point.
        color: RGBA color.
    """
    h, w = len(image), len(image[0])
    dx = x1 - x0
    dy = y1 - y0
    steps = max(abs(dx), abs(dy))
    if steps < 1:
        return

    x_inc = dx / steps
    y_inc = dy / steps

    x, y = float(x0), float(y0)
    for _ in range(int(steps) + 1):
        xi, yi = int(x + 0.5), int(y + 0.5)
        if 0 <= xi < w and 0 <= yi < h:
            image[yi][xi] = color
        x += x_inc
        y += y_inc


def draw_line_bresenham(
    image: List[List[RGBA]],
    x0: int, y0: int,
    x1: int, y1: int,
    color: RGBA,
) -> None:
    """Draw line using Bresenham's algorithm."""
    h, w = len(image), len(image[0])

    dx = abs(x1 - x0)
    dy = abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx - dy

    while True:
        if 0 <= x0 < w and 0 <= y0 < h:
            image[y0][x0] = color

        if x0 == x1 and y0 == y1:
            break

        e2 = 2 * err
        if e2 > -dy:
            err -= dy
            x0 += sx
        if e2 < dx:
            err += dx
            y0 += sy


def draw_circle_bresenham(
    image: List[List[RGBA]],
    cx: int, cy: int,
    radius: int,
    color: RGBA,
) -> None:
    """Draw circle using Bresenham's algorithm."""
    h, w = len(image), len(image[0])
    x = radius
    y = 0
    err = 0

    while x >= y:
        def plot4(cx: int, cy: int, x: int, y: int) -> None:
            pts = [(cx + x, cy + y), (cx - x, cy + y), (cx + x, cy - y), (cx - x, cy - y),
                   (cx + y, cy + x), (cx - y, cy + x), (cx + y, cy - x), (cx - y, cy - x)]
            for px, py in pts:
                if 0 <= px < w and 0 <= py < h:
                    image[py][px] = color

        plot4(cx, cy, x, y)
        y += 1
        err += 1 + 2 * y
        if 2 * (err - x) + 1 > 0:
            x -= 1
            err += 1 - 2 * x


def fill_triangle(
    image: List[List[RGBA]],
    x0: int, y0: int,
    x1: int, y1: int,
    x2: int, y2: int,
    color: RGBA,
) -> None:
    """Fill triangle using scanline algorithm."""
    h, w = len(image), len(image[0])

    # Sort vertices by y
    if y0 > y1:
        x0, y0, x1, y1 = x1, y1, x0, y0
    if y0 > y2:
        x0, y0, x2, y2 = x2, y2, x0, y0
    if y1 > y2:
        x1, y1, x2, y2 = x2, y2, x1, y1

    def edge_cross(ax: float, ay: float, bx: float, by: float, cx: float, cy: float) -> float:
        return (bx - ax) * (cy - ay) - (by - ay) * (cx - ax)

    for y in range(max(0, y0), min(h, y2 + 1)):
        if y1 == y0:
            continue
        t1 = (y - y0) / (y1 - y0)
        t2 = (y - y0) / (y2 - y0) if y2 != y0 else 0
        if y1 == y0:
            continue
        if y2 == y0:
            continue
        x_left = int(x0 + (x1 - x0) * t1)
        x_right = int(x0 + (x2 - x0) * t2)
        if x_left > x_right:
            x_left, x_right = x_right, x_left
        for x in range(max(0, x_left), min(w, x_right + 1)):
            # Point-in-triangle test
            if edge_cross(x1, y1, x2, y2, x, y) >= 0 and \
               edge_cross(x2, y2, x0, y0, x, y) >= 0 and \
               edge_cross(x0, y0, x1, y1, x, y) >= 0:
                image[y][x] = color


def rasterize_triangle(
    x0: float, y0: float,
    x1: float, y1: float,
    x2: float, y2: float,
    callback: Callable[[int, int], None],
) -> None:
    """Rasterize triangle and call callback for each pixel.

    Args:
        x0, y0, x1, y1, x2, y2: Triangle vertices.
        callback: Function(x, y) called for each covered pixel.
    """
    # Sort by y
    if y0 > y1:
        x0, y0, x1, y1 = x1, y1, x0, y0
    if y0 > y2:
        x0, y0, x2, y2 = x2, y2, x0, y0
    if y1 > y2:
        x1, y1, x2, y2 = x2, y2, x1, y1

    y_min = max(0, int(y0))
    y_max = int(y2)

    for y in range(y_min, y_max + 1):
        if y1 != y0 and y2 != y0:
            t1 = (y - y0) / (y1 - y0) if y1 != y0 else 0
            t2 = (y - y0) / (y2 - y0) if y2 != y0 else 0
            x_left = int(x0 + (x1 - x0) * t1)
            x_right = int(x0 + (x2 - x0) * t2)
            if x_left > x_right:
                x_left, x_right = x_right, x_left
            for x in range(x_left, x_right + 1):
                callback(x, y)


def create_z_buffer(width: int, height: int) -> List[List[float]]:
    """Create z-buffer (depth buffer)."""
    return [[float('inf')] * width for _ in range(height)]


def rasterize_triangle_z(
    x0: float, y0: float, z0: float,
    x1: float, y1: float, z1: float,
    x2: float, y2: float, z2: float,
    color: RGBA,
    framebuffer: List[List[RGBA]],
    zbuffer: List[List[float]],
) -> None:
    """Rasterize triangle with z-buffering.

    Args:
        x0, y0, z0: Vertex 0.
        x1, y1, z1: Vertex 1.
        x2, y2, z2: Vertex 2.
        color: Pixel color.
        framebuffer: Output color buffer.
        zbuffer: Depth buffer.
    """
    h, w = len(framebuffer), len(framebuffer[0])

    # Sort by y
    if y0 > y1:
        x0, y0, z0, x1, y1, z1 = x1, y1, z1, x0, y0, z0
    if y0 > y2:
        x0, y0, z0, x2, y2, z2 = x2, y2, z2, x0, y0, z0
    if y1 > y2:
        x1, y1, z1, x2, y2, z2 = x2, y2, z2, x1, y1, z1

    def interpolate(val0: float, val1: float, val2: float, t1: float, t2: float, y: float) -> float:
        if abs(y1 - y0) < 1e-10 or abs(y2 - y0) < 1e-10:
            return val0
        v_a = val0 + (val1 - val0) * (y - y0) / (y1 - y0) if y1 != y0 else val0
        v_b = val0 + (val2 - val0) * (y - y0) / (y2 - y0) if y2 != y0 else val0
        return (v_a + v_b) / 2

    for y in range(max(0, int(y0)), min(h, int(y2) + 1)):
        if y1 != y0 and y2 != y0:
            t1 = (y - y0) / (y1 - y0) if abs(y1 - y0) > 1e-10 else 0
            t2 = (y - y0) / (y2 - y0) if abs(y2 - y0) > 1e-10 else 0
            x_l = int(x0 + (x1 - x0) * t1)
            x_r = int(x0 + (x2 - x0) * t2)
            z_l = z0 + (z1 - z0) * t1
            z_r = z0 + (z2 - z0) * t2
            if x_l > x_r:
                x_l, x_r = x_r, x_l
                z_l, z_r = z_r, z_l

            for x in range(x_l, x_r + 1):
                if 0 <= x < w and 0 <= y < h:
                    t = (x - x_l) / max(x_r - x_l, 1)
                    z = z_l + (z_r - z_l) * t
                    if z < zbuffer[y][x]:
                        zbuffer[y][x] = z
                        framebuffer[y][x] = color
