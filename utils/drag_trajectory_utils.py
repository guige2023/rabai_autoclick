"""Drag trajectory utilities for curved and shaped drag paths.

This module provides utilities for calculating smooth, curved drag
trajectories for natural-looking drag operations.
"""

from __future__ import annotations

import math
from typing import Callable, Sequence


# Bezier curve implementation
def linear_bezier(p0: float, p1: float, t: float) -> float:
    """Linear Bezier interpolation."""
    return p0 + (p1 - p0) * t


def quadratic_bezier(p0: float, p1: float, p2: float, t: float) -> float:
    """Quadratic Bezier interpolation."""
    one_minus_t = 1 - t
    return one_minus_t ** 2 * p0 + 2 * one_minus_t * t * p1 + t ** 2 * p2


def cubic_bezier(p0: float, p1: float, p2: float, p3: float, t: float) -> float:
    """Cubic Bezier interpolation."""
    one_minus_t = 1 - t
    return (one_minus_t ** 3 * p0 + 
            3 * one_minus_t ** 2 * t * p1 + 
            3 * one_minus_t * t ** 2 * p2 + 
            t ** 3 * p3)


def bezier_curve_2d(
    start: tuple[int, int],
    control1: tuple[int, int],
    control2: tuple[int, int],
    end: tuple[int, int],
    steps: int = 50,
) -> list[tuple[int, int]]:
    """Generate points along a cubic Bezier curve.
    
    Args:
        start: Starting point (x, y).
        control1: First control point.
        control2: Second control point.
        end: Ending point.
        steps: Number of points to generate.
    
    Returns:
        List of (x, y) tuples along the curve.
    """
    points = []
    for i in range(steps + 1):
        t = i / steps
        x = cubic_bezier(start[0], control1[0], control2[0], end[0], t)
        y = cubic_bezier(start[1], control1[1], control2[1], end[1], t)
        points.append((int(x), int(y)))
    return points


def arc_trajectory(
    start: tuple[int, int],
    end: tuple[int, int],
    curvature: float = 0.5,
    steps: int = 50,
) -> list[tuple[int, int]]:
    """Generate an arc-shaped drag trajectory.
    
    Args:
        start: Starting point (x, y).
        end: Ending point (x, y).
        curvature: Curvature factor (-1.0 to 1.0). Positive curves upward/left,
                   negative curves downward/right.
        steps: Number of points to generate.
    
    Returns:
        List of (x, y) tuples along the arc.
    """
    mid_x = (start[0] + end[0]) / 2
    mid_y = (start[1] + end[1]) / 2
    
    # Calculate perpendicular offset for control point
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length = math.sqrt(dx * dx + dy * dy)
    
    if length == 0:
        return [start, end]
    
    # Perpendicular vector
    px = -dy / length
    py = dx / length
    
    # Control point offset
    offset = curvature * length * 0.5
    control = (mid_x + px * offset, mid_y + py * offset)
    
    return bezier_curve_2d(start, control, control, end, steps)


def wave_trajectory(
    start: tuple[int, int],
    end: tuple[int, int],
    amplitude: float = 50.0,
    frequency: float = 2.0,
    steps: int = 50,
) -> list[tuple[int, int]]:
    """Generate a wave-shaped drag trajectory.
    
    Args:
        start: Starting point (x, y).
        end: Ending point (x, y).
        amplitude: Wave amplitude in pixels.
        frequency: Number of complete waves.
        steps: Number of points to generate.
    
    Returns:
        List of (x, y) tuples along the wave.
    """
    points = []
    for i in range(steps + 1):
        t = i / steps
        
        # Base linear interpolation
        x = start[0] + (end[0] - start[0]) * t
        y = start[1] + (end[1] - start[1]) * t
        
        # Add wave perpendicular to the line
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        length = math.sqrt(dx * dx + dy * dy)
        
        if length > 0:
            px = -dy / length
            py = dx / length
            
            wave = amplitude * math.sin(t * frequency * 2 * math.pi)
            x += px * wave
            y += py * wave
        
        points.append((int(x), int(y)))
    return points


def spiral_trajectory(
    center: tuple[int, int],
    start_radius: float,
    end_radius: float,
    start_angle: float = 0.0,
    revolutions: float = 1.0,
    steps: int = 50,
) -> list[tuple[int, int]]:
    """Generate a spiral drag trajectory.
    
    Args:
        center: Center point of the spiral.
        start_radius: Starting radius.
        end_radius: Ending radius.
        start_angle: Starting angle in radians.
        revolutions: Number of complete revolutions.
        steps: Number of points to generate.
    
    Returns:
        List of (x, y) tuples along the spiral.
    """
    points = []
    for i in range(steps + 1):
        t = i / steps
        angle = start_angle + revolutions * 2 * math.pi * t
        radius = start_radius + (end_radius - start_radius) * t
        
        x = center[0] + radius * math.cos(angle)
        y = center[1] + radius * math.sin(angle)
        points.append((int(x), int(y)))
    return points


def smooth_path(
    points: Sequence[tuple[int, int]],
    tension: float = 0.5,
    steps_per_segment: int = 10,
) -> list[tuple[int, int]]:
    """Generate a smooth path through a series of points using Catmull-Rom spline.
    
    Args:
        points: Sequence of points to smooth through.
        tension: Tension parameter (0.0 = linear, 0.5 = smooth, 1.0 = max smoothing).
        steps_per_segment: Number of interpolated points per segment.
    
    Returns:
        List of (x, y) tuples along the smooth path.
    """
    if len(points) < 2:
        return list(points)
    if len(points) == 2:
        return bezier_curve_2d(
            points[0],
            points[0],
            points[1],
            points[1],
            steps_per_segment,
        )
    
    result = []
    
    # Extend points with endpoints for Catmull-Rom
    extended = [points[0], *points, points[-1]]
    
    for i in range(1, len(extended) - 2):
        p0 = extended[i - 1]
        p1 = extended[i]
        p2 = extended[i + 1]
        p3 = extended[i + 2]
        
        for j in range(steps_per_segment):
            t = j / steps_per_segment
            t2 = t * t
            t3 = t2 * t
            
            x = 0.5 * ((2 * p1[0]) +
                       (-p0[0] + p2[0]) * t +
                       (2 * p0[0] - 5 * p1[0] + 4 * p2[0] - p3[0]) * t2 +
                       (-p0[0] + 3 * p1[0] - 3 * p2[0] + p3[0]) * t3)
            
            y = 0.5 * ((2 * p1[1]) +
                       (-p0[1] + p2[1]) * t +
                       (2 * p0[1] - 5 * p1[1] + 4 * p2[1] - p3[1]) * t2 +
                       (-p0[1] + 3 * p1[1] - 3 * p2[1] + p3[1]) * t3)
            
            result.append((int(x), int(y)))
    
    result.append(points[-1])
    return result


def parabolic_trajectory(
    start: tuple[int, int],
    peak: tuple[int, int],
    end: tuple[int, int],
    steps: int = 50,
) -> list[tuple[int, int]]:
    """Generate a parabolic (arc) trajectory through three points.
    
    Args:
        start: Starting point.
        peak: Peak/high point of the parabola.
        end: Ending point.
        steps: Number of points to generate.
    
    Returns:
        List of (x, y) tuples along the parabola.
    """
    return bezier_curve_2d(start, peak, peak, end, steps)
