"""
Touch surface mapping utilities.

Map touch coordinates across different screen densities and surfaces.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class SurfaceConfig:
    """Configuration for a touch surface."""
    width: int
    height: int
    density: float = 1.0
    offset_x: int = 0
    offset_y: int = 0
    rotation: int = 0


@dataclass
class Point:
    """2D point with optional pressure."""
    x: float
    y: float
    pressure: float = 1.0


class TouchSurfaceMapper:
    """Map touch points between different surfaces."""
    
    def __init__(self, source: SurfaceConfig, target: SurfaceConfig):
        self.source = source
        self.target = target
    
    def map_point(self, point: Point) -> Point:
        """Map a point from source to target surface."""
        x, y = point.x, point.y
        
        x = (x - self.source.offset_x) / self.source.density
        y = (y - self.source.offset_y) / self.source.density
        
        if self.source.rotation != 0:
            x, y = self._rotate(x, y, self.source.rotation, self.source.width, self.source.height)
        
        x = x * self.target.density + self.target.offset_x
        y = y * self.target.density + self.target.offset_y
        
        x = max(0, min(self.target.width, x))
        y = max(0, min(self.target.height, y))
        
        return Point(x=x, y=y, pressure=point.pressure)
    
    def _rotate(self, x: float, y: float, degrees: int, w: int, h: int) -> tuple[float, float]:
        """Rotate point around center."""
        if degrees == 90:
            return y, h - x
        elif degrees == 180:
            return w - x, h - y
        elif degrees == 270:
            return w - y, x
        return x, y
    
    def map_path(self, points: list[Point]) -> list[Point]:
        """Map a path of points."""
        return [self.map_point(p) for p in points]


class GestureNormalizer:
    """Normalize gestures to a standard coordinate space."""
    
    def __init__(self, surface_width: int = 1000, surface_height: int = 1000):
        self.surface_width = surface_width
        self.surface_height = surface_height
    
    def normalize_path(self, points: list[Point]) -> list[Point]:
        """Normalize gesture path to standard space."""
        if not points:
            return []
        
        min_x = min(p.x for p in points)
        max_x = max(p.x for p in points)
        min_y = min(p.y for p in points)
        max_y = max(p.y for p in points)
        
        width = max_x - min_x or 1
        height = max_y - min_y or 1
        
        scale_x = self.surface_width / width
        scale_y = self.surface_height / height
        scale = min(scale_x, scale_y)
        
        normalized = []
        for p in points:
            nx = (p.x - min_x) * scale
            ny = (p.y - min_y) * scale
            normalized.append(Point(nx, ny, p.pressure))
        
        return normalized
    
    def denormalize_path(self, points: list[Point], target_bounds: tuple[float, float, float, float]) -> list[Point]:
        """Denormalize path back to original space."""
        if not points:
            return []
        
        min_x, min_y, width, height = target_bounds
        
        scale_x = width / self.surface_width
        scale_y = height / self.surface_height
        
        denormalized = []
        for p in points:
            dx = p.x * scale_x + min_x
            dy = p.y * scale_y + min_y
            denormalized.append(Point(dx, dy, p.pressure))
        
        return denormalized


class TouchCalibrator:
    """Calibrate touch input for accuracy."""
    
    def __init__(self):
        self._calibration_points: list[tuple[Point, Point]] = []
    
    def add_calibration_pair(self, expected: Point, actual: Point) -> None:
        """Add a calibration point pair."""
        self._calibration_points.append((expected, actual))
    
    def clear_calibration(self) -> None:
        """Clear all calibration points."""
        self._calibration_points.clear()
    
    def calibrate_point(self, point: Point) -> Point:
        """Apply calibration to a point."""
        if len(self._calibration_points) < 2:
            return point
        
        errors_x = [actual.x - expected.x for expected, actual in self._calibration_points]
        errors_y = [actual.y - expected.y for expected, actual in self._calibration_points]
        
        avg_error_x = sum(errors_x) / len(errors_x)
        avg_error_y = sum(errors_y) / len(errors_y)
        
        return Point(
            x=point.x + avg_error_x,
            y=point.y + avg_error_y,
            pressure=point.pressure
        )
    
    def calibrate_path(self, points: list[Point]) -> list[Point]:
        """Apply calibration to a path."""
        return [self.calibrate_point(p) for p in points]
