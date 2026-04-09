"""
Animation curve builder utilities.

Build custom animation timing curves for smooth UI transitions.
"""

from __future__ import annotations

import math
from typing import Callable


class CurveBuilder:
    """Builder for custom animation curves."""
    
    def __init__(self):
        self._control_points: list[tuple[float, float]] = []
    
    def add_point(self, x: float, y: float) -> "CurveBuilder":
        """Add a control point (x and y should be 0-1)."""
        self._control_points.append((max(0, min(1, x)), max(0, min(1, y))))
        return self
    
    def clear(self) -> "CurveBuilder":
        """Clear all control points."""
        self._control_points.clear()
        return self
    
    def build(self) -> Callable[[float], float]:
        """Build the curve function."""
        if not self._control_points:
            return lambda t: t
        
        points = sorted(self._control_points)
        
        if points[0][0] > 0:
            points.insert(0, (0.0, 0.0))
        if points[-1][0] < 1:
            points.append((1.0, 1.0))
        
        def curve(t: float) -> float:
            t = max(0, min(1, t))
            
            for i in range(len(points) - 1):
                x0, y0 = points[i]
                x1, y1 = points[i + 1]
                
                if x0 <= t <= x1:
                    local_t = (t - x0) / (x1 - x0) if x1 != x0 else 0
                    return y0 + (y1 - y0) * self._ease(local_t)
            
            return t
        
        return curve
    
    def _ease(self, t: float) -> float:
        """Default easing within segments."""
        return t * t * (3 - 2 * t)


class CubicBezierBuilder:
    """Builder for cubic Bezier curves."""
    
    def __init__(self):
        self.p0 = (0.0, 0.0)
        self.p1 = (0.0, 0.0)
        self.p2 = (1.0, 1.0)
        self.p3 = (1.0, 1.0)
    
    def set_start(self, x: float, y: float) -> "CubicBezierBuilder":
        """Set start point (should be 0,0)."""
        self.p0 = (x, y)
        return self
    
    def set_control1(self, x: float, y: float) -> "CubicBezierBuilder":
        """Set first control point."""
        self.p1 = (x, y)
        return self
    
    def set_control2(self, x: float, y: float) -> "CubicBezierBuilder":
        """Set second control point."""
        self.p2 = (x, y)
        return self
    
    def set_end(self, x: float, y: float) -> "CubicBezierBuilder":
        """Set end point (should be 1,1)."""
        self.p3 = (x, y)
        return self
    
    def build(self) -> Callable[[float], float]:
        """Build the Bezier curve function."""
        def bezier(t: float) -> float:
            t = max(0, min(1, t))
            mt = 1 - t
            
            x = (
                mt * mt * mt * self.p0[1] +
                3 * mt * mt * t * self.p1[1] +
                3 * mt * t * t * self.p2[1] +
                t * t * t * self.p3[1]
            )
            return x
        
        return bezier


class ElasticCurve:
    """Predefined elastic easing curves."""
    
    @staticmethod
    def ease_in(t: float, amplitude: float = 1.0, period: float = 0.3) -> float:
        """Elastic ease-in."""
        if t == 0 or t == 1:
            return t
        return -(2 ** (10 * t - 10)) * math.sin((t * 10 - 10.75) * (2 * math.pi) / period) * amplitude
    
    @staticmethod
    def ease_out(t: float, amplitude: float = 1.0, period: float = 0.3) -> float:
        """Elastic ease-out."""
        if t == 0 or t == 1:
            return t
        return (2 ** (-10 * t)) * math.sin((t * 10 - 0.75) * (2 * math.pi) / period) * amplitude + 1
    
    @staticmethod
    def ease_in_out(t: float, amplitude: float = 1.0, period: float = 0.3) -> float:
        """Elastic ease-in-out."""
        if t == 0 or t == 1:
            return t
        
        if t < 0.5:
            return -((2 ** (20 * t - 10)) * math.sin((20 * t - 11.125) * (2 * math.pi) / period)) * amplitude / 2
        return ((2 ** (-20 * t + 10)) * math.sin((20 * t - 11.125) * (2 * math.pi) / period)) * amplitude / 2 + 1


class BackCurve:
    """Predefined back easing curves."""
    
    @staticmethod
    def ease_in(t: float, overshoot: float = 1.70158) -> float:
        """Back ease-in."""
        return t * t * ((overshoot + 1) * t - overshoot)
    
    @staticmethod
    def ease_out(t: float, overshoot: float = 1.70158) -> float:
        """Back ease-out."""
        return 1 + (t - 1) ** 2 * ((overshoot + 1) * (t - 1) + overshoot)
    
    @staticmethod
    def ease_in_out(t: float, overshoot: float = 1.70158) -> float:
        """Back ease-in-out."""
        if t < 0.5:
            return (t * t * ((overshoot * 1.525 + 1) * t - overshoot * 1.525)) * 2
        return ((t * 2 - 2) ** 2 * ((overshoot * 1.525 + 1) * (t * 2 - 2) + overshoot * 1.525) + 2) / 2


class StepCurve:
    """Step easing for discrete animations."""
    
    @staticmethod
    def steps(count: int, position: str = "end") -> Callable[[float], float]:
        """Create a step function.
        
        Args:
            count: Number of steps
            position: 'start', 'end', or 'middle'
        """
        def step(t: float) -> float:
            if position == "start":
                return math.ceil(t * count) / count
            elif position == "middle":
                return math.floor(t * count + 0.5) / count
            else:
                return math.floor(t * count + 0.9999999999) / count
        
        return step
