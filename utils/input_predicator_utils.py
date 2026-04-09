"""
Input prediction utilities for reducing perceived latency.

Predict user input trajectories to pre-render UI responses.
"""

from __future__ import annotations

import math
from typing import Optional
from dataclasses import dataclass


@dataclass
class PredictedPath:
    """A predicted input trajectory."""
    points: list[tuple[float, float]]
    confidence: float
    timestamp: float


class TrajectoryPredictor:
    """Predict future input positions based on history."""
    
    def __init__(self, history_size: int = 10, prediction_horizon: int = 5):
        self.history_size = history_size
        self.prediction_horizon = prediction_horizon
        self._velocity_history: list[tuple[float, float, float]] = []
    
    def add_point(self, x: float, y: float, timestamp: float) -> PredictedPath:
        """Add a point and return prediction."""
        self._update_velocity(x, y, timestamp)
        
        if len(self._velocity_history) < 2:
            return PredictedPath([(x, y)], 0.0, timestamp)
        
        vx, vy = self._get_average_velocity()
        confidence = self._calculate_confidence()
        
        points = []
        for i in range(1, self.prediction_horizon + 1):
            px = x + vx * i * 0.016
            py = y + vy * i * 0.016
            points.append((px, py))
        
        return PredictedPath(points, confidence, timestamp)
    
    def _update_velocity(self, x: float, y: float, timestamp: float) -> None:
        """Update velocity history."""
        if self._velocity_history:
            last_x, last_y, last_ts = self._velocity_history[-1]
            dt = timestamp - last_ts
            if dt > 0:
                vx = (x - last_x) / dt
                vy = (y - last_y) / dt
                self._velocity_history.append((vx, vy, timestamp))
        else:
            self._velocity_history.append((0.0, 0.0, timestamp))
        
        if len(self._velocity_history) > self.history_size:
            self._velocity_history.pop(0)
    
    def _get_average_velocity(self) -> tuple[float, float]:
        """Get average velocity from history."""
        if not self._velocity_history:
            return 0.0, 0.0
        
        vx_sum = sum(v[0] for v in self._velocity_history)
        vy_sum = sum(v[1] for v in self._velocity_history)
        n = len(self._velocity_history)
        return vx_sum / n, vy_sum / n
    
    def _calculate_confidence(self) -> float:
        """Calculate prediction confidence based on consistency."""
        if len(self._velocity_history) < 3:
            return 0.5
        
        vx, vy = self._get_average_velocity()
        speed = math.sqrt(vx ** 2 + vy ** 2)
        
        if speed < 10:
            return 0.3
        
        if speed > 2000:
            return 0.4
        
        return 0.7
    
    def reset(self) -> None:
        """Reset predictor state."""
        self._velocity_history.clear()


class BezierPredictor:
    """Predict using Bezier curve fitting."""
    
    def __init__(self, degree: int = 3):
        self.degree = degree
        self._control_points: list[tuple[float, float]] = []
    
    def fit_curve(self, points: list[tuple[float, float]]) -> None:
        """Fit a Bezier curve to the given points."""
        if len(points) < 2:
            return
        
        if len(points) <= self.degree:
            self._control_points = list(points)
        else:
            self._control_points = self._sample_control_points(points)
    
    def _sample_control_points(self, points: list[tuple[float, float]]) -> list[tuple[float, float]]:
        """Sample control points from trajectory."""
        n = len(points)
        step = n / self.degree
        return [points[min(int(i * step), n - 1)] for i in range(self.degree + 1)]
    
    def predict(self, t: float) -> Optional[tuple[float, float]]:
        """Evaluate Bezier curve at parameter t."""
        if not self._control_points:
            return None
        
        n = len(self._control_points) - 1
        x = 0.0
        y = 0.0
        
        for i, (cx, cy) in enumerate(self._control_points):
            coeff = self._binomial(n, i) * (1 - t) ** (n - i) * t ** i
            x += coeff * cx
            y += coeff * cy
        
        return x, y
    
    def _binomial(self, n: int, k: int) -> float:
        """Calculate binomial coefficient."""
        if k < 0 or k > n:
            return 0
        return math.factorial(n) // (math.factorial(k) * math.factorial(n - k))
