"""
Input Prediction Utilities

Predict the likely next input position or gesture based on
historical input data, supporting smoother automation playback.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, List, Tuple


@dataclass
class PredictionResult:
    """Result of an input prediction."""
    predicted_x: float
    predicted_y: float
    confidence: float  # 0.0 to 1.0
    method: str  # 'linear', 'exponential_smoothing', 'velocity'
    lookahead_ms: float


class InputPredictor:
    """
    Predict next input positions based on historical input events.

    Uses multiple prediction methods and selects the most confident one.
    """

    def __init__(
        self,
        history_window: int = 10,
        smoothing_factor: float = 0.3,
        min_confidence: float = 0.5,
    ):
        self.history_window = history_window
        self.smoothing_factor = smoothing_factor
        self.min_confidence = min_confidence
        self._x_history: List[float] = []
        self._y_history: List[float] = []
        self._t_history: List[float] = []

    def add_sample(self, x: float, y: float, timestamp_ms: float) -> None:
        """Add an input sample to the history."""
        self._x_history.append(x)
        self._y_history.append(y)
        self._t_history.append(timestamp_ms)

        if len(self._x_history) > self.history_window:
            self._x_history.pop(0)
            self._y_history.pop(0)
            self._t_history.pop(0)

    def predict(self, lookahead_ms: float = 50.0) -> Optional[PredictionResult]:
        """
        Predict the next input position.

        Args:
            lookahead_ms: How far ahead to predict in milliseconds.

        Returns:
            PredictionResult or None if not enough data.
        """
        if len(self._x_history) < 3:
            return None

        # Method 1: Velocity-based prediction
        vel_result = self._predict_velocity(lookahead_ms)
        vel_confidence = vel_result.confidence if vel_result else 0.0

        # Method 2: Exponential smoothing
        smooth_result = self._predict_exponential_smoothing()
        smooth_confidence = smooth_result.confidence if smooth_result else 0.0

        # Select best method
        if vel_confidence > smooth_confidence and vel_result:
            return vel_result
        elif smooth_result:
            return smooth_result

        return None

    def _predict_velocity(self, lookahead_ms: float) -> Optional[PredictionResult]:
        """Predict using velocity extrapolation."""
        if len(self._x_history) < 3:
            return None

        # Compute average velocity from last few samples
        n = min(5, len(self._x_history))
        dx_total = 0.0
        dy_total = 0.0
        dt_total = 0.001  # avoid div by zero

        for i in range(-n + 1, 0):
            dt = (self._t_history[i] - self._t_history[i - 1]) / 1000.0
            dx_total += self._x_history[i] - self._x_history[i - 1]
            dy_total += self._y_history[i] - self._y_history[i - 1]
            dt_total += dt

        vx = dx_total / dt_total
        vy = dy_total / dt_total

        # Confidence based on velocity consistency
        speeds = []
        for i in range(-n + 1, 0):
            dt = max(0.001, (self._t_history[i] - self._t_history[i - 1]) / 1000.0)
            dx = self._x_history[i] - self._x_history[i - 1]
            dy = self._y_history[i] - self._y_history[i - 1]
            speeds.append(math.sqrt(dx * dx + dy * dy) / dt)
        avg_speed = sum(speeds) / len(speeds)
        confidence = min(1.0, avg_speed / 2000.0) if avg_speed > 0 else 0.5

        last_x = self._x_history[-1]
        last_y = self._y_history[-1]
        lookahead_s = lookahead_ms / 1000.0

        return PredictionResult(
            predicted_x=last_x + vx * lookahead_s,
            predicted_y=last_y + vy * lookahead_s,
            confidence=max(0.0, min(1.0, confidence)),
            method="velocity",
            lookahead_ms=lookahead_ms,
        )

    def _predict_exponential_smoothing(self) -> Optional[PredictionResult]:
        """Predict using exponential smoothing of recent positions."""
        if len(self._x_history) < 2:
            return None

        alpha = self.smoothing_factor
        # Smooth the last position
        sx = self._x_history[-1]
        sy = self._y_history[-1]
        for i in range(len(self._x_history) - 2, -1, -1):
            sx = alpha * self._x_history[i] + (1 - alpha) * sx
            sy = alpha * self._y_history[i] + (1 - alpha) * sy

        # Confidence based on convergence
        last_dist = math.sqrt(
            (self._x_history[-1] - sx) ** 2 + (self._y_history[-1] - sy) ** 2
        )
        confidence = max(0.0, 1.0 - last_dist / 100.0)

        return PredictionResult(
            predicted_x=sx,
            predicted_y=sy,
            confidence=min(1.0, confidence),
            method="exponential_smoothing",
            lookahead_ms=0.0,
        )
