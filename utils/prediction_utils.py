"""Prediction utilities for RabAI AutoClick.

Provides:
- Velocity-based position prediction
- Kalman filter for trajectory prediction
- Momentum-based forecasting
- Input prediction for reduced latency
"""

from typing import List, Optional, Tuple, Deque
from collections import deque
import math


class VelocityPredictor:
    """Predict future positions based on velocity history."""

    def __init__(self, history_size: int = 10):
        """Initialize predictor.

        Args:
            history_size: Number of recent samples to track.
        """
        self.history_size = history_size
        self.positions: Deque[Tuple[float, float, float]] = deque(maxlen=history_size)  # x, y, t

    def add_sample(self, x: float, y: float, t: float) -> None:
        """Add a position sample."""
        self.positions.append((x, y, t))

    def predict(self, dt: float) -> Tuple[float, float]:
        """Predict position after dt seconds.

        Args:
            dt: Time ahead to predict.

        Returns:
            (x, y) predicted position.
        """
        if len(self.positions) < 2:
            last = self.positions[-1] if self.positions else (0.0, 0.0, 0.0)
            return (last[0], last[1])

        # Use recent velocity
        p1 = self.positions[-2]
        p2 = self.positions[-1]
        vx = (p2[0] - p1[0]) / max(p2[2] - p1[2], 0.001)
        vy = (p2[1] - p1[1]) / max(p2[2] - p1[2], 0.001)

        return (p2[0] + vx * dt, p2[1] + vy * dt)

    def predict_with_acceleration(self, dt: float) -> Tuple[float, float]:
        """Predict using velocity AND acceleration.

        Args:
            dt: Time ahead.

        Returns:
            (x, y) predicted position.
        """
        if len(self.positions) < 3:
            return self.predict(dt)

        p0 = self.positions[-3]
        p1 = self.positions[-2]
        p2 = self.positions[-1]

        dt1 = max(p1[2] - p0[2], 0.001)
        dt2 = max(p2[2] - p1[2], 0.001)

        vx1 = (p1[0] - p0[0]) / dt1
        vy1 = (p1[1] - p0[1]) / dt1
        vx2 = (p2[0] - p1[0]) / dt2
        vy2 = (p2[1] - p1[1]) / dt2

        ax = (vx2 - vx1) / dt2
        ay = (vy2 - vy1) / dt2

        return (
            p2[0] + vx2 * dt + 0.5 * ax * dt * dt,
            p2[1] + vy2 * dt + 0.5 * ay * dt * dt,
        )

    def reset(self) -> None:
        """Clear history."""
        self.positions.clear()


def exponential_moving_average(
    values: List[float],
    alpha: float = 0.3,
) -> List[float]:
    """Compute exponential moving average.

    Args:
        values: Input time series.
        alpha: Smoothing factor (0-1, lower = smoother).

    Returns:
        EMA values.
    """
    if not values:
        return []
    result: List[float] = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


def double_exponential_smoothing(
    values: List[float],
    alpha: float = 0.3,
    beta: float = 0.1,
) -> List[float]:
    """Double exponential smoothing (trend + level).

    Args:
        values: Input time series.
        alpha: Level smoothing factor.
        beta: Trend smoothing factor.

    Returns:
        Smoothed values.
    """
    if len(values) < 2:
        return values[:]

    level = values[0]
    trend = values[1] - values[0]
    result: List[float] = [level]

    for v in values[1:]:
        prev_level = level
        level = alpha * v + (1 - alpha) * (level + trend)
        trend = beta * (level - prev_level) + (1 - beta) * trend
        result.append(level + trend)

    return result


def predict_trajectory_linear(
    points: List[Tuple[float, float]],
    dt: float,
    num_steps: int = 5,
) -> List[Tuple[float, float]]:
    """Predict trajectory using linear extrapolation.

    Args:
        points: Recent path points.
        dt: Time between points.
        num_steps: Number of future steps to predict.

    Returns:
        Predicted future positions.
    """
    if len(points) < 2:
        return [points[-1]] * num_steps if points else [(0, 0)] * num_steps

    # Fit velocity from recent points
    recent = points[-min(5, len(points)):]
    vx = sum(p[0] - points[max(0, i-1)][0] for i, p in enumerate(recent[1:], 1)) / (len(recent) - 1)
    vy = sum(p[1] - points[max(0, i-1)][1] for i, p in enumerate(recent[1:], 1)) / (len(recent) - 1)

    predictions: List[Tuple[float, float]] = []
    last = points[-1]
    for i in range(1, num_steps + 1):
        predictions.append((
            last[0] + vx * dt * i,
            last[1] + vy * dt * i,
        ))
    return predictions


def predict_curved_trajectory(
    points: List[Tuple[float, float]],
    dt: float,
    num_steps: int = 5,
) -> List[Tuple[float, float]]:
    """Predict trajectory using parabolic/curved extrapolation.

    Args:
        points: Recent path points.
        dt: Time between points.
        num_steps: Future steps to predict.

    Returns:
        Predicted positions.
    """
    if len(points) < 3:
        return predict_trajectory_linear(points, dt, num_steps)

    # Use last 5 points for quadratic fit
    n = min(5, len(points))
    recent = points[-n:]

    # Fit quadratic in x: x = a*t^2 + b*t + c
    # Simple fit using differences
    t_vals = list(range(len(recent)))
    x_vals = [p[0] for p in recent]
    y_vals = [p[1] for p in recent]

    # Compute second differences for acceleration
    x_diff = [x_vals[i+1] - x_vals[i] for i in range(n-1)]
    y_diff = [y_vals[i+1] - y_vals[i] for i in range(n-1)]

    x_accel = sum(x_diff[i+1] - x_diff[i] for i in range(n-2)) / max(n-2, 1) if n > 2 else 0
    y_accel = sum(y_diff[i+1] - y_diff[i] for i in range(n-2)) / max(n-2, 1) if n > 2 else 0

    vx = x_diff[-1] if x_diff else 0
    vy = y_diff[-1] if y_diff else 0

    last_t = len(recent) - 1
    last_x = x_vals[-1]
    last_y = y_vals[-1]

    predictions: List[Tuple[float, float]] = []
    for i in range(1, num_steps + 1):
        t = last_t + i
        dt_local = i * dt
        x = last_x + vx * dt_local + 0.5 * x_accel * dt_local * dt_local
        y = last_y + vy * dt_local + 0.5 * y_accel * dt_local * dt_local
        predictions.append((x, y))

    return predictions


def weighted_average_prediction(
    samples: List[Tuple[float, float]],
    weights: Optional[List[float]] = None,
) -> Tuple[float, float]:
    """Weighted average of samples for prediction.

    Args:
        samples: List of (x, y) samples.
        weights: Optional weights (default: linearly decreasing).

    Returns:
        (x, y) weighted average.
    """
    if not samples:
        return (0.0, 0.0)
    if weights is None:
        n = len(samples)
        weights = [1.0 - 0.1 * i for i in range(n)]
    total_w = sum(weights)
    x = sum(s[0] * w for s, w in zip(samples, weights)) / total_w
    y = sum(s[1] * w for s, w in zip(samples, weights)) / total_w
    return (x, y)
