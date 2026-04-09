"""Kalman filter utilities for RabAI AutoClick.

Provides:
- 1D and 2D Kalman filters for trajectory smoothing
- Mouse/touch point prediction
- Noise reduction for input streams
"""

from typing import List, Optional, Tuple, Any
from dataclasses import dataclass, field
import math


@dataclass
class Kalman1DState:
    """State for 1D Kalman filter."""
    estimate: float = 0.0
    error_estimate: float = 1.0
    error_measurement: float = 1.0
    last_measurement: Optional[float] = None
    q: float = 0.1  # Process noise


class Kalman1D:
    """1D Kalman filter for smoothing scalar input streams."""

    def __init__(self, q: float = 0.1, r: float = 1.0):
        """Initialize 1D Kalman filter.

        Args:
            q: Process noise covariance.
            r: Measurement noise covariance.
        """
        self.state = Kalman1DState(error_measurement=r, q=q)

    def update(self, measurement: float) -> float:
        """Update filter with new measurement.

        Args:
            measurement: New input value.

        Returns:
            Filtered estimate.
        """
        s = self.state
        if s.last_measurement is None:
            s.estimate = measurement
            s.last_measurement = measurement
            return s.estimate

        # Prediction
        s.error_estimate += s.q

        # Update
        kalman_gain = s.error_estimate / (s.error_estimate + s.error_measurement)
        s.estimate += kalman_gain * (measurement - s.estimate)
        s.error_estimate *= (1.0 - kalman_gain)

        s.last_measurement = measurement
        return s.estimate

    def reset(self, initial: float = 0.0) -> None:
        """Reset filter state."""
        self.state = Kalman1DState(estimate=initial, last_measurement=None)


@dataclass
class Kalman2DState:
    """State for 2D Kalman filter (position only)."""
    x: float = 0.0
    y: float = 0.0
    vx: float = 0.0
    vy: float = 0.0
    px: float = 1.0  # Position error
    py: float = 1.0
    pvx: float = 1.0
    pvy: float = 1.0


class Kalman2D:
    """2D Kalman filter with velocity for mouse/touch trajectory smoothing."""

    def __init__(
        self,
        q_pos: float = 0.01,
        q_vel: float = 0.1,
        r_pos: float = 0.1,
    ):
        """Initialize 2D Kalman filter.

        Args:
            q_pos: Position process noise.
            q_vel: Velocity process noise.
            r_pos: Position measurement noise.
        """
        self.state = Kalman2DState()
        self.q_pos = q_pos
        self.q_vel = q_vel
        self.r_pos = r_pos
        self._initialized = False
        self._last_t: Optional[float] = None

    def update(self, x: float, y: float, t: Optional[float] = None) -> Tuple[float, float]:
        """Update filter with new position.

        Args:
            x: Measured X position.
            y: Measured Y position.
            t: Optional timestamp (seconds).

        Returns:
            Filtered (x, y) position.
        """
        s = self.state

        if not self._initialized:
            s.x = x
            s.y = y
            self._initialized = True
            self._last_t = t
            return (s.x, s.y)

        dt = 1.0
        if t is not None and self._last_t is not None:
            dt = max(t - self._last_t, 0.001)
            self._last_t = t

        # Predict: propagate state
        s.x += s.vx * dt
        s.y += s.vy * dt
        s.px += dt * dt * self.q_vel
        s.py += dt * dt * self.q_vel

        # Update
        kx = s.px / (s.px + self.r_pos)
        ky = s.py / (s.py + self.r_pos)

        s.x += kx * (x - s.x)
        s.y += ky * (y - s.y)

        s.px *= (1.0 - kx)
        s.py *= (1.0 - ky)

        if dt > 0:
            s.vx = (s.x - s.x) / dt
            s.vy = (s.y - s.y) / dt

        return (s.x, s.y)

    def predict(self, dt: float) -> Tuple[float, float]:
        """Predict position after dt seconds.

        Args:
            dt: Time delta in seconds.

        Returns:
            Predicted (x, y) position.
        """
        s = self.state
        return (s.x + s.vx * dt, s.y + s.vy * dt)

    def reset(self, x: float = 0.0, y: float = 0.0) -> None:
        """Reset filter state."""
        self.state = Kalman2DState(x=x, y=y)
        self._initialized = False
        self._last_t = None


class TrajectoryKalmanSmoother:
    """Kalman smoother for a stream of 2D points."""

    def __init__(
        self,
        q_pos: float = 0.005,
        q_vel: float = 0.05,
        r_pos: float = 0.05,
    ):
        """Initialize smoother.

        Args:
            q_pos: Position process noise.
            q_vel: Velocity process noise.
            r_pos: Position measurement noise.
        """
        self.filter = Kalman2D(q_pos=q_pos, q_vel=q_vel, r_pos=r_pos)
        self.points: List[Tuple[float, float]] = []

    def add_point(self, x: float, y: float, t: Optional[float] = None) -> Tuple[float, float]:
        """Add a point and get smoothed result.

        Args:
            x: X coordinate.
            y: Y coordinate.
            t: Optional timestamp.

        Returns:
            Smoothed (x, y).
        """
        smoothed = self.filter.update(x, y, t)
        self.points.append(smoothed)
        return smoothed

    def smooth(self, points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Smooth a list of points.

        Args:
            points: List of (x, y) coordinates.

        Returns:
            Smoothed coordinates.
        """
        self.reset()
        results = []
        for x, y in points:
            results.append(self.add_point(x, y))
        return results

    def reset(self) -> None:
        """Reset all state."""
        self.filter.reset()
        self.points.clear()


def smooth_trajectory_kalman(
    points: List[Tuple[float, float]],
    q_pos: float = 0.005,
    q_vel: float = 0.05,
    r_pos: float = 0.05,
) -> List[Tuple[float, float]]:
    """Convenience function to smooth a trajectory.

    Args:
        points: List of (x, y) coordinates.
        q_pos: Position process noise.
        q_vel: Velocity process noise.
        r_pos: Position measurement noise.

    Returns:
        Smoothed trajectory.
    """
    smoother = TrajectoryKalmanSmoother(q_pos=q_pos, q_vel=q_vel, r_pos=r_pos)
    return smoother.smooth(points)
