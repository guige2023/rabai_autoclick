"""Input prediction smoother for reducing jitter in touch/click input."""
from typing import List, Tuple, Optional, Deque
from collections import deque
import math


class InputPredictionSmoother:
    """Smooths input trajectories using various prediction and filtering algorithms.
    
    Reduces jitter in raw input data by applying Kalman filtering,
    moving averages, or exponential smoothing.
    
    Example:
        smoother = InputPredictionSmoother(method="kalman", window_size=5)
        smoothed_point = smoother.smooth((100, 100), velocity=(10, 5))
    """

    def __init__(
        self,
        method: str = "exponential",
        window_size: int = 5,
        smoothing_factor: float = 0.3,
        noise_threshold: float = 2.0,
    ) -> None:
        """Initialize the input smoother.
        
        Args:
            method: Smoothing method - "kalman", "moving_average", "exponential", "median".
            window_size: Size of smoothing window for applicable methods.
            smoothing_factor: Weight for exponential smoothing (0-1).
            noise_threshold: Minimum distance to register as movement.
        """
        self._method = method
        self._window_size = window_size
        self._smoothing_factor = smoothing_factor
        self._noise_threshold = noise_threshold
        self._history: Deque[Tuple[float, float]] = deque(maxlen=window_size)
        self._velocity_history: Deque[Tuple[float, float]] = deque(maxlen=window_size)
        self._last_point: Optional[Tuple[float, float]] = None
        
        # Kalman filter state
        self._kalman_x = KalmanState()
        self._kalman_y = KalmanState()

    def smooth(
        self,
        point: Tuple[float, float],
        velocity: Optional[Tuple[float, float]] = None,
    ) -> Tuple[float, float]:
        """Smooth an input point using the configured method.
        
        Args:
            point: Raw (x, y) input coordinates.
            velocity: Optional velocity (vx, vy) for Kalman filtering.
            
        Returns:
            Smoothed (x, y) coordinates.
        """
        if self._method == "kalman":
            return self._smooth_kalman(point, velocity)
        elif self._method == "moving_average":
            return self._smooth_moving_average(point)
        elif self._method == "median":
            return self._smooth_median(point)
        else:
            return self._smooth_exponential(point)

    def _smooth_kalman(
        self,
        point: Tuple[float, float],
        velocity: Optional[Tuple[float, float]],
    ) -> Tuple[float, float]:
        """Apply Kalman filter smoothing."""
        x = self._kalman_x.update(point[0], velocity[0] if velocity else None)
        y = self._kalman_y.update(point[1], velocity[1] if velocity else None)
        return (x, y)

    def _smooth_moving_average(self, point: Tuple[float, float]) -> Tuple[float, float]:
        """Apply moving average smoothing."""
        self._history.append(point)
        if len(self._history) == 1:
            return point
        
        sum_x = sum(p[0] for p in self._history)
        sum_y = sum(p[1] for p in self._history)
        count = len(self._history)
        return (sum_x / count, sum_y / count)

    def _smooth_median(self, point: Tuple[float, float]) -> Tuple[float, float]:
        """Apply median filter smoothing."""
        self._history.append(point)
        if len(self._history) == 1:
            return point
        
        xs = sorted(p[0] for p in self._history)
        ys = sorted(p[1] for p in self._history)
        n = len(xs)
        mid = n // 2
        
        if n % 2 == 0:
            median_x = (xs[mid - 1] + xs[mid]) / 2
            median_y = (ys[mid - 1] + ys[mid]) / 2
        else:
            median_x = xs[mid]
            median_y = ys[mid]
        
        return (median_x, median_y)

    def _smooth_exponential(self, point: Tuple[float, float]) -> Tuple[float, float]:
        """Apply exponential smoothing."""
        if self._last_point is None:
            self._last_point = point
            return point
        
        alpha = self._smoothing_factor
        smoothed_x = alpha * point[0] + (1 - alpha) * self._last_point[0]
        smoothed_y = alpha * point[1] + (1 - alpha) * self._last_point[1]
        
        self._last_point = (smoothed_x, smoothed_y)
        return (smoothed_x, smoothed_y)

    def predict(
        self,
        point: Tuple[float, float],
        dt: float = 0.016,
    ) -> Tuple[float, float]:
        """Predict the next position based on velocity.
        
        Args:
            point: Current (x, y) position.
            dt: Time delta in seconds.
            
        Returns:
            Predicted (x, y) position.
        """
        if len(self._velocity_history) < 2:
            return point
        
        # Calculate average velocity from history
        avg_vx = sum(v[0] for v in self._velocity_history) / len(self._velocity_history)
        avg_vy = sum(v[1] for v in self._velocity_history) / len(self._velocity_history)
        
        return (point[0] + avg_vx * dt, point[1] + avg_vy * dt)

    def is_noise(self, point: Tuple[float, float]) -> bool:
        """Check if a point represents noise vs actual movement.
        
        Args:
            point: Point to check.
            
        Returns:
            True if point is likely noise.
        """
        if self._last_point is None:
            return False
        
        dx = point[0] - self._last_point[0]
        dy = point[1] - self._last_point[1]
        distance = math.sqrt(dx * dx + dy * dy)
        
        return distance < self._noise_threshold

    def reset(self) -> None:
        """Reset all smoothing state."""
        self._history.clear()
        self._velocity_history.clear()
        self._last_point = None
        self._kalman_x = KalmanState()
        self._kalman_y = KalmanState()


class KalmanState:
    """Simple 1D Kalman filter state."""
    
    def __init__(self) -> None:
        self._estimate = 0.0
        self._error_estimate = 1.0
        self._error_measurement = 1.0
        self._last_measurement = 0.0
    
    def update(self, measurement: float, velocity: Optional[float] = None) -> float:
        """Update state with new measurement.
        
        Args:
            measurement: New measurement value.
            velocity: Optional velocity for prediction.
            
        Returns:
            Filtered estimate.
        """
        self._last_measurement = measurement
        
        # Prediction step
        if velocity is not None:
            predicted = self._estimate + velocity * 0.016
        else:
            predicted = self._estimate
        
        # Update step
        self._error_estimate = self._error_estimate + self._error_measurement ** 2
        kalman_gain = self._error_estimate / (self._error_estimate + self._error_measurement ** 2)
        self._estimate = predicted + kalman_gain * (measurement - predicted)
        self._error_estimate = (1 - kalman_gain) * self._error_estimate
        
        return self._estimate
    
    def reset(self) -> None:
        """Reset filter state."""
        self._estimate = 0.0
        self._error_estimate = 1.0
        self._last_measurement = 0.0
