"""Input prediction utilities for UI automation.

Provides utilities for predicting user input trajectories,
anticipating touch locations, and reducing perceived latency.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class PredictedPoint:
    """A predicted input point."""
    x: float
    y: float
    timestamp_ms: float
    confidence: float
    prediction_horizon_ms: float


@dataclass
class TrajectoryState:
    """State of a trajectory for prediction."""
    points: Deque[Tuple[float, float, float]] = field(default_factory=deque)
    velocities: Deque[float] = field(default_factory=deque)
    accelerations: Deque[float] = field(default_factory=deque)
    last_update_ms: float = 0.0


class InputPredictor:
    """Predicts future input positions based on historical data.
    
    Uses velocity and acceleration models to predict
    where a user will touch next, reducing perceived latency.
    """
    
    def __init__(
        self,
        history_size: int = 10,
        prediction_horizon_ms: float = 50.0,
        min_confidence: float = 0.5
    ) -> None:
        """Initialize the input predictor.
        
        Args:
            history_size: Number of points to keep in history.
            prediction_horizon_ms: How far ahead to predict.
            min_confidence: Minimum confidence threshold.
        """
        self.history_size = history_size
        self.prediction_horizon_ms = prediction_horizon_ms
        self.min_confidence = min_confidence
        self._state = TrajectoryState()
    
    def add_sample(
        self,
        x: float,
        y: float,
        timestamp_ms: float
    ) -> Optional[PredictedPoint]:
        """Add a sample and return a prediction.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp_ms: Timestamp in milliseconds.
            
        Returns:
            Predicted point or None.
        """
        self._update_state(x, y, timestamp_ms)
        return self._predict(timestamp_ms)
    
    def _update_state(
        self,
        x: float,
        y: float,
        timestamp_ms: float
    ) -> None:
        """Update trajectory state with new sample.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp_ms: Timestamp in milliseconds.
        """
        self._state.points.append((x, y, timestamp_ms))
        
        while len(self._state.points) > self.history_size:
            self._state.points.popleft()
        
        if len(self._state.points) >= 2:
            p1 = self._state.points[-2]
            p2 = self._state.points[-1]
            dx = p2[0] - p1[0]
            dy = p2[1] - p1[1]
            dt = max(p2[2] - p1[2], 0.001)
            velocity = math.sqrt(dx * dx + dy * dy) / dt * 1000
            self._state.velocities.append(velocity)
        
        while self._state.velocities:
            self._state.velocities.popleft()
        
        if len(self._state.velocities) >= 2:
            v1 = self._state.velocities[-2]
            v2 = self._state.velocities[-1]
            dt = 0.001
            accel = (v2 - v1) / dt
            self._state.accelerations.append(accel)
        
        while self._state.accelerations:
            self._state.accelerations.popleft()
        
        self._state.last_update_ms = timestamp_ms
    
    def _predict(self, current_time_ms: float) -> Optional[PredictedPoint]:
        """Generate prediction based on current state.
        
        Args:
            current_time_ms: Current time in milliseconds.
            
        Returns:
            Predicted point or None.
        """
        if len(self._state.points) < 2:
            return None
        
        last_x, last_y, last_time = self._state.points[-1]
        
        avg_velocity = sum(self._state.velocities) / len(self._state.velocities)
        
        if self._state.velocities:
            recent_velocity = self._state.velocities[-1]
        else:
            recent_velocity = avg_velocity
        
        if self._state.accelerations:
            avg_accel = sum(self._state.accelerations) / len(
                self._state.accelerations
            )
        else:
            avg_accel = 0.0
        
        direction_x = 0.0
        direction_y = 0.0
        if len(self._state.points) >= 2:
            p1 = self._state.points[-2]
            p2 = self._state.points[-1]
            dx = p2[0] - p1[0]
            dy = p2[1] - p1[1]
            dist = math.sqrt(dx * dx + dy * dy)
            if dist > 0:
                direction_x = dx / dist
                direction_y = dy / dist
        
        t = self.prediction_horizon_ms / 1000.0
        
        predicted_x = last_x + direction_x * recent_velocity * t
        predicted_y = last_y + direction_y * recent_velocity * t
        
        confidence = self._calculate_confidence()
        
        if confidence < self.min_confidence:
            return None
        
        return PredictedPoint(
            x=predicted_x,
            y=predicted_y,
            timestamp_ms=current_time_ms + self.prediction_horizon_ms,
            confidence=confidence,
            prediction_horizon_ms=self.prediction_horizon_ms
        )
    
    def _calculate_confidence(self) -> float:
        """Calculate prediction confidence.
        
        Returns:
            Confidence value between 0 and 1.
        """
        if len(self._state.points) < 3:
            return 0.3
        
        if len(self._state.velocities) < 2:
            return 0.5
        
        v1 = self._state.velocities[-2]
        v2 = self._state.velocities[-1]
        
        if v1 == 0:
            return 0.3
        
        velocity_change_ratio = abs(v2 - v1) / v1
        
        if velocity_change_ratio > 0.5:
            return 0.4
        
        consistency = max(0.0, 1.0 - velocity_change_ratio)
        
        history_factor = min(1.0, len(self._state.points) / self.history_size)
        
        confidence = consistency * 0.7 + history_factor * 0.3
        
        return confidence
    
    def get_current_velocity(self) -> float:
        """Get the current estimated velocity.
        
        Returns:
            Velocity in units per second.
        """
        if self._state.velocities:
            return self._state.velocities[-1]
        return 0.0
    
    def get_current_direction(self) -> Tuple[float, float]:
        """Get the current estimated direction.
        
        Returns:
            Tuple of (dx, dy) normalized direction.
        """
        if len(self._state.points) < 2:
            return (0.0, 0.0)
        
        p1 = self._state.points[-2]
        p2 = self._state.points[-1]
        
        dx = p2[0] - p1[0]
        dy = p2[1] - p1[1]
        dist = math.sqrt(dx * dx + dy * dy)
        
        if dist == 0:
            return (0.0, 0.0)
        
        return (dx / dist, dy / dist)
    
    def reset(self) -> None:
        """Reset predictor state."""
        self._state = TrajectoryState()


class KalmanInputPredictor(InputPredictor):
    """Input predictor using Kalman filtering.
    
    Uses a Kalman filter to smooth trajectory data
    and provide more accurate predictions.
    """
    
    def __init__(
        self,
        history_size: int = 10,
        prediction_horizon_ms: float = 50.0,
        process_noise: float = 0.01,
        measurement_noise: float = 1.0
    ) -> None:
        """Initialize the Kalman predictor.
        
        Args:
            history_size: Number of points to keep in history.
            prediction_horizon_ms: How far ahead to predict.
            process_noise: Process noise variance.
            measurement_noise: Measurement noise variance.
        """
        super().__init__(history_size, prediction_horizon_ms)
        self.process_noise = process_noise
        self.measurement_noise = measurement_noise
        self._kalman_x = _Kalman1D(process_noise, measurement_noise)
        self._kalman_y = _Kalman1D(process_noise, measurement_noise)
    
    def _update_state(
        self,
        x: float,
        y: float,
        timestamp_ms: float
    ) -> None:
        """Update state with Kalman-filtered values.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp_ms: Timestamp in milliseconds.
        """
        filtered_x = self._kalman_x.update(x)
        filtered_y = self._kalman_y.update(y)
        super()._update_state(filtered_x, filtered_y, timestamp_ms)


class _Kalman1D:
    """1D Kalman filter for smoothing values."""
    
    def __init__(self, process_noise: float, measurement_noise: float) -> None:
        """Initialize 1D Kalman filter.
        
        Args:
            process_noise: Process noise variance.
            measurement_noise: Measurement noise variance.
        """
        self.process_noise = process_noise
        self.measurement_noise = measurement_noise
        self.estimate = 0.0
        self.estimate_error = 1.0
        self.is_initialized = False
    
    def update(self, measurement: float) -> float:
        """Update filter with new measurement.
        
        Args:
            measurement: New measurement.
            
        Returns:
            Filtered estimate.
        """
        if not self.is_initialized:
            self.estimate = measurement
            self.is_initialized = True
            return self.estimate
        
        prediction_error = self.estimate_error + self.process_noise
        kalman_gain = prediction_error / (
            prediction_error + self.measurement_noise
        )
        
        self.estimate = self.estimate + kalman_gain * (
            measurement - self.estimate
        )
        self.estimate_error = (1 - kalman_gain) * prediction_error
        
        return self.estimate


class GesturePredictor:
    """Predicts gesture completion and type.
    
    Analyzes partial gesture input to predict
    what gesture the user intends.
    """
    
    def __init__(self) -> None:
        """Initialize the gesture predictor."""
        self._points: Deque[Tuple[float, float, float]] = deque(maxlen=50)
        self._gesture_templates: Dict[str, List[Tuple[float, float]]] = {}
        self._register_default_templates()
    
    def _register_default_templates(self) -> None:
        """Register default gesture templates."""
        self._gesture_templates["horizontal_swipe"] = [
            (0.0, 0.0), (0.3, 0.0), (0.7, 0.0), (1.0, 0.0)
        ]
        self._gesture_templates["vertical_swipe"] = [
            (0.0, 0.0), (0.0, 0.3), (0.0, 0.7), (0.0, 1.0)
        ]
        self._gesture_templates["tap"] = [
            (0.0, 0.0), (0.5, 0.0), (1.0, 0.0)
        ]
        self._gesture_templates["diagonal_swipe"] = [
            (0.0, 0.0), (0.5, 0.5), (1.0, 1.0)
        ]
    
    def add_point(
        self,
        x: float,
        y: float,
        timestamp_ms: float
    ) -> None:
        """Add a point to the gesture.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp_ms: Timestamp in milliseconds.
        """
        self._points.append((x, y, timestamp_ms))
    
    def predict_gesture(self) -> Optional[Tuple[str, float]]:
        """Predict the current gesture type.
        
        Returns:
            Tuple of (gesture_type, confidence) or None.
        """
        if len(self._points) < 3:
            return None
        
        normalized = self._normalize_points()
        
        best_match = None
        best_score = 0.0
        
        for gesture_type, template in self._gesture_templates.items():
            score = self._match_template(normalized, template)
            if score > best_score:
                best_score = score
                best_match = gesture_type
        
        if best_score < 0.5:
            return None
        
        return (best_match, best_score)
    
    def _normalize_points(self) -> List[Tuple[float, float]]:
        """Normalize points to 0-1 range.
        
        Returns:
            Normalized points.
        """
        if not self._points:
            return []
        
        xs = [p[0] for p in self._points]
        ys = [p[1] for p in self._points]
        
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        
        width = max_x - min_x if max_x != min_x else 1
        height = max_y - min_y if max_y != min_y else 1
        
        normalized = []
        for x, y, _ in self._points:
            nx = (x - min_x) / width
            ny = (y - min_y) / height
            normalized.append((nx, ny))
        
        return normalized
    
    def _match_template(
        self,
        points: List[Tuple[float, float]],
        template: List[Tuple[float, float]]
    ) -> float:
        """Match normalized points against a template.
        
        Args:
            points: Normalized gesture points.
            template: Template points.
            
        Returns:
            Match score between 0 and 1.
        """
        if len(points) < len(template):
            return 0.0
        
        step = len(points) / len(template)
        
        score = 0.0
        for i, template_point in enumerate(template):
            idx = int(i * step)
            if idx >= len(points):
                idx = len(points) - 1
            
            gesture_point = points[idx]
            
            dx = gesture_point[0] - template_point[0]
            dy = gesture_point[1] - template_point[1]
            distance = math.sqrt(dx * dx + dy * dy)
            
            score += max(0, 1 - distance)
        
        return score / len(template)
    
    def reset(self) -> None:
        """Reset the gesture predictor."""
        self._points.clear()


def create_velocity_predictor(
    prediction_horizon_ms: float = 50.0
) -> InputPredictor:
    """Create a velocity-based input predictor.
    
    Args:
        prediction_horizon_ms: How far ahead to predict.
        
    Returns:
        Configured InputPredictor.
    """
    return InputPredictor(
        history_size=10,
        prediction_horizon_ms=prediction_horizon_ms,
        min_confidence=0.5
    )


def create_kalman_predictor(
    prediction_horizon_ms: float = 50.0
) -> KalmanInputPredictor:
    """Create a Kalman-filtered input predictor.
    
    Args:
        prediction_horizon_ms: How far ahead to predict.
        
    Returns:
        Configured KalmanInputPredictor.
    """
    return KalmanInputPredictor(
        history_size=10,
        prediction_horizon_ms=prediction_horizon_ms,
        process_noise=0.01,
        measurement_noise=1.0
    )
