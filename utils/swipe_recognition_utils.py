"""
Swipe Recognition Utilities

Provides swipe gesture recognition, direction detection, and velocity
calculation for touch-based automation.
"""

from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import math


class SwipeDirection(Enum):
    """Cardinal and diagonal swipe directions."""
    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"
    UP_LEFT = "up_left"
    UP_RIGHT = "up_right"
    DOWN_LEFT = "down_left"
    DOWN_RIGHT = "down_right"
    UNKNOWN = "unknown"


@dataclass
class SwipeEvent:
    """Represents a recognized swipe gesture."""
    direction: SwipeDirection
    start_point: Tuple[float, float]
    end_point: Tuple[float, float]
    velocity: float
    duration: float
    distance: float
    confidence: float = 1.0
    
    @property
    def displacement(self) -> Tuple[float, float]:
        """Calculate displacement vector."""
        return (
            self.end_point[0] - self.start_point[0],
            self.end_point[1] - self.start_point[1]
        )
    
    @property
    def angle(self) -> float:
        """Calculate swipe angle in radians."""
        dx, dy = self.displacement
        return math.atan2(dy, dx)


@dataclass
class SwipeConfig:
    """Configuration for swipe recognition."""
    min_distance: float = 50.0
    max_duration: float = 2.0
    min_velocity: float = 100.0
    direction_threshold: float = 0.5
    velocity_weight: float = 0.3
    distance_weight: float = 0.4
    direction_weight: float = 0.3


class SwipeRecognizer:
    """
    Recognizes swipe gestures from a sequence of touch points.
    
    Analyzes touch point sequences to detect swipe direction, velocity,
    and other characteristics for gesture classification.
    
    Example:
        >>> recognizer = SwipeRecognizer(SwipeConfig(min_distance=80.0))
        >>> for point in touch_points:
        ...     recognizer.add_point(point)
        >>> swipe = recognizer.recognize()
        >>> if swipe:
        ...     print(f"Swipe {swipe.direction.value} at {swipe.velocity:.1f} px/s")
    """
    
    def __init__(self, config: Optional[SwipeConfig] = None) -> None:
        """
        Initialize swipe recognizer.
        
        Args:
            config: Recognition configuration parameters.
        """
        self._config = config or SwipeConfig()
        self._points: deque[Tuple[float, float, float]] = deque(maxlen=1000)
        self._start_time: Optional[float] = None
        self._is_tracking = False
    
    def add_point(
        self,
        point: Tuple[float, float],
        timestamp: Optional[float] = None
    ) -> None:
        """
        Add a touch point to the recognition buffer.
        
        Args:
            point: (x, y) coordinates of the touch point.
            timestamp: Optional timestamp in seconds.
        """
        if timestamp is None:
            import time
            timestamp = time.time()
        
        self._points.append((point[0], point[1], timestamp))
        
        if not self._is_tracking:
            self._start_time = timestamp
            self._is_tracking = True
    
    def recognize(self) -> Optional[SwipeEvent]:
        """
        Attempt to recognize a swipe gesture from accumulated points.
        
        Returns:
            SwipeEvent if a swipe is recognized, None otherwise.
        """
        if len(self._points) < 2:
            return None
        
        first = self._points[0]
        last = self._points[-1]
        
        start_point = (first[0], first[1])
        end_point = (last[0], last[1])
        
        dx = end_point[0] - start_point[0]
        dy = end_point[1] - start_point[1]
        distance = math.sqrt(dx * dx + dy * dy)
        duration = last[2] - first[2]
        
        if distance < self._config.min_distance:
            return None
        
        if duration > self._config.max_duration:
            return None
        
        velocity = distance / duration if duration > 0 else 0.0
        
        if velocity < self._config.min_velocity:
            return None
        
        direction = self._classify_direction(dx, dy, distance)
        
        confidence = self._calculate_confidence(
            distance, velocity, direction, duration
        )
        
        return SwipeEvent(
            direction=direction,
            start_point=start_point,
            end_point=end_point,
            velocity=velocity,
            duration=duration,
            distance=distance,
            confidence=confidence
        )
    
    def _classify_direction(
        self,
        dx: float,
        dy: float,
        distance: float
    ) -> SwipeDirection:
        """Classify swipe direction based on displacement."""
        if distance < 1e-6:
            return SwipeDirection.UNKNOWN
        
        dx_norm = dx / distance
        dy_norm = dy / distance
        threshold = self._config.direction_threshold
        
        if dx_norm > threshold and abs(dy_norm) < threshold:
            return SwipeDirection.RIGHT
        elif dx_norm < -threshold and abs(dy_norm) < threshold:
            return SwipeDirection.LEFT
        elif dy_norm > threshold and abs(dx_norm) < threshold:
            return SwipeDirection.DOWN
        elif dy_norm < -threshold and abs(dx_norm) < threshold:
            return SwipeDirection.UP
        elif dx_norm > threshold and dy_norm < -threshold:
            return SwipeDirection.UP_RIGHT
        elif dx_norm < -threshold and dy_norm < -threshold:
            return SwipeDirection.UP_LEFT
        elif dx_norm > threshold and dy_norm > threshold:
            return SwipeDirection.DOWN_RIGHT
        elif dx_norm < -threshold and dy_norm > threshold:
            return SwipeDirection.DOWN_LEFT
        
        return SwipeDirection.UNKNOWN
    
    def _calculate_confidence(
        self,
        distance: float,
        velocity: float,
        direction: SwipeDirection,
        duration: float
    ) -> float:
        """Calculate confidence score for the recognized swipe."""
        distance_score = min(distance / 300.0, 1.0)
        velocity_score = min(velocity / 1500.0, 1.0)
        duration_score = max(0.0, 1.0 - duration / 1.5)
        
        direction_score = 1.0 if direction != SwipeDirection.UNKNOWN else 0.0
        
        confidence = (
            distance_score * self._config.distance_weight +
            velocity_score * self._config.velocity_weight +
            direction_score * self._config.direction_weight
        )
        
        return min(confidence, 1.0)
    
    def reset(self) -> None:
        """Reset recognition state."""
        self._points.clear()
        self._start_time = None
        self._is_tracking = False
    
    @property
    def points(self) -> List[Tuple[float, float]]:
        """Get current list of tracked points."""
        return [(p[0], p[1]) for p in self._points]


class MultiSwipeDetector:
    """
    Detects multi-finger swipe gestures.
    
    Supports two-finger and three-finger swipe detection with
    independent direction tracking for each finger.
    """
    
    def __init__(self, config: Optional[SwipeConfig] = None) -> None:
        """
        Initialize multi-swipe detector.
        
        Args:
            config: Recognition configuration parameters.
        """
        self._config = config or SwipeConfig()
        self._finger_trackers: Dict[int, SwipeRecognizer] = {}
    
    def add_finger_point(
        self,
        finger_id: int,
        point: Tuple[float, float],
        timestamp: Optional[float] = None
    ) -> None:
        """
        Add a point for a specific finger.
        
        Args:
            finger_id: Identifier for the finger (0, 1, 2, etc.).
            point: (x, y) coordinates of the touch point.
            timestamp: Optional timestamp in seconds.
        """
        if finger_id not in self._finger_trackers:
            self._finger_trackers[finger_id] = SwipeRecognizer(self._config)
        self._finger_trackers[finger_id].add_point(point, timestamp)
    
    def recognize_all(self) -> Dict[int, Optional[SwipeEvent]]:
        """
        Attempt to recognize swipes for all tracked fingers.
        
        Returns:
            Dictionary mapping finger_id to recognized SwipeEvent or None.
        """
        return {
            fid: tracker.recognize()
            for fid, tracker in self._finger_trackers.items()
        }
    
    def get_parallel_swipes(self) -> List[SwipeEvent]:
        """
        Get swipes that occurred in parallel across fingers.
        
        Returns:
            List of SwipeEvents where swipes happened simultaneously.
        """
        all_swipes = self.recognize_all()
        parallel: List[SwipeEvent] = []
        
        for swipe in all_swipes.values():
            if swipe is not None and swipe.confidence > 0.7:
                parallel.append(swipe)
        
        return parallel
    
    def reset(self) -> None:
        """Reset all finger trackers."""
        for tracker in self._finger_trackers.values():
            tracker.reset()


def calculate_swipe_velocity(
    points: List[Tuple[float, float, float]]
) -> float:
    """
    Calculate average velocity for a swipe gesture.
    
    Args:
        points: List of (x, y, timestamp) tuples.
        
    Returns:
        Average velocity in pixels per second.
    """
    if len(points) < 2:
        return 0.0
    
    total_distance = 0.0
    total_time = points[-1][2] - points[0][2]
    
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        total_distance += math.sqrt(dx * dx + dy * dy)
    
    return total_distance / total_time if total_time > 0 else 0.0


def is_flick_gesture(
    points: List[Tuple[float, float, float]],
    min_velocity: float = 800.0,
    max_duration: float = 0.3
) -> bool:
    """
    Determine if a gesture is a quick flick.
    
    Args:
        points: List of (x, y, timestamp) tuples.
        min_velocity: Minimum velocity threshold in pixels per second.
        max_duration: Maximum duration for a flick in seconds.
        
    Returns:
        True if the gesture matches flick characteristics.
    """
    if len(points) < 2:
        return False
    
    duration = points[-1][2] - points[0][2]
    if duration > max_duration:
        return False
    
    velocity = calculate_swipe_velocity(points)
    return velocity >= min_velocity


def get_swipe_characteristics(
    start: Tuple[float, float],
    end: Tuple[float, float],
    duration: float
) -> Dict[str, float]:
    """
    Calculate characteristics of a swipe gesture.
    
    Args:
        start: Starting (x, y) coordinates.
        end: Ending (x, y) coordinates.
        duration: Duration of the swipe in seconds.
        
    Returns:
        Dictionary with swipe characteristics.
    """
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    distance = math.sqrt(dx * dx + dy * dy)
    velocity = distance / duration if duration > 0 else 0.0
    angle = math.degrees(math.atan2(dy, dx))
    
    return {
        "distance": distance,
        "velocity": velocity,
        "angle_degrees": angle,
        "horizontal_component": abs(dx),
        "vertical_component": abs(dy),
        "is_horizontal": abs(dx) > abs(dy),
        "is_vertical": abs(dy) > abs(dx),
        "is_diagonal": abs(dx) > 10 and abs(dy) > 10,
    }
