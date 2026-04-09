"""Input noise filter for filtering out spurious input events."""
from typing import List, Tuple, Optional, Callable
from dataclasses import dataclass
import math


@dataclass
class FilterConfig:
    """Configuration for noise filtering."""
    distance_threshold: float = 5.0
    time_threshold_ms: float = 50.0
    velocity_threshold: float = 10.0


class InputNoiseFilter:
    """Filters out noise and spurious input events.
    
    Removes input events that appear to be noise based on
    distance, timing, and velocity thresholds.
    
    Example:
        filter = InputNoiseFilter()
        clean_points = filter.filter_raw_input(
            points=[(0,0), (1,1), (100,100)],
            timestamps=[0.0, 0.01, 0.1]
        )
    """

    def __init__(self, config: Optional[FilterConfig] = None) -> None:
        self._config = config or FilterConfig()
        self._last_valid_point: Optional[Tuple[float, float]] = None
        self._last_valid_time: float = 0

    def filter_raw_input(
        self,
        points: List[Tuple[float, float]],
        timestamps: List[float],
    ) -> List[Tuple[Tuple[float, float], float]]:
        """Filter raw input and return only valid points with timestamps."""
        if not points or not timestamps:
            return []
        
        result = []
        
        for i, (point, ts) in enumerate(zip(points, timestamps)):
            if self._is_valid_point(point, ts):
                result.append((point, ts))
                self._last_valid_point = point
                self._last_valid_time = ts
        
        return result

    def _is_valid_point(self, point: Tuple[float, float], timestamp: float) -> bool:
        """Check if a point is valid (not noise)."""
        if self._last_valid_point is None:
            return True
        
        dx = point[0] - self._last_valid_point[0]
        dy = point[1] - self._last_valid_point[1]
        distance = math.sqrt(dx * dx + dy * dy)
        
        dt = timestamp - self._last_valid_time
        dt_ms = dt * 1000
        
        if distance < self._config.distance_threshold and dt_ms < self._config.time_threshold_ms:
            return False
        
        if dt > 0:
            velocity = distance / dt
            if velocity < self._config.velocity_threshold and distance < self._config.distance_threshold:
                return False
        
        return True

    def reset(self) -> None:
        """Reset filter state."""
        self._last_valid_point = None
        self._last_valid_time = 0

    def get_config(self) -> FilterConfig:
        """Get current filter configuration."""
        return self._config

    def set_distance_threshold(self, threshold: float) -> None:
        """Set minimum distance threshold."""
        self._config.distance_threshold = threshold

    def set_time_threshold_ms(self, threshold_ms: float) -> None:
        """Set minimum time threshold in milliseconds."""
        self._config.time_threshold_ms = threshold_ms
