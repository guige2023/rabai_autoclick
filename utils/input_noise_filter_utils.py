"""Input noise filtering utilities for UI automation.

Provides utilities for filtering out noisy or spurious input events,
smoothing input streams, and detecting meaningful gestures.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class InputSample:
    """A single input sample with coordinates and timestamp."""
    x: float
    y: float
    timestamp_ms: float
    pressure: float = 1.0
    source: str = "unknown"


@dataclass
class NoiseFilterConfig:
    """Configuration for noise filtering."""
    min_distance_threshold: float = 2.0
    min_time_threshold_ms: float = 5.0
    velocity_threshold: float = 0.0
    acceleration_threshold: float = 0.0
    outlier_threshold_std: float = 2.5
    smoothing_window_size: int = 3


class InputNoiseFilter:
    """Filters noisy input events from input streams.
    
    Uses multiple filtering strategies including velocity-based
    filtering, distance thresholds, and outlier detection.
    """
    
    def __init__(self, config: Optional[NoiseFilterConfig] = None) -> None:
        """Initialize the noise filter.
        
        Args:
            config: Filter configuration.
        """
        self.config = config or NoiseFilterConfig()
        self._previous_sample: Optional[InputSample] = None
        self._sample_buffer: Deque[InputSample] = deque(maxlen=self.config.smoothing_window_size)
    
    def is_valid(self, sample: InputSample) -> bool:
        """Check if a sample is valid (not noise).
        
        Args:
            sample: Input sample to check.
            
        Returns:
            True if valid, False if noise.
        """
        if self._previous_sample is None:
            self._previous_sample = sample
            return True
        
        dx = sample.x - self._previous_sample.x
        dy = sample.y - self._previous_sample.y
        distance = math.sqrt(dx * dx + dy * dy)
        
        dt = sample.timestamp_ms - self._previous_sample.timestamp_ms
        dt = max(dt, 0.001)
        
        velocity = distance / dt
        
        if distance < self.config.min_distance_threshold:
            if dt < self.config.min_time_threshold_ms:
                return False
        
        if (self.config.velocity_threshold > 0 and 
                velocity > self.config.velocity_threshold):
            return False
        
        self._previous_sample = sample
        return True
    
    def filter_sample(self, sample: InputSample) -> Optional[InputSample]:
        """Filter a single sample.
        
        Args:
            sample: Sample to filter.
            
        Returns:
            Filtered sample or None if filtered out.
        """
        if self.is_valid(sample):
            self._sample_buffer.append(sample)
            return sample
        return None
    
    def filter_samples(
        self,
        samples: List[InputSample]
    ) -> List[InputSample]:
        """Filter a list of samples.
        
        Args:
            samples: List of samples to filter.
            
        Returns:
            List of valid samples.
        """
        self.reset()
        return [s for s in samples if self.filter_sample(s) is not None]
    
    def reset(self) -> None:
        """Reset the filter state."""
        self._previous_sample = None
        self._sample_buffer.clear()


class MovingAverageFilter:
    """Moving average filter for smoothing input coordinates.
    
    Uses a sliding window to compute average coordinates,
    reducing jitter in input streams.
    """
    
    def __init__(self, window_size: int = 3) -> None:
        """Initialize the moving average filter.
        
        Args:
            window_size: Size of the smoothing window.
        """
        self.window_size = window_size
        self._buffer: Deque[InputSample] = deque(maxlen=window_size)
    
    def add_sample(self, sample: InputSample) -> InputSample:
        """Add a sample and get smoothed result.
        
        Args:
            sample: Input sample.
            
        Returns:
            Smoothed sample.
        """
        self._buffer.append(sample)
        return self.get_smoothed()
    
    def get_smoothed(self) -> Optional[InputSample]:
        """Get the smoothed sample.
        
        Returns:
            Smoothed sample or None if buffer empty.
        """
        if not self._buffer:
            return None
        
        if len(self._buffer) == 1:
            return self._buffer[0]
        
        sum_x = sum(s.x for s in self._buffer)
        sum_y = sum(s.y for s in self._buffer)
        sum_pressure = sum(s.pressure for s in self._buffer)
        count = len(self._buffer)
        
        last = self._buffer[-1]
        
        return InputSample(
            x=sum_x / count,
            y=sum_y / count,
            timestamp_ms=last.timestamp_ms,
            pressure=sum_pressure / count,
            source=last.source
        )
    
    def reset(self) -> None:
        """Reset the filter."""
        self._buffer.clear()


class KalmanFilter:
    """1D Kalman filter for smoothing input values.
    
    Provides optimal noise reduction for linear Gaussian systems.
    """
    
    def __init__(
        self,
        process_variance: float = 0.1,
        measurement_variance: float = 1.0,
        initial_estimate: float = 0.0
    ) -> None:
        """Initialize the Kalman filter.
        
        Args:
            process_variance: Expected process noise variance.
            measurement_variance: Expected measurement noise variance.
            initial_estimate: Initial state estimate.
        """
        self.process_variance = process_variance
        self.measurement_variance = measurement_variance
        self.estimate = initial_estimate
        self.estimate_error = 1.0
    
    def update(self, measurement: float) -> float:
        """Update the filter with a new measurement.
        
        Args:
            measurement: New measurement value.
            
        Returns:
            Updated estimate.
        """
        prediction_error = self.estimate_error + self.process_variance
        
        kalman_gain = prediction_error / (prediction_error + self.measurement_variance)
        
        self.estimate = self.estimate + kalman_gain * (measurement - self.estimate)
        self.estimate_error = (1 - kalman_gain) * prediction_error
        
        return self.estimate
    
    def reset(self, initial_estimate: float = 0.0) -> None:
        """Reset the filter.
        
        Args:
            initial_estimate: Initial state estimate.
        """
        self.estimate = initial_estimate
        self.estimate_error = 1.0


class DualKalmanFilter:
    """Dual Kalman filter for 2D coordinates (x, y).
    
    Applies independent Kalman filters to x and y coordinates.
    """
    
    def __init__(
        self,
        process_variance: float = 0.1,
        measurement_variance: float = 1.0
    ) -> None:
        """Initialize the dual Kalman filter.
        
        Args:
            process_variance: Expected process noise variance.
            measurement_variance: Expected measurement noise variance.
        """
        self.x_filter = KalmanFilter(process_variance, measurement_variance)
        self.y_filter = KalmanFilter(process_variance, measurement_variance)
    
    def update(self, x: float, y: float) -> Tuple[float, float]:
        """Update filters with new coordinates.
        
        Args:
            x: X coordinate measurement.
            y: Y coordinate measurement.
            
        Returns:
            Tuple of (filtered_x, filtered_y).
        """
        return self.x_filter.update(x), self.y_filter.update(y)
    
    def reset(self) -> None:
        """Reset both filters."""
        self.x_filter.reset()
        self.y_filter.reset()


class OutlierDetector:
    """Detects and removes outliers from input streams.
    
    Uses statistical methods to identify samples that deviate
    significantly from the expected pattern.
    """
    
    def __init__(
        self,
        window_size: int = 20,
        threshold_std: float = 2.5
    ) -> None:
        """Initialize the outlier detector.
        
        Args:
            window_size: Size of the sliding window.
            threshold_std: Number of standard deviations for outlier threshold.
        """
        self.window_size = window_size
        self.threshold_std = threshold_std
        self._buffer: Deque[InputSample] = deque(maxlen=window_size)
    
    def is_outlier(self, sample: InputSample) -> bool:
        """Check if a sample is an outlier.
        
        Args:
            sample: Sample to check.
            
        Returns:
            True if outlier, False otherwise.
        """
        self._buffer.append(sample)
        
        if len(self._buffer) < 3:
            return False
        
        xs = [s.x for s in self._buffer]
        ys = [s.y for s in self._buffer]
        
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        
        variance_x = sum((x - mean_x) ** 2 for x in xs) / len(xs)
        variance_y = sum((y - mean_y) ** 2 for y in ys) / len(ys)
        
        std_x = math.sqrt(variance_x)
        std_y = math.sqrt(variance_y)
        
        z_x = abs(sample.x - mean_x) / std_x if std_x > 0 else 0
        z_y = abs(sample.y - mean_y) / std_y if std_y > 0 else 0
        
        return z_x > self.threshold_std or z_y > self.threshold_std
    
    def reset(self) -> None:
        """Reset the detector."""
        self._buffer.clear()


class GestureDetector:
    """Detects meaningful gestures from filtered input streams.
    
    Analyzes input patterns to identify taps, swipes, and other
    gesture types while filtering out noise.
    """
    
    def __init__(
        self,
        tap_max_distance: float = 20.0,
        tap_max_duration_ms: float = 300.0,
        swipe_min_distance: float = 50.0
    ) -> None:
        """Initialize the gesture detector.
        
        Args:
            tap_max_distance: Max distance for a tap gesture.
            tap_max_duration_ms: Max duration for a tap gesture.
            swipe_min_distance: Min distance for a swipe gesture.
        """
        self.tap_max_distance = tap_max_distance
        self.tap_max_duration_ms = tap_max_duration_ms
        self.swipe_min_distance = swipe_min_distance
        self._samples: Deque[InputSample] = deque(maxlen=1000)
        self._is_tracking = False
    
    def begin_tracking(self) -> None:
        """Begin tracking input samples."""
        self._is_tracking = True
        self._samples.clear()
    
    def add_sample(self, sample: InputSample) -> None:
        """Add an input sample to the gesture stream.
        
        Args:
            sample: Input sample to add.
        """
        if self._is_tracking:
            self._samples.append(sample)
    
    def end_tracking(self) -> Optional[str]:
        """End tracking and return detected gesture.
        
        Returns:
            Gesture type string or None.
        """
        self._is_tracking = False
        
        if len(self._samples) < 2:
            return None
        
        first = self._samples[0]
        last = self._samples[-1]
        
        dx = last.x - first.x
        dy = last.y - first.y
        distance = math.sqrt(dx * dx + dy * dy)
        duration_ms = last.timestamp_ms - first.timestamp_ms
        
        if distance < self.tap_max_distance and duration_ms < self.tap_max_duration_ms:
            return "tap"
        
        if distance >= self.swipe_min_distance:
            return "swipe"
        
        return None
    
    def get_samples(self) -> List[InputSample]:
        """Get all tracked samples.
        
        Returns:
            List of tracked samples.
        """
        return list(self._samples)
    
    def reset(self) -> None:
        """Reset the detector."""
        self._samples.clear()
        self._is_tracking = False


def create_noise_filter_chain(
    config: Optional[NoiseFilterConfig] = None
) -> List[Callable[[InputSample], Optional[InputSample]]]:
    """Create a chain of noise filters.
    
    Args:
        config: Filter configuration.
        
    Returns:
        List of filter functions.
    """
    noise_filter = InputNoiseFilter(config)
    avg_filter = MovingAverageFilter(window_size=3)
    kalman_filter = DualKalmanFilter()
    
    def chain_filter(sample: InputSample) -> Optional[InputSample]:
        filtered = noise_filter.filter_sample(sample)
        if filtered is None:
            return None
        avg_result = avg_filter.add_sample(filtered)
        if avg_result is None:
            return filtered
        fx, fy = kalman_filter.update(avg_result.x, avg_result.y)
        return InputSample(
            x=fx,
            y=fy,
            timestamp_ms=avg_result.timestamp_ms,
            pressure=avg_result.pressure,
            source=avg_result.source
        )
    
    return [chain_filter]
