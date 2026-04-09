"""Animation frame timing utilities for UI automation.

Provides utilities for calculating animation frame timing,
interpolation, and synchronization for smooth UI animations.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class FrameTiming:
    """Stores timing information for an animation frame."""
    frame_number: int
    timestamp: float
    delta_time: float
    progress: float


@dataclass
class AnimationConfig:
    """Configuration for an animation sequence."""
    duration_ms: float
    fps: int = 60
    easing: str = "linear"
    loop: bool = False
    reverse: bool = False


class FrameRateController:
    """Controls frame rate for animations.
    
    Ensures consistent frame timing by tracking actual frame times
    and adjusting sleep duration to maintain target FPS.
    """
    
    def __init__(self, target_fps: int = 60) -> None:
        """Initialize the frame rate controller.
        
        Args:
            target_fps: Target frames per second.
        """
        self.target_fps = target_fps
        self.target_frame_time = 1.0 / target_fps
        self._last_frame_time = time.perf_counter()
        self._frame_count = 0
        self._start_time = self._last_frame_time
        self._drift_ms = 0.0
    
    def wait_for_next_frame(self) -> float:
        """Wait until the next frame should start.
        
        Returns:
            Actual time slept in seconds.
        """
        current_time = time.perf_counter()
        elapsed = current_time - self._last_frame_time
        sleep_time = max(0, self.target_frame_time - elapsed)
        
        if sleep_time > 0:
            time.sleep(sleep_time)
        
        self._last_frame_time = time.perf_counter()
        self._frame_count += 1
        
        return sleep_time
    
    def get_current_fps(self) -> float:
        """Get the current estimated FPS.
        
        Returns:
            Estimated frames per second.
        """
        if self._frame_count == 0:
            return 0.0
        total_time = time.perf_counter() - self._start_time
        return self._frame_count / total_time if total_time > 0 else 0.0
    
    def reset(self) -> None:
        """Reset the frame rate controller."""
        self._last_frame_time = time.perf_counter()
        self._frame_count = 0
        self._start_time = self._last_frame_time


class EasingFunctions:
    """Collection of easing functions for animations.
    
    Each function takes a progress value (0.0 to 1.0) and returns
    a transformed progress value.
    """
    
    @staticmethod
    def linear(t: float) -> float:
        """Linear easing (no transformation)."""
        return t
    
    @staticmethod
    def ease_in_quad(t: float) -> float:
        """Quadratic ease-in."""
        return t * t
    
    @staticmethod
    def ease_out_quad(t: float) -> float:
        """Quadratic ease-out."""
        return t * (2 - t)
    
    @staticmethod
    def ease_in_out_quad(t: float) -> float:
        """Quadratic ease-in-out."""
        if t < 0.5:
            return 2 * t * t
        return -1 + (4 - 2 * t) * t
    
    @staticmethod
    def ease_in_cubic(t: float) -> float:
        """Cubic ease-in."""
        return t * t * t
    
    @staticmethod
    def ease_out_cubic(t: float) -> float:
        """Cubic ease-out."""
        t -= 1
        return t * t * t + 1
    
    @staticmethod
    def ease_in_out_cubic(t: float) -> float:
        """Cubic ease-in-out."""
        if t < 0.5:
            return 4 * t * t * t
        t = (2 * t) - 2
        return (t * t * t + 2) / 2
    
    @staticmethod
    def ease_in_sine(t: float) -> float:
        """Sine ease-in."""
        return 1 - math.cos((t * math.pi) / 2)
    
    @staticmethod
    def ease_out_sine(t: float) -> float:
        """Sine ease-out."""
        return math.sin((t * math.pi) / 2)
    
    @staticmethod
    def ease_in_out_sine(t: float) -> float:
        """Sine ease-in-out."""
        return -(math.cos(math.pi * t) - 1) / 2
    
    @staticmethod
    def ease_in_elastic(t: float) -> float:
        """Elastic ease-in (approximation)."""
        if t == 0 or t == 1:
            return t
        return -math.pow(2, 10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)
    
    @staticmethod
    def ease_out_elastic(t: float) -> float:
        """Elastic ease-out (approximation)."""
        if t == 0 or t == 1:
            return t
        return math.pow(2, -10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1
    
    @staticmethod
    def ease_in_bounce(t: float) -> float:
        """Bounce ease-in."""
        return 1 - EasingFunctions.ease_out_bounce(1 - t)
    
    @staticmethod
    def ease_out_bounce(t: float) -> float:
        """Bounce ease-out (approximation)."""
        n1, d1 = 7.5625, 2.75
        if t < 1 / d1:
            return n1 * t * t
        elif t < 2 / d1:
            t -= 1.5 / d1
            return n1 * t * t + 0.75
        elif t < 2.5 / d1:
            t -= 2.25 / d1
            return n1 * t * t + 0.9375
        else:
            t -= 2.625 / d1
            return n1 * t * t + 0.984375
    
    @staticmethod
    def ease_in_out_bounce(t: float) -> float:
        """Bounce ease-in-out."""
        if t < 0.5:
            return (1 - EasingFunctions.ease_out_bounce(1 - 2 * t)) / 2
        return (1 + EasingFunctions.ease_out_bounce(2 * t - 1)) / 2
    
    @staticmethod
    def get_easing(name: str) -> Callable[[float], float]:
        """Get an easing function by name.
        
        Args:
            name: Name of the easing function.
            
        Returns:
            The easing function.
        """
        easing_map: Dict[str, Callable[[float], float]] = {
            "linear": EasingFunctions.linear,
            "ease_in_quad": EasingFunctions.ease_in_quad,
            "ease_out_quad": EasingFunctions.ease_out_quad,
            "ease_in_out_quad": EasingFunctions.ease_in_out_quad,
            "ease_in_cubic": EasingFunctions.ease_in_cubic,
            "ease_out_cubic": EasingFunctions.ease_out_cubic,
            "ease_in_out_cubic": EasingFunctions.ease_in_out_cubic,
            "ease_in_sine": EasingFunctions.ease_in_sine,
            "ease_out_sine": EasingFunctions.ease_out_sine,
            "ease_in_out_sine": EasingFunctions.ease_in_out_sine,
            "ease_in_elastic": EasingFunctions.ease_in_elastic,
            "ease_out_elastic": EasingFunctions.ease_out_elastic,
            "ease_in_bounce": EasingFunctions.ease_in_bounce,
            "ease_out_bounce": EasingFunctions.ease_out_bounce,
            "ease_in_out_bounce": EasingFunctions.ease_in_out_bounce,
        }
        return easing_map.get(name, EasingFunctions.linear)


class AnimationTimeline:
    """Manages an animation timeline with keyframes.
    
    Supports linear interpolation between keyframes and
    can invoke callbacks at specific times.
    """
    
    def __init__(self, config: Optional[AnimationConfig] = None) -> None:
        """Initialize the animation timeline.
        
        Args:
            config: Animation configuration.
        """
        self.config = config or AnimationConfig(duration_ms=1000)
        self._keyframes: Dict[float, Dict[str, float]] = {}
        self._callbacks: List[Tuple[float, Callable[[float], None]]] = []
        self._start_time: Optional[float] = None
        self._is_running = False
    
    def add_keyframe(self, time_ms: float, values: Dict[str, float]) -> None:
        """Add a keyframe to the timeline.
        
        Args:
            time_ms: Time in milliseconds.
            values: Dictionary of property values at this keyframe.
        """
        self._keyframes[time_ms] = values
    
    def add_callback(self, time_ms: float, callback: Callable[[float], None]) -> None:
        """Add a callback to be invoked at a specific time.
        
        Args:
            time_ms: Time in milliseconds.
            callback: Function to call when time is reached.
        """
        self._callbacks.append((time_ms, callback))
    
    def get_value_at(self, time_ms: float, property_name: str) -> Optional[float]:
        """Get the interpolated value of a property at a given time.
        
        Args:
            time_ms: Time in milliseconds.
            property_name: Name of the property.
            
        Returns:
            Interpolated value or None if property not found.
        """
        if not self._keyframes:
            return None
        
        sorted_times = sorted(self._keyframes.keys())
        before_time = None
        after_time = None
        
        for t in sorted_times:
            if t <= time_ms:
                before_time = t
            if t >= time_ms and after_time is None:
                after_time = t
                break
        
        if before_time is None:
            before_time = after_time
        if after_time is None:
            after_time = before_time
        
        if before_time == after_time:
            frame = self._keyframes[before_time]
            return frame.get(property_name)
        
        progress = (time_ms - before_time) / (after_time - before_time)
        easing = EasingFunctions.get_easing(self.config.easing)
        progress = easing(progress)
        
        before_values = self._keyframes[before_time]
        after_values = self._keyframes[after_time]
        
        before_val = before_values.get(property_name, 0.0)
        after_val = after_values.get(property_name, 0.0)
        
        return before_val + (after_val - before_val) * progress
    
    def start(self) -> None:
        """Start the timeline."""
        self._start_time = time.perf_counter()
        self._is_running = True
    
    def stop(self) -> None:
        """Stop the timeline."""
        self._is_running = False
    
    def get_current_time_ms(self) -> float:
        """Get the current time in milliseconds.
        
        Returns:
            Time in milliseconds since start.
        """
        if self._start_time is None:
            return 0.0
        return (time.perf_counter() - self._start_time) * 1000
    
    def is_complete(self) -> bool:
        """Check if the animation is complete.
        
        Returns:
            True if complete, False otherwise.
        """
        return self.get_current_time_ms() >= self.config.duration_ms


class FrameInterpolator:
    """Interpolates values between frames for smooth animations."""
    
    def __init__(self) -> None:
        """Initialize the interpolator."""
        self._previous_values: Dict[str, float] = {}
        self._current_values: Dict[str, float] = {}
        self._target_values: Dict[str, float] = {}
    
    def set_target(self, values: Dict[str, float]) -> None:
        """Set target values to interpolate towards.
        
        Args:
            values: Target property values.
        """
        self._previous_values = dict(self._current_values)
        self._target_values = values
    
    def update_current(self, values: Dict[str, float]) -> None:
        """Update current values directly.
        
        Args:
            values: Current property values.
        """
        self._current_values = values
    
    def interpolate(self, progress: float, property_name: str) -> Optional[float]:
        """Interpolate a property value.
        
        Args:
            progress: Interpolation progress (0.0 to 1.0).
            property_name: Name of the property.
            
        Returns:
            Interpolated value or None.
        """
        prev = self._previous_values.get(property_name, 0.0)
        target = self._target_values.get(property_name, prev)
        return prev + (target - prev) * progress
    
    def interpolate_all(self, progress: float) -> Dict[str, float]:
        """Interpolate all property values.
        
        Args:
            progress: Interpolation progress (0.0 to 1.0).
            
        Returns:
            Dictionary of interpolated values.
        """
        result = {}
        all_keys = set(self._previous_values.keys()) | set(self._target_values.keys())
        for key in all_keys:
            result[key] = self.interpolate(progress, key) or 0.0
        return result


def calculate_frame_timing(
    start_time: float,
    frame_number: int,
    duration_ms: float,
    total_frames: int
) -> FrameTiming:
    """Calculate timing information for a frame.
    
    Args:
        start_time: Start time (from time.perf_counter).
        frame_number: Current frame number.
        duration_ms: Total animation duration in milliseconds.
        total_frames: Total number of frames.
        
    Returns:
        FrameTiming object with timing information.
    """
    current_time = time.perf_counter()
    timestamp = (current_time - start_time) * 1000
    progress = min(1.0, timestamp / duration_ms) if duration_ms > 0 else 1.0
    
    delta_time = 0.0
    if frame_number > 0:
        delta_time = timestamp / frame_number
    
    return FrameTiming(
        frame_number=frame_number,
        timestamp=timestamp,
        delta_time=delta_time,
        progress=progress
    )
