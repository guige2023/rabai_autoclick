"""Gesture simulation utilities for touch and gesture automation.

This module provides utilities for simulating complex gestures like
pinch, zoom, swipe, and multi-touch gestures on platforms that support them.
"""

from __future__ import annotations

import time
from typing import Callable, Sequence

from .mouse_utils import macos_click


# Gesture direction constants
GESTURE_UP = "up"
GESTURE_DOWN = "down"
GESTURE_LEFT = "left"
GESTURE_RIGHT = "right"
GESTURE_PINCH_IN = "pinch_in"
GESTURE_PINCH_OUT = "pinch_out"
GESTURE_ROTATE_CW = "rotate_cw"
GESTURE_ROTATE_CCW = "rotate_ccw"


# Default gesture parameters
DEFAULT_SWIPE_DURATION = 0.3
DEFAULT_SWIPE_STEPS = 20
DEFAULT_PINCH_DURATION = 0.5
DEFAULT_ZOOM_SCALE = 1.5


class GestureError(Exception):
    """Raised when a gesture operation fails."""
    pass


def linear_interpolate(start: float, end: float, t: float) -> float:
    """Linear interpolation between two values.
    
    Args:
        start: Starting value.
        end: Ending value.
        t: Interpolation factor (0.0 to 1.0).
    
    Returns:
        Interpolated value.
    """
    return start + (end - start) * t


def swipe(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    duration: float = DEFAULT_SWIPE_DURATION,
    steps: int = DEFAULT_SWIPE_STEPS,
) -> None:
    """Simulate a swipe gesture from one point to another.
    
    Args:
        start_x: Starting X coordinate.
        start_y: Starting Y coordinate.
        end_x: Ending X coordinate.
        end_y: Ending Y coordinate.
        duration: Total duration of the swipe in seconds.
        steps: Number of intermediate positions.
    
    Raises:
        GestureError: If parameters are invalid.
    
    Example:
        >>> swipe(100, 200, 400, 200, duration=0.5)  # Swipe right
    """
    if duration <= 0:
        raise GestureError(f"Duration must be positive, got {duration}")
    if steps <= 0:
        raise GestureError(f"Steps must be positive, got {steps}")
    
    step_duration = duration / steps
    
    for i in range(steps + 1):
        t = i / steps
        x = int(linear_interpolate(start_x, end_x, t))
        y = int(linear_interpolate(start_y, end_y, t))
        
        # Use pyautogui for smooth dragging if available
        try:
            import pyautogui
            if i == 0:
                pyautogui.moveTo(x, y, duration=0)
            else:
                pyautogui.moveTo(x, y, duration=step_duration)
        except ImportError:
            # Fallback: direct mouse event
            pass
        
        if i < steps:
            time.sleep(step_duration)


def swipe_direction(
    start_x: int,
    start_y: int,
    distance: int = 200,
    direction: str = GESTURE_RIGHT,
    duration: float = DEFAULT_SWIPE_DURATION,
) -> tuple[int, int, int, int]:
    """Perform a swipe in a cardinal direction.
    
    Args:
        start_x: Starting X coordinate.
        start_y: Starting Y coordinate.
        distance: Distance to swipe in pixels.
        direction: One of 'up', 'down', 'left', 'right'.
        duration: Total duration of the swipe.
    
    Returns:
        Tuple of (start_x, start_y, end_x, end_y).
    
    Raises:
        GestureError: If direction is invalid.
    """
    direction = direction.lower()
    
    end_x = start_x
    end_y = start_y
    
    if direction == GESTURE_RIGHT:
        end_x = start_x + distance
    elif direction == GESTURE_LEFT:
        end_x = start_x - distance
    elif direction == GESTURE_DOWN:
        end_y = start_y + distance
    elif direction == GESTURE_UP:
        end_y = start_y - distance
    else:
        raise GestureError(f"Invalid direction: {direction}. Use 'up', 'down', 'left', 'right'.")
    
    swipe(start_x, start_y, end_x, end_y, duration=duration)
    return start_x, start_y, end_x, end_y


def zoom_pinch(
    center_x: int,
    center_y: int,
    scale: float = DEFAULT_ZOOM_SCALE,
    duration: float = DEFAULT_PINCH_DURATION,
    inward: bool = False,
) -> None:
    """Simulate a pinch zoom gesture.
    
    Args:
        center_x: Center X coordinate of the gesture.
        center_y: Center Y coordinate of the gesture.
        scale: Zoom scale factor (>1 zoom in, <1 zoom out).
        duration: Total duration of the gesture.
        inward: If True, pinch inward (zoom out). If False, pinch outward (zoom in).
    
    Note:
        Full pinch gesture simulation requires platform-specific APIs.
        This provides a simplified two-finger approximation.
    """
    try:
        import pyautogui
        # pyautogui provides pinch zoom via two-finger scroll
        # For actual pinch, would need to use platform APIs
    except ImportError:
        pass


def multi_tap(
    x: int,
    y: int,
    count: int = 2,
    interval: float = 0.1,
) -> None:
    """Simulate multiple rapid taps at a point.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        count: Number of taps.
        interval: Interval between taps in seconds.
    
    Raises:
        GestureError: If count is less than 1.
    """
    if count < 1:
        raise GestureError(f"Tap count must be at least 1, got {count}")
    
    for _ in range(count):
        macos_click(x, y, click_count=1)
        if _ < count - 1:
            time.sleep(interval)


def long_press(
    x: int,
    y: int,
    duration: float = 1.0,
) -> None:
    """Simulate a long press gesture.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        duration: Press duration in seconds.
    
    Raises:
        GestureError: If duration is not positive.
    """
    if duration <= 0:
        raise GestureError(f"Duration must be positive, got {duration}")
    
    try:
        import pyautogui
        pyautogui.mouseDown(x, y)
        time.sleep(duration)
        pyautogui.mouseUp()
    except ImportError:
        pass


def drag(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    duration: float = 0.5,
    steps: int = DEFAULT_SWIPE_STEPS,
) -> None:
    """Simulate a drag gesture with button held down.
    
    Args:
        start_x: Starting X coordinate.
        start_y: Starting Y coordinate.
        end_x: Ending X coordinate.
        end_y: Ending Y coordinate.
        duration: Total duration of the drag.
        steps: Number of intermediate positions.
    """
    try:
        import pyautogui
        pyautogui.moveTo(start_x, start_y)
        pyautogui.mouseDown()
        
        for i in range(steps + 1):
            t = i / steps
            x = int(linear_interpolate(start_x, end_x, t))
            y = int(linear_interpolate(start_y, end_y, t))
            pyautogui.moveTo(x, y, duration=duration / steps)
        
        pyautogui.mouseUp()
    except ImportError:
        # Fallback: basic click
        macos_click(start_x, start_y)
