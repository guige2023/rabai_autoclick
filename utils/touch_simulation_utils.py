"""Touch simulation utilities for mobile-style gestures.

This module provides utilities for simulating touch gestures on touch-enabled
devices or trackpad-based touch emulation, including tap, swipe, pinch,
and drag gestures for UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple, Callable
import time


class GestureType(Enum):
    """Type of touch gesture."""
    TAP = auto()
    LONG_PRESS = auto()
    SWIPE = auto()
    DRAG = auto()
    PINCH = auto()
    MULTI_TAP = auto()


@dataclass
class TouchPoint:
    """A single touch point."""
    x: int
    y: int
    pressure: float = 1.0
    timestamp: float = 0.0


@dataclass
class GestureConfig:
    """Configuration for gesture simulation."""
    duration_ms: float = 200.0
    steps: int = 20
    pressure: float = 1.0
    interpolation: str = "linear"


@dataclass
class GestureResult:
    """Result of gesture execution."""
    success: bool
    points_count: int
    duration_ms: float
    error_message: Optional[str] = None


def simulate_tap(
    x: int,
    y: int,
    on_move: Optional[Callable[[int, int], None]] = None,
    on_click: Optional[Callable[[int, int], None]] = None,
) -> GestureResult:
    """Simulate a tap gesture.
    
    Args:
        x: Tap X coordinate.
        y: Tap Y coordinate.
        on_move: Optional callback for move events.
        on_click: Optional callback for click/tap event.
    
    Returns:
        GestureResult with execution details.
    """
    start_time = time.time()
    
    try:
        if on_move:
            on_move(x, y)
        
        if on_click:
            on_click(x, y)
        
        duration = (time.time() - start_time) * 1000
        
        return GestureResult(
            success=True,
            points_count=1,
            duration_ms=duration,
        )
    except Exception as e:
        return GestureResult(
            success=False,
            points_count=0,
            duration_ms=0,
            error_message=str(e),
        )


def simulate_swipe(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    duration_ms: float = 300.0,
    on_move: Optional[Callable[[int, int], None]] = None,
    on_complete: Optional[Callable[[], None]] = None,
) -> GestureResult:
    """Simulate a swipe gesture.
    
    Args:
        start_x: Start X coordinate.
        start_y: Start Y coordinate.
        end_x: End X coordinate.
        end_y: End Y coordinate.
        duration_ms: Gesture duration in milliseconds.
        on_move: Callback for move events.
        on_complete: Callback when gesture completes.
    
    Returns:
        GestureResult with execution details.
    """
    start_time = time.time()
    steps = 20
    
    try:
        for i in range(steps + 1):
            t = i / steps
            x = int(start_x + (end_x - start_x) * t)
            y = int(start_y + (end_y - start_y) * t)
            
            if on_move:
                on_move(x, y)
            
            if i < steps:
                time.sleep(duration_ms / 1000.0 / steps)
        
        duration = (time.time() - start_time) * 1000
        
        if on_complete:
            on_complete()
        
        return GestureResult(
            success=True,
            points_count=steps + 1,
            duration_ms=duration,
        )
    except Exception as e:
        return GestureResult(
            success=False,
            points_count=0,
            duration_ms=0,
            error_message=str(e),
        )


def simulate_drag(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    hold_duration_ms: float = 500.0,
    move_duration_ms: float = 300.0,
    on_press: Optional[Callable[[int, int], None]] = None,
    on_move: Optional[Callable[[int, int], None]] = None,
    on_release: Optional[Callable[[int, int], None]] = None,
) -> GestureResult:
    """Simulate a drag gesture with press-hold-move-release.
    
    Args:
        start_x: Start X coordinate.
        start_y: Start Y coordinate.
        end_x: End X coordinate.
        end_y: End Y coordinate.
        hold_duration_ms: Hold duration at start.
        move_duration_ms: Movement duration.
        on_press: Callback for press event.
        on_move: Callback for move events.
        on_release: Callback for release event.
    
    Returns:
        GestureResult with execution details.
    """
    start_time = time.time()
    
    try:
        if on_press:
            on_press(start_x, start_y)
        
        time.sleep(hold_duration_ms / 1000.0)
        
        steps = 20
        for i in range(steps + 1):
            t = i / steps
            x = int(start_x + (end_x - start_x) * t)
            y = int(start_y + (end_y - start_y) * t)
            
            if on_move:
                on_move(x, y)
            
            if i < steps:
                time.sleep(move_duration_ms / 1000.0 / steps)
        
        if on_release:
            on_release(end_x, end_y)
        
        duration = (time.time() - start_time) * 1000
        
        return GestureResult(
            success=True,
            points_count=steps + 2,
            duration_ms=duration,
        )
    except Exception as e:
        return GestureResult(
            success=False,
            points_count=0,
            duration_ms=0,
            error_message=str(e),
        )


def simulate_pinch(
    center_x: int,
    center_y: int,
    scale: float,
    duration_ms: float = 500.0,
    on_update: Optional[Callable[[int, int, float], None]] = None,
) -> GestureResult:
    """Simulate a pinch gesture.
    
    Args:
        center_x: Center X coordinate.
        center_y: Center Y coordinate.
        scale: Scale factor (0.5 = zoom out, 2.0 = zoom in).
        duration_ms: Gesture duration.
        on_update: Callback with (x, y, current_scale).
    
    Returns:
        GestureResult with execution details.
    """
    start_time = time.time()
    steps = 20
    
    try:
        for i in range(steps + 1):
            t = i / steps
            current_scale = 1.0 + (scale - 1.0) * t
            
            if on_update:
                on_update(center_x, center_y, current_scale)
            
            if i < steps:
                time.sleep(duration_ms / 1000.0 / steps)
        
        duration = (time.time() - start_time) * 1000
        
        return GestureResult(
            success=True,
            points_count=steps + 1,
            duration_ms=duration,
        )
    except Exception as e:
        return GestureResult(
            success=False,
            points_count=0,
            duration_ms=0,
            error_message=str(e),
        )


def generate_swipe_path(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    steps: int = 20,
    curve_factor: float = 0.0,
) -> List[Tuple[int, int]]:
    """Generate a path for swipe with optional curve.
    
    Args:
        start_x: Start X coordinate.
        start_y: Start Y coordinate.
        end_x: End X coordinate.
        end_y: End Y coordinate.
        steps: Number of path points.
        curve_factor: Curve intensity (-1.0 to 1.0).
    
    Returns:
        List of (x, y) path coordinates.
    """
    path = []
    
    for i in range(steps + 1):
        t = i / steps
        
        x = int(start_x + (end_x - start_x) * t)
        y = int(start_y + (end_y - start_y) * t)
        
        if curve_factor != 0:
            curve_offset = int(curve_factor * (end_y - start_y) * t * (1 - t) * 4)
            x += curve_offset
        
        path.append((x, y))
    
    return path
