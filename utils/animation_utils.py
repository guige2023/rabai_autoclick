"""Animation utilities for UI automation sequences.

This module provides utilities for creating and executing animation
sequences, including timed transitions, staged actions, and easing
functions for smooth automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable, List
import time


class EasingFunction(Enum):
    """Easing function for animations."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    BOUNCE = auto()
    ELASTIC = auto()


@dataclass
class AnimationFrame:
    """A single frame in an animation."""
    x: int
    y: int
    timestamp: float
    alpha: float = 1.0


@dataclass
class AnimationSequence:
    """A sequence of animated steps."""
    name: str
    frames: List[AnimationFrame] = field(default_factory=list)
    duration_ms: float = 1000.0
    
    @property
    def total_frames(self) -> int:
        return len(self.frames)
    
    def add_frame(self, x: int, y: int, timestamp: float, alpha: float = 1.0):
        """Add a frame to the sequence."""
        self.frames.append(AnimationFrame(x, y, timestamp, alpha))


@dataclass
class AnimationConfig:
    """Configuration for animation execution."""
    easing: EasingFunction = EasingFunction.EASE_IN_OUT
    duration_ms: float = 500.0
    steps: int = 20
    on_update: Optional[Callable[[int, int, float], None]] = None
    on_complete: Optional[Callable[[], None]] = None


def ease_linear(t: float) -> float:
    """Linear easing (no easing)."""
    return t


def ease_in(t: float) -> float:
    """Ease in (slow start)."""
    return t * t


def ease_out(t: float) -> float:
    """Ease out (slow end)."""
    return t * (2 - t)


def ease_in_out(t: float) -> float:
    """Ease in-out (slow start and end)."""
    if t < 0.5:
        return 2 * t * t
    return -1 + (4 - 2 * t) * t


def ease_bounce(t: float) -> float:
    """Bounce easing."""
    if t < 0.5:
        return 2 * t * t
    t -= 1
    return 1 - 2 * t * t * (1 if t >= 0 else -1)


def ease_elastic(t: float) -> float:
    """Elastic easing."""
    if t == 0 or t == 1:
        return t
    return -2 ** (-10 * t) * ((-1 if (t * 10 - 1) < 0 else 1) * 1) + 1


def get_easing_function(easing: EasingFunction) -> Callable[[float], float]:
    """Get easing function by type."""
    functions = {
        EasingFunction.LINEAR: ease_linear,
        EasingFunction.EASE_IN: ease_in,
        EasingFunction.EASE_OUT: ease_out,
        EasingFunction.EASE_IN_OUT: ease_in_out,
        EasingFunction.BOUNCE: ease_bounce,
        EasingFunction.ELASTIC: ease_elastic,
    }
    return functions.get(easing, ease_linear)


def interpolate_position(
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    t: float,
) -> Tuple[int, int]:
    """Interpolate between two positions.
    
    Args:
        x1: Start X.
        y1: Start Y.
        x2: End X.
        y2: End Y.
        t: Progress (0.0 to 1.0).
    
    Returns:
        Tuple of interpolated (x, y).
    """
    x = int(x1 + (x2 - x1) * t)
    y = int(y1 + (y2 - y1) * t)
    return (x, y)


def create_animation_sequence(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    config: AnimationConfig,
) -> AnimationSequence:
    """Create an animation sequence between two positions.
    
    Args:
        start_x: Start X coordinate.
        start_y: Start Y coordinate.
        end_x: End X coordinate.
        end_y: End Y coordinate.
        config: Animation configuration.
    
    Returns:
        AnimationSequence with frames.
    """
    easing_func = get_easing_function(config.easing)
    
    sequence = AnimationSequence(
        name=f"move_{start_x}_{start_y}_to_{end_x}_{end_y}",
        duration_ms=config.duration_ms,
    )
    
    for i in range(config.steps):
        t = i / (config.steps - 1) if config.steps > 1 else 0
        eased_t = easing_func(t)
        
        x, y = interpolate_position(start_x, start_y, end_x, end_y, eased_t)
        
        timestamp = t * config.duration_ms / 1000.0
        
        sequence.add_frame(x, y, timestamp)
    
    return sequence


def execute_animation(
    sequence: AnimationSequence,
    on_frame: Callable[[int, int, float], None],
    on_complete: Optional[Callable[[], None]] = None,
) -> None:
    """Execute an animation sequence.
    
    Args:
        sequence: Animation sequence to execute.
        on_frame: Callback for each frame with (x, y, progress).
        on_complete: Optional callback when animation completes.
    """
    if not sequence.frames:
        if on_complete:
            on_complete()
        return
    
    start_time = time.time()
    
    for i, frame in enumerate(sequence.frames):
        elapsed = time.time() - start_time
        
        target_time = frame.timestamp
        
        if elapsed < target_time:
            time.sleep(target_time - elapsed)
        
        progress = (i + 1) / len(sequence.frames)
        
        on_frame(frame.x, frame.y, progress)
    
    if on_complete:
        on_complete()


def create_staged_action_sequence(
    actions: List[Tuple[int, int, int]],  # (x, y, delay_ms)
    config: Optional[AnimationConfig] = None,
) -> List[Tuple[int, int, float]]:
    """Create a sequence of staged actions with timing.
    
    Args:
        actions: List of (x, y, delay_ms) tuples.
        config: Optional animation config.
    
    Returns:
        List of (x, y, timestamp_seconds) tuples.
    """
    config = config or AnimationConfig()
    
    sequence = []
    current_time = 0.0
    
    for x, y, delay_ms in actions:
        sequence.append((x, y, current_time))
        current_time += delay_ms / 1000.0
    
    return sequence
