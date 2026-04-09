"""Animation sequence utilities for creating multi-step animations.

This module provides utilities for creating and managing animation sequences
with multiple steps, transitions, and timing, useful for creating smooth
UI animations and transitions in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, List, Callable
import time


class TransitionType(Enum):
    """Type of transition between animation steps."""
    INSTANT = auto()
    FADE = auto()
    SLIDE = auto()
    ZOOM = auto()
    ROTATE = auto()


@dataclass
class AnimationStep:
    """A single step in an animation sequence."""
    step_id: str
    start_state: dict
    end_state: dict
    duration_ms: float
    transition: TransitionType = TransitionType.INSTANT
    delay_ms: float = 0.0
    easing: str = "ease_in_out"
    
    @property
    def total_duration_ms(self) -> float:
        return self.delay_ms + self.duration_ms


@dataclass
class AnimationSequence:
    """A complete animation sequence."""
    name: str
    steps: List[AnimationStep] = field(default_factory=list)
    loop: bool = False
    on_step_complete: Optional[Callable[[str], None]] = None
    on_sequence_complete: Optional[Callable[[], None]] = None
    
    @property
    def total_duration_ms(self) -> float:
        return sum(step.total_duration_ms for step in self.steps)


@dataclass
class AnimationState:
    """Current state of animation playback."""
    current_step_index: int
    step_progress: float  # 0.0 to 1.0
    sequence_progress: float  # 0.0 to 1.0
    is_playing: bool
    is_paused: bool
    elapsed_ms: float


def create_animation_sequence(
    name: str,
    keyframes: List[Tuple[dict, float]],  # (state_dict, duration_ms)
    transitions: Optional[List[TransitionType]] = None,
) -> AnimationSequence:
    """Create an animation sequence from keyframes.
    
    Args:
        name: Sequence name.
        keyframes: List of (state, duration_ms) tuples.
        transitions: Optional list of transition types.
    
    Returns:
        AnimationSequence object.
    """
    if len(keyframes) < 2:
        raise ValueError("At least 2 keyframes are required")
    
    steps = []
    transitions = transitions or [TransitionType.INSTANT] * (len(keyframes) - 1)
    
    for i in range(len(keyframes) - 1):
        start_state, duration = keyframes[i]
        end_state, _ = keyframes[i + 1]
        transition = transitions[i] if i < len(transitions) else TransitionType.INSTANT
        
        steps.append(AnimationStep(
            step_id=f"{name}_step_{i}",
            start_state=start_state,
            end_state=end_state,
            duration_ms=duration,
            transition=transition,
        ))
    
    return AnimationSequence(name=name, steps=steps)


def interpolate_state(
    start_state: dict,
    end_state: dict,
    progress: float,
) -> dict:
    """Interpolate between two states.
    
    Args:
        start_state: Starting state dictionary.
        end_state: Ending state dictionary.
        progress: Interpolation progress (0.0 to 1.0).
    
    Returns:
        Interpolated state dictionary.
    """
    result = {}
    
    all_keys = set(start_state.keys()) | set(end_state.keys())
    
    for key in all_keys:
        start_val = start_state.get(key, 0)
        end_val = end_state.get(key, start_val)
        
        if isinstance(start_val, (int, float)) and isinstance(end_val, (int, float)):
            result[key] = start_val + (end_val - start_val) * progress
        else:
            result[key] = end_val if progress >= 0.5 else start_val
    
    return result


def execute_sequence(
    sequence: AnimationSequence,
    on_update: Callable[[dict, float], None],
    on_step_complete: Optional[Callable[[str], None]] = None,
    on_complete: Optional[Callable[[], None]] = None,
) -> None:
    """Execute an animation sequence.
    
    Args:
        sequence: Animation sequence to execute.
        on_update: Callback with (state_dict, overall_progress).
        on_step_complete: Optional callback when each step completes.
        on_complete: Optional callback when sequence completes.
    """
    total_duration = sequence.total_duration_ms / 1000.0
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed >= total_duration:
            break
        
        overall_progress = elapsed / total_duration
        
        current_step_index = 0
        step_start_time = 0.0
        
        accumulated_time = 0.0
        for i, step in enumerate(sequence.steps):
            if accumulated_time + step.total_duration_ms / 1000.0 >= elapsed:
                current_step_index = i
                step_start_time = accumulated_time
                break
            accumulated_time += step.total_duration_ms / 1000.0
        
        current_step = sequence.steps[current_step_index]
        
        step_elapsed = elapsed - step_start_time
        if step_elapsed < current_step.delay_ms / 1000.0:
            step_progress = 0.0
        else:
            adjusted_elapsed = step_elapsed - current_step.delay_ms / 1000.0
            step_progress = min(1.0, adjusted_elapsed / (current_step.duration_ms / 1000.0))
        
        eased_progress = _apply_easing(step_progress, current_step.easing)
        
        current_state = interpolate_state(
            current_step.start_state,
            current_step.end_state,
            eased_progress,
        )
        
        on_update(current_state, overall_progress)
        
        if step_progress >= 1.0 and on_step_complete:
            on_step_complete(current_step.step_id)
        
        time.sleep(0.01)
    
    if on_complete:
        on_complete()


def _apply_easing(t: float, easing: str) -> float:
    """Apply easing function to progress."""
    if easing == "linear":
        return t
    elif easing == "ease_in":
        return t * t
    elif easing == "ease_out":
        return t * (2 - t)
    elif easing == "ease_in_out":
        if t < 0.5:
            return 2 * t * t
        return -1 + (4 - 2 * t) * t
    else:
        return t


def create_fade_sequence(
    name: str,
    start_alpha: float,
    end_alpha: float,
    duration_ms: float,
) -> AnimationSequence:
    """Create a simple fade animation.
    
    Args:
        name: Sequence name.
        start_alpha: Starting opacity (0.0 to 1.0).
        end_alpha: Ending opacity.
        duration_ms: Animation duration.
    
    Returns:
        AnimationSequence for fading.
    """
    return create_animation_sequence(
        name,
        [
            ({"alpha": start_alpha}, duration_ms / 2),
            ({"alpha": end_alpha}, duration_ms / 2),
        ],
        [TransitionType.FADE],
    )


def create_slide_sequence(
    name: str,
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    duration_ms: float,
) -> AnimationSequence:
    """Create a slide animation.
    
    Args:
        name: Sequence name.
        start_x: Starting X.
        start_y: Starting Y.
        end_x: Ending X.
        end_y: Ending Y.
        duration_ms: Animation duration.
    
    Returns:
        AnimationSequence for sliding.
    """
    return create_animation_sequence(
        name,
        [
            ({"x": start_x, "y": start_y}, duration_ms / 2),
            ({"x": end_x, "y": end_y}, duration_ms / 2),
        ],
        [TransitionType.SLIDE],
    )


def create_zoom_sequence(
    name: str,
    start_scale: float,
    end_scale: float,
    duration_ms: float,
) -> AnimationSequence:
    """Create a zoom animation.
    
    Args:
        name: Sequence name.
        start_scale: Starting scale factor.
        end_scale: Ending scale factor.
        duration_ms: Animation duration.
    
    Returns:
        AnimationSequence for zooming.
    """
    return create_animation_sequence(
        name,
        [
            ({"scale": start_scale}, duration_ms / 2),
            ({"scale": end_scale}, duration_ms / 2),
        ],
        [TransitionType.ZOOM],
    )
