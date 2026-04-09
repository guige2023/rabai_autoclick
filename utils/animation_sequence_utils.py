"""
Animation sequence builder utilities.

Build complex multi-step animations.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class AnimationStep:
    """A single step in an animation sequence."""
    step_id: str
    duration_ms: float
    start_value: float
    end_value: float
    easing: str = "ease_in_out"
    on_complete: Optional[Callable] = None


@dataclass
class SequenceState:
    """Current state of a sequence."""
    current_step_index: int
    elapsed_ms: float
    current_value: float
    is_complete: bool = False


class AnimationSequence:
    """A sequence of animation steps."""
    
    def __init__(self, name: str):
        self.name = name
        self._steps: list[AnimationStep] = []
        self._on_sequence_complete: Optional[Callable] = None
    
    def add_step(
        self,
        duration_ms: float,
        start_value: float,
        end_value: float,
        easing: str = "ease_in_out",
        on_complete: Optional[Callable] = None
    ) -> "AnimationSequence":
        """Add a step to the sequence."""
        step = AnimationStep(
            step_id=f"step_{len(self._steps)}",
            duration_ms=duration_ms,
            start_value=start_value,
            end_value=end_value,
            easing=easing,
            on_complete=on_complete
        )
        self._steps.append(step)
        return self
    
    def on_complete(self, callback: Callable) -> "AnimationSequence":
        """Set callback for sequence completion."""
        self._on_sequence_complete = callback
        return self
    
    def create_state(self) -> SequenceState:
        """Create initial state for the sequence."""
        if not self._steps:
            return SequenceState(0, 0, 0, True)
        return SequenceState(0, 0, self._steps[0].start_value)
    
    def update_state(self, state: SequenceState, delta_ms: float) -> SequenceState:
        """Update sequence state by delta time."""
        if state.is_complete or not self._steps:
            return state
        
        state.elapsed_ms += delta_ms
        
        while state.current_step_index < len(self._steps):
            step = self._steps[state.current_step_index]
            
            if state.elapsed_ms <= step.duration_ms:
                t = state.elapsed_ms / step.duration_ms
                eased_t = self._apply_easing(t, step.easing)
                state.current_value = step.start_value + (step.end_value - step.start_value) * eased_t
                break
            
            state.elapsed_ms -= step.duration_ms
            state.current_value = step.end_value
            state.current_step_index += 1
            
            if state.current_step_index < len(self._steps):
                prev_step = step
                next_step = self._steps[state.current_step_index]
                state.current_value = next_step.start_value
        
        if state.current_step_index >= len(self._steps):
            state.is_complete = True
            if self._on_sequence_complete:
                self._on_sequence_complete()
        
        return state
    
    def _apply_easing(self, t: float, easing: str) -> float:
        """Apply easing function to progress."""
        if easing == "linear":
            return t
        elif easing == "ease_in":
            return t * t
        elif easing == "ease_out":
            return 1 - (1 - t) ** 2
        elif easing == "ease_in_out":
            return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2
        return t


class AnimationSequenceBuilder:
    """Builder for animation sequences."""
    
    def __init__(self, name: str):
        self._sequence = AnimationSequence(name)
    
    def then(
        self,
        duration_ms: float,
        end_value: float,
        easing: str = "ease_in_out"
    ) -> "AnimationSequenceBuilder":
        """Add a step from current value to end value."""
        current_value = 0
        if self._sequence._steps:
            current_value = self._sequence._steps[-1].end_value
        
        self._sequence.add_step(duration_ms, current_value, end_value, easing)
        return self
    
    def build(self) -> AnimationSequence:
        """Build the sequence."""
        return self._sequence
