"""
Animation sequence utilities for creating multi-step animations.

Provides animation sequencing with support for parallel
and sequential animation groups, callbacks, and completion handling.
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
    from_value: float
    to_value: float
    easing: str = "ease_in_out"
    property_name: str = "opacity"  # "opacity", "x", "y", "scale", etc.


@dataclass
class AnimationSequence:
    """A sequence of animation steps."""
    name: str
    steps: list[AnimationStep] = field(default_factory=list)
    loop: bool = False
    on_complete: Optional[Callable] = None


class AnimationSequenceEngine:
    """Engine for running animation sequences."""

    def __init__(self):
        self._sequences: dict[str, AnimationSequence] = {}
        self._is_running = False
        self._current_sequence: Optional[str] = None

    def create_sequence(self, name: str) -> AnimationSequenceBuilder:
        """Create a new animation sequence builder."""
        return AnimationSequenceBuilder(self, name)

    def add_sequence(self, sequence: AnimationSequence) -> None:
        """Add a sequence to the engine."""
        self._sequences[sequence.name] = sequence

    def run(
        self,
        name: str,
        on_step: Optional[Callable[[AnimationStep, float], None]] = None,
        on_complete: Optional[Callable] = None,
    ) -> bool:
        """Run an animation sequence.

        Args:
            name: Sequence name
            on_step: Called with (step, progress 0-1) for each step
            on_complete: Called when sequence completes

        Returns:
            True if successful, False if sequence not found
        """
        sequence = self._sequences.get(name)
        if not sequence:
            return False

        self._is_running = True
        self._current_sequence = name

        try:
            if sequence.loop:
                while self._is_running:
                    self._run_sequence(sequence, on_step)
            else:
                self._run_sequence(sequence, on_step)

            if on_complete:
                on_complete()
            if sequence.on_complete:
                sequence.on_complete()

            return True
        finally:
            self._is_running = False
            self._current_sequence = None

    def _run_sequence(
        self,
        sequence: AnimationSequence,
        on_step: Optional[Callable[[AnimationStep, float], None]],
    ) -> None:
        """Run a single pass of a sequence."""
        start_time = time.time() * 1000

        for step in sequence.steps:
            if not self._is_running:
                break

            step_end = start_time + step.duration_ms

            while self._is_running:
                elapsed = (time.time() * 1000) - start_time
                progress = min(1.0, elapsed / step.duration_ms)

                # Apply easing
                eased_progress = self._apply_easing(progress, step.easing)

                # Compute current value
                value = step.from_value + (step.to_value - step.from_value) * eased_progress

                if on_step:
                    on_step(step, value)

                if elapsed >= step.duration_ms:
                    break

                time.sleep(0.01)  # 10ms granularity

            start_time = step_end

    def stop(self) -> None:
        """Stop the currently running sequence."""
        self._is_running = False

    def _apply_easing(self, t: float, easing: str) -> float:
        """Apply easing function."""
        if easing == "linear":
            return t
        elif easing == "ease_in":
            return t * t
        elif easing == "ease_out":
            return 1 - (1 - t) ** 2
        elif easing == "ease_in_out":
            return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2
        elif easing == "ease_in_cubic":
            return t * t * t
        elif easing == "ease_out_cubic":
            return 1 - (1 - t) ** 3
        elif easing == "bounce":
            import math
            if t < 0.5:
                return 2 * t * t
            return 1 - (-2 * t + 2) ** 2 / 2 + math.sin(t * math.pi) * 0.1
        return t


class AnimationSequenceBuilder:
    """Fluent builder for animation sequences."""

    def __init__(self, engine: AnimationSequenceEngine, name: str):
        self._engine = engine
        self._sequence = AnimationSequence(name=name)

    def add_step(
        self,
        step_id: str,
        from_value: float,
        to_value: float,
        duration_ms: float = 300.0,
        easing: str = "ease_in_out",
        property_name: str = "opacity",
    ) -> AnimationSequenceBuilder:
        """Add an animation step."""
        self._sequence.steps.append(AnimationStep(
            step_id=step_id,
            duration_ms=duration_ms,
            from_value=from_value,
            to_value=to_value,
            easing=easing,
            property_name=property_name,
        ))
        return self

    def then(
        self,
        step_id: str,
        from_value: float,
        to_value: float,
        duration_ms: float = 300.0,
        easing: str = "ease_in_out",
        property_name: str = "opacity",
    ) -> AnimationSequenceBuilder:
        """Add a sequential step (alias for add_step)."""
        return self.add_step(step_id, from_value, to_value, duration_ms, easing, property_name)

    def fade_in(self, duration_ms: float = 300.0) -> AnimationSequenceBuilder:
        """Add a fade-in step."""
        return self.add_step("fade_in", 0.0, 1.0, duration_ms, "ease_out", "opacity")

    def fade_out(self, duration_ms: float = 300.0) -> AnimationSequenceBuilder:
        """Add a fade-out step."""
        return self.add_step("fade_out", 1.0, 0.0, duration_ms, "ease_in", "opacity")

    def scale_to(self, to: float, duration_ms: float = 300.0) -> AnimationSequenceBuilder:
        """Add a scale step."""
        return self.add_step("scale", 1.0, to, duration_ms, "ease_in_out", "scale")

    def move_to(self, to_x: float, to_y: float, duration_ms: float = 300.0) -> AnimationSequenceBuilder:
        """Add separate X and Y move steps."""
        self.add_step("move_x", 0.0, to_x, duration_ms, "ease_in_out", "x")
        return self.add_step("move_y", 0.0, to_y, duration_ms, "ease_in_out", "y")

    def loop(self) -> AnimationSequenceBuilder:
        """Set the sequence to loop."""
        self._sequence.loop = True
        return self

    def on_complete(self, callback: Callable) -> AnimationSequenceBuilder:
        """Set completion callback."""
        self._sequence.on_complete = callback
        return self

    def build(self) -> AnimationSequence:
        """Build and register the sequence."""
        self._engine.add_sequence(self._sequence)
        return self._sequence


__all__ = ["AnimationSequenceEngine", "AnimationSequence", "AnimationStep", "AnimationSequenceBuilder"]
