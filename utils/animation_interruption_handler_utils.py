"""
Animation Interruption Handler Utilities

Handle interruption of animations during automation, including
detecting when an animation is mid-flight and deciding whether
to wait, skip, or abort.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Callable, Optional


class InterruptionStrategy(Enum):
    """How to handle animation interruptions."""
    WAIT = auto()
    SKIP = auto()
    ABORT = auto()
    RESUME = auto()


@dataclass
class AnimationState:
    """Current state of an animation."""
    is_running: bool
    progress: float  # 0.0 to 1.0
    frame_index: int
    total_frames: int
    start_time_ms: float
    expected_end_ms: float


@dataclass
class InterruptionDecision:
    """Decision made by the interruption handler."""
    strategy: InterruptionStrategy
    reason: str
    wait_until_ms: Optional[float] = None


class AnimationInterruptionHandler:
    """
    Handle interruption of ongoing animations during automation.

    When automation needs to interact with an element that is
    animating, this handler decides whether to wait for completion,
    skip to the end, or abort and report an error.
    """

    def __init__(
        self,
        max_wait_ms: float = 2000.0,
        skip_threshold_progress: float = 0.8,
    ):
        self.max_wait_ms = max_wait_ms
        self.skip_threshold_progress = skip_threshold_progress
        self._animation_state: Optional[AnimationState] = None

    def set_animation_state(
        self,
        is_running: bool,
        progress: float,
        frame_index: int = 0,
        total_frames: int = 0,
        start_time_ms: Optional[float] = None,
        expected_end_ms: Optional[float] = None,
    ) -> None:
        """Update the current animation state."""
        now = time.time() * 1000
        self._animation_state = AnimationState(
            is_running=is_running,
            progress=progress,
            frame_index=frame_index,
            total_frames=total_frames,
            start_time_ms=start_time_ms or now,
            expected_end_ms=expected_end_ms or now,
        )

    def decide_interruption(
        self,
        urgency: float = 0.5,  # 0.0 = flexible, 1.0 = must act now
    ) -> InterruptionDecision:
        """
        Decide how to handle an animation that is blocking an action.

        Args:
            urgency: How urgently the action needs to proceed (0.0 to 1.0).

        Returns:
            InterruptionDecision with strategy and reasoning.
        """
        if not self._animation_state or not self._animation_state.is_running:
            return InterruptionDecision(
                strategy=InterruptionStrategy.WAIT,
                reason="No animation running",
            )

        state = self._animation_state
        now = time.time() * 1000

        # If animation is nearly complete, wait
        if state.progress >= self.skip_threshold_progress:
            remaining_ms = state.expected_end_ms - now
            if remaining_ms <= self.max_wait_ms:
                return InterruptionDecision(
                    strategy=InterruptionStrategy.WAIT,
                    reason=f"Animation {state.progress:.0%} complete, waiting {remaining_ms:.0f}ms",
                    wait_until_ms=state.expected_end_ms,
                )

        # High urgency - skip or abort
        if urgency > 0.7:
            if state.progress > 0.5:
                return InterruptionDecision(
                    strategy=InterruptionStrategy.SKIP,
                    reason=f"High urgency, skipping at {state.progress:.0%} progress",
                )
            return InterruptionDecision(
                strategy=InterruptionStrategy.ABORT,
                reason="Animation not ready, high urgency abort",
            )

        # Medium urgency - skip if far along
        if urgency > 0.4 and state.progress >= self.skip_threshold_progress:
            return InterruptionDecision(
                strategy=InterruptionStrategy.SKIP,
                reason=f"Medium urgency, skipping at {state.progress:.0%} progress",
            )

        # Low urgency - wait
        return InterruptionDecision(
            strategy=InterruptionStrategy.WAIT,
            reason=f"Low urgency, waiting for animation completion ({state.progress:.0%})",
            wait_until_ms=state.expected_end_ms,
        )
