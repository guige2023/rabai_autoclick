"""
Animation state machine utilities.

This module provides state machine utilities for managing animation
transitions, states, and playback control.
"""

from __future__ import annotations

import time
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum, auto


class AnimationState(Enum):
    """Animation playback states."""
    IDLE = auto()
    PLAYING = auto()
    PAUSED = auto()
    STOPPED = auto()
    COMPLETED = auto()


class AnimationTrigger(Enum):
    """Events that can trigger state transitions."""
    PLAY = auto()
    PAUSE = auto()
    STOP = auto()
    COMPLETE = auto()
    RESET = auto()
    SEEK = auto()
    TIMEOUT = auto()


@dataclass
class AnimationTransition:
    """Represents a state transition."""
    from_state: AnimationState
    to_state: AnimationState
    trigger: AnimationTrigger
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None


@dataclass
class AnimationSMConfig:
    """Configuration for animation state machine."""
    auto_complete: bool = True
    completion_threshold_ms: float = 50.0
    allow_idle_transitions: bool = True


class AnimationStateMachine:
    """State machine for managing animation playback."""

    def __init__(self, config: Optional[AnimationSMConfig] = None):
        self.config = config or AnimationSMConfig()
        self._state: AnimationState = AnimationState.IDLE
        self._transitions: Dict[AnimationState, List[AnimationTransition]] = {}
        self._progress: float = 0.0
        self._start_time: Optional[float] = None
        self._pause_time: Optional[float] = None
        self._total_paused_duration: float = 0.0
        self._state_history: List[AnimationState] = [AnimationState.IDLE]
        self._listeners: Dict[str, List[Callable[[AnimationState, AnimationState], None]]] = {}

        self._register_default_transitions()

    def _register_default_transitions(self) -> None:
        """Register the default animation state transitions."""
        transitions = [
            AnimationTransition(AnimationState.IDLE, AnimationState.PLAYING, AnimationTrigger.PLAY),
            AnimationTransition(AnimationState.PLAYING, AnimationState.PAUSED, AnimationTrigger.PAUSE),
            AnimationTransition(AnimationState.PLAYING, AnimationState.STOPPED, AnimationTrigger.STOP),
            AnimationTransition(AnimationState.PLAYING, AnimationState.COMPLETED, AnimationTrigger.COMPLETE),
            AnimationTransition(AnimationState.PLAYING, AnimationState.PLAYING, AnimationTrigger.SEEK),
            AnimationTransition(AnimationState.PAUSED, AnimationState.PLAYING, AnimationTrigger.PLAY),
            AnimationTransition(AnimationState.PAUSED, AnimationState.STOPPED, AnimationTrigger.STOP),
            AnimationTransition(AnimationState.STOPPED, AnimationState.IDLE, AnimationTrigger.RESET),
            AnimationTransition(AnimationState.COMPLETED, AnimationState.IDLE, AnimationTrigger.RESET),
            AnimationTransition(AnimationState.COMPLETED, AnimationState.PLAYING, AnimationTrigger.PLAY),
        ]
        for t in transitions:
            self.add_transition(t)

    def add_transition(self, transition: AnimationTransition) -> None:
        """Add a state transition to the machine."""
        if transition.from_state not in self._transitions:
            self._transitions[transition.from_state] = []
        self._transitions[transition.from_state].append(transition)

    def trigger(self, event: AnimationTrigger, **kwargs: Any) -> bool:
        """
        Trigger a state transition.

        Args:
            event: The trigger event.
            **kwargs: Additional context for guards/actions.

        Returns:
            True if transition was successful.
        """
        current_transitions = self._transitions.get(self._state, [])
        for t in current_transitions:
            if t.trigger == event:
                if t.guard is None or t.guard():
                    old_state = self._state
                    self._apply_transition(t)
                    if t.action:
                        t.action()
                    self._notify_listeners(old_state, self._state)
                    return True
        return False

    def _apply_transition(self, transition: AnimationTransition) -> None:
        """Apply a transition, updating internal state."""
        self._state = transition.to_state
        self._state_history.append(self._state)

        if transition.to_state == AnimationState.PLAYING:
            if transition.from_state == AnimationState.IDLE or transition.from_state == AnimationState.STOPPED:
                self._start_time = time.time()
                self._total_paused_duration = 0.0
            elif transition.from_state == AnimationState.PAUSED and self._pause_time is not None:
                self._total_paused_duration += time.time() - self._pause_time
                self._pause_time = None
        elif transition.to_state == AnimationState.PAUSED:
            self._pause_time = time.time()
        elif transition.to_state == AnimationState.STOPPED:
            self._progress = 0.0
            self._start_time = None
            self._pause_time = None
            self._total_paused_duration = 0.0
        elif transition.to_state == AnimationState.COMPLETED:
            self._progress = 1.0

    def play(self) -> bool:
        """Start or resume playback."""
        return self.trigger(AnimationTrigger.PLAY)

    def pause(self) -> bool:
        """Pause playback."""
        return self.trigger(AnimationTrigger.PAUSE)

    def stop(self) -> bool:
        """Stop playback and reset."""
        return self.trigger(AnimationTrigger.STOP)

    def reset(self) -> bool:
        """Reset to idle state."""
        return self.trigger(AnimationTrigger.RESET)

    @property
    def state(self) -> AnimationState:
        """Current state."""
        return self._state

    @property
    def is_playing(self) -> bool:
        """True if currently playing."""
        return self._state == AnimationState.PLAYING

    @property
    def progress(self) -> float:
        """Current playback progress (0-1)."""
        if self._state != AnimationState.PLAYING:
            return self._progress

        if self._start_time is None:
            return 0.0

        elapsed = time.time() - self._start_time - self._total_paused_duration
        return min(1.0, elapsed)

    def set_progress(self, progress: float) -> bool:
        """Set playback progress directly."""
        if not 0.0 <= progress <= 1.0:
            return False
        self._progress = progress
        return True

    def add_state_listener(self, event: str, callback: Callable[[AnimationState, AnimationState], None]) -> None:
        """Register a state change listener."""
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(callback)

    def _notify_listeners(self, old: AnimationState, new: AnimationState) -> None:
        """Notify all listeners of a state change."""
        for callbacks in self._listeners.values():
            for cb in callbacks:
                cb(old, new)
