"""
Touch Gesture Composer Utilities for UI Automation.

This module provides utilities for composing complex multi-step
gestures from simpler touch actions in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class GesturePhase(Enum):
    """Phases of a composed gesture."""
    IDLE = "idle"
    RECORDING = "recording"
    COMPOSING = "composing"
    READY = "ready"
    PLAYING = "playing"


@dataclass
class GestureStep:
    """A single step in a composed gesture."""
    step_id: int
    action_type: str
    points: List[Tuple[float, float]] = field(default_factory=list)
    hold_duration_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComposedGesture:
    """A complete composed gesture with multiple steps."""
    name: str
    steps: List[GestureStep] = field(default_factory=list)
    total_duration_ms: float = 0.0
    repeat_count: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


class GestureComposer:
    """Composes complex gestures from simpler touch actions."""

    def __init__(self) -> None:
        self._phase: GesturePhase = GesturePhase.IDLE
        self._current_gesture: Optional[ComposedGesture] = None
        self._current_step: Optional[GestureStep] = None
        self._step_id_counter: int = 0
        self._recorded_points: List[Tuple[float, float]] = []
        self._gesture_library: Dict[str, ComposedGesture] = {}

    def start_recording(self, name: str) -> None:
        """Start recording a new composed gesture."""
        self._current_gesture = ComposedGesture(name=name)
        self._phase = GesturePhase.RECORDING
        self._recorded_points.clear()

    def start_step(self, action_type: str) -> None:
        """Start a new step in the current gesture."""
        if self._phase != GesturePhase.RECORDING:
            return

        self._current_step = GestureStep(
            step_id=self._step_id_counter,
            action_type=action_type,
        )
        self._recorded_points.clear()
        self._step_id_counter += 1

    def add_point(self, x: float, y: float) -> None:
        """Add a point to the current step."""
        if self._phase != GesturePhase.RECORDING or self._current_step is None:
            return

        self._recorded_points.append((x, y))
        self._current_step.points.append((x, y))

    def end_step(self, hold_duration_ms: float = 0.0) -> None:
        """End the current step."""
        if self._phase != GesturePhase.RECORDING or self._current_step is None:
            return

        self._current_step.hold_duration_ms = hold_duration_ms
        if self._current_gesture is not None:
            self._current_gesture.steps.append(self._current_step)

        self._current_step = None
        self._recorded_points.clear()

    def stop_recording(self) -> Optional[ComposedGesture]:
        """Stop recording and finalize the gesture."""
        if self._phase != GesturePhase.RECORDING:
            return None

        self._phase = GesturePhase.READY

        if self._current_gesture is not None:
            total_duration = sum(
                step.hold_duration_ms for step in self._current_gesture.steps
            )
            self._current_gesture.total_duration_ms = total_duration
            gesture = self._current_gesture
            self._gesture_library[gesture.name] = gesture
            self._current_gesture = None
            return gesture

        return None

    def cancel_recording(self) -> None:
        """Cancel the current recording."""
        self._phase = GesturePhase.IDLE
        self._current_gesture = None
        self._current_step = None
        self._recorded_points.clear()
        self._step_id_counter = 0

    def register_gesture(self, gesture: ComposedGesture) -> None:
        """Register a composed gesture to the library."""
        self._gesture_library[gesture.name] = gesture

    def get_gesture(self, name: str) -> Optional[ComposedGesture]:
        """Get a gesture from the library by name."""
        return self._gesture_library.get(name)

    def get_gesture_names(self) -> List[str]:
        """Get all registered gesture names."""
        return list(self._gesture_library.keys())

    def delete_gesture(self, name: str) -> bool:
        """Delete a gesture from the library."""
        if name in self._gesture_library:
            del self._gesture_library[name]
            return True
        return False

    def get_phase(self) -> GesturePhase:
        """Get the current composer phase."""
        return self._phase

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._phase == GesturePhase.RECORDING

    def get_current_recording(self) -> Optional[ComposedGesture]:
        """Get the gesture currently being recorded."""
        return self._current_gesture

    def set_repeat_count(self, name: str, count: int) -> bool:
        """Set the repeat count for a registered gesture."""
        gesture = self._gesture_library.get(name)
        if gesture is not None:
            gesture.repeat_count = max(1, count)
            return True
        return False
