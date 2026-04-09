"""
Input Recording Action Module

Records and manages input sequences for playback,
supporting macros, demonstrations, and regression testing.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class InputAction(Enum):
    """Input action types."""

    MOUSE_MOVE = "mouse_move"
    MOUSE_CLICK = "mouse_click"
    MOUSE_DOWN = "mouse_down"
    MOUSE_UP = "mouse_up"
    MOUSE_WHEEL = "mouse_wheel"
    KEY_DOWN = "key_down"
    KEY_UP = "key_up"
    KEY_PRESS = "key_press"
    DELAY = "delay"


@dataclass
class InputStep:
    """Single input step."""

    action: InputAction
    timestamp: float
    x: Optional[float] = None
    y: Optional[float] = None
    button: Optional[int] = None
    key: Optional[str] = None
    modifiers: List[str] = field(default_factory=list)
    delta: Optional[int] = None
    duration: Optional[float] = None


@dataclass
class Recording:
    """Input recording."""

    id: str
    name: str
    steps: List[InputStep] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class InputRecorder:
    """
    Records input sequences for playback.

    Supports pause/resume, step editing,
    and various playback options.
    """

    def __init__(
        self,
        input_handler: Optional[Callable[[InputStep], None]] = None,
    ):
        self.input_handler = input_handler
        self._current_recording: Optional[Recording] = None
        self._is_recording: bool = False
        self._recordings: Dict[str, Recording] = {}

    def start_recording(self, name: str) -> str:
        """
        Start a new recording.

        Args:
            name: Recording name

        Returns:
            Recording ID
        """
        recording_id = f"rec_{time.time()}"
        self._current_recording = Recording(
            id=recording_id,
            name=name,
        )
        self._is_recording = True
        logger.info(f"Started recording: {name}")
        return recording_id

    def stop_recording(self) -> Optional[Recording]:
        """Stop current recording."""
        if not self._is_recording or not self._current_recording:
            return None

        self._is_recording = False

        if len(self._current_recording.steps) > 1:
            last_ts = self._current_recording.steps[-1].timestamp
            first_ts = self._current_recording.steps[0].timestamp
            self._current_recording.duration = last_ts - first_ts

        recording = self._current_recording
        self._recordings[recording.id] = recording
        self._current_recording = None

        logger.info(f"Stopped recording: {recording.name} ({len(recording.steps)} steps)")
        return recording

    def record_step(self, step: InputStep) -> None:
        """Record an input step."""
        if not self._is_recording or not self._current_recording:
            return

        self._current_recording.steps.append(step)

        if self.input_handler:
            self.input_handler(step)

    def pause_recording(self) -> bool:
        """Pause recording."""
        if self._is_recording:
            logger.info("Recording paused")
            return True
        return False

    def resume_recording(self) -> bool:
        """Resume recording."""
        if not self._is_recording:
            self._is_recording = True
            logger.info("Recording resumed")
            return True
        return False

    def cancel_recording(self) -> None:
        """Cancel current recording."""
        self._current_recording = None
        self._is_recording = False

    def get_recording(self, recording_id: str) -> Optional[Recording]:
        """Get recording by ID."""
        return self._recordings.get(recording_id)

    def list_recordings(self) -> List[str]:
        """List all recording IDs."""
        return list(self._recordings.keys())

    def delete_recording(self, recording_id: str) -> bool:
        """Delete a recording."""
        if recording_id in self._recordings:
            del self._recordings[recording_id]
            return True
        return False

    def save_recording(
        self,
        recording_id: str,
        path: str,
    ) -> bool:
        """Save recording to file."""
        recording = self._recordings.get(recording_id)
        if not recording:
            return False

        try:
            data = {
                "id": recording.id,
                "name": recording.name,
                "duration": recording.duration,
                "created_at": recording.created_at,
                "metadata": recording.metadata,
                "steps": [
                    {
                        "action": s.action.value,
                        "timestamp": s.timestamp,
                        "x": s.x,
                        "y": s.y,
                        "button": s.button,
                        "key": s.key,
                        "modifiers": s.modifiers,
                        "delta": s.delta,
                        "duration": s.duration,
                    }
                    for s in recording.steps
                ],
            }

            with open(path, "w") as f:
                json.dump(data, f, indent=2)

            return True

        except Exception as e:
            logger.error(f"Save recording failed: {e}")
            return False

    def load_recording(self, path: str) -> Optional[Recording]:
        """Load recording from file."""
        try:
            with open(path, "r") as f:
                data = json.load(f)

            steps = [
                InputStep(
                    action=InputAction(s["action"]),
                    timestamp=s["timestamp"],
                    x=s.get("x"),
                    y=s.get("y"),
                    button=s.get("button"),
                    key=s.get("key"),
                    modifiers=s.get("modifiers", []),
                    delta=s.get("delta"),
                    duration=s.get("duration"),
                )
                for s in data.get("steps", [])
            ]

            recording = Recording(
                id=data["id"],
                name=data["name"],
                steps=steps,
                created_at=data.get("created_at", 0),
                duration=data.get("duration", 0),
                metadata=data.get("metadata", {}),
            )

            self._recordings[recording.id] = recording
            return recording

        except Exception as e:
            logger.error(f"Load recording failed: {e}")
            return None


def create_input_recorder() -> InputRecorder:
    """Factory function."""
    return InputRecorder()
