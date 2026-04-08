"""
Sequence Recorder Utilities

Provides utilities for recording action sequences
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from datetime import datetime


@dataclass
class RecordedStep:
    """A recorded step in a sequence."""
    step_number: int
    action: str
    params: dict[str, Any]
    timestamp: datetime


class SequenceRecorder:
    """
    Records sequences of actions for replay.
    
    Captures action details with timestamps
    and parameters.
    """

    def __init__(self) -> None:
        self._steps: list[RecordedStep] = []
        self._is_recording = False
        self._step_number = 0

    def start_recording(self) -> None:
        """Start recording a sequence."""
        self._is_recording = True
        self._steps.clear()
        self._step_number = 0

    def stop_recording(self) -> None:
        """Stop recording."""
        self._is_recording = False

    def record_step(
        self,
        action: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        """Record a single step."""
        if self._is_recording:
            self._step_number += 1
            step = RecordedStep(
                step_number=self._step_number,
                action=action,
                params=params or {},
                timestamp=datetime.now(),
            )
            self._steps.append(step)

    def get_steps(self) -> list[RecordedStep]:
        """Get all recorded steps."""
        return list(self._steps)

    def get_duration(self) -> float:
        """Get total recording duration in seconds."""
        if len(self._steps) < 2:
            return 0.0
        first = self._steps[0].timestamp
        last = self._steps[-1].timestamp
        return (last - first).total_seconds()

    def export_to_dict_list(self) -> list[dict[str, Any]]:
        """Export steps as list of dicts."""
        return [
            {
                "step": s.step_number,
                "action": s.action,
                "params": s.params,
                "timestamp": s.timestamp.isoformat(),
            }
            for s in self._steps
        ]
