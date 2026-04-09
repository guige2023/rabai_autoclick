"""
Step Recorder Action Module.

Records automation steps for later playback, supporting
variable speed, step skipping, and conditional execution.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class StepType(Enum):
    """Types of recorded steps."""
    CLICK = "click"
    TYPE = "type"
    SCROLL = "scroll"
    WAIT = "wait"
    KEYBOARD = "keyboard"
    CUSTOM = "custom"


@dataclass
class RecordedStep:
    """A single recorded automation step."""
    step_id: str
    step_type: StepType
    timestamp: float
    data: dict
    duration: float = 0.0
    description: str = ""


class StepRecorder:
    """Records automation steps for playback."""

    def __init__(self, max_steps: int = 10000):
        """
        Initialize step recorder.

        Args:
            max_steps: Maximum steps to retain.
        """
        self.max_steps = max_steps
        self._steps: deque[RecordedStep] = deque(maxlen=max_steps)
        self._recording = False
        self._start_time: Optional[float] = None

    def start_recording(self) -> None:
        """Start recording steps."""
        self._recording = True
        self._start_time = time.time()

    def stop_recording(self) -> None:
        """Stop recording steps."""
        self._recording = False

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._recording

    def record_step(
        self,
        step_type: StepType,
        data: dict,
        description: str = "",
    ) -> str:
        """
        Record a step.

        Args:
            step_type: Type of step.
            data: Step data dictionary.
            description: Optional description.

        Returns:
            Step ID.
        """
        if not self._recording:
            return ""

        step_id = f"step_{len(self._steps) + 1}"
        timestamp = time.time() - (self._start_time or time.time())

        step = RecordedStep(
            step_id=step_id,
            step_type=step_type,
            timestamp=timestamp,
            data=data,
            description=description,
        )

        self._steps.append(step)
        return step_id

    def record_click(
        self,
        target: str,
        coordinates: tuple,
        button: str = "left",
    ) -> str:
        """Record a click step."""
        return self.record_step(
            StepType.CLICK,
            {"target": target, "coordinates": coordinates, "button": button},
            f"Click {target}",
        )

    def record_type(
        self,
        target: str,
        text: str,
    ) -> str:
        """Record a type step."""
        return self.record_step(
            StepType.TYPE,
            {"target": target, "text": text},
            f"Type '{text}' in {target}",
        )

    def record_scroll(
        self,
        direction: str,
        amount: int,
    ) -> str:
        """Record a scroll step."""
        return self.record_step(
            StepType.SCROLL,
            {"direction": direction, "amount": amount},
            f"Scroll {direction} {amount}px",
        )

    def record_wait(
        self,
        duration: float,
    ) -> str:
        """Record a wait step."""
        return self.record_step(
            StepType.WAIT,
            {"duration": duration},
            f"Wait {duration}s",
        )

    def get_steps(
        self,
        since: Optional[float] = None,
        step_type: Optional[StepType] = None,
    ) -> list[RecordedStep]:
        """
        Get recorded steps.

        Args:
            since: Only steps after this timestamp.
            step_type: Filter by step type.

        Returns:
            List of RecordedStep objects.
        """
        steps = list(self._steps)

        if since is not None:
            steps = [s for s in steps if s.timestamp >= since]

        if step_type is not None:
            steps = [s for s in steps if s.step_type == step_type]

        return steps

    def get_step_count(self) -> int:
        """Get total number of recorded steps."""
        return len(self._steps)

    def clear(self) -> None:
        """Clear all recorded steps."""
        self._steps.clear()
        self._start_time = None

    def export(self) -> list[dict]:
        """
        Export steps to dictionary format.

        Returns:
            List of step dictionaries.
        """
        return [
            {
                "step_id": s.step_id,
                "step_type": s.step_type.value,
                "timestamp": s.timestamp,
                "data": s.data,
                "description": s.description,
            }
            for s in self._steps
        ]
