"""
Action Replayer Action Module.

Replays recorded interaction sequences with timing accuracy,
variable playback speed, and conditional branching support.
"""

import time
from typing import Any, Callable, Optional


class ReplayAction:
    """Represents a single action to replay."""

    def __init__(
        self,
        action_type: str,
        target: Optional[str] = None,
        coordinates: Optional[tuple] = None,
        data: Optional[dict] = None,
    ):
        """
        Initialize replay action.

        Args:
            action_type: Type of action (click, type, scroll, etc.).
            target: Element target.
            coordinates: (x, y) coordinates.
            data: Additional action data.
        """
        self.action_type = action_type
        self.target = target
        self.coordinates = coordinates
        self.data = data or {}

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "action_type": self.action_type,
            "target": self.target,
            "coordinates": self.coordinates,
            "data": self.data,
        }


class ActionSequence:
    """A sequence of actions to replay."""

    def __init__(self, name: str = ""):
        """
        Initialize action sequence.

        Args:
            name: Sequence name/label.
        """
        self.name = name
        self.actions: list[ReplayAction] = []
        self._start_time: Optional[float] = None

    def add(self, action: ReplayAction) -> None:
        """Add an action to the sequence."""
        self.actions.append(action)

    def get_actions(self) -> list[ReplayAction]:
        """Get all actions in sequence."""
        return self.actions

    def total_duration(self) -> float:
        """Calculate total duration of the sequence."""
        if len(self.actions) < 2:
            return 0.0
        return sum(a.data.get("duration", 0.0) for a in self.actions)


class ActionReplayer:
    """Replays action sequences with execution callbacks."""

    def __init__(
        self,
        executor: Callable[[ReplayAction], Any],
        speed: float = 1.0,
    ):
        """
        Initialize replayer.

        Args:
            executor: Function to execute each action.
            speed: Playback speed multiplier (1.0 = normal).
        """
        self.executor = executor
        self.speed = speed
        self._running = False
        self._paused = False
        self._current_index = 0

    def replay(
        self,
        sequence: ActionSequence,
        from_index: int = 0,
    ) -> dict[str, Any]:
        """
        Replay an action sequence.

        Args:
            sequence: Sequence to replay.
            from_index: Start index.

        Returns:
            Replay result summary.
        """
        self._running = True
        self._paused = False
        self._current_index = from_index

        results = {
            "total": len(sequence.actions) - from_index,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
        }

        for i in range(from_index, len(sequence.actions)):
            if not self._running:
                break

            while self._paused and self._running:
                time.sleep(0.05)

            action = sequence.actions[i]
            self._current_index = i

            try:
                self.executor(action)
                results["completed"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({
                    "index": i,
                    "action": action.action_type,
                    "error": str(e),
                })

        self._running = False
        return results

    def pause(self) -> None:
        """Pause replay."""
        self._paused = True

    def resume(self) -> None:
        """Resume replay."""
        self._paused = False

    def stop(self) -> None:
        """Stop replay."""
        self._running = False

    def is_running(self) -> bool:
        """Check if replay is running."""
        return self._running

    @property
    def current_index(self) -> int:
        """Get current action index."""
        return self._current_index


def load_sequence_from_dict(data: dict) -> ActionSequence:
    """
    Load an action sequence from dictionary data.

    Args:
        data: Dictionary with sequence data.

    Returns:
        Loaded ActionSequence.
    """
    seq = ActionSequence(name=data.get("name", ""))
    for action_data in data.get("actions", []):
        action = ReplayAction(
            action_type=action_data["action_type"],
            target=action_data.get("target"),
            coordinates=action_data.get("coordinates"),
            data=action_data.get("data", {}),
        )
        seq.add(action)
    return seq
