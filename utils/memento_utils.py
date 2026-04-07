"""
Memento Pattern Implementation

Provides state snapshot and restoration capabilities for implementing
undo/redo, checkpointing, and state history management.
"""

from __future__ import annotations

import copy
import json
import pickle
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class MementoFormat(Enum := __import__("enum").Enum):
    """Supported serialization formats for mementos."""
    PICKLE = "pickle"
    JSON = "json"
    DEEPCOPY = "deepcopy"


@dataclass
class Memento(Generic[T]):
    """
    A snapshot of state at a specific point in time.

    Type Parameters:
        T: The type of state being captured.
    """
    state: T
    timestamp: float = field(default_factory=time.time)
    label: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"<Memento {self.label or 'snapshot'} @ {self.timestamp:.2f}>"


class MementoOriginator(ABC, Generic[T]):
    """
    Abstract base class for objects that can create and restore mementos.

    Type Parameters:
        T: The type of state this originator manages.
    """

    @abstractmethod
    def create_memento(self, label: str = "") -> Memento[T]:
        """Create a snapshot of the current state."""
        pass

    @abstractmethod
    def restore_memento(self, memento: Memento[T]) -> None:
        """Restore state from a memento."""
        pass

    def save_to_dict(self) -> dict[str, Any]:
        """Export current state as a dictionary."""
        return {"state": self.create_memento().state, "timestamp": time.time()}

    def load_from_dict(self, data: dict[str, Any]) -> None:
        """Import state from a dictionary."""
        memento = Memento(state=data["state"], timestamp=data.get("timestamp", time.time()))
        self.restore_memento(memento)


class SimpleOriginator(MementoOriginator[T]):
    """Simple implementation for plain state objects."""

    def __init__(self, initial_state: T):
        self._state = initial_state

    @property
    def state(self) -> T:
        return self._state

    @state.setter
    def state(self, value: T) -> None:
        self._state = value

    def create_memento(self, label: str = "") -> Memento[T]:
        return Memento(state=copy.deepcopy(self._state), label=label)

    def restore_memento(self, memento: Memento[T]) -> None:
        self._state = copy.deepcopy(memento.state)


@dataclass
class CaretakerEntry:
    """Single entry in the caretaker's history."""
    memento: Memento
    index: int = 0
    can_undo: bool = True
    can_redo: bool = False


class MementoCaretaker(Generic[T]):
    """
    Manages memento storage and retrieval for undo/redo operations.

    Type Parameters:
        T: The type of state being managed.
    """

    def __init__(
        self,
        originator: MementoOriginator[T],
        max_history: int = 100,
        auto_save: bool = True,
    ):
        self.originator = originator
        self.max_history = max_history
        self.auto_save = auto_save
        self._history: list[CaretakerEntry] = []
        self._current_index: int = -1
        self._on_change_callbacks: list[Callable[[int, int], None]] = []

    @property
    def can_undo(self) -> bool:
        return self._current_index >= 0

    @property
    def can_redo(self) -> bool:
        return self._current_index < len(self._history) - 1

    @property
    def current_index(self) -> int:
        return self._current_index

    @property
    def history_size(self) -> int:
        return len(self._history)

    def save(self, label: str = "", metadata: dict[str, Any] | None = None) -> Memento[T]:
        """
        Save the current state as a memento.

        Args:
            label: Optional label for this save point.
            metadata: Optional metadata to attach.

        Returns:
            The created memento.
        """
        # Clear redo history when new save is made
        if self._current_index < len(self._history) - 1:
            self._history = self._history[: self._current_index + 1]

        memento = self.originator.create_memento(label)
        if metadata:
            memento.metadata.update(metadata)

        entry = CaretakerEntry(memento=memento, index=len(self._history))
        self._history.append(entry)
        self._current_index = len(self._history) - 1

        # Trim if exceeds max
        if len(self._history) > self.max_history:
            excess = len(self._history) - self.max_history
            self._history = self._history[excess:]
            self._current_index = max(0, self._current_index - excess)

        self._notify_change()
        return memento

    def undo(self) -> bool:
        """
        Undo to the previous state.

        Returns:
            True if undo was successful, False if at the beginning.
        """
        if not self.can_undo:
            return False

        entry = self._history[self._current_index]
        self.originator.restore_memento(entry.memento)
        self._current_index -= 1
        self._notify_change()
        return True

    def redo(self) -> bool:
        """
        Redo to the next state.

        Returns:
            True if redo was successful, False if at the end.
        """
        if not self.can_redo:
            return False

        self._current_index += 1
        entry = self._history[self._current_index]
        self.originator.restore_memento(entry.memento)
        self._notify_change()
        return True

    def jump_to(self, index: int) -> bool:
        """
        Jump to a specific index in history.

        Args:
            index: The target index (0-based).

        Returns:
            True if jump was successful.
        """
        if index < 0 or index >= len(self._history):
            return False

        self._current_index = index
        entry = self._history[index]
        self.originator.restore_memento(entry.memento)
        self._notify_change()
        return True

    def get_memento(self, index: int) -> Memento[T] | None:
        """Get a memento at a specific index without jumping."""
        if 0 <= index < len(self._history):
            return self._history[index].memento
        return None

    def get_current_memento(self) -> Memento[T] | None:
        """Get the current memento."""
        return self.get_memento(self._current_index)

    def get_history_labels(self) -> list[str]:
        """Get labels for all history entries."""
        return [entry.memento.label or f"Index {entry.index}" for entry in self._history]

    def clear(self) -> None:
        """Clear all history."""
        self._history.clear()
        self._current_index = -1
        self._notify_change()

    def on_change(self, callback: Callable[[int, int], None]) -> None:
        """Register callback for history changes."""
        self._on_change_callbacks.append(callback)

    def _notify_change(self) -> None:
        for cb in self._on_change_callbacks:
            cb(self._current_index, len(self._history))


class CheckpointManager(Generic[T]):
    """
    High-level checkpoint management with named checkpoints,
    auto-checkpoints, and checkpoint comparison.
    """

    def __init__(self, caretaker: MementoCaretaker[T]):
        self.caretaker = caretaker
        self._named_checkpoints: dict[str, Memento[T]] = {}
        self._auto_checkpoint_interval: float | None = None
        self._last_auto_checkpoint: float = 0

    def create_checkpoint(self, name: str, label: str = "") -> Memento[T]:
        """Create a named checkpoint."""
        memento = self.caretaker.save(label or f"Checkpoint: {name}")
        self._named_checkpoints[name] = memento
        return memento

    def restore_checkpoint(self, name: str) -> bool:
        """Restore to a named checkpoint."""
        if name not in self._named_checkpoints:
            return False

        memento = self._named_checkpoints[name]
        self.caretaker.originator.restore_memento(memento)

        # Find and set the index
        for i, entry in enumerate(self.caretaker._history):
            if entry.memento is memento:
                self.caretaker._current_index = i
                break

        return True

    def list_checkpoints(self) -> list[str]:
        """List all named checkpoints."""
        return list(self._named_checkpoints.keys())

    def delete_checkpoint(self, name: str) -> bool:
        """Delete a named checkpoint."""
        if name in self._named_checkpoints:
            del self._named_checkpoints[name]
            return True
        return False

    def auto_checkpoint(
        self,
        interval: float,
        condition: Callable[[], bool] | None = None,
    ) -> None:
        """
        Enable auto-checkpointing.

        Args:
            interval: Minimum seconds between auto-checkpoints.
            condition: Optional function that returns True when to checkpoint.
        """
        self._auto_checkpoint_interval = interval
        if condition and condition():
            self.checkpoint()

    def checkpoint(self, label: str = "") -> Memento[T] | None:
        """Manually create a checkpoint if enough time has passed."""
        now = time.time()
        if (
            self._auto_checkpoint_interval
            and now - self._last_auto_checkpoint < self._auto_checkpoint_interval
        ):
            return None

        self._last_auto_checkpoint = now
        return self.caretaker.save(label=label or f"Auto {now:.0f}")

    def compare_checkpoints(self, name1: str, name2: str) -> dict[str, Any] | None:
        """Compare two named checkpoints."""
        if name1 not in self._named_checkpoints or name2 not in self._named_checkpoints:
            return None

        m1 = self._named_checkpoints[name1]
        m2 = self._named_checkpoints[name2]

        return {
            "name1": name1,
            "name2": name2,
            "time1": m1.timestamp,
            "time2": m2.timestamp,
            "time_diff": m2.timestamp - m1.timestamp,
            "label1": m1.label,
            "label2": m2.label,
        }


def make_originator(obj: Any) -> SimpleOriginator:
    """Create a SimpleOriginator wrapper for any object."""
    return SimpleOriginator(copy.deepcopy(obj))
