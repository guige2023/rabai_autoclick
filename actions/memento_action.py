"""Memento action module for RabAI AutoClick.

Provides state snapshot and undo/redo functionality:
- Memento: State snapshot container
- MementoCaretaker: Manages memento history
- UndoableAction: Actions that support undo/redo
- StateManager: Manages state snapshots
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
import copy
import json

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Memento:
    """State snapshot."""
    id: str
    timestamp: float
    state: Dict[str, Any]
    label: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "state": self.state,
            "label": self.label,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Memento":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            timestamp=data["timestamp"],
            state=data["state"],
            label=data.get("label", ""),
            metadata=data.get("metadata", {}),
        )


class MementoCaretaker:
    """Manages memento history."""

    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self._mementos: List[Memento] = []
        self._current_index: int = -1

    def save(self, memento: Memento) -> None:
        """Save a memento."""
        if self._current_index < len(self._mementos) - 1:
            self._mementos = self._mementos[: self._current_index + 1]

        self._mementos.append(memento)

        if len(self._mementos) > self.max_history:
            self._mementos.pop(0)
        else:
            self._current_index += 1

    def undo(self) -> Optional[Memento]:
        """Undo to previous state."""
        if self._current_index <= 0:
            return None
        self._current_index -= 1
        return self._mementos[self._current_index]

    def redo(self) -> Optional[Memento]:
        """Redo to next state."""
        if self._current_index >= len(self._mementos) - 1:
            return None
        self._current_index += 1
        return self._mementos[self._current_index]

    def get_current(self) -> Optional[Memento]:
        """Get current memento."""
        if 0 <= self._current_index < len(self._mementos):
            return self._mementos[self._current_index]
        return None

    def can_undo(self) -> bool:
        """Check if undo is available."""
        return self._current_index > 0

    def can_redo(self) -> bool:
        """Check if redo is available."""
        return self._current_index < len(self._mementos) - 1

    def get_history(self) -> List[Memento]:
        """Get full history."""
        return self._mementos.copy()

    def clear(self) -> None:
        """Clear history."""
        self._mementos.clear()
        self._current_index = -1


class StateManager:
    """Manages state snapshots."""

    def __init__(self, caretaker: Optional[MementoCaretaker] = None):
        self.caretaker = caretaker or MementoCaretaker()
        self._state: Dict[str, Any] = {}
        self._originator: Optional["StateOriginator"] = None

    def set_state(self, state: Dict[str, Any], label: str = "") -> Memento:
        """Set state and save memento."""
        self._state = copy.deepcopy(state)
        memento = self._create_memento(label)
        self.caretaker.save(memento)
        return memento

    def get_state(self) -> Dict[str, Any]:
        """Get current state."""
        return copy.deepcopy(self._state)

    def restore(self, memento: Memento) -> None:
        """Restore state from memento."""
        self._state = copy.deepcopy(memento.state)

    def undo(self) -> bool:
        """Undo last change."""
        memento = self.caretaker.undo()
        if memento:
            self.restore(memento)
            return True
        return False

    def redo(self) -> bool:
        """Redo last undone change."""
        memento = self.caretaker.redo()
        if memento:
            self.restore(memento)
            return True
        return False

    def _create_memento(self, label: str) -> Memento:
        """Create a memento."""
        import uuid
        return Memento(
            id=str(uuid.uuid4()),
            timestamp=datetime.now().timestamp(),
            state=copy.deepcopy(self._state),
            label=label,
        )


class StateOriginator:
    """Creates and restores mementos."""

    def __init__(self, initial_state: Optional[Dict[str, Any]] = None):
        self._state = initial_state or {}

    def get_state(self) -> Dict[str, Any]:
        """Get current state."""
        return copy.deepcopy(self._state)

    def set_state(self, state: Dict[str, Any]) -> None:
        """Set state."""
        self._state = copy.deepcopy(state)

    def create_memento(self, label: str = "") -> Memento:
        """Create memento from current state."""
        import uuid
        return Memento(
            id=str(uuid.uuid4()),
            timestamp=datetime.now().timestamp(),
            state=copy.deepcopy(self._state),
            label=label,
        )

    def restore(self, memento: Memento) -> None:
        """Restore state from memento."""
        self._state = copy.deepcopy(memento.state)


class UndoableAction(BaseAction):
    """Base action with undo/redo support."""

    action_type = "undoable"
    display_name = "可撤销操作"
    description = "支持撤销/重做的操作"

    def __init__(self):
        super().__init__()
        self._caretaker = MementoCaretaker()
        self._originator = StateOriginator()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "execute":
                return self._execute_action(params)
            elif operation == "undo":
                return self._undo_action()
            elif operation == "redo":
                return self._redo_action()
            elif operation == "snapshot":
                return self._snapshot(params)
            elif operation == "restore":
                return self._restore(params)
            elif operation == "history":
                return self._get_history()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Undoable action error: {str(e)}")

    def _execute_action(self, params: Dict[str, Any]) -> ActionResult:
        """Execute and save snapshot."""
        label = params.get("label", "")
        state = params.get("state", {})

        self._originator.set_state(state)
        memento = self._originator.create_memento(label)
        self._caretaker.save(memento)

        return ActionResult(
            success=True,
            message=f"Action executed and saved: {label}",
            data={"memento_id": memento.id},
        )

    def _undo_action(self) -> ActionResult:
        """Undo last action."""
        if not self._caretaker.can_undo():
            return ActionResult(success=False, message="Nothing to undo")

        memento = self._caretaker.undo()
        if memento:
            self._originator.restore(memento)
            return ActionResult(
                success=True,
                message=f"Undone: {memento.label}",
                data={"state": memento.state},
            )
        return ActionResult(success=False, message="Undo failed")

    def _redo_action(self) -> ActionResult:
        """Redo last undone action."""
        if not self._caretaker.can_redo():
            return ActionResult(success=False, message="Nothing to redo")

        memento = self._caretaker.redo()
        if memento:
            self._originator.restore(memento)
            return ActionResult(
                success=True,
                message=f"Redone: {memento.label}",
                data={"state": memento.state},
            )
        return ActionResult(success=False, message="Redo failed")

    def _snapshot(self, params: Dict[str, Any]) -> ActionResult:
        """Create a named snapshot."""
        label = params.get("label", "")
        state = params.get("state", {})

        self._originator.set_state(state)
        memento = self._originator.create_memento(label)
        self._caretaker.save(memento)

        return ActionResult(
            success=True,
            message=f"Snapshot created: {label}",
            data={"memento": memento.to_dict()},
        )

    def _restore(self, params: Dict[str, Any]) -> ActionResult:
        """Restore to specific memento."""
        memento_id = params.get("memento_id")
        if not memento_id:
            return ActionResult(success=False, message="memento_id is required")

        for m in self._caretaker.get_history():
            if m.id == memento_id:
                self._originator.restore(m)
                return ActionResult(
                    success=True,
                    message=f"Restored to: {m.label}",
                    data={"state": m.state},
                )

        return ActionResult(success=False, message=f"Memento not found: {memento_id}")

    def _get_history(self) -> ActionResult:
        """Get action history."""
        history = self._caretaker.get_history()
        return ActionResult(
            success=True,
            message=f"{len(history)} snapshots",
            data={
                "can_undo": self._caretaker.can_undo(),
                "can_redo": self._caretaker.can_redo(),
                "history": [m.to_dict() for m in history],
            },
        )
