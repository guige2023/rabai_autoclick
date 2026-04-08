"""
Action History Utilities

Tracks and manages action history for UI automation workflows,
enabling undo/redo, action replay, and debugging.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import datetime
from enum import Enum, auto
import json


class ActionType(Enum):
    """Types of actions that can be tracked."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    RIGHT_CLICK = auto()
    TYPE = auto()
    PRESS_KEY = auto()
    SCROLL = auto()
    DRAG = auto()
    SWIPE = auto()
    HOVER = auto()
    CUSTOM = auto()


@dataclass
class ActionRecord:
    """Single action record in history."""
    action_type: ActionType
    timestamp: datetime
    target: str | None
    params: dict[str, Any]
    result: str | None = None
    duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type.name,
            "timestamp": self.timestamp.isoformat(),
            "target": self.target,
            "params": self.params,
            "result": self.result,
            "duration_ms": self.duration_ms,
        }


class ActionHistory:
    """
    Manages action history with undo/redo support.
    
    Tracks all UI actions and allows replay, undo, and analysis.
    """

    def __init__(self, max_size: int = 1000) -> None:
        self._history: list[ActionRecord] = []
        self._redo_stack: list[ActionRecord] = []
        self._max_size = max_size
        self._listeners: list[Callable[[ActionRecord], None]] = []

    def record(
        self,
        action_type: ActionType,
        target: str | None = None,
        params: dict[str, Any] | None = None,
        result: str | None = None,
        duration_ms: float = 0.0,
    ) -> ActionRecord:
        """Record a new action."""
        params = params or {}
        record = ActionRecord(
            action_type=action_type,
            timestamp=datetime.now(),
            target=target,
            params=params,
            result=result,
            duration_ms=duration_ms,
        )
        self._history.append(record)
        self._redo_stack.clear()
        if len(self._history) > self._max_size:
            self._history.pop(0)
        for listener in self._listeners:
            listener(record)
        return record

    def undo(self) -> ActionRecord | None:
        """Undo the last action."""
        if not self._history:
            return None
        record = self._history.pop()
        self._redo_stack.append(record)
        return record

    def redo(self) -> ActionRecord | None:
        """Redo the last undone action."""
        if not self._redo_stack:
            return None
        record = self._redo_stack.pop()
        self._history.append(record)
        return record

    def get_history(
        self,
        limit: int | None = None,
        action_type: ActionType | None = None
    ) -> list[ActionRecord]:
        """Get action history with optional filtering."""
        history = self._history
        if action_type:
            history = [r for r in history if r.action_type == action_type]
        if limit:
            history = history[-limit:]
        return list(history)

    def export_json(self) -> str:
        """Export history as JSON string."""
        return json.dumps(
            [r.to_dict() for r in self._history],
            indent=2,
            ensure_ascii=False
        )

    def add_listener(self, listener: Callable[[ActionRecord], None]) -> None:
        """Add a listener for new action records."""
        self._listeners.append(listener)

    def clear(self) -> None:
        """Clear all history."""
        self._history.clear()
        self._redo_stack.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about recorded actions."""
        type_counts: dict[str, int] = {}
        for record in self._history:
            name = record.action_type.name
            type_counts[name] = type_counts.get(name, 0) + 1
        return {
            "total_actions": len(self._history),
            "undo_count": len(self._redo_stack),
            "type_counts": type_counts,
        }
