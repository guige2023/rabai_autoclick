"""Memento Pattern Action Module.

Provides memento pattern for state
snapshot and rollback.
"""

import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Memento:
    """Memento state snapshot."""
    memento_id: str
    state: Dict
    timestamp: float = field(default_factory=time.time)
    label: Optional[str] = None


class MementoManager:
    """Manages memento snapshots."""

    def __init__(self):
        self._mementos: Dict[str, List[Memento]] = {}

    def save(
        self,
        originator_id: str,
        state: Dict,
        label: Optional[str] = None
    ) -> str:
        """Save state memento."""
        memento_id = f"memento_{int(time.time() * 1000)}"

        memento = Memento(
            memento_id=memento_id,
            state=state.copy(),
            label=label
        )

        if originator_id not in self._mementos:
            self._mementos[originator_id] = []

        self._mementos[originator_id].append(memento)
        return memento_id

    def restore(self, originator_id: str, memento_id: str) -> Optional[Dict]:
        """Restore from memento."""
        mementos = self._mementos.get(originator_id, [])
        for m in mementos:
            if m.memento_id == memento_id:
                return m.state.copy()
        return None

    def get_latest(self, originator_id: str) -> Optional[Dict]:
        """Get latest memento."""
        mementos = self._mementos.get(originator_id, [])
        if not mementos:
            return None
        return mementos[-1].state.copy()

    def get_history(self, originator_id: str) -> List[Dict]:
        """Get memento history."""
        mementos = self._mementos.get(originator_id, [])
        return [
            {
                "memento_id": m.memento_id,
                "label": m.label,
                "timestamp": m.timestamp
            }
            for m in mementos
        ]


class MementoPatternAction(BaseAction):
    """Action for memento pattern operations."""

    def __init__(self):
        super().__init__("memento")
        self._manager = MementoManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute memento action."""
        try:
            operation = params.get("operation", "save")

            if operation == "save":
                return self._save(params)
            elif operation == "restore":
                return self._restore(params)
            elif operation == "latest":
                return self._latest(params)
            elif operation == "history":
                return self._history(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _save(self, params: Dict) -> ActionResult:
        """Save memento."""
        memento_id = self._manager.save(
            originator_id=params.get("originator_id", ""),
            state=params.get("state", {}),
            label=params.get("label")
        )
        return ActionResult(success=True, data={"memento_id": memento_id})

    def _restore(self, params: Dict) -> ActionResult:
        """Restore memento."""
        state = self._manager.restore(
            originator_id=params.get("originator_id", ""),
            memento_id=params.get("memento_id", "")
        )
        if state is None:
            return ActionResult(success=False, message="Memento not found")
        return ActionResult(success=True, data={"state": state})

    def _latest(self, params: Dict) -> ActionResult:
        """Get latest memento."""
        state = self._manager.get_latest(params.get("originator_id", ""))
        if state is None:
            return ActionResult(success=False, message="No mementos found")
        return ActionResult(success=True, data={"state": state})

    def _history(self, params: Dict) -> ActionResult:
        """Get history."""
        history = self._manager.get_history(params.get("originator_id", ""))
        return ActionResult(success=True, data={"history": history})
