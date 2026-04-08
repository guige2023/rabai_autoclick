"""Write Ahead Log Action Module.

Provides WAL pattern for data
integrity.
"""

import time
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WALOperation(Enum):
    """WAL operation type."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class WALEntry:
    """WAL entry."""
    entry_id: str
    operation: WALOperation
    key: str
    value: Any
    timestamp: float = field(default_factory=time.time)
    applied: bool = False


class WriteAheadLogManager:
    """Manages write-ahead log."""

    def __init__(self):
        self._log: List[WALEntry] = []
        self._data: Dict[str, Any] = {}

    def append(
        self,
        operation: WALOperation,
        key: str,
        value: Any
    ) -> str:
        """Append entry to WAL."""
        entry_id = f"wal_{int(time.time() * 1000)}"

        entry = WALEntry(
            entry_id=entry_id,
            operation=operation,
            key=key,
            value=value
        )

        self._log.append(entry)
        return entry_id

    def apply_entry(self, entry_id: str) -> bool:
        """Apply WAL entry to data."""
        for entry in self._log:
            if entry.entry_id != entry_id:
                continue

            if entry.applied:
                return False

            if entry.operation == WALOperation.INSERT:
                self._data[entry.key] = entry.value

            elif entry.operation == WALOperation.UPDATE:
                self._data[entry.key] = entry.value

            elif entry.operation == WALOperation.DELETE:
                if entry.key in self._data:
                    del self._data[entry.key]

            entry.applied = True
            return True

        return False

    def replay(self) -> int:
        """Replay all unapplied entries."""
        applied = 0
        for entry in self._log:
            if not entry.applied:
                self.apply_entry(entry.entry_id)
                applied += 1
        return applied

    def checkpoint(self) -> int:
        """Remove applied entries."""
        original_len = len(self._log)
        self._log = [e for e in self._log if not e.applied]
        return original_len - len(self._log)

    def get_log(self, limit: int = 100) -> List[Dict]:
        """Get log entries."""
        entries = self._log[-limit:]
        return [
            {
                "entry_id": e.entry_id,
                "operation": e.operation.value,
                "key": e.key,
                "applied": e.applied
            }
            for e in entries
        ]


class WriteAheadLogAction(BaseAction):
    """Action for WAL operations."""

    def __init__(self):
        super().__init__("write_ahead_log")
        self._manager = WriteAheadLogManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute WAL action."""
        try:
            operation = params.get("operation", "append")

            if operation == "append":
                return self._append(params)
            elif operation == "apply":
                return self._apply(params)
            elif operation == "replay":
                return self._replay(params)
            elif operation == "checkpoint":
                return self._checkpoint(params)
            elif operation == "log":
                return self._log(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _append(self, params: Dict) -> ActionResult:
        """Append entry."""
        entry_id = self._manager.append(
            operation=WALOperation(params.get("operation", "insert")),
            key=params.get("key", ""),
            value=params.get("value")
        )
        return ActionResult(success=True, data={"entry_id": entry_id})

    def _apply(self, params: Dict) -> ActionResult:
        """Apply entry."""
        success = self._manager.apply_entry(params.get("entry_id", ""))
        return ActionResult(success=success)

    def _replay(self, params: Dict) -> ActionResult:
        """Replay entries."""
        count = self._manager.replay()
        return ActionResult(success=True, data={"applied": count})

    def _checkpoint(self, params: Dict) -> ActionResult:
        """Checkpoint."""
        removed = self._manager.checkpoint()
        return ActionResult(success=True, data={"removed": removed})

    def _log(self, params: Dict) -> ActionResult:
        """Get log."""
        log = self._manager.get_log(params.get("limit", 100))
        return ActionResult(success=True, data={"log": log})
