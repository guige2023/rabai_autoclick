"""Mediator Action Module.

Provides mediator pattern for coordinated
component communication.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Colleague:
    """Colleague in mediator."""
    colleague_id: str
    name: str


class MediatorManager:
    """Manages mediator pattern."""

    def __init__(self):
        self._colleagues: Dict[str, Colleague] = {}
        self._handlers: Dict[str, Callable] = {}

    def register_colleague(self, name: str) -> str:
        """Register a colleague."""
        colleague_id = f"col_{name.lower().replace(' ', '_')}"
        self._colleagues[colleague_id] = Colleague(
            colleague_id=colleague_id,
            name=name
        )
        return colleague_id

    def send_message(
        self,
        from_id: str,
        to_id: str,
        message: Any
    ) -> Any:
        """Send message between colleagues."""
        if from_id not in self._colleagues or to_id not in self._colleagues:
            return None

        handler = self._handlers.get(to_id)
        if handler:
            return handler(message)

        return None

    def broadcast(self, from_id: str, message: Any) -> int:
        """Broadcast message to all."""
        count = 0
        for colleague_id in self._colleagues:
            if colleague_id != from_id:
                result = self.send_message(from_id, colleague_id, message)
                if result is not None:
                    count += 1
        return count


class MediatorAction(BaseAction):
    """Action for mediator operations."""

    def __init__(self):
        super().__init__("mediator")
        self._manager = MediatorManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute mediator action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "send":
                return self._send(params)
            elif operation == "broadcast":
                return self._broadcast(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register colleague."""
        colleague_id = self._manager.register_colleague(params.get("name", ""))
        return ActionResult(success=True, data={"colleague_id": colleague_id})

    def _send(self, params: Dict) -> ActionResult:
        """Send message."""
        result = self._manager.send_message(
            params.get("from_id", ""),
            params.get("to_id", ""),
            params.get("message")
        )
        return ActionResult(success=True, data={"result": result})

    def _broadcast(self, params: Dict) -> ActionResult:
        """Broadcast message."""
        count = self._manager.broadcast(
            params.get("from_id", ""),
            params.get("message")
        )
        return ActionResult(success=True, data={"count": count})
