"""CQRS Action Module.

Provides Command Query Responsibility
Segregation pattern.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Command:
    """Command definition."""
    command_id: str
    command_type: str
    payload: Dict
    timestamp: float = field(default_factory=time.time)


@dataclass
class Query:
    """Query definition."""
    query_id: str
    query_type: str
    params: Dict


class CQRSManager:
    """Manages CQRS pattern."""

    def __init__(self):
        self._command_handlers: Dict[str, Callable] = {}
        self._query_handlers: Dict[str, Callable] = {}
        self._event_log: List[Command] = []
        self._lock = threading.RLock()

    def register_command(
        self,
        command_type: str,
        handler: Callable
    ) -> None:
        """Register command handler."""
        self._command_handlers[command_type] = handler

    def register_query(
        self,
        query_type: str,
        handler: Callable
    ) -> None:
        """Register query handler."""
        self._query_handlers[query_type] = handler

    def execute_command(
        self,
        command_type: str,
        payload: Dict
    ) -> Any:
        """Execute a command."""
        handler = self._command_handlers.get(command_type)
        if not handler:
            raise ValueError(f"Unknown command type: {command_type}")

        command = Command(
            command_id=f"cmd_{int(time.time() * 1000)}",
            command_type=command_type,
            payload=payload
        )

        with self._lock:
            self._event_log.append(command)

        return handler(payload)

    def execute_query(
        self,
        query_type: str,
        params: Dict
    ) -> Any:
        """Execute a query."""
        handler = self._query_handlers.get(query_type)
        if not handler:
            raise ValueError(f"Unknown query type: {query_type}")

        return handler(params)

    def get_event_log(self, limit: int = 100) -> List[Dict]:
        """Get event log."""
        with self._lock:
            events = self._event_log[-limit:]
            return [
                {
                    "command_id": e.command_id,
                    "command_type": e.command_type,
                    "timestamp": e.timestamp
                }
                for e in events
            ]


class CQRSAction(BaseAction):
    """Action for CQRS operations."""

    def __init__(self):
        super().__init__("cqrs")
        self._manager = CQRSManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute CQRS action."""
        try:
            operation = params.get("operation", "register_command")

            if operation == "register_command":
                return self._register_command(params)
            elif operation == "register_query":
                return self._register_query(params)
            elif operation == "execute_command":
                return self._execute_command(params)
            elif operation == "execute_query":
                return self._execute_query(params)
            elif operation == "event_log":
                return self._event_log(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register_command(self, params: Dict) -> ActionResult:
        """Register command."""
        self._manager.register_command(
            params.get("command_type", ""),
            params.get("handler") or (lambda p: {})
        )
        return ActionResult(success=True)

    def _register_query(self, params: Dict) -> ActionResult:
        """Register query."""
        self._manager.register_query(
            params.get("query_type", ""),
            params.get("handler") or (lambda p: {})
        )
        return ActionResult(success=True)

    def _execute_command(self, params: Dict) -> ActionResult:
        """Execute command."""
        try:
            result = self._manager.execute_command(
                params.get("command_type", ""),
                params.get("payload", {})
            )
            return ActionResult(success=True, data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _execute_query(self, params: Dict) -> ActionResult:
        """Execute query."""
        try:
            result = self._manager.execute_query(
                params.get("query_type", ""),
                params.get("params", {})
            )
            return ActionResult(success=True, data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _event_log(self, params: Dict) -> ActionResult:
        """Get event log."""
        log = self._manager.get_event_log(params.get("limit", 100))
        return ActionResult(success=True, data={"log": log})
