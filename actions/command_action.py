"""Command Action Module.

Provides command pattern implementation for
request/command handling.
"""

import time
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Command:
    """Command definition."""
    command_id: str
    name: str
    handler: Callable
    params_schema: Dict = field(default_factory=dict)


class CommandManager:
    """Manages commands."""

    def __init__(self):
        self._commands: Dict[str, Command] = {}

    def register(
        self,
        name: str,
        handler: Callable,
        params_schema: Optional[Dict] = None
    ) -> str:
        """Register a command."""
        command_id = f"cmd_{name.lower().replace(' ', '_')}"

        command = Command(
            command_id=command_id,
            name=name,
            handler=handler,
            params_schema=params_schema or {}
        )

        self._commands[command_id] = command
        return command_id

    def execute(self, command_id: str, params: Optional[Dict] = None) -> Any:
        """Execute a command."""
        command = self._commands.get(command_id)
        if not command:
            raise ValueError(f"Command not found: {command_id}")

        return command.handler(params or {})

    def list_commands(self) -> list:
        """List all commands."""
        return [
            {
                "command_id": c.command_id,
                "name": c.name,
                "params_schema": c.params_schema
            }
            for c in self._commands.values()
        ]


class CommandAction(BaseAction):
    """Action for command operations."""

    def __init__(self):
        super().__init__("command")
        self._manager = CommandManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute command action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register command."""
        command_id = self._manager.register(
            name=params.get("name", ""),
            handler=params.get("handler") or (lambda p: {}),
            params_schema=params.get("params_schema")
        )
        return ActionResult(success=True, data={"command_id": command_id})

    def _execute(self, params: Dict) -> ActionResult:
        """Execute command."""
        result = self._manager.execute(
            params.get("command_id", ""),
            params.get("params")
        )
        return ActionResult(success=True, data={"result": result})

    def _list(self, params: Dict) -> ActionResult:
        """List commands."""
        commands = self._manager.list_commands()
        return ActionResult(success=True, data={"commands": commands})
