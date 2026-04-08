"""Chain of Responsibility Action Module.

Provides chain of responsibility pattern for
request processing pipelines.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Handler:
    """Handler in the chain."""
    handler_id: str
    name: str
    handler_func: Callable
    next_handler: Optional['Handler'] = None


class ChainManager:
    """Manages handler chains."""

    def __init__(self):
        self._chains: Dict[str, Handler] = {}

    def create_chain(self, name: str) -> str:
        """Create a chain."""
        self._chains[name] = None
        return name

    def add_handler(
        self,
        chain_name: str,
        handler_name: str,
        handler_func: Callable
    ) -> bool:
        """Add handler to chain."""
        if chain_name not in self._chains:
            return False

        new_handler = Handler(
            handler_id=f"{chain_name}_{handler_name}",
            name=handler_name,
            handler_func=handler_func
        )

        if self._chains[chain_name] is None:
            self._chains[chain_name] = new_handler
        else:
            current = self._chains[chain_name]
            while current.next_handler:
                current = current.next_handler
            current.next_handler = new_handler

        return True

    def execute(self, chain_name: str, data: Any) -> Any:
        """Execute chain."""
        handler = self._chains.get(chain_name)
        if not handler:
            return None

        while handler:
            data = handler.handler_func(data)
            handler = handler.next_handler

        return data


class ChainOfResponsibilityAction(BaseAction):
    """Action for chain of responsibility operations."""

    def __init__(self):
        super().__init__("chain_of_responsibility")
        self._manager = ChainManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute chain action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_handler":
                return self._add_handler(params)
            elif operation == "execute":
                return self._execute(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create chain."""
        chain_name = self._manager.create_chain(params.get("name", ""))
        return ActionResult(success=True, data={"chain_name": chain_name})

    def _add_handler(self, params: Dict) -> ActionResult:
        """Add handler."""
        def default_handler(data):
            return data

        success = self._manager.add_handler(
            params.get("chain_name", ""),
            params.get("handler_name", ""),
            params.get("handler") or default_handler
        )
        return ActionResult(success=success)

    def _execute(self, params: Dict) -> ActionResult:
        """Execute chain."""
        result = self._manager.execute(
            params.get("chain_name", ""),
            params.get("data")
        )
        return ActionResult(success=True, data={"result": result})
