"""Adapter Pattern Action Module.

Provides adapter pattern for interface
compatibility.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Adapter:
    """Adapter implementation."""
    adapter_id: str
    name: str
    source_interface: str
    target_interface: str
    adapt_func: Callable


class AdapterManager:
    """Manages adapter pattern."""

    def __init__(self):
        self._adapters: Dict[str, Adapter] = {}

    def register(
        self,
        name: str,
        source_interface: str,
        target_interface: str,
        adapt_func: Callable
    ) -> str:
        """Register an adapter."""
        adapter_id = f"adapt_{name.lower().replace(' ', '_')}"

        adapter = Adapter(
            adapter_id=adapter_id,
            name=name,
            source_interface=source_interface,
            target_interface=target_interface,
            adapt_func=adapt_func
        )

        self._adapters[adapter_id] = adapter
        return adapter_id

    def adapt(self, adapter_id: str, data: Any) -> Any:
        """Adapt data through adapter."""
        adapter = self._adapters.get(adapter_id)
        if not adapter:
            raise ValueError(f"Adapter not found: {adapter_id}")

        return adapter.adapt_func(data)

    def get_adapter(self, adapter_id: str) -> Optional[Adapter]:
        """Get adapter by ID."""
        return self._adapters.get(adapter_id)


class AdapterPatternAction(BaseAction):
    """Action for adapter pattern operations."""

    def __init__(self):
        super().__init__("adapter")
        self._manager = AdapterManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute adapter action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "adapt":
                return self._adapt(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register adapter."""
        def default_adapt(data):
            return data

        adapter_id = self._manager.register(
            name=params.get("name", ""),
            source_interface=params.get("source_interface", ""),
            target_interface=params.get("target_interface", ""),
            adapt_func=params.get("adapt_func") or default_adapt
        )
        return ActionResult(success=True, data={"adapter_id": adapter_id})

    def _adapt(self, params: Dict) -> ActionResult:
        """Adapt data."""
        try:
            result = self._manager.adapt(
                params.get("adapter_id", ""),
                params.get("data")
            )
            return ActionResult(success=True, data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=str(e))
