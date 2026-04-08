"""Prototype Pattern Action Module.

Provides prototype pattern for object
cloning.
"""

import time
import copy
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Prototype:
    """Prototype object."""
    prototype_id: str
    name: str
    data: Dict = field(default_factory=dict)


class PrototypeRegistry:
    """Registry for prototypes."""

    def __init__(self):
        self._prototypes: Dict[str, Prototype] = {}

    def register(self, prototype: Prototype) -> None:
        """Register a prototype."""
        self._prototypes[prototype.prototype_id] = prototype

    def get(self, prototype_id: str) -> Optional[Prototype]:
        """Get prototype."""
        return self._prototypes.get(prototype_id)

    def clone(self, prototype_id: str, deep: bool = True) -> Optional[Dict]:
        """Clone a prototype."""
        prototype = self._prototypes.get(prototype_id)
        if not prototype:
            return None

        if deep:
            return copy.deepcopy(prototype.data)
        return copy.copy(prototype.data)

    def list_prototypes(self) -> List[str]:
        """List all prototype IDs."""
        return list(self._prototypes.keys())


class PrototypePatternAction(BaseAction):
    """Action for prototype pattern operations."""

    def __init__(self):
        super().__init__("prototype")
        self._registry = PrototypeRegistry()

    def execute(self, params: Dict) -> ActionResult:
        """Execute prototype action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "clone":
                return self._clone(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register prototype."""
        prototype_id = params.get("prototype_id", "")
        prototype = Prototype(
            prototype_id=prototype_id,
            name=params.get("name", ""),
            data=params.get("data", {})
        )
        self._registry.register(prototype)
        return ActionResult(success=True, data={"prototype_id": prototype_id})

    def _clone(self, params: Dict) -> ActionResult:
        """Clone prototype."""
        data = self._registry.clone(
            params.get("prototype_id", ""),
            params.get("deep", True)
        )
        if data is None:
            return ActionResult(success=False, message="Prototype not found")
        return ActionResult(success=True, data={"cloned_data": data})

    def _list(self, params: Dict) -> ActionResult:
        """List prototypes."""
        return ActionResult(success=True, data={
            "prototypes": self._registry.list_prototypes()
        })
