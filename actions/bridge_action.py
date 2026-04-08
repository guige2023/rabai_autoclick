"""Bridge Pattern Action Module.

Provides bridge pattern for abstraction
and implementation separation.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Implementation:
    """Implementation interface."""
    impl_id: str
    name: str
    implement: Callable


class Abstraction:
    """Abstraction layer."""
    def __init__(self, abstraction_id: str, name: str):
        self.abstraction_id = abstraction_id
        self.name = name
        self._implementation: Optional[Implementation] = None

    def set_implementation(self, impl: Implementation) -> None:
        """Set implementation."""
        self._implementation = impl

    def operation(self, *args, **kwargs) -> Any:
        """Execute operation through implementation."""
        if not self._implementation:
            raise RuntimeError("No implementation set")
        return self._implementation.implement(*args, **kwargs)


class BridgeManager:
    """Manages bridge pattern."""

    def __init__(self):
        self._abstractions: Dict[str, Abstraction] = {}
        self._implementations: Dict[str, Implementation] = {}

    def create_abstraction(self, name: str) -> str:
        """Create abstraction."""
        abstraction_id = f"abs_{name.lower().replace(' ', '_')}"
        self._abstractions[abstraction_id] = Abstraction(abstraction_id, name)
        return abstraction_id

    def create_implementation(
        self,
        name: str,
        implement: Callable
    ) -> str:
        """Create implementation."""
        impl_id = f"impl_{name.lower().replace(' ', '_')}"
        self._implementations[impl_id] = Implementation(impl_id, name, implement)
        return impl_id

    def set_implementation(
        self,
        abstraction_id: str,
        impl_id: str
    ) -> bool:
        """Set implementation for abstraction."""
        abstraction = self._abstractions.get(abstraction_id)
        implementation = self._implementations.get(impl_id)

        if not abstraction or not implementation:
            return False

        abstraction.set_implementation(implementation)
        return True

    def get_abstraction(self, abstraction_id: str) -> Optional[Abstraction]:
        """Get abstraction."""
        return self._abstractions.get(abstraction_id)


class BridgePatternAction(BaseAction):
    """Action for bridge pattern operations."""

    def __init__(self):
        super().__init__("bridge")
        self._manager = BridgeManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute bridge action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create_abstraction":
                return self._create_abstraction(params)
            elif operation == "create_implementation":
                return self._create_implementation(params)
            elif operation == "set_implementation":
                return self._set_implementation(params)
            elif operation == "operate":
                return self._operate(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_abstraction(self, params: Dict) -> ActionResult:
        """Create abstraction."""
        abstraction_id = self._manager.create_abstraction(params.get("name", ""))
        return ActionResult(success=True, data={"abstraction_id": abstraction_id})

    def _create_implementation(self, params: Dict) -> ActionResult:
        """Create implementation."""
        def default_impl(*args, **kwargs):
            return {}

        impl_id = self._manager.create_implementation(
            params.get("name", ""),
            params.get("implement") or default_impl
        )
        return ActionResult(success=True, data={"impl_id": impl_id})

    def _set_implementation(self, params: Dict) -> ActionResult:
        """Set implementation."""
        success = self._manager.set_implementation(
            params.get("abstraction_id", ""),
            params.get("impl_id", "")
        )
        return ActionResult(success=success)

    def _operate(self, params: Dict) -> ActionResult:
        """Operate."""
        abstraction = self._manager.get_abstraction(params.get("abstraction_id", ""))
        if not abstraction:
            return ActionResult(success=False, message="Abstraction not found")

        result = abstraction.operation()
        return ActionResult(success=True, data={"result": result})
