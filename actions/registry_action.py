"""Registry Pattern Action Module.

Provides registry for centralized
service/component registration.
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
class RegistryEntry:
    """Registry entry."""
    key: str
    value: Any
    metadata: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


class Registry:
    """Service registry."""
    def __init__(self, registry_id: str):
        self.registry_id = registry_id
        self._entries: Dict[str, RegistryEntry] = {}
        self._lock = threading.RLock()

    def register(
        self,
        key: str,
        value: Any,
        metadata: Optional[Dict] = None
    ) -> None:
        """Register a service."""
        with self._lock:
            self._entries[key] = RegistryEntry(
                key=key,
                value=value,
                metadata=metadata or {}
            )

    def get(self, key: str) -> Optional[Any]:
        """Get registered value."""
        entry = self._entries.get(key)
        return entry.value if entry else None

    def unregister(self, key: str) -> bool:
        """Unregister a service."""
        with self._lock:
            if key in self._entries:
                del self._entries[key]
                return True
        return False

    def list_services(self) -> List[str]:
        """List all registered services."""
        with self._lock:
            return list(self._entries.keys())


class RegistryManager:
    """Manages registries."""

    def __init__(self):
        self._registries: Dict[str, Registry] = {}

    def create_registry(self, registry_id: str) -> str:
        """Create a registry."""
        self._registries[registry_id] = Registry(registry_id)
        return registry_id

    def get_registry(self, registry_id: str) -> Optional[Registry]:
        """Get registry."""
        return self._registries.get(registry_id)


class RegistryPatternAction(BaseAction):
    """Action for registry pattern operations."""

    def __init__(self):
        super().__init__("registry")
        self._manager = RegistryManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute registry action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "register":
                return self._register(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "unregister":
                return self._unregister(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create registry."""
        registry_id = self._manager.create_registry(params.get("registry_id", ""))
        return ActionResult(success=True, data={"registry_id": registry_id})

    def _register(self, params: Dict) -> ActionResult:
        """Register service."""
        registry = self._manager.get_registry(params.get("registry_id", ""))
        if not registry:
            return ActionResult(success=False, message="Registry not found")

        registry.register(
            params.get("key", ""),
            params.get("value"),
            params.get("metadata")
        )
        return ActionResult(success=True)

    def _get(self, params: Dict) -> ActionResult:
        """Get service."""
        registry = self._manager.get_registry(params.get("registry_id", ""))
        if not registry:
            return ActionResult(success=False, message="Registry not found")

        value = registry.get(params.get("key", ""))
        return ActionResult(success=value is not None, data={"value": value})

    def _unregister(self, params: Dict) -> ActionResult:
        """Unregister service."""
        registry = self._manager.get_registry(params.get("registry_id", ""))
        if not registry:
            return ActionResult(success=False, message="Registry not found")

        success = registry.unregister(params.get("key", ""))
        return ActionResult(success=success)

    def _list(self, params: Dict) -> ActionResult:
        """List services."""
        registry = self._manager.get_registry(params.get("registry_id", ""))
        if not registry:
            return ActionResult(success=False, message="Registry not found")

        return ActionResult(success=True, data={
            "services": registry.list_services()
        })
