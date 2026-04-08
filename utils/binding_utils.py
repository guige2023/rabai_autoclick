"""
Binding Utilities

Provides key binding management and hotkey registration
for UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Any
from enum import Enum, auto


class BindingScope(Enum):
    """Scope of key binding."""
    GLOBAL = auto()
    APPLICATION = auto()
    ELEMENT = auto()


@dataclass
class KeyBinding:
    """Represents a key binding configuration."""
    keys: str
    handler: Callable[..., Any]
    scope: BindingScope = BindingScope.GLOBAL
    description: str = ""
    enabled: bool = True
    context: str | None = None


class BindingRegistry:
    """
    Registry for managing key bindings.
    
    Allows registration, lookup, and handling of
    keyboard shortcuts and hotkeys.
    """

    def __init__(self) -> None:
        self._bindings: dict[str, KeyBinding] = {}
        self._context_bindings: dict[str, dict[str, KeyBinding]] = {}

    def register(
        self,
        keys: str,
        handler: Callable[..., Any],
        scope: BindingScope = BindingScope.GLOBAL,
        description: str = "",
        context: str | None = None,
    ) -> None:
        """
        Register a key binding.
        
        Args:
            keys: Key combination string (e.g., "ctrl+c", "cmd+shift+s").
            handler: Function to call when binding is triggered.
            scope: Binding scope.
            description: Description of the binding.
            context: Optional context identifier.
        """
        binding = KeyBinding(
            keys=keys,
            handler=handler,
            scope=scope,
            description=description,
            context=context,
        )
        self._bindings[keys] = binding
        if context:
            if context not in self._context_bindings:
                self._context_bindings[context] = {}
            self._context_bindings[context][keys] = binding

    def unregister(self, keys: str) -> bool:
        """Unregister a key binding."""
        if keys in self._bindings:
            binding = self._bindings[keys]
            if binding.context and binding.context in self._context_bindings:
                del self._context_bindings[binding.context][keys]
            del self._bindings[keys]
            return True
        return False

    def get_binding(self, keys: str) -> KeyBinding | None:
        """Get a binding by key combination."""
        return self._bindings.get(keys)

    def get_bindings_for_context(
        self,
        context: str
    ) -> list[KeyBinding]:
        """Get all bindings for a specific context."""
        if context in self._context_bindings:
            return list(self._context_bindings[context].values())
        return []

    def enable(self, keys: str) -> bool:
        """Enable a binding."""
        if keys in self._bindings:
            self._bindings[keys].enabled = True
            return True
        return False

    def disable(self, keys: str) -> bool:
        """Disable a binding."""
        if keys in self._bindings:
            self._bindings[keys].enabled = False
            return True
        return False

    def trigger(self, keys: str) -> Any | None:
        """Trigger a binding handler."""
        binding = self._bindings.get(keys)
        if binding and binding.enabled:
            return binding.handler()
        return None

    def list_bindings(
        self,
        scope: BindingScope | None = None
    ) -> list[KeyBinding]:
        """List all registered bindings."""
        bindings = self._bindings.values()
        if scope:
            bindings = [b for b in bindings if b.scope == scope]
        return list(bindings)


def normalize_keys(keys: str) -> str:
    """
    Normalize key combination string.
    
    Args:
        keys: Raw key combination string.
        
    Returns:
        Normalized key combination.
    """
    parts = keys.lower().replace(" ", "").split("+")
    return "+".join(sorted(parts))
