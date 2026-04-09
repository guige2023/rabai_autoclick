"""
UI Hotkey Context Manager Utilities

Manage keyboard shortcut contexts in a UI automation environment,
supporting context switching when windows or panels change.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, List, Set


@dataclass
class HotkeyBinding:
    """A keyboard shortcut binding."""
    key_combo: str  # e.g., 'cmd+c', 'ctrl+shift+a'
    action: Callable[[], None]
    description: str = ""
    context: str = "global"  # 'global' or context name
    priority: int = 0


@dataclass
class HotkeyContext:
    """A named keyboard shortcut context with its bindings."""
    name: str
    bindings: Dict[str, HotkeyBinding] = field(default_factory=dict)
    is_active: bool = False
    activated_at_ms: float = 0.0


class UIHotkeyContextManager:
    """
    Manage keyboard shortcut contexts that change based on
    the active window, panel, or UI state.
    """

    def __init__(self):
        self._contexts: Dict[str, HotkeyContext] = {}
        self._active_contexts: List[str] = []  # stack, most recent last
        self._global_bindings: Dict[str, HotkeyBinding] = {}

    def create_context(self, name: str) -> HotkeyContext:
        """Create a new hotkey context."""
        ctx = HotkeyContext(name=name)
        self._contexts[name] = ctx
        return ctx

    def register_binding(
        self,
        context_name: str,
        key_combo: str,
        action: Callable[[], None],
        description: str = "",
        priority: int = 0,
    ) -> None:
        """Register a hotkey binding in a specific context."""
        ctx = self._contexts.get(context_name)
        if ctx is None:
            ctx = self.create_context(context_name)

        binding = HotkeyBinding(
            key_combo=key_combo,
            action=action,
            description=description,
            context=context_name,
            priority=priority,
        )
        ctx.bindings[key_combo] = binding

    def register_global(
        self,
        key_combo: str,
        action: Callable[[], None],
        description: str = "",
    ) -> None:
        """Register a global hotkey binding (active in all contexts)."""
        binding = HotkeyBinding(
            key_combo=key_combo,
            action=action,
            description=description,
            context="global",
        )
        self._global_bindings[key_combo] = binding

    def activate_context(self, context_name: str) -> bool:
        """Activate a context, pushing it on top of the context stack."""
        ctx = self._contexts.get(context_name)
        if not ctx:
            return False

        if context_name in self._active_contexts:
            self._active_contexts.remove(context_name)

        self._active_contexts.append(context_name)
        ctx.is_active = True
        ctx.activated_at_ms = time.time() * 1000
        return True

    def deactivate_context(self, context_name: str) -> bool:
        """Deactivate a context, removing it from the stack."""
        if context_name not in self._active_contexts:
            return False
        self._active_contexts.remove(context_name)
        self._contexts[context_name].is_active = False
        return True

    def handle_hotkey(self, key_combo: str) -> bool:
        """
        Handle a hotkey event, dispatching to the appropriate action.

        Returns True if the hotkey was handled.
        """
        # Check active contexts in reverse order (top of stack first)
        for ctx_name in reversed(self._active_contexts):
            ctx = self._contexts.get(ctx_name)
            if ctx and key_combo in ctx.bindings:
                binding = ctx.bindings[key_combo]
                binding.action()
                return True

        # Fall back to global bindings
        if key_combo in self._global_bindings:
            self._global_bindings[key_combo].action()
            return True

        return False

    def get_active_bindings(self) -> List[HotkeyBinding]:
        """Get all currently active bindings."""
        bindings = list(self._global_bindings.values())
        for ctx_name in self._active_contexts:
            ctx = self._contexts.get(ctx_name)
            if ctx:
                bindings.extend(ctx.bindings.values())
        return bindings
