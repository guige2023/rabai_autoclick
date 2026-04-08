"""UI hotkey manager for keyboard shortcuts in automation.

Manages global hotkey registration and handling for
triggering automation actions.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class HotkeyBinding:
    """A hotkey binding.

    Attributes:
        hotkey_id: Unique identifier.
        key: The key name (e.g., 'f5', 'ctrl+shift+a').
        callback: Function to call when hotkey is triggered.
        description: Human-readable description.
        is_active: Whether this binding is currently active.
    """
    key: str
    callback: Callable[[], Any]
    hotkey_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    description: str = ""
    is_active: bool = True

    def matches(self, key: str) -> bool:
        """Check if a key string matches this binding."""
        return self.key.lower() == key.lower()

    def parse_key(self) -> tuple[set[str], str]:
        """Parse key into (modifiers, main_key)."""
        parts = self.key.lower().split("+")
        modifiers = set(parts[:-1]) if len(parts) > 1 else set()
        main_key = parts[-1] if parts else ""
        return modifiers, main_key


class UIHotkeyManager:
    """Manages global hotkey bindings for automation.

    Provides registration, unregistration, and handling of
    global keyboard shortcuts.
    """

    def __init__(self) -> None:
        """Initialize empty hotkey manager."""
        self._bindings: dict[str, HotkeyBinding] = {}
        self._on_hotkey_callbacks: list[Callable[[str], None]] = []

    def register(
        self,
        key: str,
        callback: Callable[[], Any],
        description: str = "",
    ) -> str:
        """Register a hotkey binding.

        Returns the binding ID.
        """
        binding = HotkeyBinding(
            key=key,
            callback=callback,
            description=description,
        )
        self._bindings[binding.hotkey_id] = binding
        return binding.hotkey_id

    def unregister(self, hotkey_id: str) -> bool:
        """Remove a hotkey binding. Returns True if found."""
        if hotkey_id in self._bindings:
            del self._bindings[hotkey_id]
            return True
        return False

    def unregister_by_key(self, key: str) -> bool:
        """Remove a binding by key string. Returns True if found."""
        for hotkey_id, binding in list(self._bindings.items()):
            if binding.matches(key):
                del self._bindings[hotkey_id]
                return True
        return False

    def get_binding(self, hotkey_id: str) -> Optional[HotkeyBinding]:
        """Get a binding by ID."""
        return self._bindings.get(hotkey_id)

    def get_bindings_for_key(self, key: str) -> list[HotkeyBinding]:
        """Get all bindings that match a key string."""
        return [
            b for b in self._bindings.values()
            if b.matches(key) and b.is_active
        ]

    def handle_key(self, key: str) -> int:
        """Handle a key event.

        Calls all matching active bindings.
        Returns number of bindings triggered.
        """
        triggered = 0
        for binding in self._bindings.values():
            if binding.is_active and binding.matches(key):
                try:
                    binding.callback()
                    triggered += 1
                    self._notify_hotkey(key)
                except Exception:
                    pass
        return triggered

    def enable(self, hotkey_id: str) -> bool:
        """Enable a binding."""
        binding = self._bindings.get(hotkey_id)
        if binding:
            binding.is_active = True
            return True
        return False

    def disable(self, hotkey_id: str) -> bool:
        """Disable a binding."""
        binding = self._bindings.get(hotkey_id)
        if binding:
            binding.is_active = False
            return True
        return False

    def disable_all(self) -> None:
        """Disable all bindings."""
        for binding in self._bindings.values():
            binding.is_active = False

    def enable_all(self) -> None:
        """Enable all bindings."""
        for binding in self._bindings.values():
            binding.is_active = True

    def on_hotkey(self, callback: Callable[[str], None]) -> None:
        """Register a callback for any hotkey trigger."""
        self._on_hotkey_callbacks.append(callback)

    def _notify_hotkey(self, key: str) -> None:
        """Notify hotkey callbacks."""
        for cb in self._on_hotkey_callbacks:
            try:
                cb(key)
            except Exception:
                pass

    @property
    def binding_count(self) -> int:
        """Return number of registered bindings."""
        return len(self._bindings)

    @property
    def active_count(self) -> int:
        """Return number of active bindings."""
        return sum(1 for b in self._bindings.values() if b.is_active)

    @property
    def all_bindings(self) -> list[HotkeyBinding]:
        """Return all bindings."""
        return list(self._bindings.values())


# Convenience parser
def parse_hotkey(key_string: str) -> tuple[set[str], str]:
    """Parse a hotkey string into (modifiers, main_key)."""
    parts = key_string.lower().split("+")
    modifiers = set(parts[:-1]) if len(parts) > 1 else set()
    main_key = parts[-1] if parts else ""
    return modifiers, main_key


def format_hotkey(modifiers: set[str], key: str) -> str:
    """Format modifiers and key into a hotkey string."""
    all_parts = sorted(modifiers) + [key]
    return "+".join(all_parts)
