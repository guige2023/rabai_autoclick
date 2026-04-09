"""
Hotkey Action Module.

Manages global hotkeys for triggering automation actions,
supporting key chords, modifier combinations, and action bindings.
"""

import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class HotkeyBinding:
    """A registered hotkey binding."""
    key_combo: str
    handler: Callable[[], None]
    description: str = ""
    enabled: bool = True


class HotkeyManager:
    """Manages global hotkey bindings."""

    def __init__(self):
        """Initialize hotkey manager."""
        self._bindings: dict[str, HotkeyBinding] = {}
        self._last_trigger: dict[str, float] = {}

    def register(
        self,
        key_combo: str,
        handler: Callable[[], None],
        description: str = "",
    ) -> bool:
        """
        Register a hotkey.

        Args:
            key_combo: Key combination string (e.g., "ctrl+shift+a").
            handler: Function to call on trigger.
            description: Hotkey description.

        Returns:
            True if registered, False if already exists.
        """
        normalized = self._normalize_combo(key_combo)
        if normalized in self._bindings:
            return False

        binding = HotkeyBinding(
            key_combo=normalized,
            handler=handler,
            description=description,
        )
        self._bindings[normalized] = binding
        return True

    def unregister(self, key_combo: str) -> bool:
        """
        Unregister a hotkey.

        Args:
            key_combo: Key combination string.

        Returns:
            True if unregistered, False if not found.
        """
        normalized = self._normalize_combo(key_combo)
        if normalized in self._bindings:
            del self._bindings[normalized]
            return True
        return False

    def trigger(self, key_combo: str) -> bool:
        """
        Trigger the handler for a hotkey.

        Args:
            key_combo: Key combination string.

        Returns:
            True if triggered, False if not found or disabled.
        """
        normalized = self._normalize_combo(key_combo)
        binding = self._bindings.get(normalized)

        if binding is None or not binding.enabled:
            return False

        binding.handler()
        self._last_trigger[normalized] = time.time()
        return True

    def enable(self, key_combo: str) -> bool:
        """Enable a hotkey."""
        normalized = self._normalize_combo(key_combo)
        if normalized in self._bindings:
            self._bindings[normalized].enabled = True
            return True
        return False

    def disable(self, key_combo: str) -> bool:
        """Disable a hotkey."""
        normalized = self._normalize_combo(key_combo)
        if normalized in self._bindings:
            self._bindings[normalized].enabled = False
            return True
        return False

    def get_bindings(self) -> list[HotkeyBinding]:
        """Get all registered bindings."""
        return list(self._bindings.values())

    def get_last_trigger(self, key_combo: str) -> Optional[float]:
        """Get timestamp of last trigger for a hotkey."""
        normalized = self._normalize_combo(key_combo)
        return self._last_trigger.get(normalized)

    @staticmethod
    def _normalize_combo(combo: str) -> str:
        """Normalize a key combination string."""
        parts = [p.strip().lower() for p in combo.replace("+", " ").split()]
        parts.sort()
        return "+".join(parts)
