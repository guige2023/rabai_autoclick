"""
Keyboard Shortcut Manager Action Module.

Manages keyboard shortcuts for UI automation, supporting
hotkey registration, conflict detection, and sequence-based
shortcuts (chorded key combinations).
"""

import re
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ShortcutBinding:
    """A registered keyboard shortcut binding."""
    keys: str
    handler: Callable[[], None]
    description: str = ""
    enabled: bool = True
    once: bool = False


class KeyboardShortcutManager:
    """Manages keyboard shortcuts for automation."""

    MODIFIER_KEYS = {"ctrl", "control", "alt", "shift", "meta", "cmd", "command", "super"}

    def __init__(self):
        """Initialize shortcut manager."""
        self._bindings: dict[str, ShortcutBinding] = {}
        self._sequence_buffer: list[str] = []
        self._sequence_timeout: float = 1.0
        self._last_key_time: float = 0.0

    def register(
        self,
        keys: str,
        handler: Callable[[], None],
        description: str = "",
        once: bool = False,
    ) -> bool:
        """
        Register a keyboard shortcut.

        Args:
            keys: Key combination string (e.g., "ctrl+c", "ctrl+shift+s").
            handler: Function to call when shortcut is triggered.
            description: Shortcut description.
            once: If True, auto-unregister after first trigger.

        Returns:
            True if registered, False if keys conflict.
        """
        normalized = self._normalize_keys(keys)
        if normalized in self._bindings:
            return False

        binding = ShortcutBinding(
            keys=normalized,
            handler=handler,
            description=description,
            once=once,
        )
        self._bindings[normalized] = binding
        return True

    def unregister(self, keys: str) -> bool:
        """
        Unregister a shortcut.

        Args:
            keys: Key combination string.

        Returns:
            True if unregistered, False if not found.
        """
        normalized = self._normalize_keys(keys)
        if normalized in self._bindings:
            del self._bindings[normalized]
            return True
        return False

    def trigger(self, keys: str) -> bool:
        """
        Trigger handler for a key combination.

        Args:
            keys: Key combination string.

        Returns:
            True if a handler was triggered, False otherwise.
        """
        normalized = self._normalize_keys(keys)

        if normalized in self._bindings:
            binding = self._bindings[normalized]
            if binding.enabled:
                binding.handler()
                if binding.once:
                    del self._bindings[normalized]
                return True
        return False

    def enable(self, keys: str) -> bool:
        """
        Enable a shortcut.

        Args:
            keys: Key combination string.

        Returns:
            True if enabled, False if not found.
        """
        normalized = self._normalize_keys(keys)
        if normalized in self._bindings:
            self._bindings[normalized].enabled = True
            return True
        return False

    def disable(self, keys: str) -> bool:
        """
        Disable a shortcut.

        Args:
            keys: Key combination string.

        Returns:
            True if disabled, False if not found.
        """
        normalized = self._normalize_keys(keys)
        if normalized in self._bindings:
            self._bindings[normalized].enabled = False
            return True
        return False

    def get_bindings(self) -> list[ShortcutBinding]:
        """
        Get all registered bindings.

        Returns:
            List of ShortcutBinding objects.
        """
        return list(self._bindings.values())

    def find_conflicts(self, keys: str) -> list[str]:
        """
        Find conflicting key bindings.

        Args:
            keys: Key combination to check.

        Returns:
            List of conflicting key strings.
        """
        normalized = self._normalize_keys(keys)
        conflicts = []

        for bound_keys in self._bindings:
            if self._keys_overlap(normalized, bound_keys):
                conflicts.append(bound_keys)

        return conflicts

    def _normalize_keys(self, keys: str) -> str:
        """Normalize key combination string."""
        parts = keys.lower().replace("+", " ").split()
        normalized = []

        for part in parts:
            part = part.strip()
            if part in {"cmd", "command"}:
                part = "meta"
            elif part in {"control"}:
                part = "ctrl"
            normalized.append(part)

        normalized.sort(key=lambda k: (0 if k in self.MODIFIER_KEYS else 1, k))
        return "+".join(normalized)

    @staticmethod
    def _keys_overlap(keys1: str, keys2: str) -> bool:
        """Check if two key combinations could overlap."""
        set1 = set(keys1.split("+"))
        set2 = set(keys2.split("+"))
        modifiers1 = set1 & KeyboardShortcutManager.MODIFIER_KEYS
        modifiers2 = set2 & KeyboardShortcutManager.MODIFIER_KEYS
        return modifiers1 == modifiers2
