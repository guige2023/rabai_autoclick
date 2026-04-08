"""Hotkey binding utilities.

This module provides utilities for managing hotkey bindings
and keyboard shortcuts.
"""

from __future__ import annotations

from typing import Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto


class ModifierKey(Enum):
    """Keyboard modifier keys."""
    CMD = "cmd"
    CTRL = "ctrl"
    ALT = "alt"
    SHIFT = "shift"
    META = "meta"


@dataclass
class HotkeyBinding:
    """A hotkey binding."""
    keys: Set[str]
    modifiers: Set[ModifierKey] = field(default_factory=set)
    action: Optional[Callable[[], None]] = None
    description: str = ""
    enabled: bool = True
    context: str = "global"


@dataclass
class HotkeyEvent:
    """A hotkey event."""
    keys: Set[str]
    modifiers: Set[ModifierKey]
    timestamp: float


class HotkeyRegistry:
    """Registry for hotkey bindings."""

    def __init__(self) -> None:
        self._bindings: Dict[str, HotkeyBinding] = {}
        self._on_execute: Optional[Callable[[HotkeyBinding], None]] = None

    def register(
        self,
        binding: HotkeyBinding,
        action: Optional[Callable[[], None]] = None,
    ) -> None:
        key = self._make_key(binding.keys, binding.modifiers)
        if action:
            binding.action = action
        self._bindings[key] = binding

    def unregister(self, keys: Set[str], modifiers: Optional[Set[ModifierKey]] = None) -> bool:
        key = self._make_key(keys, modifiers or set())
        if key in self._bindings:
            del self._bindings[key]
            return True
        return False

    def find_binding(
        self,
        keys: Set[str],
        modifiers: Optional[Set[ModifierKey]] = None,
    ) -> Optional[HotkeyBinding]:
        key = self._make_key(keys, modifiers or set())
        return self._bindings.get(key)

    def execute(
        self,
        keys: Set[str],
        modifiers: Optional[Set[ModifierKey]] = None,
    ) -> bool:
        binding = self.find_binding(keys, modifiers)
        if binding and binding.enabled and binding.action:
            binding.action()
            if self._on_execute:
                self._on_execute(binding)
            return True
        return False

    def set_enabled(self, keys: Set[str], enabled: bool, modifiers: Optional[Set[ModifierKey]] = None) -> bool:
        binding = self.find_binding(keys, modifiers)
        if binding:
            binding.enabled = enabled
            return True
        return False

    def all_bindings(self) -> List[HotkeyBinding]:
        return list(self._bindings.values())

    def bindings_for_context(self, context: str) -> List[HotkeyBinding]:
        return [b for b in self._bindings.values() if b.context == context]

    def on_execute(self, handler: Callable[[HotkeyBinding], None]) -> None:
        self._on_execute = handler

    @staticmethod
    def _make_key(keys: Set[str], modifiers: Set[ModifierKey]) -> str:
        sorted_keys = sorted(keys | {m.value for m in modifiers})
        return "+".join(sorted_keys)


def parse_hotkey_string(hotkey_str: str) -> Tuple[Set[str], Set[ModifierKey]]:
    """Parse a hotkey string like 'cmd+shift+a'.

    Args:
        hotkey_str: Hotkey string to parse.

    Returns:
        Tuple of (keys, modifiers).
    """
    parts = hotkey_str.lower().split("+")
    keys: Set[str] = set()
    modifiers: Set[ModifierKey] = set()

    for part in parts:
        part = part.strip()
        if part == "cmd":
            modifiers.add(ModifierKey.CMD)
        elif part == "ctrl":
            modifiers.add(ModifierKey.CTRL)
        elif part == "alt":
            modifiers.add(ModifierKey.ALT)
        elif part == "shift":
            modifiers.add(ModifierKey.SHIFT)
        elif part == "meta":
            modifiers.add(ModifierKey.META)
        else:
            keys.add(part)

    return keys, modifiers


def format_hotkey(keys: Set[str], modifiers: Set[ModifierKey]) -> str:
    """Format a hotkey as a string.

    Args:
        keys: Set of key names.
        modifiers: Set of modifier keys.

    Returns:
        Hotkey string like 'cmd+shift+a'.
    """
    all_parts = [m.value for m in sorted(modifiers, key=lambda m: m.value)] + sorted(keys)
    return "+".join(all_parts)


__all__ = [
    "ModifierKey",
    "HotkeyBinding",
    "HotkeyEvent",
    "HotkeyRegistry",
    "parse_hotkey_string",
    "format_hotkey",
]
