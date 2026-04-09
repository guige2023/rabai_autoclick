"""
Hotkey Combo Action Module

Maps complex hotkey combinations to automation actions
with chord support and custom binding patterns.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class Modifier(Enum):
    """Modifier keys."""

    CTRL = "ctrl"
    ALT = "alt"
    SHIFT = "shift"
    SUPER = "super"
    FN = "fn"


@dataclass
class HotkeyCombo:
    """Represents a hotkey combination."""

    modifiers: Set[Modifier]
    key: str
    chord_next: Optional["HotkeyCombo"] = None


@dataclass
class HotkeyBinding:
    """Binds a hotkey combo to an action."""

    combo: HotkeyCombo
    callback: Callable[[], Any]
    description: str = ""
    enabled: bool = True


class HotkeyComboManager:
    """
    Manages hotkey combos and chords.

    Supports modifier combinations, sequential chords,
    and complex binding patterns.
    """

    def __init__(
        self,
        executor: Optional[Callable[[str], None]] = None,
    ):
        self.executor = executor
        self._bindings: Dict[str, HotkeyBinding] = {}
        self._chord_timeout: float = 0.5
        self._last_key_time: float = 0
        self._pending_chord: Optional[HotkeyCombo] = None
        self._enabled: bool = True

    def register(
        self,
        name: str,
        modifiers: List[Modifier],
        key: str,
        callback: Callable[[], Any],
        description: str = "",
    ) -> bool:
        """Register a hotkey binding."""
        combo = HotkeyCombo(modifiers=set(modifiers), key=key)
        binding = HotkeyBinding(
            combo=combo,
            callback=callback,
            description=description,
        )
        self._bindings[name] = binding
        logger.info(f"Registered hotkey: {name}")
        return True

    def unregister(self, name: str) -> bool:
        """Unregister a binding."""
        if name in self._bindings:
            del self._bindings[name]
            return True
        return False

    def handle_key(
        self,
        modifiers: Set[Modifier],
        key: str,
        pressed: bool = True,
    ) -> bool:
        """Handle a key event."""
        if not self._enabled:
            return False

        current_time = time.time()

        if self._pending_chord:
            if current_time - self._last_key_time > self._chord_timeout:
                self._pending_chord = None

        for name, binding in self._bindings.items():
            if not binding.enabled:
                continue

            combo = binding.combo

            if combo.modifiers == modifiers and combo.key == key:
                if combo.chord_next:
                    if pressed:
                        self._pending_chord = combo.chord_next
                        self._last_key_time = current_time
                        return True
                else:
                    if pressed:
                        try:
                            binding.callback()
                            if self.executor:
                                self.executor(name)
                            return True
                        except Exception as e:
                            logger.error(f"Hotkey callback failed: {e}")

        return False

    def enable(self, name: Optional[str] = None) -> None:
        """Enable hotkey(s)."""
        if name:
            if name in self._bindings:
                self._bindings[name].enabled = True
        else:
            self._enabled = True

    def disable(self, name: Optional[str] = None) -> None:
        """Disable hotkey(s)."""
        if name:
            if name in self._bindings:
                self._bindings[name].enabled = False
        else:
            self._enabled = False

    def list_bindings(self) -> List[Dict[str, Any]]:
        """List all bindings."""
        return [
            {
                "name": name,
                "modifiers": [m.value for m in b.combo.modifiers],
                "key": b.combo.key,
                "description": b.description,
                "enabled": b.enabled,
            }
            for name, b in self._bindings.items()
        ]


def create_hotkey_combo_manager() -> HotkeyComboManager:
    """Factory function."""
    return HotkeyComboManager()
