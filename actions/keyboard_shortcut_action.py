"""
Keyboard Shortcut Action Module

Manages hotkey bindings, keyboard shortcuts, chord sequences,
and modifier key handling for automation workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


class ModifierKey(Enum):
    """Keyboard modifier keys."""

    CTRL = "ctrl"
    ALT = "alt"
    SHIFT = "shift"
    META = "meta"  # Command on Mac, Win on Windows
    SUPER = "super"


class KeyCode(Enum):
    """Common key codes."""

    A = "a"
    B = "b"
    C = "c"
    D = "d"
    E = "e"
    F = "f"
    G = "g"
    H = "h"
    I = "i"
    J = "j"
    K = "k"
    L = "l"
    M = "m"
    N = "n"
    O = "o"
    P = "p"
    Q = "q"
    R = "r"
    S = "s"
    T = "t"
    U = "u"
    V = "v"
    W = "w"
    X = "x"
    Y = "y"
    Z = "z"
    F1 = "f1"
    F2 = "f2"
    F3 = "f3"
    F4 = "f4"
    F5 = "f5"
    F6 = "f6"
    F7 = "f7"
    F8 = "f8"
    F9 = "f9"
    F10 = "f10"
    F11 = "f11"
    F12 = "f12"
    ESCAPE = "escape"
    ENTER = "enter"
    TAB = "tab"
    SPACE = "space"
    BACKSPACE = "backspace"
    DELETE = "delete"
    ARROW_UP = "arrow_up"
    ARROW_DOWN = "arrow_down"
    ARROW_LEFT = "arrow_left"
    ARROW_RIGHT = "arrow_right"
    HOME = "home"
    END = "end"
    PAGE_UP = "page_up"
    PAGE_DOWN = "page_down"


@dataclass
class HotkeyBinding:
    """Represents a hotkey binding."""

    keys: Set[ModifierKey]
    key_code: KeyCode
    callback: Callable[[], Any]
    description: str = ""
    enabled: bool = True
    repeat: bool = False


@dataclass
class ChordSequence:
    """Represents a sequence of key chords."""

    name: str
    chords: List[Tuple[Set[ModifierKey], KeyCode]]
    callback: Callable[[], Any]
    timeout_ms: float = 1000.0
    description: str = ""


@dataclass
class ShortcutConfig:
    """Configuration for keyboard shortcuts."""

    default_timeout_ms: float = 1000.0
    enable_chords: bool = True
    enable_repeat: bool = False
    case_sensitive: bool = False
    capture_global: bool = False


@dataclass
class KeyEvent:
    """Represents a keyboard event."""

    key_code: KeyCode
    modifiers: Set[ModifierKey]
    timestamp: float
    is_pressed: bool
    is_repeat: bool = False


class KeyboardShortcutManager:
    """
    Manages keyboard shortcuts, hotkeys, and chord sequences.

    Supports single-key shortcuts, modifier combinations,
    and multi-key chord sequences with timeout handling.
    """

    def __init__(
        self,
        config: Optional[ShortcutConfig] = None,
        key_executor: Optional[Callable[[KeyEvent], None]] = None,
    ):
        self.config = config or ShortcutConfig()
        self.key_executor = key_executor or self._default_executor
        self._bindings: Dict[Tuple[frozenset, KeyCode], HotkeyBinding] = {}
        self._chords: Dict[str, ChordSequence] = {}
        self._active_chord: List[KeyEvent] = []
        self._chord_start_time: float = 0
        self._enabled: bool = True

    def _default_executor(self, event: KeyEvent) -> None:
        """Default key executor."""
        mod_str = "+".join(m.value for m in event.modifiers)
        logger.debug(f"Key event: {mod_str}+{event.key_code.value if event.key_code else '?'}")

    def register_hotkey(
        self,
        modifiers: List[ModifierKey],
        key: KeyCode,
        callback: Callable[[], Any],
        description: str = "",
        enabled: bool = True,
    ) -> bool:
        """
        Register a hotkey binding.

        Args:
            modifiers: List of modifier keys
            key: Key code
            callback: Function to call when hotkey is triggered
            description: Human-readable description
            enabled: Whether binding is initially enabled

        Returns:
            True if registration succeeded
        """
        key_set = frozenset(modifiers)
        binding = HotkeyBinding(
            keys=set(modifiers),
            key_code=key,
            callback=callback,
            description=description,
            enabled=enabled,
        )

        self._bindings[(key_set, key)] = binding
        logger.debug(f"Registered hotkey: {'+'.join(m.value for m in modifiers)}+{key.value}")
        return True

    def unregister_hotkey(
        self,
        modifiers: List[ModifierKey],
        key: KeyCode,
    ) -> bool:
        """Unregister a hotkey binding."""
        key_set = frozenset(modifiers)
        binding_key = (key_set, key)

        if binding_key in self._bindings:
            del self._bindings[binding_key]
            return True

        return False

    def register_chord(
        self,
        name: str,
        sequence: List[Tuple[List[ModifierKey], KeyCode]],
        callback: Callable[[], Any],
        timeout_ms: Optional[float] = None,
        description: str = "",
    ) -> bool:
        """
        Register a chord sequence (multi-key shortcut).

        Args:
            name: Sequence identifier
            sequence: List of (modifiers, key) tuples
            callback: Function to call when sequence is triggered
            timeout_ms: Max time between keys in sequence
            description: Human-readable description

        Returns:
            True if registration succeeded
        """
        if not self.config.enable_chords:
            logger.warning("Chord sequences are disabled")
            return False

        chords = [
            (set(mods), key)
            for mods, key in sequence
        ]

        chord = ChordSequence(
            name=name,
            chords=chords,
            callback=callback,
            timeout_ms=timeout_ms or self.config.default_timeout_ms,
            description=description,
        )

        self._chords[name] = chord
        logger.debug(f"Registered chord: {name} ({len(chords)} keys)")
        return True

    def handle_key_event(
        self,
        key: KeyCode,
        modifiers: Optional[List[ModifierKey]] = None,
        is_pressed: bool = True,
        is_repeat: bool = False,
    ) -> bool:
        """
        Handle an incoming key event.

        Args:
            key: Key code
            modifiers: Active modifier keys
            is_pressed: True if key down, False if key up
            is_repeat: True if this is a key repeat event

        Returns:
            True if event was handled
        """
        if not self._enabled:
            return False

        modifiers = modifiers or []
        mod_set = set(modifiers)

        event = KeyEvent(
            key_code=key,
            modifiers=mod_set,
            timestamp=time.time(),
            is_pressed=is_pressed,
            is_repeat=is_repeat,
        )

        self.key_executor(event)

        if is_pressed:
            handled = self._check_hotkeys(mod_set, key, is_repeat)
            if handled:
                return True

            if self.config.enable_chords:
                self._check_chords(event)

        return False

    def _check_hotkeys(
        self,
        modifiers: Set[ModifierKey],
        key: KeyCode,
        is_repeat: bool,
    ) -> bool:
        """Check and execute matching hotkey bindings."""
        binding_key = (frozenset(modifiers), key)

        if binding_key in self._bindings:
            binding = self._bindings[binding_key]

            if not binding.enabled:
                return False

            if is_repeat and not binding.repeat:
                return False

            try:
                binding.callback()
                return True
            except Exception as e:
                logger.error(f"Hotkey callback failed: {e}")
                return False

        return False

    def _check_chords(self, event: KeyEvent) -> bool:
        """Check if event matches a chord sequence."""
        current_time = time.time()

        if self._active_chord:
            elapsed = (current_time - self._chord_start_time) * 1000
            if elapsed > self.config.default_timeout_ms:
                self._reset_chord()

        self._active_chord.append(event)
        self._chord_start_time = current_time

        for chord in self._chords.values():
            if self._match_chord_prefix(chord.chords):
                if self._match_chord_complete(chord.chords):
                    try:
                        chord.callback()
                    except Exception as e:
                        logger.error(f"Chord callback failed: {e}")
                    finally:
                        self._reset_chord()
                    return True

        return False

    def _match_chord_prefix(
        self,
        chord_sequence: List[Tuple[Set[ModifierKey], KeyCode]],
    ) -> bool:
        """Check if current input matches prefix of chord."""
        if len(self._active_chord) > len(chord_sequence):
            return False

        for i, (expected_mods, expected_key) in enumerate(chord_sequence[:len(self._active_chord)]):
            event = self._active_chord[i]
            if event.key_code != expected_key:
                return False
            if event.modifiers != expected_mods:
                return False

        return True

    def _match_chord_complete(
        self,
        chord_sequence: List[Tuple[Set[ModifierKey], KeyCode]],
    ) -> bool:
        """Check if current input exactly matches full chord sequence."""
        if len(self._active_chord) != len(chord_sequence):
            return False

        return self._match_chord_prefix(chord_sequence)

    def _reset_chord(self) -> None:
        """Reset the current chord input."""
        self._active_chord = []
        self._chord_start_time = 0

    def enable(self) -> None:
        """Enable all hotkeys and chords."""
        self._enabled = True

    def disable(self) -> None:
        """Disable all hotkeys and chords."""
        self._enabled = False

    def set_enabled(
        self,
        modifiers: Optional[List[ModifierKey]] = None,
        key: Optional[KeyCode] = None,
        enabled: bool = True,
    ) -> bool:
        """
        Enable or disable specific hotkey(s).

        Args:
            modifiers: Modifier keys (None for all)
            key: Key code (None for all)
            enabled: New enabled state

        Returns:
            True if any binding was modified
        """
        if modifiers is None and key is None:
            for binding in self._bindings.values():
                binding.enabled = enabled
            return True

        if modifiers is not None and key is not None:
            binding_key = (frozenset(modifiers), key)
            if binding_key in self._bindings:
                self._bindings[binding_key].enabled = enabled
                return True

        return False

    def list_bindings(self) -> List[Dict[str, Any]]:
        """List all registered hotkey bindings."""
        result = []

        for (mods, key), binding in self._bindings.items():
            result.append({
                "modifiers": list(mods),
                "key": key.value,
                "description": binding.description,
                "enabled": binding.enabled,
                "repeat": binding.repeat,
            })

        return result

    def list_chords(self) -> List[Dict[str, Any]]:
        """List all registered chord sequences."""
        return [
            {
                "name": chord.name,
                "sequence": [
                    (list(mods), key.value)
                    for mods, key in chord.chords
                ],
                "description": chord.description,
                "timeout_ms": chord.timeout_ms,
            }
            for chord in self._chords.values()
        ]

    def execute_shortcut(
        self,
        modifiers: List[ModifierKey],
        key: KeyCode,
    ) -> bool:
        """
        Directly execute a shortcut without real key event.

        Args:
            modifiers: Modifier keys
            key: Key code

        Returns:
            True if shortcut was executed
        """
        binding_key = (frozenset(modifiers), key)

        if binding_key in self._bindings:
            binding = self._bindings[binding_key]
            if binding.enabled:
                try:
                    binding.callback()
                    return True
                except Exception as e:
                    logger.error(f"Shortcut execution failed: {e}")

        return False


def create_shortcut_manager(
    config: Optional[ShortcutConfig] = None,
) -> KeyboardShortcutManager:
    """Factory function to create a KeyboardShortcutManager."""
    return KeyboardShortcutManager(config=config)
