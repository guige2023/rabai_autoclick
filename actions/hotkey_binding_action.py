"""Hotkey binding action for UI automation.

Manages keyboard shortcuts and hotkeys:
- Key combination handling
- Hotkey registration
- Action binding
- Modifier key management
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class KeyCode(Enum):
    """Virtual key codes."""
    # Letters
    A = 0x00
    B = 0x0B
    C = 0x08
    D = 0x02
    E = 0x0E
    F = 0x03
    G = 0x04
    H = 0x05
    I = 0x22
    J = 0x26
    K = 0x28
    L = 0x25
    M = 0x2E
    N = 0x2D
    O = 0x1F
    P = 0x23
    Q = 0x0C
    R = 0x0F
    S = 0x01
    T = 0x11
    U = 0x20
    V = 0x09
    W = 0x0D
    X = 0x07
    Y = 0x10
    Z = 0x06

    # Numbers
    NUM_0 = 0x1D
    NUM_1 = 0x12
    NUM_2 = 0x13
    NUM_3 = 0x14
    NUM_4 = 0x15
    NUM_5 = 0x17
    NUM_6 = 0x16
    NUM_7 = 0x1A
    NUM_8 = 0x1C
    NUM_9 = 0x19

    # Function keys
    F1 = 0x7A
    F2 = 0x78
    F3 = 0x63
    F4 = 0x76
    F5 = 0x60
    F6 = 0x61
    F7 = 0x62
    F8 = 0x64
    F9 = 0x65
    F10 = 0x6D
    F11 = 0x67
    F12 = 0x6F

    # Special keys
    RETURN = 0x24
    TAB = 0x30
    SPACE = 0x31
    DELETE = 0x33
    ESCAPE = 0x35
    BACKSPACE = 0x33
    UP = 0x7E
    DOWN = 0x7D
    LEFT = 0x7B
    RIGHT = 0x7C

    # Modifiers
    COMMAND = 0x37
    SHIFT = 0x38
    OPTION = 0x3A
    CONTROL = 0x3B


class ModifierMask(Enum):
    """Modifier key masks."""
    NONE = 0
    COMMAND = 1 << 0
    SHIFT = 1 << 1
    OPTION = 1 << 2
    CONTROL = 1 << 3


@dataclass
class HotkeyBinding:
    """Hotkey binding."""
    key_code: KeyCode
    modifiers: int = 0  # Bitmask of ModifierMask
    action: Callable | None = None
    description: str = ""
    enabled: bool = True
    repeat: bool = False


@dataclass
class HotkeyEvent:
    """Hotkey event."""
    key_code: KeyCode
    modifiers: int
    timestamp: float
    pressed: bool  # True = keydown, False = keyup


class HotkeyManager:
    """Manages hotkey bindings for UI automation.

    Features:
    - Hotkey registration
    - Action binding
    - Modifier key handling
    - Global hotkeys
    - Hotkey conflict detection
    """

    def __init__(self):
        self._bindings: dict[int, HotkeyBinding] = {}  # key -> binding
        self._callbacks: list[Callable[[HotkeyEvent], None]] = []
        self._key_handler: Callable | None = None
        self._enabled = True

    def set_key_handler(self, handler: Callable[[HotkeyEvent], bool]) -> None:
        """Set global key handler.

        Args:
            handler: Function(HotkeyEvent) -> bool (True if handled)
        """
        self._key_handler = handler

    def bind(
        self,
        key_code: KeyCode,
        modifiers: int = 0,
        action: Callable | None = None,
        description: str = "",
        replace: bool = False,
    ) -> bool:
        """Bind hotkey to action.

        Args:
            key_code: Key code
            modifiers: Modifier mask
            action: Action to execute
            description: Description for debugging
            replace: Replace existing binding

        Returns:
            True if binding succeeded
        """
        binding_key = self._make_binding_key(key_code, modifiers)

        if binding_key in self._bindings and not replace:
            return False

        binding = HotkeyBinding(
            key_code=key_code,
            modifiers=modifiers,
            action=action,
            description=description,
        )
        self._bindings[binding_key] = binding
        return True

    def unbind(self, key_code: KeyCode, modifiers: int = 0) -> bool:
        """Unbind hotkey.

        Args:
            key_code: Key code
            modifiers: Modifier mask

        Returns:
            True if unbound
        """
        binding_key = self._make_binding_key(key_code, modifiers)
        if binding_key in self._bindings:
            del self._bindings[binding_key]
            return True
        return False

    def unbind_all(self) -> None:
        """Unbind all hotkeys."""
        self._bindings.clear()

    def get_binding(
        self,
        key_code: KeyCode,
        modifiers: int = 0,
    ) -> HotkeyBinding | None:
        """Get binding for key combination."""
        binding_key = self._make_binding_key(key_code, modifiers)
        return self._bindings.get(binding_key)

    def handle_event(self, event: HotkeyEvent) -> bool:
        """Handle hotkey event.

        Args:
            event: Hotkey event

        Returns:
            True if event was handled
        """
        if not self._enabled:
            return False

        # Check global handler first
        if self._key_handler:
            if self._key_handler(event):
                return True

        # Check bindings
        binding = self.get_binding(event.key_code, event.modifiers)
        if binding and binding.enabled and binding.action:
            if event.pressed:
                binding.action()
            return True

        # Notify callbacks
        for cb in self._callbacks:
            try:
                if cb(event):
                    return True
            except Exception:
                pass

        return False

    def register_callback(
        self,
        callback: Callable[[HotkeyEvent], bool],
    ) -> None:
        """Register for hotkey events.

        Args:
            callback: Function(HotkeyEvent) -> bool (True if handled)
        """
        self._callbacks.append(callback)

    def enable(self) -> None:
        """Enable hotkey handling."""
        self._enabled = True

    def disable(self) -> None:
        """Disable hotkey handling."""
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        """Check if enabled."""
        return self._enabled

    def list_bindings(self) -> list[HotkeyBinding]:
        """List all bindings."""
        return list(self._bindings.values())

    def _make_binding_key(self, key_code: KeyCode, modifiers: int) -> int:
        """Create unique binding key."""
        return (key_code.value << 8) | modifiers

    @staticmethod
    def parse_hotkey_string(hotkey_str: str) -> tuple[KeyCode, int]:
        """Parse hotkey string like "Cmd+Shift+S".

        Args:
            hotkey_str: Hotkey string

        Returns:
            Tuple of (KeyCode, modifier_mask)
        """
        parts = hotkey_str.replace(" ", "").split("+")
        modifiers = 0
        key_name = parts[-1]

        modifier_map = {
            "Cmd": ModifierMask.COMMAND,
            "Command": ModifierMask.COMMAND,
            "Ctrl": ModifierMask.CONTROL,
            "Control": ModifierMask.CONTROL,
            "Opt": ModifierMask.OPTION,
            "Option": ModifierMask.OPTION,
            "Shift": ModifierMask.SHIFT,
        }

        for part in parts[:-1]:
            if part in modifier_map:
                modifiers |= modifier_map[part].value

        # Find key code
        key_code = None
        for KC in KeyCode:
            if KC.name == f"NUM_{key_name}" or KC.name == key_name.upper():
                key_code = KC
                break

        if key_code is None:
            raise ValueError(f"Unknown key: {key_name}")

        return (key_code, modifiers)


def create_hotkey_manager() -> HotkeyManager:
    """Create hotkey manager."""
    return HotkeyManager()
