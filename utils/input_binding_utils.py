"""
Hotkey binding and keyboard shortcut management.

Provides hotkey registration, conflict detection, and
keyboard shortcut execution for UI automation.

Author: Auto-generated
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class KeyModifier(Enum):
    """Keyboard modifier keys."""
    NONE = 0
    SHIFT = 1 << 0
    CTRL = 1 << 1
    ALT = 1 << 2
    META = 1 << 3
    SUPER = 1 << 4
    HYPER = 1 << 5
    FN = 1 << 6


class KeyAction(Enum):
    """Keyboard action types."""
    PRESS = auto()
    RELEASE = auto()
    TYPE = auto()


@dataclass
class KeyCombo:
    """
    A keyboard shortcut combination.
    
    Example:
        KeyCombo(key="c", modifiers=KeyModifier.CTRL)  # Ctrl+C
        KeyCombo(key="f5", modifiers=KeyModifier.NONE)  # F5
    """
    key: str
    modifiers: KeyModifier = KeyModifier.NONE
    
    def __str__(self) -> str:
        parts = []
        if self.modifiers & KeyModifier.CTRL:
            parts.append("Ctrl")
        if self.modifiers & KeyModifier.ALT:
            parts.append("Alt")
        if self.modifiers & KeyModifier.SHIFT:
            parts.append("Shift")
        if self.modifiers & KeyModifier.META:
            parts.append("Meta")
        if self.modifiers & KeyModifier.SUPER:
            parts.append("Super")
        parts.append(self.key.upper() if len(self.key) == 1 else self.key)
        return "+".join(parts)
    
    def matches(
        self,
        key: str,
        ctrl: bool = False,
        alt: bool = False,
        shift: bool = False,
        meta: bool = False,
    ) -> bool:
        """Check if this combo matches the given key event."""
        if key.lower() != self.key.lower():
            return False
        
        mods = KeyModifier.NONE
        if ctrl:
            mods |= KeyModifier.CTRL
        if alt:
            mods |= KeyModifier.ALT
        if shift:
            mods |= KeyModifier.SHIFT
        if meta:
            mods |= KeyModifier.META
        
        return self.modifiers == mods
    
    def to_scan_codes(self) -> list[int]:
        """Convert to list of HID scan codes."""
        # Simple mapping for common keys
        key_map = {
            "a": 4, "b": 5, "c": 6, "d": 7, "e": 8, "f": 9, "g": 10,
            "h": 11, "i": 12, "j": 13, "k": 14, "l": 15, "m": 16, "n": 17,
            "o": 18, "p": 19, "q": 20, "r": 21, "s": 22, "t": 23, "u": 24,
            "v": 25, "w": 26, "x": 27, "y": 28, "z": 29,
            "1": 30, "2": 31, "3": 32, "4": 33, "5": 34, "6": 35, "7": 36,
            "8": 37, "9": 38, "0": 39,
            "enter": 40, "return": 40, "esc": 41, "escape": 41,
            "space": 44, "tab": 43,
            "left": 80, "right": 79, "up": 82, "down": 81,
        }
        
        codes = []
        if self.modifiers & KeyModifier.CTRL:
            codes.append(224)  # Left Ctrl
        if self.modifiers & KeyModifier.ALT:
            codes.append(226)  # Left Alt
        if self.modifiers & KeyModifier.SHIFT:
            codes.append(225)  # Left Shift
        
        key_lower = self.key.lower()
        if key_lower in key_map:
            codes.append(key_map[key_lower])
        
        return codes


@dataclass
class HotkeyBinding:
    """A registered hotkey binding with callback."""
    combo: KeyCombo
    callback: Callable[[], None]
    description: str = ""
    enabled: bool = True
    priority: int = 0
    once: bool = False
    _last_triggered: float = field(default=0.0, repr=False)
    
    @property
    def cooldown_ms(self) -> float:
        """Default cooldown between triggers."""
        return 200.0


class HotkeyManager:
    """
    Manages global hotkey registrations and handling.
    
    Example:
        manager = HotkeyManager()
        manager.register(
            KeyCombo("q", KeyModifier.CTRL),
            lambda: print("Quit!"),
            "Quit application"
        )
        manager.handle_key("q", ctrl=True)
    """
    
    def __init__(self, default_cooldown_ms: float = 200.0):
        self._bindings: dict[KeyCombo, HotkeyBinding] = {}
        self._conflicts: list[tuple[KeyCombo, KeyCombo]] = []
        self._default_cooldown_ms = default_cooldown_ms
        self._enabled = True
        self._listener_active = False
    
    def register(
        self,
        combo: KeyCombo,
        callback: Callable[[], None],
        description: str = "",
        priority: int = 0,
        once: bool = False,
    ) -> HotkeyBinding:
        """
        Register a hotkey binding.
        
        Args:
            combo: The key combination
            callback: Function to call when hotkey is triggered
            description: Human-readable description
            priority: Higher priority bindings are checked first
            once: If True, binding is removed after first trigger
            
        Returns:
            The created HotkeyBinding
        """
        binding = HotkeyBinding(
            combo=combo,
            callback=callback,
            description=description,
            priority=priority,
            once=once,
        )
        
        # Check for conflicts
        for existing_combo, existing_binding in self._bindings.items():
            if existing_combo == combo:
                self._conflicts.append((combo, existing_combo))
        
        self._bindings[combo] = binding
        return binding
    
    def unregister(self, combo: KeyCombo) -> bool:
        """
        Unregister a hotkey binding.
        
        Returns:
            True if binding was found and removed
        """
        if combo in self._bindings:
            del self._bindings[combo]
            return True
        return False
    
    def handle_key(
        self,
        key: str,
        ctrl: bool = False,
        alt: bool = False,
        shift: bool = False,
        meta: bool = False,
    ) -> bool:
        """
        Handle a key event.
        
        Returns:
            True if a binding was triggered
        """
        if not self._enabled:
            return False
        
        triggered = False
        
        # Sort by priority (highest first)
        sorted_bindings = sorted(
            self._bindings.items(),
            key=lambda x: x[1].priority,
            reverse=True,
        )
        
        for combo, binding in sorted_bindings:
            if not binding.enabled:
                continue
            
            if combo.matches(key, ctrl=ctrl, alt=alt, shift=shift, meta=meta):
                now = time.time()
                cooldown = binding.cooldown_ms / 1000.0
                
                if now - binding._last_triggered < cooldown:
                    continue
                
                binding._last_triggered = now
                binding.callback()
                triggered = True
                
                if binding.once:
                    self.unregister(combo)
        
        return triggered
    
    def get_binding(self, combo: KeyCombo) -> HotkeyBinding | None:
        """Get binding for a combo."""
        return self._bindings.get(combo)
    
    def get_bindings_for_key(self, key: str) -> list[HotkeyBinding]:
        """Get all bindings that involve a specific key."""
        key_lower = key.lower()
        return [
            b for combo, b in self._bindings.items()
            if combo.key.lower() == key_lower
        ]
    
    def get_conflicts(self) -> list[tuple[KeyCombo, KeyCombo]]:
        """Get list of conflicting bindings."""
        return list(self._conflicts)
    
    def enable(self) -> None:
        """Enable hotkey handling."""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable hotkey handling."""
        self._enabled = False
    
    def enable_all(self) -> None:
        """Enable all bindings."""
        for binding in self._bindings.values():
            binding.enabled = True
    
    def disable_all(self) -> None:
        """Disable all bindings."""
        for binding in self._bindings.values():
            binding.enabled = False
    
    def list_bindings(self) -> list[HotkeyBinding]:
        """List all registered bindings."""
        return list(self._bindings.values())
    
    def clear(self) -> None:
        """Clear all bindings."""
        self._bindings.clear()
        self._conflicts.clear()


def parse_hotkey_string(hotkey_str: str) -> KeyCombo:
    """
    Parse a hotkey string like "Ctrl+Shift+S" into a KeyCombo.
    
    Supports formats:
    - "Ctrl+C", "ctrl+c", "CTRL+C"
    - "Cmd+Shift+P"
    - "Alt+F4"
    - "F5", "Enter", "Space"
    """
    parts = hotkey_str.replace(" ", "").split("+")
    
    modifiers = KeyModifier.NONE
    key = parts[-1] if parts else ""
    
    for part in parts[:-1]:
        part_lower = part.lower()
        if part_lower in ("ctrl", "control"):
            modifiers |= KeyModifier.CTRL
        elif part_lower in ("alt", "option"):
            modifiers |= KeyModifier.ALT
        elif part_lower in ("shift"):
            modifiers |= KeyModifier.SHIFT
        elif part_lower in ("meta", "cmd", "command", "win"):
            modifiers |= KeyModifier.META
        elif part_lower in ("super", "windows"):
            modifiers |= KeyModifier.SUPER
    
    return KeyCombo(key=key, modifiers=modifiers)


# Common hotkey presets
PRESET_BINDINGS = {
    "quit": KeyCombo("q", KeyModifier.CTRL),
    "copy": KeyCombo("c", KeyModifier.CTRL),
    "paste": KeyCombo("v", KeyModifier.CTRL),
    "cut": KeyCombo("x", KeyModifier.CTRL),
    "select_all": KeyCombo("a", KeyModifier.CTRL),
    "save": KeyCombo("s", KeyModifier.CTRL),
    "find": KeyCombo("f", KeyModifier.CTRL),
    "close": KeyCombo("w", KeyModifier.CTRL),
    "new_tab": KeyCombo("t", KeyModifier.CTRL),
    "refresh": KeyCombo("r", KeyModifier.CTRL),
    "fullscreen": KeyCombo("f", KeyModifier.META),
    "zoom_in": KeyCombo("+", KeyModifier.CTRL),
    "zoom_out": KeyCombo("-", KeyModifier.CTRL),
    "zoom_reset": KeyCombo("0", KeyModifier.CTRL),
}
