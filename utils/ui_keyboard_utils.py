"""
UI keyboard automation utilities.

Provide advanced keyboard automation capabilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable
from enum import Enum, auto


class KeyModifier(Enum):
    """Keyboard modifier keys."""
    SHIFT = auto()
    CONTROL = auto()
    ALT = auto()
    COMMAND = auto()
    OPTION = auto()
    FUNCTION = auto()


@dataclass
class KeyCombo:
    """A keyboard combination with modifiers."""
    key: str
    modifiers: set[KeyModifier] = None
    
    def __post_init__(self):
        if self.modifiers is None:
            self.modifiers = set()
    
    def with_modifier(self, modifier: KeyModifier) -> "KeyCombo":
        """Add a modifier to the combo."""
        new_mods = self.modifiers.copy()
        new_mods.add(modifier)
        return KeyCombo(self.key, new_mods)
    
    def __hash__(self) -> int:
        return hash((self.key, frozenset(self.modifiers)))
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, KeyCombo):
            return False
        return self.key == other.key and self.modifiers == other.modifiers


@dataclass
class KeyboardAction:
    """An action to be performed by the keyboard."""
    action_type: str
    key: Optional[str] = None
    text: Optional[str] = None
    combo: Optional[KeyCombo] = None
    delay_ms: float = 0


class KeyboardLayoutManager:
    """Manage keyboard layouts and key mappings."""
    
    def __init__(self, layout: Optional[str] = None):
        self.layout = layout or "en-US"
        self._key_map: dict[str, str] = {}
        self._shift_map: dict[str, str] = {}
        self._build_key_map()
    
    def _build_key_map(self) -> None:
        """Build key mapping for the layout."""
        self._key_map = {
            "a": "a", "b": "b", "c": "c", "d": "d", "e": "e",
            "f": "f", "g": "g", "h": "h", "i": "i", "j": "j",
            "k": "k", "l": "l", "m": "m", "n": "n", "o": "o",
            "p": "p", "q": "q", "r": "r", "s": "s", "t": "t",
            "u": "u", "v": "v", "w": "w", "x": "x", "y": "y",
            "z": "z", "0": "0", "1": "1", "2": "2", "3": "3",
            "4": "4", "5": "5", "6": "6", "7": "7", "8": "8",
            "9": "9", "space": " ", "enter": "\n", "tab": "\t",
            "escape": "\x1b", "backspace": "\x7f"
        }
        
        self._shift_map = {
            "a": "A", "b": "B", "c": "C", "d": "D", "e": "E",
            "f": "F", "g": "G", "h": "H", "i": "I", "j": "J",
            "k": "K", "l": "L", "m": "M", "n": "N", "o": "O",
            "p": "P", "q": "Q", "r": "R", "s": "S", "t": "T",
            "u": "U", "v": "V", "w": "W", "x": "X", "y": "Y",
            "z": "Z", "0": ")", "1": "!", "2": "@", "3": "#",
            "4": "$", "5": "%", "6": "^", "7": "&", "8": "*",
            "9": "(", " ": " "
        }
    
    def get_key_for_character(self, char: str) -> Optional[KeyCombo]:
        """Get the key combo needed to type a character."""
        if char in self._key_map:
            return KeyCombo(key=char)
        
        for key, shifted in self._shift_map.items():
            if shifted == char:
                return KeyCombo(key=key, modifiers={KeyModifier.SHIFT})
        
        return None
    
    def get_shortcut(self, combo: str) -> KeyCombo:
        """Parse a shortcut string like 'cmd+c' or 'ctrl+shift+s'."""
        parts = combo.lower().split("+")
        key = parts[-1]
        modifiers = set()
        
        for part in parts[:-1]:
            if part == "cmd" or part == "command":
                modifiers.add(KeyModifier.COMMAND)
            elif part == "ctrl" or part == "control":
                modifiers.add(KeyModifier.CONTROL)
            elif part == "alt" or part == "option":
                modifiers.add(KeyModifier.OPTION)
            elif part == "shift":
                modifiers.add(KeyModifier.SHIFT)
        
        return KeyCombo(key=key, modifiers=modifiers)


class KeyboardMacroBuilder:
    """Build complex keyboard macros."""
    
    def __init__(self, layout_manager: Optional[KeyboardLayoutManager] = None):
        self.layout_manager = layout_manager or KeyboardLayoutManager()
        self._actions: list[KeyboardAction] = []
    
    def type_text(self, text: str) -> "KeyboardMacroBuilder":
        """Add text typing action."""
        for char in text:
            self._actions.append(KeyboardAction(action_type="type", text=char))
        return self
    
    def press_key(self, key: str) -> "KeyboardMacroBuilder":
        """Add single key press."""
        self._actions.append(KeyboardAction(action_type="key", key=key))
        return self
    
    def press_combo(self, combo: str) -> "KeyboardMacroBuilder":
        """Add combo key press."""
        key_combo = self.layout_manager.get_shortcut(combo)
        self._actions.append(KeyboardAction(action_type="combo", combo=key_combo))
        return self
    
    def delay(self, ms: float) -> "KeyboardMacroBuilder":
        """Add delay between actions."""
        self._actions.append(KeyboardAction(action_type="delay", delay_ms=ms))
        return self
    
    def with_delay(self, delay_ms: float) -> "KeyboardMacroBuilder":
        """Set delay for subsequent actions."""
        for action in reversed(self._actions):
            if action.action_type != "delay":
                action.delay_ms = delay_ms
                break
        return self
    
    def build(self) -> list[KeyboardAction]:
        """Build the macro."""
        return list(self._actions)
    
    def clear(self) -> "KeyboardMacroBuilder":
        """Clear all actions."""
        self._actions.clear()
        return self
