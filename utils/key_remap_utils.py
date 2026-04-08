"""
Keyboard shortcut mapping and remapping utilities.

Provides utilities for mapping, remapping, and resolving keyboard shortcuts
across different layouts and international keyboards.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field


@dataclass
class KeyMapping:
    """Represents a keyboard shortcut mapping."""
    source_key: str
    target_key: str
    modifiers: Set[str] = field(default_factory=set)
    description: str = ""
    
    def __str__(self) -> str:
        parts = sorted(self.modifiers) + [self.source_key]
        return "+".join(parts)


@dataclass 
class KeyRemapRule:
    """A rule for remapping keyboard input."""
    from_key: str
    to_key: str
    from_modifiers: Set[str] = field(default_factory=set)
    to_modifiers: Set[str] = field(default_factory=set)
    conditions: Optional[Callable[[], bool]] = None


class KeyboardLayout:
    """Represents a keyboard layout mapping."""
    
    # Standard QWERTY key positions
    QWERTY_ROWS = [
        ["`", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "-", "="],
        ["q", "w", "e", "r", "t", "y", "u", "i", "o", "p", "[", "]", "\\"],
        ["a", "s", "d", "f", "g", "h", "j", "k", "l", ";", "'"],
        ["z", "x", "c", "v", "b", "n", "m", ",", ".", "/"],
    ]
    
    # DVORAK layout (simplified)
    DVORAK_ROWS = [
        ["`", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "[", "]"],
        ["'", ",", ".", "p", "y", "f", "g", "c", "h", "l", ";", "="],
        ["a", "o", "e", "u", "i", "d", "h", "t", "n", "s", "-"],
        [";", "q", "j", "k", "x", "b", "m", "w", "v", "z"],
    ]
    
    # AZERTY layout (simplified)
    AZERTY_ROWS = [
        ["²", "&", "é", '"', "'", "(", "-", "è", "_", "ç", "à", ")", "="],
        ["a", "z", "e", "r", "t", "y", "u", "i", "o", "p", "^", "$"],
        ["q", "s", "d", "f", "g", "h", "j", "k", "l", "m", "ù"],
        ["w", "x", "c", "v", "b", "n", ",", ";", ":", "!"],
    ]
    
    def __init__(self, name: str, rows: List[List[str]]):
        """Initialize keyboard layout.
        
        Args:
            name: Layout name (e.g., 'qwerty', 'dvorak', 'azerty')
            rows: 2D list of key labels per row
        """
        self.name = name
        self.rows = rows
        self._pos_to_key: Dict[Tuple[int, int], str] = {}
        self._key_to_pos: Dict[str, Tuple[int, int]] = {}
        self._build_mapping()
    
    def _build_mapping(self) -> None:
        """Build position-to-key and key-to-position mappings."""
        for row_idx, row in enumerate(self.rows):
            for col_idx, key in enumerate(row):
                self._pos_to_key[(row_idx, col_idx)] = key
                self._key_to_pos[key.lower()] = (row_idx, col_idx)
    
    def get_key_at_position(self, row: int, col: int) -> Optional[str]:
        """Get key label at a position."""
        return self._pos_to_key.get((row, col))
    
    def get_position_of_key(self, key: str) -> Optional[Tuple[int, int]]:
        """Get position of a key in this layout."""
        return self._key_to_pos.get(key.lower())
    
    def translate_key(self, key: str, from_layout: "KeyboardLayout") -> Optional[str]:
        """Translate a key press from another layout to this layout.
        
        Args:
            key: Key pressed in from_layout
            from_layout: Source layout
            
        Returns:
            Corresponding key in this layout, or None if not found
        """
        pos = from_layout.get_position_of_key(key)
        if pos is None:
            return None
        return self.get_key_at_position(*pos)
    
    @classmethod
    def qwerty(cls) -> "KeyboardLayout":
        """Create QWERTY layout."""
        return cls("qwerty", cls.QWERTY_ROWS)
    
    @classmethod
    def dvorak(cls) -> "KeyboardLayout":
        """Create DVORAK layout."""
        return cls("dvorak", cls.DVORAK_ROWS)
    
    @classmethod
    def azerty(cls) -> "KeyboardLayout":
        """Create AZERTY layout."""
        return cls("azerty", cls.AZERTY_ROWS)


class ShortcutResolver:
    """Resolves and normalizes keyboard shortcuts."""
    
    MODIFIER_KEYS = {"cmd", "command", "ctrl", "control", "alt", "option", "shift", "fn", "meta"}
    KEY_ALIASES = {
        "command": "cmd",
        "control": "ctrl",
        "option": "alt",
    }
    
    def __init__(self):
        """Initialize shortcut resolver."""
        self._remappings: List[KeyRemapRule] = []
    
    def parse_shortcut(self, shortcut_str: str) -> Tuple[Set[str], str]:
        """Parse a shortcut string into modifiers and key.
        
        Args:
            shortcut_str: Shortcut string (e.g., "Cmd+Shift+S", "ctrl-c")
            
        Returns:
            Tuple of (modifiers set, key)
        """
        # Normalize separators
        shortcut_str = shortcut_str.replace("-", "+").replace("_", "+")
        
        parts = [p.strip().lower() for p in shortcut_str.split("+")]
        
        modifiers: Set[str] = set()
        key = ""
        
        for part in parts:
            if part in self.MODIFIER_KEYS or part in self.KEY_ALIASES:
                normalized = self.KEY_ALIASES.get(part, part)
                modifiers.add(normalized)
            else:
                key = part
        
        return modifiers, key
    
    def format_shortcut(
        self,
        key: str,
        modifiers: Optional[Set[str]] = None,
        format: str = "readable"
    ) -> str:
        """Format a shortcut for display.
        
        Args:
            key: Main key
            modifiers: Set of modifier keys
            format: Output format ('readable', 'symbolic', 'compact')
            
        Returns:
            Formatted shortcut string
        """
        if modifiers is None:
            modifiers = set()
        
        sorted_mods = sorted(modifiers)
        
        if format == "readable":
            labels = {
                "cmd": "Cmd",
                "ctrl": "Ctrl",
                "alt": "Alt",
                "shift": "Shift",
                "fn": "Fn",
                "meta": "Meta",
            }
            parts = [labels.get(m, m.capitalize()) for m in sorted_mods]
            parts.append(key.upper() if len(key) == 1 else key)
            return "+".join(parts)
        
        elif format == "symbolic":
            symbols = {
                "cmd": "⌘",
                "ctrl": "⌃",
                "alt": "⌥",
                "shift": "⇧",
                "fn": "Fn",
                "meta": "◇",
            }
            parts = [symbols.get(m, m) for m in sorted_mods]
            parts.append(key.upper() if len(key) == 1 else key)
            return "".join(parts)
        
        elif format == "compact":
            abbrevs = {
                "cmd": "^",
                "ctrl": "^",
                "alt": "⌥",
                "shift": "⇧",
            }
            parts = [abbrevs.get(m, m[0]) for m in sorted_mods]
            parts.append(key.upper() if len(key) == 1 else key)
            return "+".join(parts)
        
        return f"{'+'.join(sorted_mods)}+{key}"
    
    def normalize_shortcut(self, shortcut_str: str) -> str:
        """Normalize a shortcut string to a canonical form.
        
        Args:
            shortcut_str: Shortcut string
            
        Returns:
            Normalized shortcut string
        """
        modifiers, key = self.parse_shortcut(shortcut_str)
        return self.format_shortcut(key, modifiers, "compact")
    
    def shortcuts_equal(self, shortcut1: str, shortcut2: str) -> bool:
        """Check if two shortcut strings represent the same shortcut.
        
        Args:
            shortcut1: First shortcut
            shortcut2: Second shortcut
            
        Returns:
            True if shortcuts are equivalent
        """
        return self.normalize_shortcut(shortcut1) == self.normalize_shortcut(shortcut2)
    
    def add_remap_rule(self, rule: KeyRemapRule) -> None:
        """Add a remapping rule.
        
        Args:
            rule: Remap rule to add
        """
        self._remappings.append(rule)
    
    def resolve_shortcut(self, shortcut_str: str) -> Tuple[Set[str], str]:
        """Resolve a shortcut, applying any remappings.
        
        Args:
            shortcut_str: Shortcut to resolve
            
        Returns:
            Tuple of (resolved modifiers, resolved key)
        """
        modifiers, key = self.parse_shortcut(shortcut_str)
        
        for rule in self._remappings:
            if rule.from_key == key and rule.from_modifiers == modifiers:
                if rule.conditions is None or rule.conditions():
                    return rule.to_modifiers, rule.to_key
        
        return modifiers, key
    
    def translate_layout(
        self,
        shortcut_str: str,
        from_layout: KeyboardLayout,
        to_layout: KeyboardLayout
    ) -> Optional[str]:
        """Translate a shortcut from one keyboard layout to another.
        
        Args:
            shortcut_str: Shortcut in source layout
            from_layout: Source layout
            to_layout: Target layout
            
        Returns:
            Translated shortcut string, or None if translation not possible
        """
        modifiers, key = self.parse_shortcut(shortcut_str)
        translated_key = from_layout.translate_key(key, to_layout)
        
        if translated_key is None:
            return None
        
        return self.format_shortcut(translated_key, modifiers)


class ShortcutManager:
    """Manages keyboard shortcut bindings and groups."""
    
    def __init__(self):
        """Initialize shortcut manager."""
        self.bindings: Dict[str, Callable] = {}
        self.groups: Dict[str, List[str]] = {}
        self.resolver = ShortcutResolver()
    
    def bind(self, shortcut: str, callback: Callable, group: Optional[str] = None) -> None:
        """Bind a shortcut to a callback.
        
        Args:
            shortcut: Shortcut string
            callback: Function to call when shortcut is triggered
            group: Optional group name for organization
        """
        normalized = self.resolver.normalize_shortcut(shortcut)
        self.bindings[normalized] = callback
        
        if group:
            if group not in self.groups:
                self.groups[group] = []
            if normalized not in self.groups[group]:
                self.groups[group].append(normalized)
    
    def unbind(self, shortcut: str) -> bool:
        """Unbind a shortcut.
        
        Args:
            shortcut: Shortcut to unbind
            
        Returns:
            True if shortcut was unbound
        """
        normalized = self.resolver.normalize_shortcut(shortcut)
        if normalized in self.bindings:
            del self.bindings[normalized]
            
            # Remove from groups
            for group_list in self.groups.values():
                if normalized in group_list:
                    group_list.remove(normalized)
            
            return True
        return False
    
    def trigger(self, shortcut: str) -> bool:
        """Trigger the callback for a shortcut.
        
        Args:
            shortcut: Shortcut string
            
        Returns:
            True if shortcut was triggered
        """
        normalized = self.resolver.normalize_shortcut(shortcut)
        callback = self.bindings.get(normalized)
        
        if callback:
            callback()
            return True
        return False
    
    def get_bindings_in_group(self, group: str) -> Dict[str, Callable]:
        """Get all bindings in a group.
        
        Args:
            group: Group name
            
        Returns:
            Dictionary of shortcut -> callback
        """
        shortcuts = self.groups.get(group, [])
        return {s: self.bindings[s] for s in shortcuts if s in self.bindings}
    
    def list_all_bindings(self) -> List[Tuple[str, str]]:
        """List all bindings as (shortcut, key) pairs.
        
        Returns:
            List of (formatted shortcut, key) tuples
        """
        result = []
        for normalized, callback in self.bindings.items():
            key = getattr(callback, "__name__", str(callback))
            formatted = self.resolver.format_shortcut(*self.resolver.parse_shortcut(normalized))
            result.append((formatted, key))
        return sorted(result)


def parse_hotkey_string(hotkey_str: str) -> Dict[str, any]:
    """Parse a hotkey string into structured data.
    
    Args:
        hotkey_str: Hotkey string (e.g., "Cmd+Shift+Delete")
        
    Returns:
        Dictionary with 'key', 'modifiers', and 'raw' fields
    """
    resolver = ShortcutResolver()
    modifiers, key = resolver.parse_shortcut(hotkey_str)
    
    return {
        "raw": hotkey_str,
        "key": key,
        "modifiers": modifiers,
        "normalized": resolver.normalize_shortcut(hotkey_str),
        "readable": resolver.format_shortcut(key, modifiers, "readable"),
        "symbolic": resolver.format_shortcut(key, modifiers, "symbolic"),
    }


def expand_keyboard_template(
    template: str,
    key_mapping: Dict[str, str]
) -> str:
    """Expand placeholders in a keyboard shortcut template.
    
    Args:
        template: Template with placeholders (e.g., "{copy}")
        key_mapping: Mapping of placeholder -> actual shortcut
        
    Returns:
        Expanded shortcut string
    """
    result = template
    for placeholder, shortcut in key_mapping.items():
        if placeholder in result:
            result = result.replace(placeholder, shortcut)
    return result


def get_common_shortcuts() -> Dict[str, str]:
    """Get a dictionary of common shortcut templates.
    
    Returns:
        Dictionary of common shortcut names -> default shortcuts
    """
    return {
        "copy": "Cmd+C",
        "paste": "Cmd+V",
        "cut": "Cmd+X",
        "undo": "Cmd+Z",
        "redo": "Cmd+Shift+Z",
        "save": "Cmd+S",
        "quit": "Cmd+Q",
        "close": "Cmd+W",
        "find": "Cmd+F",
        "replace": "Cmd+Option+F",
        "select_all": "Cmd+A",
        "new_tab": "Cmd+T",
        "close_tab": "Cmd+W",
        "reload": "Cmd+R",
        "force_reload": "Cmd+Shift+R",
        "fullscreen": "Ctrl+Cmd+F",
        "zoom_in": "Cmd+=",
        "zoom_out": "Cmd+-",
        "reset_zoom": "Cmd+0",
    }
