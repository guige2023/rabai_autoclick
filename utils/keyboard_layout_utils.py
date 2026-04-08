"""Keyboard layout handling utilities."""

from typing import Dict, List, Optional, Tuple, Set
import sys


class KeyboardLayout:
    """Cross-platform keyboard layout helper."""

    QWERTY_ROWS = [
        ["`", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "-", "="],
        ["q", "w", "e", "r", "t", "y", "u", "i", "o", "p", "[", "]", "\\"],
        ["a", "s", "d", "f", "g", "h", "j", "k", "l", ";", "'"],
        ["z", "x", "c", "v", "b", "n", "m", ",", ".", "/"],
    ]

    def __init__(self, layout: str = "qwerty"):
        """Initialize keyboard layout.
        
        Args:
            layout: Layout name (currently only qwerty supported).
        """
        self.layout = layout
        self._row_map: Dict[str, Tuple[int, int]] = {}
        self._build_row_map()

    def _build_row_map(self) -> None:
        """Build position map for keys."""
        for row_idx, row in enumerate(self.QWERTY_ROWS):
            for col_idx, key in enumerate(row):
                self._row_map[key] = (row_idx, col_idx)

    def key_position(self, key: str) -> Optional[Tuple[int, int]]:
        """Get position of key in layout.
        
        Args:
            key: Key character.
        
        Returns:
            (row, col) or None if not found.
        """
        return self._row_map.get(key.lower())

    def distance(self, key1: str, key2: str) -> float:
        """Calculate keyboard distance between two keys.
        
        Args:
            key1: First key.
            key2: Second key.
        
        Returns:
            Euclidean distance in key units.
        """
        p1 = self.key_position(key1)
        p2 = self.key_position(key2)
        if p1 is None or p2 is None:
            return float("inf")
        import math
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    def closest_key(self, key: str, candidates: Set[str]) -> Optional[str]:
        """Find closest key from candidates.
        
        Args:
            key: Reference key.
            candidates: Set of candidate keys.
        
        Returns:
            Closest key or None.
        """
        best = None
        best_dist = float("inf")
        for c in candidates:
            d = self.distance(key, c)
            if d < best_dist:
                best_dist = d
                best = c
        return best

    def home_row_keys(self) -> List[str]:
        """Get home row keys."""
        return list(self.QWERTY_ROWS[2])

    def numeric_row(self) -> List[str]:
        """Get numeric row keys."""
        return list(self.QWERTY_ROWS[0])


def normalize_key_name(key: str) -> str:
    """Normalize key name to standard format.
    
    Args:
        key: Key name (various formats accepted).
    
    Returns:
        Normalized key name.
    """
    key = key.lower().strip()
    aliases = {
        "space": "space",
        "spacebar": "space",
        "return": "enter",
        "esc": "escape",
        "escape": "escape",
        "tab": "tab",
        "backspace": "backspace",
        "delete": "delete",
        "up": "up",
        "down": "down",
        "left": "left",
        "right": "right",
        "cmd": "meta",
        "command": "meta",
        "option": "alt",
        "control": "ctrl",
        "ctrl": "ctrl",
    }
    if key in aliases:
        return aliases[key]
    return key


def is_modifier_key(key: str) -> bool:
    """Check if key is a modifier.
    
    Args:
        key: Key name.
    
    Returns:
        True if modifier key.
    """
    modifiers = {"ctrl", "alt", "shift", "meta", "cmd", "command", "option"}
    return normalize_key_name(key) in modifiers


def parse_hotkey(hotkey: str) -> List[str]:
    """Parse hotkey string into modifiers and key.
    
    Args:
        hotkey: Hotkey string like "ctrl+shift+p" or "Cmd+Opt+K".
    
    Returns:
        List of key names.
    """
    return [normalize_key_name(k) for k in hotkey.replace(" ", "+").split("+")]


def format_hotkey(keys: List[str]) -> str:
    """Format keys into hotkey string.
    
    Args:
        keys: List of key names.
    
    Returns:
        Hotkey string like "ctrl+shift+p".
    """
    return "+".join(k.lower() for k in keys)


def get_os_modifier() -> str:
    """Get the OS-specific modifier key name."""
    if sys.platform == "darwin":
        return "meta"
    return "ctrl"
