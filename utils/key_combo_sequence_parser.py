"""Key combo sequence parser for parsing and validating key combinations."""
from typing import List, Set, Optional, Dict
from dataclasses import dataclass
import re


@dataclass
class KeyCombo:
    """Represents a parsed key combination."""
    keys: Set[str]
    modifiers: Set[str]
    action_keys: Set[str]
    display_string: str
    is_chord: bool


class KeyComboSequenceParser:
    """Parses and validates keyboard shortcut combinations and sequences.
    
    Handles parsing of keyboard shortcut strings like "Ctrl+C", "Ctrl+Shift+Esc".
    
    Example:
        parser = KeyComboSequenceParser()
        combo = parser.parse("Ctrl+Shift+P")
        print(f"Modifiers: {combo.modifiers}, Action: {combo.action_keys}")
    """

    MODIFIER_KEYS = {"ctrl", "control", "alt", "shift", "meta", "cmd", "command", "super", "hyper"}

    def __init__(self, normalize_case: bool = True) -> None:
        self._normalize_case = normalize_case

    def parse(self, combo_string: str) -> KeyCombo:
        """Parse a key combination string into a KeyCombo."""
        keys = self._split_combo(combo_string)
        normalized = {self._normalize_key(k) for k in keys}
        
        modifiers = normalized & self.MODIFIER_KEYS
        action_keys = normalized - self.MODIFIER_KEYS
        display = self._build_display(normalized)
        is_chord = len(action_keys) > 1
        
        return KeyCombo(
            keys=normalized,
            modifiers=modifiers,
            action_keys=action_keys,
            display_string=display,
            is_chord=is_chord,
        )

    def parse_sequence(self, sequence_string: str) -> List[KeyCombo]:
        """Parse a sequence of key combos separated by spaces."""
        parts = sequence_string.split()
        return [self.parse(part) for part in parts if part.strip()]

    def matches(
        self,
        combo: KeyCombo,
        expected_modifiers: Optional[Set[str]] = None,
        expected_action: Optional[Set[str]] = None,
        require_exact: bool = False,
    ) -> bool:
        """Check if a combo matches expected criteria."""
        if require_exact:
            if expected_modifiers and combo.modifiers != expected_modifiers:
                return False
            if expected_action and combo.action_keys != expected_action:
                return False
            return True
        
        if expected_modifiers and not expected_modifiers.issubset(combo.modifiers):
            return False
        if expected_action and not expected_action.issubset(combo.action_keys):
            return False
        return True

    def isModifier(self, key: str) -> bool:
        """Check if a key is a modifier key."""
        return self._normalize_key(key) in self.MODIFIER_KEYS

    def _split_combo(self, combo_string: str) -> List[str]:
        combo_string = combo_string.strip()
        combo_string = re.sub(r'[\s]+', '', combo_string)
        keys = re.split(r'[\+\-_,]|\s+', combo_string)
        return [k for k in keys if k]

    def _normalize_key(self, key: str) -> str:
        normalized = key.lower().strip()
        if normalized == "control":
            normalized = "ctrl"
        elif normalized == "command":
            normalized = "cmd"
        return normalized

    def _build_display(self, keys: Set[str]) -> str:
        mods = []
        actions = []
        for key in keys:
            if key in self.MODIFIER_KEYS:
                mods.append(key.upper())
            else:
                actions.append(key.upper())
        return "+".join(mods) + ("+" if mods and actions else "") + "+".join(actions)
