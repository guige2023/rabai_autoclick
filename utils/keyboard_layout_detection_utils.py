"""Keyboard Layout Detection Utilities.

Detects and manages keyboard layouts for international input support.
Handles layout-specific key mappings and virtual key code translations.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class KeyMapping:
    """Mapping for a single key across layouts.

    Attributes:
        vk_code: Virtual key code.
        key_id: Physical key identifier.
        en_us: Character for US English layout.
        current: Character for current layout (if known).
    """

    vk_code: int
    key_id: str
    en_us: str = ""
    current: Optional[str] = None


@dataclass
class KeyboardLayout:
    """Represents a keyboard layout.

    Attributes:
        id: Layout identifier (e.g., 'en-US', 'de-DE').
        name: Human-readable name.
        is_right_to_left: Whether layout is for RTL language.
        mappings: Virtual key code to KeyMapping for each key.
    """

    id: str
    name: str
    is_right_to_left: bool = False
    mappings: dict[int, KeyMapping] = field(default_factory=dict)


class KeyboardLayoutDatabase:
    """Database of known keyboard layouts.

    Provides layout information and character mappings.

    Example:
        db = KeyboardLayoutDatabase()
        layout = db.get_layout("en-US")
        char = layout.get_char(vk_code=65)
    """

    # Common virtual key codes
    VK_CODES = {
        "BACK": 0x08,
        "TAB": 0x09,
        "RETURN": 0x0D,
        "SHIFT": 0x10,
        "CONTROL": 0x11,
        "MENU": 0x12,
        "ESCAPE": 0x1B,
        "SPACE": 0x20,
        "PAGEUP": 0x21,
        "PAGEDOWN": 0x22,
        "END": 0x23,
        "HOME": 0x24,
        "LEFT": 0x25,
        "UP": 0x26,
        "RIGHT": 0x27,
        "DOWN": 0x28,
        "PRINT": 0x2A,
    }

    def __init__(self):
        """Initialize the layout database."""
        self._layouts: dict[str, KeyboardLayout] = {}
        self._current_layout: Optional[KeyboardLayout] = None
        self._register_standard_layouts()

    def _register_standard_layouts(self) -> None:
        """Register standard keyboard layouts."""
        self.register(self._create_en_us_layout())
        self.register(self._create_de_de_layout())
        self.register(self._create_fr_fr_layout())

    def _create_en_us_layout(self) -> KeyboardLayout:
        """Create US English QWERTY layout."""
        layout = KeyboardLayout(id="en-US", name="US English")

        # Letter keys (VK codes 0x41-0x5A)
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        for i, letter in enumerate(letters):
            vk = 0x41 + i
            layout.mappings[vk] = KeyMapping(vk_code=vk, key_id=f"KEY_{letter}", en_us=letter)

        # Number row
        numbers = "0123456789"
        vk_start = 0x30
        for i, num in enumerate(numbers):
            vk = vk_start + i
            layout.mappings[vk] = KeyMapping(vk_code=vk, key_id=f"KEY_{num}", en_us=num)

        # Special keys
        layout.mappings[0x08] = KeyMapping(vk_code=0x08, key_id="BACK", en_us="⌫")
        layout.mappings[0x09] = KeyMapping(vk_code=0x09, key_id="TAB", en_us="⇥")
        layout.mappings[0x0D] = KeyMapping(vk_code=0x0D, key_id="RETURN", en_us="↵")
        layout.mappings[0x20] = KeyMapping(vk_code=0x20, key_id="SPACE", en_us=" ")

        return layout

    def _create_de_de_layout(self) -> KeyboardLayout:
        """Create German QWERTZ layout."""
        layout = KeyboardLayout(id="de-DE", name="German")

        # German layout differs from US in upper row
        de_upper = "QWERTZUIOPÜ"
        for i, letter in enumerate(de_upper):
            vk = 0x41 + i
            layout.mappings[vk] = KeyMapping(vk_code=vk, key_id=f"KEY_{letter}", en_us=letters[i] if i < len("QWERTZ") else "")

        # Add German-specific keys
        layout.mappings[0xBB] = KeyMapping(vk_code=0xBB, key_id="OEM_PLUS", en_us="+")
        layout.mappings[0xBD] = KeyMapping(vk_code=0xBD, key_id="OEM_MINUS", en_us="-")
        layout.mappings[0xBC] = KeyMapping(vk_code=0xBC, key_id="OEM_COMMA", en_us=",")
        layout.mappings[0xBE] = KeyMapping(vk_code=0xBE, key_id="OEM_PERIOD", en_us=".")

        return layout

    def _create_fr_fr_layout(self) -> KeyboardLayout:
        """Create French AZERTY layout."""
        layout = KeyboardLayout(id="fr-FR", name="French")

        # French AZERTY differs from QWERTY
        fr_mapping = {
            "A": "Q", "Z": "W", "Q": "A", "W": "Z",
            "M": ",", ",": "M", ";": "M", "?": ",",
        }

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        for i, letter in enumerate(letters):
            vk = 0x41 + i
            mapped = fr_mapping.get(letter, letter)
            layout.mappings[vk] = KeyMapping(vk_code=vk, key_id=f"KEY_{letter}", en_us=mapped)

        return layout

    def register(self, layout: KeyboardLayout) -> None:
        """Register a keyboard layout.

        Args:
            layout: KeyboardLayout to register.
        """
        self._layouts[layout.id] = layout

    def get_layout(self, layout_id: str) -> Optional[KeyboardLayout]:
        """Get a layout by ID.

        Args:
            layout_id: Layout identifier.

        Returns:
            KeyboardLayout or None if not found.
        """
        return self._layouts.get(layout_id)

    def set_current_layout(self, layout_id: str) -> bool:
        """Set the current active layout.

        Args:
            layout_id: Layout identifier.

        Returns:
            True if layout was found and set.
        """
        layout = self._layouts.get(layout_id)
        if layout:
            self._current_layout = layout
            return True
        return False

    def get_current_layout(self) -> Optional[KeyboardLayout]:
        """Get the current active layout.

        Returns:
            Current KeyboardLayout or None.
        """
        return self._current_layout

    def list_layouts(self) -> list[str]:
        """List all registered layout IDs.

        Returns:
            List of layout IDs.
        """
        return list(self._layouts.keys())


class KeyboardLayoutDetector:
    """Detects the current system keyboard layout.

    Example:
        detector = KeyboardLayoutDetector()
        layout_id = detector.detect_active_layout()
    """

    def __init__(self, database: Optional[KeyboardLayoutDatabase] = None):
        """Initialize the detector.

        Args:
            database: KeyboardLayoutDatabase to use.
        """
        self.database = database or KeyboardLayoutDatabase()

    def detect_active_layout(self) -> str:
        """Detect the currently active keyboard layout.

        Returns:
            Layout ID of the active layout.
        """
        # Platform-specific detection would go here
        # For now, return en-US as default
        return "en-US"

    def detect_from_input(self, text: str, expected_keys: list[str]) -> Optional[str]:
        """Infer layout from observed input text.

        Attempts to match typed characters against expected key sequences.

        Args:
            text: Text that was produced by typing.
            expected_keys: Keys that were expected to produce the text.

        Returns:
            Inferred layout ID or None.
        """
        if not text or not expected_keys:
            return None

        # Simple heuristic: check first few characters
        sample = text[: min(5, len(text))]
        has_umlauts = any(c in sample for c in "äöüß")
        has_accents = any(c in sample for c in "éèêëàâùûîï")

        if has_umlauts or "äöüÄÖÜß".strip():
            for layout_id in self.database.list_layouts():
                if layout_id.startswith("de"):
                    return layout_id
        if has_accents:
            for layout_id in self.database.list_layouts():
                if layout_id.startswith("fr"):
                    return layout_id

        return "en-US"

    def get_layout_for_locale(self, locale: str) -> Optional[str]:
        """Get likely layout ID for a locale.

        Args:
            locale: Locale string (e.g., 'en-US', 'de-DE').

        Returns:
            Matching layout ID or None.
        """
        # Try exact match
        if self.database.get_layout(locale):
            return locale

        # Try language-only match
        lang = locale.split("-")[0]
        for layout_id in self.database.list_layouts():
            if layout_id.startswith(lang):
                return layout_id

        return None


@dataclass
class KeySequenceTemplate:
    """Template for a key sequence with layout-aware placeholders.

    Attributes:
        pattern: Pattern string with {key} placeholders.
        description: Human-readable description.
        layout_dependent: Whether sequence produces different output on different layouts.
    """

    pattern: str
    description: str = ""
    layout_dependent: bool = False

    def render(
        self,
        layout: KeyboardLayout,
        **values: str,
    ) -> str:
        """Render the template for a specific layout.

        Args:
            layout: KeyboardLayout to use for rendering.
            **values: Values for placeholder keys.

        Returns:
            Rendered key sequence string.
        """
        result = self.pattern
        for key_name, char in values.items():
            placeholders = [f"{{{key_name}}}", f"{{{key_name.upper()}}}"]
            for placeholder in placeholders:
                if placeholder in result:
                    result = result.replace(placeholder, char)
                    break
        return result


class LayoutAdapter:
    """Adapts key sequences for different keyboard layouts.

    Transforms sequences designed for one layout to work with another.

    Example:
        adapter = LayoutAdapter(database)
        adapted = adapter.adapt_sequence("Hello", from_layout="en-US", to_layout="de-DE")
    """

    def __init__(self, database: Optional[KeyboardLayoutDatabase] = None):
        """Initialize the adapter.

        Args:
            database: KeyboardLayoutDatabase to use.
        """
        self.database = database or KeyboardLayoutDatabase()

    def find_key_for_char(
        self,
        char: str,
        layout: KeyboardLayout,
    ) -> Optional[int]:
        """Find virtual key code for a character in a layout.

        Args:
            char: Character to find.
            layout: KeyboardLayout to search.

        Returns:
            Virtual key code or None if not found.
        """
        char_lower = char.lower()
        for vk, mapping in layout.mappings.items():
            if mapping.current and mapping.current.lower() == char_lower:
                return vk
            if mapping.en_us.lower() == char_lower:
                return vk
        return None

    def adapt_sequence(
        self,
        sequence: str,
        from_layout_id: str,
        to_layout_id: str,
    ) -> str:
        """Adapt a key sequence from one layout to another.

        Args:
            sequence: Key sequence to adapt.
            from_layout_id: Source layout ID.
            to_layout_id: Target layout ID.

        Returns:
            Adapted sequence string.
        """
        from_layout = self.database.get_layout(from_layout_id)
        to_layout = self.database.get_layout(to_layout_id)

        if not from_layout or not to_layout:
            return sequence

        result = []
        for char in sequence:
            vk = self.find_key_for_char(char, from_layout)
            if vk is not None:
                to_mapping = to_layout.mappings.get(vk)
                if to_mapping and to_mapping.current:
                    result.append(to_mapping.current)
                elif to_mapping:
                    result.append(to_mapping.en_us or char)
                else:
                    result.append(char)
            else:
                result.append(char)

        return "".join(result)
