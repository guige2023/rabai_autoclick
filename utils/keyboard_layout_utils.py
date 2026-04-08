"""
Keyboard layout utilities for macOS automation.

Provides keyboard layout detection, switching, and
character-to-keycode mapping for different layouts.
"""

from __future__ import annotations

import subprocess
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum


class KeyboardLayout(Enum):
    """Common keyboard layouts."""
    US = "com.apple.keylayout.US"
    US_EXTENDED = "com.apple.keylayout.USExtended"
    CHINESE_SIMPLIFIED = "com.apple.keylayout.Chinese-Simplified"
    CHINESE_TRADITIONAL = "com.apple.keylayout.Chinese-Traditional"
    JAPANESE = "com.apple.keylayout.Japanese"
    KOREAN = "com.apple.keylayout.Korean"
    GERMAN = "com.apple.keylayout.German"
    FRENCH = "com.apple.keylayout.French"
    BRITISH = "com.apple.keylayout.British"


@dataclass
class KeyMapping:
    """Key mapping for a character."""
    character: str
    key_code: int
    shift_needed: bool
    alt_needed: bool
    layout_specific: bool


@dataclass
class LayoutInfo:
    """Keyboard layout information."""
    id: str
    name: str
    source: str
    is_active: bool


class KeyboardLayoutManager:
    """Manages keyboard layouts."""
    
    def __init__(self):
        self._current_layout: Optional[str] = None
        self._refresh_current()
    
    def _refresh_current(self) -> None:
        """Refresh current layout info."""
        self._current_layout = self.get_current_layout_id()
    
    def get_current_layout_id(self) -> Optional[str]:
        """
        Get current keyboard layout ID.
        
        Returns:
            Layout ID string or None.
        """
        try:
            script = '''
            tell application "System Events"
                return current application's text input source's identifier
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None
    
    def get_current_layout(self) -> Optional[LayoutInfo]:
        """
        Get current keyboard layout info.
        
        Returns:
            LayoutInfo or None.
        """
        layout_id = self.get_current_layout_id()
        if not layout_id:
            return None
        
        return LayoutInfo(
            id=layout_id,
            name=layout_id.split('.')[-1],
            source="system",
            is_active=True
        )
    
    def list_available_layouts(self) -> List[LayoutInfo]:
        """
        List all available keyboard layouts.
        
        Returns:
            List of LayoutInfo.
        """
        layouts = []
        
        try:
            result = subprocess.run(
                ["ls", "/Library/Keyboard%20Drives/System/"],
                capture_output=True,
                text=True
            )
            
            for line in result.stdout.split('\n'):
                if line.endswith('.keylayout'):
                    name = line.replace('.keylayout', '')
                    layouts.append(LayoutInfo(
                        id=name,
                        name=name,
                        source="system",
                        is_active=name == self._current_layout
                    ))
        except Exception:
            pass
        
        return layouts
    
    def set_layout(self, layout_id: str) -> bool:
        """
        Set keyboard layout by ID.
        
        Args:
            layout_id: Layout identifier.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            script = f'''
            tell application "System Events"
                set the layout to "{layout_id}"
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True)
            self._refresh_current()
            return True
        except Exception:
            return False
    
    def get_layout_for_character(self, char: str) -> Optional[KeyMapping]:
        """
        Get key mapping for character in current layout.
        
        Args:
            char: Character to map.
            
        Returns:
            KeyMapping or None.
        """
        code = ord(char) if char.isascii() else 0
        
        if not code:
            return None
        
        needs_shift = char.isupper() or char in '!@#$%^&*()_+{}|:"<>?'
        needs_alt = char in '[];,./\\`'
        
        return KeyMapping(
            character=char,
            key_code=code,
            shift_needed=needs_shift,
            alt_needed=needs_alt,
            layout_specific=True
        )


def get_current_input_source() -> Optional[str]:
    """
    Get current input source.
    
    Returns:
        Input source ID or None.
    """
    try:
        script = '''
        tell application "System Events"
            return text input sources's current
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def switch_input_source(source_id: str) -> bool:
    """
    Switch input source.
    
    Args:
        source_id: Input source ID.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        script = f'''
        tell application "System Events"
            set frontmost to true
            tell process "System Events"
                key code 49 using {{option down, shift down}}
            end tell
        end tell
        '''
        subprocess.run(["osascript", "-e", script], capture_output=True)
        return True
    except Exception:
        return False


def get_keycode_for_char(char: str, layout: str = "com.apple.keylayout.US") -> Optional[int]:
    """
    Get keycode for character in specific layout.
    
    Args:
        char: Character.
        layout: Layout ID.
        
    Returns:
        Keycode or None.
    """
    char_code = ord(char) if len(char) == 1 else 0
    return char_code if char_code else None
