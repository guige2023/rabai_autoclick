"""
Keyboard layout detection and switching utilities.

Provides utilities for detecting current keyboard layout,
switching between layouts, and handling international keyboards.
"""

from __future__ import annotations

import subprocess
import re
from typing import List, Optional, Dict, Callable
from dataclasses import dataclass, field


@dataclass
class KeyboardLayoutInfo:
    """Information about a keyboard layout."""
    id: str
    name: str
    localized_name: str
    source: str = "unknown"
    
    @property
    def is_asian_ime(self) -> bool:
        """Check if this is an Asian input method."""
        asian_names = ["pinyin", "wubi", "zhuyin", "hangul", "hiragana", "katakana", "kana", "chinese", "japanese", "korean"]
        return any(name in self.name.lower() for name in asian_names)


class KeyboardLayoutDetector:
    """Detects and tracks keyboard layout changes."""
    
    def __init__(self):
        """Initialize detector."""
        self._callbacks: List[Callable[[KeyboardLayoutInfo], None]] = []
        self._last_layout: Optional[KeyboardLayoutInfo] = None
        self._cache: Dict[str, KeyboardLayoutInfo] = {}
    
    def get_current_layout(self) -> KeyboardLayoutInfo:
        """Get the currently active keyboard layout.
        
        Returns:
            KeyboardLayoutInfo for the current layout
        """
        try:
            # Use macOS defaults to get current layout
            result = subprocess.run(
                ["defaults", "read", "com.apple.HIToolbox", "AppleCurrentKeyboardLayoutInputSourceID"],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                layout_id = result.stdout.strip()
                
                # Get all layouts to find the name
                layouts = self.get_available_layouts()
                for layout in layouts:
                    if layout.id == layout_id or layout_id.endswith(layout.id):
                        self._last_layout = layout
                        return layout
                
                # Return with unknown name if not found
                return KeyboardLayoutInfo(
                    id=layout_id,
                    name="Unknown",
                    localized_name=layout_id
                )
        except Exception:
            pass
        
        # Fallback to keyboard input source
        try:
            result = subprocess.run(
                ["osascript", "-e", 
                 'tell application "System Events" to key code 0 using command down'],
                capture_output=True,
                timeout=2
            )
        except Exception:
            pass
        
        return KeyboardLayoutInfo(
            id="com.apple.keylayout.US",
            name="U.S.",
            localized_name="U.S. English"
        )
    
    def get_available_layouts(self) -> List[KeyboardLayoutInfo]:
        """Get all available keyboard layouts.
        
        Returns:
            List of available keyboard layouts
        """
        layouts = []
        
        try:
            # Use macOS to get all input sources
            result = subprocess.run(
                ["osascript", "-e", '''
                tell application "System Events"
                    get the name of every file of folder "Keyboard Layouts" of folder "Resources" of folder "System" of folder "Library" of startup disk
                end tell
                '''],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                for name in result.stdout.strip().split(", "):
                    if name.endswith(".keylayout"):
                        layout_name = name.replace(".keylayout", "")
                        layouts.append(KeyboardLayoutInfo(
                            id=f"com.apple.keylayout.{layout_name}",
                            name=layout_name,
                            localized_name=layout_name
                        ))
        except Exception:
            pass
        
        # Add common layouts if detection failed
        common_layouts = [
            ("com.apple.keylayout.US", "U.S.", "U.S. English"),
            ("com.apple.keylayout.ABC", "ABC", "ABC"),
            ("com.apple.keylayout.French", "French", "French"),
            ("com.apple.keylayout.German", "German", "German"),
            ("com.apple.keylayout.Spanish", "Spanish", "Spanish"),
            ("com.apple.keylayout.Italian", "Italian", "Italian"),
            ("com.apple.keylayout.Portuguese", "Portuguese", "Portuguese"),
            ("com.apple.keylayout.Russian", "Russian", "Russian"),
            ("com.apple.keylayout.Japanese", "Japanese", "Japanese"),
            ("com.apple.keylayout.TraditionalChinese", "Chinese", "Traditional Chinese"),
            ("com.apple.keylayout.SimplifiedChinese", "Chinese", "Simplified Chinese"),
            ("com.apple.keylayout.Korean", "Korean", "Korean"),
        ]
        
        for layout_id, name, localized in common_layouts:
            found = False
            for existing in layouts:
                if existing.id == layout_id:
                    found = True
                    break
            if not found:
                layouts.append(KeyboardLayoutInfo(
                    id=layout_id,
                    name=name,
                    localized_name=localized
                ))
        
        return layouts
    
    def switch_layout(self, layout_id: str) -> bool:
        """Switch to a specific keyboard layout.
        
        Args:
            layout_id: Layout identifier (e.g., 'com.apple.keylayout.US')
            
        Returns:
            True if switch was successful
        """
        try:
            # Use AppleScript to switch layout
            script = f'''
            tell application "System Events"
                set the enabled input sources to a reference to every file of folder "{layout_id}" of folder "Keyboard Layouts" of folder "Resources" of folder "System" of folder "Library" of startup disk
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            pass
        
        # Try alternative method using defaults
        try:
            subprocess.run(
                ["defaults", "write", "-g", "AppleInputSourceHistory", 
                 f'-dict-add "KeyboardLayout ID" -int 0'],
                capture_output=True,
                timeout=2
            )
        except Exception:
            pass
        
        return False
    
    def next_layout(self) -> KeyboardLayoutInfo:
        """Switch to the next keyboard layout.
        
        Returns:
            New current layout
        """
        layouts = self.get_available_layouts()
        if not layouts:
            return self.get_current_layout()
        
        current = self.get_current_layout()
        
        # Find current index
        current_idx = -1
        for i, layout in enumerate(layouts):
            if layout.id == current.id:
                current_idx = i
                break
        
        # Get next layout
        next_idx = (current_idx + 1) % len(layouts)
        next_layout = layouts[next_idx]
        
        self.switch_layout(next_layout.id)
        return next_layout
    
    def previous_layout(self) -> KeyboardLayoutInfo:
        """Switch to the previous keyboard layout.
        
        Returns:
            New current layout
        """
        layouts = self.get_available_layouts()
        if not layouts:
            return self.get_current_layout()
        
        current = self.get_current_layout()
        
        # Find current index
        current_idx = -1
        for i, layout in enumerate(layouts):
            if layout.id == current.id:
                current_idx = i
                break
        
        # Get previous layout
        prev_idx = (current_idx - 1) % len(layouts)
        prev_layout = layouts[prev_idx]
        
        self.switch_layout(prev_layout.id)
        return prev_layout
    
    def on_layout_change(self, callback: Callable[[KeyboardLayoutInfo], None]) -> None:
        """Register a callback for layout changes.
        
        Args:
            callback: Function to call when layout changes
        """
        self._callbacks.append(callback)
    
    def check_for_changes(self) -> Optional[KeyboardLayoutInfo]:
        """Check if layout has changed since last call.
        
        Returns:
            New layout if changed, None otherwise
        """
        current = self.get_current_layout()
        
        if self._last_layout is None or current.id != self._last_layout.id:
            self._last_layout = current
            
            for callback in self._callbacks:
                try:
                    callback(current)
                except Exception:
                    pass
            
            return current
        
        return None


class KeyboardLayoutSwitcher:
    """High-level keyboard layout switching with context management."""
    
    def __init__(self):
        """Initialize switcher."""
        self.detector = KeyboardLayoutDetector()
        self._saved_layout: Optional[KeyboardLayoutInfo] = None
        self._context_stack: List[KeyboardLayoutInfo] = []
    
    def save_current(self) -> KeyboardLayoutInfo:
        """Save current layout for later restoration.
        
        Returns:
            The saved layout
        """
        self._saved_layout = self.detector.get_current_layout()
        return self._saved_layout
    
    def restore(self) -> bool:
        """Restore the previously saved layout.
        
        Returns:
            True if restoration was successful
        """
        if self._saved_layout is None:
            return False
        
        result = self.detector.switch_layout(self._saved_layout.id)
        return result
    
    def push_layout(self, layout_id: str) -> bool:
        """Push a layout onto the stack (for temporary switching).
        
        Args:
            layout_id: Layout to switch to
            
        Returns:
            True if switch was successful
        """
        # Save current before switching
        current = self.detector.get_current_layout()
        self._context_stack.append(current)
        
        return self.detector.switch_layout(layout_id)
    
    def pop_layout(self) -> Optional[KeyboardLayoutInfo]:
        """Pop the last layout from the stack and switch to it.
        
        Returns:
            The restored layout, or None if stack was empty
        """
        if not self._context_stack:
            return None
        
        layout = self._context_stack.pop()
        self.detector.switch_layout(layout.id)
        return layout
    
    def with_layout(self, layout_id: str) -> Callable:
        """Decorator context manager for temporary layout switching.
        
        Args:
            layout_id: Layout to switch to during execution
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                self.push_layout(layout_id)
                try:
                    return func(*args, **kwargs)
                finally:
                    self.pop_layout()
            return wrapper
        return decorator


def get_key_for_keycode(keycode: int, layout: Optional[str] = None) -> str:
    """Get the key character for a macOS keycode in the given layout.
    
    Args:
        keycode: macOS keycode (0-127)
        layout: Optional layout identifier
        
    Returns:
        Key character or keycode string
    """
    # Common keycode mappings for US layout
    KEYCODE_MAP = {
        0: "a", 1: "s", 2: "d", 3: "f", 4: "h", 5: "g", 6: "z", 7: "x",
        8: "c", 9: "v", 11: "b", 12: "q", 13: "w", 14: "e", 15: "r",
        16: "y", 17: "t", 18: "1", 19: "2", 20: "3", 21: "4", 22: "6",
        23: "5", 24: "=", 25: "9", 26: "7", 27: "-", 28: "8", 29: "0",
        30: "]", 31: "o", 32: "u", 33: "[", 34: "i", 35: "p", 36: "return",
        37: "l", 38: "j", 39: "'", 40: "k", 41: ";", 42: "\\", 43: ",",
        44: "/", 45: "n", 46: "m", 47: ".", 48: "tab", 49: "space",
        50: "`", 51: "delete", 52: "enter", 53: "escape",
        56: "shift", 57: "capslock", 58: "option", 59: "control",
        60: "rightShift", 61: "rightOption", 62: "rightControl", 63: "function",
    }
    
    # Number pad
    NUMPAD_MAP = {
        82: "0", 83: "1", 84: "2", 85: "3", 86: "4", 87: "5", 88: "6",
        89: "7", 91: "8", 92: "9", 96: "+", 85: "/", 87: "*", 89: "-",
    }
    
    if keycode in KEYCODE_MAP:
        return KEYCODE_MAP[keycode]
    if keycode in NUMPAD_MAP:
        return NUMPAD_MAP[keycode]
    
    return f"keycode_{keycode}"


def parse_layout_from_system() -> Dict[str, any]:
    """Parse keyboard layout information from system.
    
    Returns:
        Dictionary with layout information
    """
    info = {
        "layouts": [],
        "current": None,
        "input_methods": [],
    }
    
    # Get current input source
    try:
        result = subprocess.run(
            ["defaults", "read", "com.apple.HIToolbox", "AppleSelectedInputSources"],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        if result.returncode == 0:
            # Parse the plist-like output
            content = result.stdout
            layouts = re.findall(r'TRSInputSourceID" = "([^"]+)"', content)
            info["input_methods"] = layouts
            if layouts:
                info["current"] = layouts[0]
    except Exception:
        pass
    
    # Get all input sources
    try:
        result = subprocess.run(
            ["defaults", "read", "com.apple.HIToolbox", "AppleInputSourceHistory"],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        if result.returncode == 0:
            content = result.stdout
            all_layouts = re.findall(r'TRSInputSourceID" = "([^"]+)"', content)
            info["layouts"] = all_layouts
    except Exception:
        pass
    
    return info


def is_ime_active() -> bool:
    """Check if an Input Method Editor (IME) is currently active.
    
    Returns:
        True if IME is active
    """
    try:
        # Check if any Asian input method is selected
        result = subprocess.run(
            ["defaults", "read", "com.apple.HIToolbox", "AppleCurrentKeyboardLayoutInputSourceID"],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        if result.returncode == 0:
            layout_id = result.stdout.strip().lower()
            asian_imes = ["pinyin", "wubi", "zhuyin", "hangul", "hiragana", "katakana", "kana"]
            return any(ime in layout_id for ime in asian_imes)
    except Exception:
        pass
    
    return False
