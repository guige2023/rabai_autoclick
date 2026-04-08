"""
Menu bar and system menu utilities for macOS automation.

Provides utilities for reading menu bar state, menu items,
and performing menu operations through accessibility APIs.
"""

from __future__ import annotations

import subprocess
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class MenuType(Enum):
    """Types of menus."""
    APPLE = "apple"
    APPLICATION = "application"
    ITEM = "item"
    SUBMENU = "submenu"


@dataclass
class MenuItem:
    """Represents a menu item."""
    title: str
    action: Optional[str] = None
    key_equivalent: Optional[str] = None
    modifiers: int = 0
    enabled: bool = True
    checked: bool = False
    children: List["MenuItem"] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    @property
    def has_submenu(self) -> bool:
        return len(self.children) > 0
    
    @property
    def shortcut_display(self) -> str:
        if not self.key_equivalent:
            return ""
        
        parts = []
        if self.modifiers & 1:  # cmd
            parts.append("⌘")
        if self.modifiers & 2:  # shift
            parts.append("⇧")
        if self.modifiers & 4:  # option
            parts.append("⌥")
        if self.modifiers & 8:  # control
            parts.append("⌃")
        
        parts.append(self.key_equivalent.upper())
        return "+".join(parts)


@dataclass
class MenuBarState:
    """Represents the current state of a menu bar."""
    application_menus: Dict[str, List[MenuItem]] = None
    menu_bar_items: List[str] = None
    
    def __post_init__(self):
        if self.application_menus is None:
            self.application_menus = {}
        if self.menu_bar_items is None:
            self.menu_bar_items = []


class MenuBarReader:
    """Reads menu bar state using accessibility APIs."""
    
    def __init__(self):
        """Initialize menu bar reader."""
        self._cache: Optional[MenuBarState] = None
    
    def get_menu_bar_state(self, force_refresh: bool = False) -> MenuBarState:
        """Get current menu bar state.
        
        Args:
            force_refresh: Force cache refresh
            
        Returns:
            MenuBarState with current menu bar info
        """
        if not force_refresh and self._cache:
            return self._cache
        
        state = MenuBarState()
        
        # Get menu bar items using AppleScript
        try:
            result = subprocess.run(
                ["osascript", "-e", '''
                tell application "System Events"
                    get the name of every menu bar item of menu bar 1
                end tell
                '''],
                capture_output=True,
                text=True,
                timeout=3
            )
            
            if result.returncode == 0:
                items = [item.strip() for item in result.stdout.strip().split(", ")]
                state.menu_bar_items = items
        except Exception:
            pass
        
        # Get frontmost application menus
        try:
            result = subprocess.run(
                ["osascript", "-e", '''
                tell application "System Events"
                    set frontApp to first application process whose frontmost is true
                    get the name of frontApp
                end tell
                '''],
                capture_output=True,
                text=True,
                timeout=3
            )
            
            if result.returncode == 0:
                app_name = result.stdout.strip()
                menus = self._get_application_menus(app_name)
                state.application_menus[app_name] = menus
        except Exception:
            pass
        
        self._cache = state
        return state
    
    def _get_application_menus(self, app_name: str) -> List[MenuItem]:
        """Get menus for a specific application.
        
        Args:
            app_name: Application name
            
        Returns:
            List of MenuItem objects
        """
        menus = []
        
        try:
            script = f'''
            tell application "System Events"
                tell process "{app_name}"
                    get every menu of menu bar 1
                end tell
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=3
            )
            
            if result.returncode == 0:
                # Parse menu names
                menu_names = result.stdout.strip().split(", ")
                for name in menu_names:
                    menu = MenuItem(title=name.strip())
                    menus.append(menu)
        except Exception:
            pass
        
        return menus
    
    def get_menu_item(
        self,
        app_name: str,
        menu_path: List[str]
    ) -> Optional[MenuItem]:
        """Get a specific menu item by path.
        
        Args:
            app_name: Application name
            menu_path: Path to menu item (e.g., ["File", "Open"])
            
        Returns:
            MenuItem if found, None otherwise
        """
        if not menu_path:
            return None
        
        # Navigate through menus
        menus = self._get_application_menus(app_name)
        
        current_items = menus
        for i, path_component in enumerate(menu_path):
            found = None
            for item in current_items:
                if item.title == path_component:
                    if i == len(menu_path) - 1:
                        return item
                    found = item
                    break
            
            if found is None:
                return None
            
            current_items = found.children
        
        return None
    
    def find_menu_item(
        self,
        app_name: str,
        search_text: str
    ) -> Optional[Tuple[List[str], MenuItem]]:
        """Search for a menu item by text.
        
        Args:
            app_name: Application name
            search_text: Text to search for
            
        Returns:
            Tuple of (path, MenuItem) if found
        """
        menus = self._get_application_menus(app_name)
        
        def search_recursive(
            items: List[MenuItem],
            path: List[str]
        ) -> Optional[Tuple[List[str], MenuItem]]:
            for item in items:
                new_path = path + [item.title]
                if search_text.lower() in item.title.lower():
                    return (new_path, item)
                if item.children:
                    result = search_recursive(item.children, new_path)
                    if result:
                        return result
            return None
        
        return search_recursive(menus, [])
    
    def get_menu_bar_item_at_position(self, x: int, y: int) -> Optional[str]:
        """Get menu bar item at screen position.
        
        Args:
            x: Screen X coordinate
            y: Screen Y coordinate
            
        Returns:
            Menu bar item name or None
        """
        state = self.get_menu_bar_state()
        
        # Approximate position calculation
        # This is a simplified implementation
        if y > 20:  # Menu bar is typically 22px tall
            return None
        
        if not state.menu_bar_items:
            return None
        
        # Approximate item width
        item_width = 80
        index = x // item_width
        
        if 0 <= index < len(state.menu_bar_items):
            return state.menu_bar_items[index]
        
        return None


class MenuBarController:
    """Controls menu bar items and menus."""
    
    def __init__(self):
        """Initialize menu bar controller."""
        self.reader = MenuBarReader()
    
    def click_menu_bar_item(self, item_name: str) -> bool:
        """Click a menu bar item to open its menu.
        
        Args:
            item_name: Name of menu bar item
            
        Returns:
            True if successful
        """
        try:
            script = f'''
            tell application "System Events"
                click menu bar item "{item_name}" of menu bar 1
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            return True
        except Exception:
            return False
    
    def open_menu(self, menu_name: str) -> bool:
        """Open a menu by name.
        
        Args:
            menu_name: Name of menu to open
            
        Returns:
            True if successful
        """
        return self.click_menu_bar_item(menu_name)
    
    def close_menu(self) -> bool:
        """Close any open menu."""
        try:
            # Press Escape to close menu
            script = '''
            tell application "System Events"
                key code 53
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False
    
    def select_menu_item(
        self,
        app_name: str,
        menu_path: List[str]
    ) -> bool:
        """Select a menu item by path.
        
        Args:
            app_name: Application name
            menu_path: Path to menu item (e.g., ["File", "Open"])
            
        Returns:
            True if successful
        """
        if not menu_path:
            return False
        
        try:
            # Build AppleScript for menu selection
            path_str = ", ".join([f'"{p}"' for p in menu_path])
            
            script = f'''
            tell application "System Events"
                tell process "{app_name}"
                    click menu item {path_str} of menu 1 of menu bar item "{menu_path[0]}" of menu bar 1
                end tell
            end tell
            '''
            
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            return True
        except Exception:
            return False
    
    def press_menu_shortcut(
        self,
        key: str,
        modifiers: Optional[List[str]] = None
    ) -> bool:
        """Simulate keyboard shortcut equivalent to menu item.
        
        Args:
            key: Key character
            modifiers: List of modifiers (e.g., ["cmd", "shift"])
            
        Returns:
            True if successful
        """
        if modifiers is None:
            modifiers = ["cmd"]
        
        # Build key code mapping
        key_codes = {
            "a": 0, "s": 1, "d": 2, "f": 3, "h": 4, "g": 5, "z": 6, "x": 7,
            "c": 8, "v": 9, "b": 11, "q": 12, "w": 13, "e": 14, "r": 15,
            "y": 16, "t": 17, "1": 18, "2": 19, "3": 20, "4": 21, "6": 22,
            "5": 23, "=": 24, "9": 25, "7": 26, "-": 27, "8": 28, "0": 29,
            "]": 30, "o": 31, "u": 32, "[": 33, "i": 34, "p": 35, "l": 37,
            "j": 38, "'": 39, "k": 40, ";": 41, "\\": 42, ",": 43, "/": 44,
            "n": 45, "m": 46, ".": 47, " ": 49, "`": 50,
        }
        
        key_code = key_codes.get(key.lower())
        if key_code is None:
            return False
        
        try:
            # Build modifiers
            mod_str = ""
            if "cmd" in modifiers or "command" in modifiers:
                mod_str += "command "
            if "shift" in modifiers:
                mod_str += "shift "
            if "option" in modifiers or "alt" in modifiers:
                mod_str += "option "
            if "ctrl" in modifiers or "control" in modifiers:
                mod_str += "control "
            
            script = f'''
            tell application "System Events"
                keystroke "{key}" using {mod_str}down
            end tell
            '''
            
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False
    
    def open_apple_menu(self) -> bool:
        """Open the Apple menu."""
        try:
            script = '''
            tell application "System Events"
                click menu bar item "Apple" of menu bar 1
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False
    
    def open_app_menu(self) -> bool:
        """Open the application menu (next to Apple menu)."""
        state = self.reader.get_menu_bar_state()
        
        if len(state.menu_bar_items) > 1:
            return self.click_menu_bar_item(state.menu_bar_items[1])
        
        return False


def get_menu_shortcuts_for_app(app_name: str) -> Dict[str, str]:
    """Get common menu shortcuts for an application.
    
    Args:
        app_name: Application name
        
    Returns:
        Dictionary of action -> shortcut display
    """
    common_shortcuts = {
        "About": "⌘,",
        "Preferences": "⌘,",
        "Quit": "⌘Q",
        "Hide": "⌘H",
        "Hide Others": "⌥⌘H",
        "Show All": "⌘H",
        "New Window": "⌘N",
        "New Tab": "⌘T",
        "Open": "⌘O",
        "Close": "⌘W",
        "Save": "⌘S",
        "Print": "⌘P",
        "Undo": "⌘Z",
        "Redo": "⇧⌘Z",
        "Cut": "⌘X",
        "Copy": "⌘C",
        "Paste": "⌘V",
        "Select All": "⌘A",
        "Find": "⌘F",
        "Replace": "⌥⌘F",
    }
    
    # Filter based on what's available
    available = {}
    
    try:
        controller = MenuBarController()
        for action in common_shortcuts:
            result = controller.reader.find_menu_item(app_name, action)
            if result:
                available[action] = common_shortcuts[action]
    except Exception:
        # Return all common shortcuts if we can't check
        return common_shortcuts
    
    return available
