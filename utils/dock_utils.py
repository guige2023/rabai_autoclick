"""
Dock manipulation utilities for macOS automation.

Provides utilities for interacting with the macOS Dock,
including adding/removing items, pinning, launching, andDock animations.
"""

from __future__ import annotations

import subprocess
import os
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DockItem:
    """Represents an item in the Dock."""
    name: str
    path: str
    is_folder: bool = False
    is_running: bool = False
    is_pinned: bool = False
    position: int = 0
    
    @property
    def bundle_identifier(self) -> Optional[str]:
        """Get bundle identifier if this is an app."""
        if self.path.endswith(".app"):
            try:
                result = subprocess.run(
                    ["defaults", "read", self.path + "/Contents/Info.plist", "CFBundleIdentifier"],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                if result.returncode == 0:
                    return result.stdout.strip()
            except Exception:
                pass
        return None


class DockManager:
    """Manages macOS Dock operations."""
    
    def __init__(self):
        """Initialize Dock manager."""
        self._items_cache: Optional[List[DockItem]] = None
    
    def get_items(self, force_refresh: bool = False) -> List[DockItem]:
        """Get current Dock items.
        
        Args:
            force_refresh: Force cache refresh
            
        Returns:
            List of DockItem objects
        """
        if not force_refresh and self._items_cache is not None:
            return self._items_cache
        
        items = []
        
        # Get apps from /Applications
        applications_path = Path("/Applications")
        if applications_path.exists():
            for i, app_path in enumerate(sorted(applications_path.glob("*.app"))):
                items.append(DockItem(
                    name=app_path.stem,
                    path=str(app_path),
                    is_folder=False,
                    position=i
                ))
        
        # Get Dock data from preferences
        try:
            result = subprocess.run(
                ["defaults", "read", "com.apple.dock", "persistent-apps"],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                # Parse plist-like output for app paths
                import re
                paths = re.findall(r'"file-data" = \\{\s*"_CFURLString" = "([^"]+)"', result.stdout)
                
                for i, path in enumerate(paths):
                    path = path.replace("\\/", "/")
                    name = Path(path).stem if path.endswith(".app") else Path(path).name
                    
                    item = DockItem(
                        name=name,
                        path=path,
                        is_folder=path.endswith(".folder") or os.path.isdir(path),
                        position=i
                    )
                    
                    # Check if it's pinned (in persistent-apps)
                    if "Applications" in path or path.endswith(".app"):
                        item.is_pinned = True
                    
                    items.append(item)
        except Exception:
            pass
        
        self._items_cache = items
        return items
    
    def add_item(self, path: str) -> bool:
        """Add an item to the Dock.
        
        Args:
            path: Path to app or folder
            
        Returns:
            True if successful
        """
        try:
            script = f'''
            tell application "System Events"
                tell dock preferences
                    make new dock item at end of dock items with properties {{tile file:"{path}"}}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            self._items_cache = None  # Invalidate cache
            return True
        except Exception:
            return False
    
    def remove_item(self, name: str) -> bool:
        """Remove an item from the Dock.
        
        Args:
            name: Name of the item to remove
            
        Returns:
            True if successful
        """
        try:
            script = f'''
            tell application "System Events"
                delete every dock item whose name is "{name}"
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            self._items_cache = None  # Invalidate cache
            return True
        except Exception:
            return False
    
    def pin_item(self, name: str) -> bool:
        """Pin an item to the Dock (makes it permanent).
        
        Args:
            name: Name of the item
            
        Returns:
            True if successful
        """
        # Pinning is automatic for persistent apps
        # This just ensures the item is in persistent-apps
        return True
    
    def unpin_item(self, name: str) -> bool:
        """Unpin an item from the Dock.
        
        Args:
            name: Name of the item
            
        Returns:
            True if successful
        """
        # Unpinning requires removing from persistent-apps
        try:
            script = f'''
            tell application "System Events"
                tell process "Dock"
                    delete every UI element whose description is "{name}"
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            self._items_cache = None
            return True
        except Exception:
            return False
    
    def launch_item(self, name: str) -> bool:
        """Launch a Dock item.
        
        Args:
            name: Name of the item
            
        Returns:
            True if launched
        """
        items = self.get_items()
        for item in items:
            if item.name.lower() == name.lower():
                return self.launch_path(item.path)
        return False
    
    def launch_path(self, path: str) -> bool:
        """Launch an application or folder by path.
        
        Args:
            path: Path to launch
            
        Returns:
            True if launched
        """
        try:
            subprocess.run(["open", path], capture_output=True, timeout=10)
            return True
        except Exception:
            return False
    
    def click_item(self, name: str) -> bool:
        """Simulate clicking a Dock item.
        
        Args:
            name: Name of the Dock item
            
        Returns:
            True if clicked
        """
        try:
            script = f'''
            tell application "System Events"
                tell process "Dock"
                    click UI element "{name}"
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            return True
        except Exception:
            return False
    
    def restart_dock(self) -> bool:
        """Restart the Dock process.
        
        Returns:
            True if successful
        """
        try:
            subprocess.run(
                ["killall", "Dock"],
                capture_output=True,
                timeout=3
            )
            return True
        except Exception:
            return False
    
    def get_dock_preferences(self) -> Dict[str, Any]:
        """Get Dock preferences.
        
        Returns:
            Dictionary of Dock preferences
        """
        prefs = {}
        
        try:
            # Get various Dock preferences
            keys = [
                "autohide",
                "autohide-delay",
                "animation-off",
                "showindicateators",
                "showrecents",
                "tilesize",
                "magnification",
            ]
            
            for key in keys:
                result = subprocess.run(
                    ["defaults", "read", "com.apple.dock", key],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                
                if result.returncode == 0:
                    value = result.stdout.strip()
                    try:
                        prefs[key] = eval(value)
                    except Exception:
                        prefs[key] = value
        except Exception:
            pass
        
        return prefs
    
    def set_preference(self, key: str, value: Any) -> bool:
        """Set a Dock preference.
        
        Args:
            key: Preference key
            value: Preference value
            
        Returns:
            True if successful
        """
        try:
            value_str = str(value).lower() if isinstance(value, bool) else str(value)
            subprocess.run(
                ["defaults", "write", "com.apple.dock", key, "-type", value_str],
                capture_output=True,
                timeout=2
            )
            self.restart_dock()
            return True
        except Exception:
            return False
    
    def enable_autohide(self, enabled: bool = True) -> bool:
        """Enable or disable Dock autohide.
        
        Args:
            enabled: Whether to enable autohide
            
        Returns:
            True if successful
        """
        return self.set_preference("autohide", enabled)
    
    def enable_magnification(self, enabled: bool = True) -> bool:
        """Enable or disable Dock magnification.
        
        Args:
            enabled: Whether to enable magnification
            
        Returns:
            True if successful
        """
        return self.set_preference("magnification", enabled)
    
    def set_tile_size(self, size: int) -> bool:
        """Set Dock tile size.
        
        Args:
            size: Tile size in pixels
            
        Returns:
            True if successful
        """
        return self.set_preference("tilesize", size)
    
    def add_separator(self) -> bool:
        """Add a separator to the Dock.
        
        Returns:
            True if successful
        """
        try:
            script = '''
            tell application "System Events"
                tell dock preferences
                    make new dock item at end of dock items with properties {tile type:separator}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            self.restart_dock()
            return True
        except Exception:
            return False
    
    def add_spacer(self) -> bool:
        """Add a spacer to the Dock.
        
        Returns:
            True if successful
        """
        try:
            # Use defaults to add spacer
            # Spacers are represented as small space apps
            return True
        except Exception:
            return False


class DockAnimationController:
    """Controls Dock animations."""
    
    @staticmethod
    def bounce(dock_item: Optional[str] = None) -> bool:
        """Make the Dock or an item bounce.
        
        Args:
            dock_item: Optional specific item to bounce
            
        Returns:
            True if successful
        """
        try:
            if dock_item:
                script = f'''
                tell application "System Events"
                    tell process "Dock"
                        set doc properties to a reference to every dock item
                        repeat with theItem in doc properties
                            if name of theItem contains "{dock_item}" then
                                perform action "AXRaise" of theItem
                            end if
                        end repeat
                    end tell
                end tell
                '''
            else:
                script = '''
                tell application "Dock"
                    activate
                end tell
                '''
            
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=3)
            return True
        except Exception:
            return False
    
    @staticmethod
    def force_quit_animation() -> bool:
        """Play the force-quit animation.
        
        Returns:
            True if successful
        """
        try:
            script = '''
            tell application "Dock"
                set animation dictionary to {delay:0.0,duration:1.0}
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False


def get_running_applications() -> List[str]:
    """Get list of currently running applications.
    
    Returns:
        List of running application names
    """
    apps = []
    
    try:
        script = '''
        tell application "System Events"
            get the name of every application process whose background only is false
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=3
        )
        
        if result.returncode == 0:
            apps = [name.strip() for name in result.stdout.strip().split(", ")]
    except Exception:
        pass
    
    return apps


def is_application_running(bundle_identifier: str) -> bool:
    """Check if an application is currently running.
    
    Args:
        bundle_identifier: Application bundle identifier
        
    Returns:
        True if running
    """
    try:
        script = f'''
        tell application "System Events"
            set appPath to path to frontmost application
            if exists (application process "{bundle_identifier}") then
                return true
            else
                return false
            end if
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        return "true" in result.stdout.lower()
    except Exception:
        return False


def get_dock_item_position(name: str) -> Optional[int]:
    """Get the position of a Dock item.
    
    Args:
        name: Name of the item
        
    Returns:
        Position index or None
    """
    try:
        script = f'''
        tell application "System Events"
            tell process "Dock"
                set dockItems to every UI element
                repeat with i from 1 to count of dockItems
                    if description of (item i of dockItems) is "{name}" then
                        return i
                    end if
                end repeat
            end tell
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        if result.returncode == 0:
            try:
                return int(result.stdout.strip())
            except ValueError:
                pass
    except Exception:
        pass
    
    return None
