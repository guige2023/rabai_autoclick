"""Focus ring utilities for macOS accessibility focus management.

This module provides utilities for managing the macOS focus ring,
which indicates which UI element currently has keyboard focus.
"""

from __future__ import annotations

import platform
from typing import Optional


IS_MACOS = platform.system() == "Darwin"


def get_focus_ring_info() -> dict:
    """Get information about the current focus ring state.
    
    Returns:
        Dictionary with focus ring information.
    """
    if not IS_MACOS:
        return {"supported": False}
    
    try:
        import subprocess
        script = '''
        tell application "System Events"
            tell process 1
                try
                    set focusedElement to (first UI element whose value of attribute "AXFocused" is true)
                    set focusedRole to role of focusedElement
                    set focusedTitle to title of focusedElement
                    return focusedRole & "|" & focusedTitle
                on error
                    return "none|none"
                end try
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
            parts = result.stdout.strip().split("|")
            if len(parts) >= 2:
                return {
                    "supported": True,
                    "focused_role": parts[0],
                    "focused_title": parts[1],
                }
    except Exception:
        pass
    
    return {"supported": True, "focused_role": "unknown", "focused_title": ""}


def set_focus_to_element(
    element_role: str,
    element_title: Optional[str] = None,
) -> bool:
    """Attempt to set focus to a specific UI element.
    
    Args:
        element_role: The role of the element (e.g., 'AXButton', 'AXTextField').
        element_title: Optional title of the element.
    
    Returns:
        True if focus was set successfully.
    """
    if not IS_MACOS:
        return False
    
    try:
        import subprocess
        if element_title:
            script = f'''
            tell application "System Events"
                tell process 1
                    set targetElement to (first UI element whose role is "{element_role}" and title is "{element_title}")
                    if exists targetElement then
                        set focused of targetElement to true
                        return "success"
                    else
                        return "not found"
                    end if
                end tell
            end tell
            '''
        else:
            script = f'''
            tell application "System Events"
                tell process 1
                    set targetElement to (first UI element whose role is "{element_role}")
                    if exists targetElement then
                        set focused of targetElement to true
                        return "success"
                    else
                        return "not found"
                    end if
                end tell
            end tell
            '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=5
        )
        return "success" in result.stdout.decode()
    except Exception:
        return False


def is_focus_ring_visible() -> bool:
    """Check if the focus ring is visible.
    
    Returns:
        True if focus ring is enabled in accessibility settings.
    """
    if not IS_MACOS:
        return False
    
    try:
        import subprocess
        # Check system preferences for keyboard navigation
        result = subprocess.run(
            ["defaults", "read", "com.apple.universalaccess", "showFocusRing"],
            capture_output=True,
            text=True,
            timeout=3
        )
        return "1" in result.stdout
    except Exception:
        return True  # Assume visible by default
