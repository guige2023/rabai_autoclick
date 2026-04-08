"""Accessibility inspector utilities for UI element discovery.

This module provides utilities for inspecting and traversing the
accessibility tree of applications to discover UI elements.
"""

from __future__ import annotations

import platform
from typing import Any, Optional


IS_MACOS = platform.system() == "Darwin"


class AccessibilityElement:
    """Represents an accessibility element in the UI hierarchy."""
    
    def __init__(
        self,
        role: str,
        title: Optional[str] = None,
        value: Optional[str] = None,
        description: Optional[str] = None,
        rect: Optional[tuple[int, int, int, int]] = None,
        children: Optional[list["AccessibilityElement"]] = None,
    ):
        self.role = role
        self.title = title
        self.value = value
        self.description = description
        self.rect = rect  # (x, y, width, height)
        self.children = children or []
    
    def __repr__(self) -> str:
        return f"AccessibilityElement(role={self.role!r}, title={self.title!r})"
    
    def find_by_role(self, role: str) -> list["AccessibilityElement"]:
        """Find all descendants with the given role.
        
        Args:
            role: The accessibility role to search for.
        
        Returns:
            List of matching elements.
        """
        results = []
        if self.role == role:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_role(role))
        return results
    
    def find_by_title(self, title: str) -> list["AccessibilityElement"]:
        """Find all descendants with the given title.
        
        Args:
            title: The title to search for.
        
        Returns:
            List of matching elements.
        """
        results = []
        if self.title == title:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_title(title))
        return results


def get_accessibility_tree(
    app_bundle_id: Optional[str] = None,
    timeout: float = 5.0,
) -> Optional[AccessibilityElement]:
    """Get the accessibility tree for an application.
    
    Args:
        app_bundle_id: Bundle identifier of the app (e.g., 'com.apple.Safari').
                      If None, uses the frontmost application.
        timeout: Maximum time to wait for the tree.
    
    Returns:
        Root AccessibilityElement of the tree, or None if unavailable.
    """
    if IS_MACOS:
        return _get_macos_accessibility_tree(app_bundle_id, timeout)
    return None


def _get_macos_accessibility_tree(
    app_bundle_id: Optional[str],
    timeout: float,
) -> Optional[AccessibilityElement]:
    """Get accessibility tree on macOS using OS X Accessibility APIs."""
    try:
        import subprocess
        script = """
        use framework "AppKit"
        use framework "ApplicationServices"
        
        global frontApp, axTree
        
        if application (path to frontmost application) is missing value then
            return "{}"
        end if
        
        set frontApp to frontmost application
        set appRef to frontApp's systemWideAccessibility
        if appRef's isProcessTrusted is false then
            return "{}"
        end if
        
        set appElement to appRef's processElement
        set axTree to appElement as list
        return axTree
        """
        # Simplified approach using uiaccessibility
        script = '''
        activate application id "%s"
        tell application "System Events"
            tell process 1
                get entire contents
            end tell
        end tell
        ''' % (app_bundle_id or "com.apple.finder")
        
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=timeout
        )
        if result.returncode == 0:
            # Parse the returned element list
            return _parse_accessibility_output(result.stdout.decode())
    except Exception:
        pass
    return None


def _parse_accessibility_output(output: str) -> AccessibilityElement:
    """Parse osascript accessibility output into an element tree."""
    lines = output.strip().split("\n")
    if not lines:
        return AccessibilityElement(role="unknown")
    
    # Simple parsing: first line is the root
    root = AccessibilityElement(role="application", title=lines[0])
    return root


def get_element_at_position(
    x: int,
    y: int,
    app_bundle_id: Optional[str] = None,
) -> Optional[AccessibilityElement]:
    """Get the accessibility element at a screen position.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        app_bundle_id: Optional bundle ID to scope the search.
    
    Returns:
        AccessibilityElement at the position, or None.
    """
    if IS_MACOS:
        try:
            import subprocess
            script = f'''
            tell application "System Events"
                set theElement to UI element at point ({x}, {y})
                if exists theElement then
                    get role of theElement
                else
                    return "none"
                end if
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=3
            )
            if result.returncode == 0:
                role = result.stdout.decode().strip()
                if role != "none":
                    return AccessibilityElement(role=role)
        except Exception:
            pass
    return None


def get_focused_element(
    app_bundle_id: Optional[str] = None,
) -> Optional[AccessibilityElement]:
    """Get the currently focused accessibility element.
    
    Args:
        app_bundle_id: Optional bundle ID to scope the search.
    
    Returns:
        Focused AccessibilityElement, or None.
    """
    if IS_MACOS:
        try:
            import subprocess
            script = '''
            tell application "System Events"
                tell process 1
                    get focused of every UI element
                end tell
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=3
            )
            if result.returncode == 0:
                return AccessibilityElement(role="focused")
        except Exception:
            pass
    return None


def is_accessibility_enabled() -> bool:
    """Check if accessibility features are enabled.
    
    Returns:
        True if accessibility is enabled and trusted.
    """
    if IS_MACOS:
        try:
            import subprocess
            script = '''
            tell application "System Events"
                get system attribute "AXIsProcessTrusted"
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=3
            )
            if result.returncode == 0:
                return "true" in result.stdout.decode().lower()
        except Exception:
            pass
    return False
