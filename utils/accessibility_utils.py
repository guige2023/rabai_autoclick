"""Accessibility utilities for macOS accessibility API interactions.

This module provides helpers for working with macOS Accessibility APIs,
including element discovery, attribute inspection, and AXUIElement
operations commonly needed for GUI automation.

Example:
    >>> from utils.accessibility_utils import get_element_at_point, get_element_role
    >>> element = get_element_at_point(500, 300)
    >>> role = get_element_role(element)
    >>> print(f"Element role: {role}")
"""

from __future__ import annotations

import sys
from typing import Any, Optional

__all__ = [
    "get_element_at_point",
    "get_element_role",
    "get_element_title",
    "get_element_value",
    "get_element_children",
    "get_focused_element",
    "is_element_enabled",
    "get_element_rect",
    "set_element_value",
    "get_all_windows",
    "get_application_element",
    "AccessibilityError",
]


class AccessibilityError(Exception):
    """Raised when an accessibility operation fails."""

    pass


# Requires ApplicationServices framework - only available on macOS
if sys.platform == "darwin":
    try:
        from ApplicationServices import (
            AXUIElementCopyAttributeValue,
            AXUIElementCopyElementAtPosition,
            AXUIElementCreateSystemWide,
            AXUIElementGetAttributeValue,
            AXUIElementGetPid,
            AXUIElementSetAttributeValue,
            kAXChildrenAttribute,
            kAXEnabledAttribute,
            kAXFocusedAttribute,
            kAXFocusedUIElementAttribute,
            kAXParentAttribute,
            kAXPositionAttribute,
            kAXRoleAttribute,
            kAXSizeAttribute,
            kAXTitleAttribute,
            kAXValueAttribute,
            kAXWindowsAttribute,
        )

        _AX_AVAILABLE = True
    except ImportError:
        _AX_AVAILABLE = False
else:
    _AX_AVAILABLE = False


def _check_availability() -> None:
    """Verify accessibility API is available."""
    if not _AX_AVAILABLE:
        raise AccessibilityError(
            "Accessibility API not available. Requires macOS with ApplicationServices."
        )


def get_element_at_point(x: float, y: float) -> Any:
    """Get the accessibility element at the given screen coordinates.

    Args:
        x: Screen X coordinate (from left).
        y: Screen Y coordinate (from top).

    Returns:
        AXUIElement at the specified point, or None if no element found.

    Raises:
        AccessibilityError: If the accessibility API is unavailable.
    """
    _check_availability()
    system_wide = AXUIElementCreateSystemWide()
    element = AXUIElementCopyElementAtPosition(system_wide, 1.0, x, y)
    if element[1] is not None:
        return element[1]
    return None


def get_element_role(element: Any) -> Optional[str]:
    """Get the accessibility role of an element.

    Args:
        element: AXUIElement to query.

    Returns:
        Role string (e.g., 'AXButton', 'AXTextField') or None.
    """
    _check_availability()
    if element is None:
        return None
    role = AXUIElementCopyAttributeValue(element, kAXRoleAttribute, None)
    if role[1] is not None:
        return role[1]
    return None


def get_element_title(element: Any) -> Optional[str]:
    """Get the title attribute of an accessibility element.

    Args:
        element: AXUIElement to query.

    Returns:
        Title string or None.
    """
    _check_availability()
    if element is None:
        return None
    title = AXUIElementCopyAttributeValue(element, kAXTitleAttribute, None)
    if title[1] is not None:
        return title[1]
    return None


def get_element_value(element: Any) -> Optional[str]:
    """Get the value attribute of an accessibility element.

    Args:
        element: AXUIElement to query.

    Returns:
        Value string or None.
    """
    _check_availability()
    if element is None:
        return None
    value = AXUIElementCopyAttributeValue(element, kAXValueAttribute, None)
    if value[1] is not None:
        return value[1]
    return None


def get_element_children(element: Any) -> list[Any]:
    """Get the children of an accessibility element.

    Args:
        element: AXUIElement to query.

    Returns:
        List of child AXUIElements.
    """
    _check_availability()
    if element is None:
        return []
    children = AXUIElementCopyAttributeValue(element, kAXChildrenAttribute, None)
    if children[1] is not None:
        return list(children[1])
    return []


def get_focused_element() -> Any:
    """Get the currently focused accessibility element.

    Returns:
        Focused AXUIElement or None.
    """
    _check_availability()
    system_wide = AXUIElementCreateSystemWide()
    focused = AXUIElementCopyAttributeValue(
        system_wide, kAXFocusedUIElementAttribute, None
    )
    if focused[1] is not None:
        return focused[1]
    return None


def is_element_enabled(element: Any) -> bool:
    """Check if an accessibility element is enabled.

    Args:
        element: AXUIElement to query.

    Returns:
        True if enabled, False otherwise.
    """
    _check_availability()
    if element is None:
        return False
    enabled = AXUIElementCopyAttributeValue(element, kAXEnabledAttribute, None)
    if enabled[1] is not None:
        return bool(enabled[1])
    return False


def get_element_rect(element: Any) -> Optional[tuple[float, float, float, float]]:
    """Get the frame rectangle of an accessibility element.

    Args:
        element: AXUIElement to query.

    Returns:
        Tuple of (x, y, width, height), or None if unavailable.
    """
    _check_availability()
    if element is None:
        return None

    pos = AXUIElementCopyAttributeValue(element, kAXPositionAttribute, None)
    size = AXUIElementCopyAttributeValue(element, kAXSizeAttribute, None)

    if pos[1] is not None and size[1] is not None:
        pos_val = pos[1]
        size_val = size[1]
        return (pos_val.x, pos_val.y, size_val.width, size_val.height)
    return None


def set_element_value(element: Any, value: str) -> bool:
    """Set the value of an accessibility element (e.g., text field).

    Args:
        element: AXUIElement to modify.
        value: Value to set.

    Returns:
        True if successful, False otherwise.
    """
    _check_availability()
    if element is None:
        return False
    result = AXUIElementSetAttributeValue(element, kAXValueAttribute, value)
    return result[0] == 0


def get_all_windows(pid: int) -> list[Any]:
    """Get all windows for a process ID.

    Args:
        pid: Process identifier.

    Returns:
        List of window AXUIElements.
    """
    _check_availability()
    app = AXUIElementCreateApplication(pid)
    windows = AXUIElementCopyAttributeValue(app, kAXWindowsAttribute, None)
    if windows[1] is not None:
        return list(windows[1])
    return []


def get_application_element(pid: int) -> Any:
    """Get the application element for a process ID.

    Args:
        pid: Process identifier.

    Returns:
        Application AXUIElement.
    """
    _check_availability()
    return AXUIElementCreateApplication(pid)
