"""Widget inspector utilities for examining UI widget properties.

This module provides utilities for inspecting and querying properties
of UI widgets in applications.
"""

from __future__ import annotations

import platform
import subprocess
from typing import Any, Optional


IS_MACOS = platform.system() == "Darwin"


class WidgetInfo:
    """Information about a UI widget."""
    
    def __init__(
        self,
        widget_type: str,
        identifier: Optional[str] = None,
        label: Optional[str] = None,
        value: Optional[str] = None,
        enabled: bool = True,
        visible: bool = True,
        children: Optional[list["WidgetInfo"]] = None,
    ):
        self.widget_type = widget_type
        self.identifier = identifier
        self.label = label
        self.value = value
        self.enabled = enabled
        self.visible = visible
        self.children = children or []
    
    def __repr__(self) -> str:
        return f"WidgetInfo(type={self.widget_type!r}, label={self.label!r})"
    
    def find_by_type(self, widget_type: str) -> list["WidgetInfo"]:
        """Find all widgets of the given type."""
        results = []
        if self.widget_type == widget_type:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_type(widget_type))
        return results
    
    def find_by_label(self, label: str) -> list["WidgetInfo"]:
        """Find all widgets with the given label."""
        results = []
        if self.label == label:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_label(label))
        return results


def inspect_widget_at(x: int, y: int) -> Optional[WidgetInfo]:
    """Inspect the widget at a screen position.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        WidgetInfo if a widget was found, None otherwise.
    """
    if IS_MACOS:
        return _inspect_widget_macos(x, y)
    return None


def _inspect_widget_macos(x: int, y: int) -> Optional[WidgetInfo]:
    """Inspect widget on macOS using accessibility APIs."""
    try:
        script = f'''
        tell application "System Events"
            set theElement to (first UI element of process 1 whose role is not null)
            try
                set elemRole to role of theElement
                set elemValue to value of theElement
                set elemName to name of theElement
                set elemEnabled to enabled of theElement
                set elemVisible to visible of theElement
                return elemRole & "|" & elemName & "|" & elemValue & "|" & elemEnabled & "|" & elemVisible
            on error
                return "unknown"
            end try
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split("|")
            if len(parts) >= 2:
                return WidgetInfo(
                    widget_type=parts[0],
                    label=parts[1] if len(parts) > 1 else None,
                    value=parts[2] if len(parts) > 2 else None,
                    enabled=parts[3] == "true" if len(parts) > 3 else True,
                    visible=parts[4] == "true" if len(parts) > 4 else True,
                )
    except Exception:
        pass
    return None


def get_app_widgets(app_name: Optional[str] = None) -> list[WidgetInfo]:
    """Get all widgets from an application.
    
    Args:
        app_name: Name of the application. If None, uses frontmost.
    
    Returns:
        List of WidgetInfo for all widgets found.
    """
    if IS_MACOS:
        return _get_app_widgets_macos(app_name)
    return []


def _get_app_widgets_macos(app_name: Optional[str]) -> list[WidgetInfo]:
    """Get app widgets on macOS."""
    widgets = []
    try:
        if app_name:
            script = f'''
            tell application "System Events"
                tell process "{app_name}"
                    get entire contents
                end tell
            end tell
            '''
        else:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    get entire contents
                end tell
            end tell
            '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            # Parse output
            lines = result.stdout.strip().split("\n")
            for line in lines:
                if line.strip():
                    widgets.append(WidgetInfo(widget_type="unknown", label=line.strip()))
    except Exception:
        pass
    return widgets


def get_widget_property(
    widget: WidgetInfo,
    property_name: str,
) -> Any:
    """Get a property value from a widget.
    
    Args:
        widget: WidgetInfo to query.
        property_name: Name of the property.
    
    Returns:
        Property value, or None if not found.
    """
    return getattr(widget, property_name, None)


def set_widget_property(
    widget: WidgetInfo,
    property_name: str,
    value: Any,
) -> bool:
    """Attempt to set a property on a widget.
    
    Note: This may not work for all widgets as it depends
    on the application's accessibility support.
    
    Args:
        widget: WidgetInfo to modify.
        property_name: Name of the property.
        value: New value for the property.
    
    Returns:
        True if successful.
    """
    # Widget modification would require specific platform APIs
    return False
