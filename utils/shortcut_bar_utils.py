"""Shortcut Bar Widget Utilities.

This module provides shortcut bar and quick action widget utilities for
macOS applications, including button groups, segmented controls, and
quick action menus.

Example:
    >>> from shortcut_bar_utils import ShortcutBar, ActionButton
    >>> bar = ShortcutBar()
    >>> bar.add_button(ActionButton("Copy", lambda: copy()))
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple


class ButtonStyle(Enum):
    """Button visual styles."""
    REGULAR = auto()
    ROUNDED = auto()
    CIRCULAR = auto()
    TEXT_ONLY = auto()
    ICON_ONLY = auto()


class ButtonState(Enum):
    """Button states."""
    NORMAL = auto()
    HIGHLIGHTED = auto()
    DISABLED = auto()
    SELECTED = auto()


@dataclass
class ActionButton:
    """Represents an action button in a shortcut bar.
    
    Attributes:
        id: Unique button identifier
        title: Button display title
        icon: Optional icon name
        action: Callback when button activated
        style: Visual style
        is_enabled: Whether button is enabled
        tooltip: Optional tooltip text
    """
    id: str
    title: str
    icon: Optional[str] = None
    action: Optional[Callable[[], None]] = None
    style: ButtonStyle = ButtonStyle.ROUNDED
    is_enabled: bool = True
    is_selected: bool = False
    tooltip: Optional[str] = None
    badge: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def trigger(self) -> None:
        """Trigger button action."""
        if self.is_enabled and self.action:
            self.action()


@dataclass
class ShortcutBarSection:
    """A section in a shortcut bar with grouped buttons."""
    id: str
    title: Optional[str] = None
    buttons: List[ActionButton] = field(default_factory=list)
    is_expanded: bool = True
    order: int = 0
    
    def add_button(self, button: ActionButton) -> None:
        self.buttons.append(button)
    
    def get_button(self, button_id: str) -> Optional[ActionButton]:
        for btn in self.buttons:
            if btn.id == button_id:
                return btn
        return None
    
    def remove_button(self, button_id: str) -> bool:
        for i, btn in enumerate(self.buttons):
            if btn.id == button_id:
                self.buttons.pop(i)
                return True
        return False


class ShortcutBar:
    """A shortcut bar widget with action buttons.
    
    Provides a horizontal or vertical bar of action buttons
    organized into sections.
    
    Attributes:
        orientation: Horizontal or vertical layout
        sections: List of button sections
    """
    
    def __init__(
        self,
        orientation: str = "horizontal",
        spacing: int = 8,
    ):
        self.orientation = orientation
        self.spacing = spacing
        self.sections: List[ShortcutBarSection] = []
        self._selected_button_id: Optional[str] = None
        self._callbacks: Dict[str, List[Callable]] = {
            'button_clicked': [],
            'selection_changed': [],
            'section_expanded': [],
        }
    
    def add_section(self, section: ShortcutBarSection) -> None:
        """Add a section to the bar."""
        self.sections.append(section)
        self.sections.sort(key=lambda s: s.order)
    
    def remove_section(self, section_id: str) -> bool:
        """Remove a section by ID."""
        for i, section in enumerate(self.sections):
            if section.id == section_id:
                self.sections.pop(i)
                return True
        return False
    
    def get_section(self, section_id: str) -> Optional[ShortcutBarSection]:
        """Get section by ID."""
        for section in self.sections:
            if section.id == section_id:
                return section
        return None
    
    def add_button(
        self,
        button: ActionButton,
        section_id: Optional[str] = None,
    ) -> None:
        """Add a button to a section."""
        if section_id:
            section = self.get_section(section_id)
            if section:
                section.add_button(button)
        elif self.sections:
            self.sections[-1].add_button(button)
    
    def trigger_button(self, button_id: str) -> bool:
        """Trigger a button by ID."""
        for section in self.sections:
            btn = section.get_button(button_id)
            if btn:
                btn.trigger()
                self._notify('button_clicked', btn)
                return True
        return False
    
    def select_button(self, button_id: Optional[str]) -> None:
        """Set button selection state."""
        if self._selected_button_id:
            self._deselect_current()
        
        self._selected_button_id = button_id
        
        if button_id:
            for section in self.sections:
                btn = section.get_button(button_id)
                if btn:
                    btn.is_selected = True
                    self._notify('selection_changed', btn)
                    break
    
    def _deselect_current(self) -> None:
        """Deselect currently selected button."""
        if self._selected_button_id:
            for section in self.sections:
                btn = section.get_button(self._selected_button_id)
                if btn:
                    btn.is_selected = False
                    break
    
    def get_all_buttons(self) -> List[ActionButton]:
        """Get all buttons from all sections."""
        buttons = []
        for section in self.sections:
            buttons.extend(section.buttons)
        return buttons
    
    def get_button(self, button_id: str) -> Optional[ActionButton]:
        """Get button by ID across all sections."""
        for section in self.sections:
            btn = section.get_button(button_id)
            if btn:
                return btn
        return None
    
    def on(self, event: str, callback: Callable) -> None:
        """Register event callback."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)
    
    def _notify(self, event: str, *args) -> None:
        """Notify callbacks of event."""
        for callback in self._callbacks.get(event, []):
            try:
                callback(*args)
            except Exception:
                pass


class SegmentedControl:
    """Segmented control widget (like NSSegmentedControl).
    
    Provides a control with multiple segments where only one
    can be selected at a time.
    
    Attributes:
        segments: List of segment definitions
        selected_index: Currently selected segment index
    """
    
    def __init__(
        self,
        labels: Optional[List[str]] = None,
        tracking_mode: str = "select_one",
    ):
        self.segments: List[Dict[str, Any]] = []
        self.selected_index: Optional[int] = None
        self.tracking_mode = tracking_mode
        
        if labels:
            for i, label in enumerate(labels):
                self.add_segment(label=label, index=i)
        
        self._callbacks: List[Callable[[int], None]] = []
    
    def add_segment(
        self,
        label: Optional[str] = None,
        icon: Optional[str] = None,
        index: Optional[int] = None,
    ) -> None:
        """Add a segment to the control."""
        segment = {
            'label': label or '',
            'icon': icon,
            'enabled': True,
            'selected': False,
        }
        
        if index is not None and 0 <= index <= len(self.segments):
            self.segments.insert(index, segment)
        else:
            self.segments.append(segment)
    
    def remove_segment(self, index: int) -> bool:
        """Remove a segment by index."""
        if 0 <= index < len(self.segments):
            self.segments.pop(index)
            if self.selected_index == index:
                self.selected_index = None
            return True
        return False
    
    def select_segment(self, index: int) -> None:
        """Select a segment."""
        if not (0 <= index < len(self.segments)):
            return
        
        if self.tracking_mode == "select_one":
            for i, seg in enumerate(self.segments):
                seg['selected'] = (i == index)
        
        self.selected_index = index
        
        for callback in self._callbacks:
            try:
                callback(index)
            except Exception:
                pass
    
    def set_segment_enabled(self, index: int, enabled: bool) -> None:
        """Enable or disable a segment."""
        if 0 <= index < len(self.segments):
            self.segments[index]['enabled'] = enabled
    
    def on_selection_changed(self, callback: Callable[[int], None]) -> None:
        """Register selection change callback."""
        self._callbacks.append(callback)


class QuickActionMenu:
    """Quick action menu widget.
    
    Provides a popup menu of quick actions, similar to macOS
    services menus or context menus.
    
    Attributes:
        title: Menu title
        items: Menu items
    """
    
    def __init__(self, title: str = ""):
        self.title = title
        self.items: List[Dict[str, Any]] = []
        self._callbacks: List[Callable[[str], None]] = []
    
    def add_item(
        self,
        id: str,
        title: str,
        icon: Optional[str] = None,
        shortcut: Optional[str] = None,
        enabled: bool = True,
    ) -> None:
        """Add an item to the menu."""
        self.items.append({
            'id': id,
            'title': title,
            'icon': icon,
            'shortcut': shortcut,
            'enabled': enabled,
        })
    
    def add_separator(self) -> None:
        """Add a separator line."""
        self.items.append({'type': 'separator'})
    
    def add_submenu(
        self,
        id: str,
        title: str,
        items: List[Dict[str, Any]],
    ) -> None:
        """Add a submenu."""
        self.items.append({
            'type': 'submenu',
            'id': id,
            'title': title,
            'items': items,
        })
    
    def remove_item(self, item_id: str) -> bool:
        """Remove an item by ID."""
        for i, item in enumerate(self.items):
            if item.get('id') == item_id:
                self.items.pop(i)
                return True
        return False
    
    def execute(self, item_id: str) -> bool:
        """Execute a menu item action."""
        for item in self.items:
            if item.get('id') == item_id:
                if item.get('enabled', True):
                    for callback in self._callbacks:
                        try:
                            callback(item_id)
                        except Exception:
                            pass
                    return True
        return False
    
    def on_executed(self, callback: Callable[[str], None]) -> None:
        """Register execution callback."""
        self._callbacks.append(callback)
