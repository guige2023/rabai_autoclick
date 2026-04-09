"""Touch Bar Integration Utilities.

This module provides Touch Bar integration utilities for macOS applications,
including Touch Bar item management, customizable control strips, and
Touch Bar event handling.

Example:
    >>> from touch_bar_utils import TouchBarManager, TouchBarButton
    >>> manager = TouchBarManager()
    >>> manager.add_button(TouchBarButton("Copy"))
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple


class TouchBarItemType(Enum):
    """Touch Bar item types."""
    BUTTON = auto()
    LABEL = auto()
    POPOVER = auto()
    SLIDER = auto()
    SEGMENTED = auto()
    SCRUBBER = auto()
    GROUP = auto()
    CUSTOM = auto()


@dataclass
class TouchBarItem:
    """Base class for Touch Bar items.
    
    Attributes:
        id: Unique item identifier
        type: Item type
        title: Display title
        is_enabled: Whether item is enabled
    """
    id: str
    type: TouchBarItemType = TouchBarItemType.BUTTON
    title: str = ""
    is_enabled: bool = True
    icon: Optional[str] = None
    action: Optional[Callable[[], None]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TouchBarButton(TouchBarItem):
    """Touch Bar button item."""
    def __init__(
        self,
        id: str,
        title: str = "",
        icon: Optional[str] = None,
        action: Optional[Callable[[], None]] = None,
    ):
        super().__init__(id, TouchBarItemType.BUTTON, title, True, icon, action)


@dataclass
class TouchBarLabel(TouchBarItem):
    """Touch Bar label item."""
    def __init__(
        self,
        id: str,
        title: str = "",
    ):
        super().__init__(id, TouchBarItemType.LABEL, title)


@dataclass
class TouchBarSegment:
    """A segment in a segmented control."""
    id: str
    title: str = ""
    icon: Optional[str] = None
    is_enabled: bool = True


class TouchBarSegmentedControl(TouchBarItem):
    """Touch Bar segmented control item."""
    def __init__(self, id: str, segments: Optional[List[TouchBarSegment]] = None):
        super().__init__(id, TouchBarItemType.SEGMENTED)
        self.segments: List[TouchBarSegment] = segments or []
        self.selected_index: Optional[int] = None
    
    def add_segment(self, segment: TouchBarSegment) -> None:
        self.segments.append(segment)
    
    def select_segment(self, index: int) -> None:
        if 0 <= index < len(self.segments):
            self.selected_index = index


class TouchBarSlider(TouchBarItem):
    """Touch Bar slider item."""
    def __init__(
        self,
        id: str,
        min_value: float = 0.0,
        max_value: float = 1.0,
        value: float = 0.5,
    ):
        super().__init__(id, TouchBarItemType.SLIDER)
        self.min_value = min_value
        self.max_value = max_value
        self.value = value
        self.on_change: Optional[Callable[[float], None]] = None


class TouchBarManager:
    """Manages Touch Bar items and configuration.
    
    Provides a high-level interface for creating and managing
    Touch Bar layouts on supported macOS applications.
    
    Attributes:
        items: Dictionary of registered items
        default_items: Items shown by default
    """
    
    def __init__(self):
        self.items: Dict[str, TouchBarItem] = {}
        self.default_items: List[str] = []
        self._callbacks: Dict[str, List[Callable]] = {
            'item_tapped': [],
            'slider_changed': [],
            'segment_selected': [],
        }
    
    def add_item(self, item: TouchBarItem) -> None:
        """Add an item to the Touch Bar."""
        self.items[item.id] = item
    
    def remove_item(self, item_id: str) -> bool:
        """Remove an item from the Touch Bar."""
        if item_id in self.items:
            del self.items[item_id]
            if item_id in self.default_items:
                self.default_items.remove(item_id)
            return True
        return False
    
    def get_item(self, item_id: str) -> Optional[TouchBarItem]:
        """Get an item by ID."""
        return self.items.get(item_id)
    
    def set_default_items(self, item_ids: List[str]) -> None:
        """Set items shown by default."""
        self.default_items = [id_ for id_ in item_ids if id_ in self.items]
    
    def add_to_default(self, item_id: str) -> None:
        """Add item to default items."""
        if item_id in self.items and item_id not in self.default_items:
            self.default_items.append(item_id)
    
    def trigger_item(self, item_id: str) -> bool:
        """Trigger an item's action."""
        item = self.items.get(item_id)
        if item and item.is_enabled and item.action:
            try:
                item.action()
                self._notify('item_tapped', item)
                return True
            except Exception:
                pass
        return False
    
    def update_label(self, item_id: str, title: str) -> bool:
        """Update a label item's text."""
        item = self.items.get(item_id)
        if item and item.type == TouchBarItemType.LABEL:
            item.title = title
            return True
        return False
    
    def update_button(self, item_id: str, title: Optional[str] = None, icon: Optional[str] = None) -> bool:
        """Update a button item."""
        item = self.items.get(item_id)
        if item and item.type == TouchBarItemType.BUTTON:
            if title is not None:
                item.title = title
            if icon is not None:
                item.icon = icon
            return True
        return False
    
    def set_slider_value(self, item_id: str, value: float) -> bool:
        """Set slider value."""
        item = self.items.get(item_id)
        if item and item.type == TouchBarItemType.SLIDER:
            item.value = max(item.min_value, min(item.max_value, value))
            self._notify('slider_changed', item, item.value)
            return True
        return False
    
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
    
    def get_layout(self) -> List[TouchBarItem]:
        """Get the current Touch Bar layout."""
        items = []
        for item_id in self.default_items:
            item = self.items.get(item_id)
            if item:
                items.append(item)
        return items


class TouchBarPreset:
    """Pre-configured Touch Bar layouts."""
    
    @staticmethod
    def editing_preset() -> TouchBarManager:
        """Preset for text editing."""
        manager = TouchBarManager()
        
        manager.add_item(TouchBarButton("undo", "Undo", action=lambda: None))
        manager.add_item(TouchBarButton("redo", "Redo", action=lambda: None))
        manager.add_item(TouchBarLabel("label1", ""))
        manager.add_item(TouchBarButton("cut", "Cut", action=lambda: None))
        manager.add_item(TouchBarButton("copy", "Copy", action=lambda: None))
        manager.add_item(TouchBarButton("paste", "Paste", action=lambda: None))
        
        manager.set_default_items(["undo", "redo", "label1", "cut", "copy", "paste"])
        return manager
    
    @staticmethod
    def media_preset() -> TouchBarManager:
        """Preset for media playback."""
        manager = TouchBarManager()
        
        manager.add_item(TouchBarButton("previous", "⏮", action=lambda: None))
        manager.add_item(TouchBarButton("play", "▶", action=lambda: None))
        manager.add_item(TouchBarButton("next", "⏭", action=lambda: None))
        
        slider = TouchBarSlider("scrubber", 0.0, 100.0, 50.0)
        manager.add_item(slider)
        
        manager.set_default_items(["previous", "play", "next", "scrubber"])
        return manager
    
    @staticmethod
    def navigation_preset() -> TouchBarManager:
        """Preset for navigation."""
        manager = TouchBarManager()
        
        manager.add_item(TouchBarButton("back", "←", action=lambda: None))
        manager.add_item(TouchBarButton("forward", "→", action=lambda: None))
        manager.add_item(TouchBarLabel("title", "Title"))
        manager.add_item(TouchBarButton("home", "⌂", action=lambda: None))
        manager.add_item(TouchBarButton("refresh", "↻", action=lambda: None))
        
        manager.set_default_items(["back", "forward", "title", "home", "refresh"])
        return manager
