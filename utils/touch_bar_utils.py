"""
Touch Bar manipulation utilities for macOS automation.

Provides utilities for interacting with the Touch Bar on MacBook Pro,
including button creation, text display, and event handling.
"""

from __future__ import annotations

from typing import List, Optional, Callable, Dict, Any
from dataclasses import dataclass
from enum import Enum


class TouchBarItemType(Enum):
    """Types of Touch Bar items."""
    BUTTON = "button"
    LABEL = "label"
    GROUP = "group"
    SLIDER = "slider"
    SCRUBBER = "scrubber"
    POPOVER = "popover"
    SEGMENTED = "segmented"


@dataclass
class TouchBarItem:
    """Represents a single Touch Bar item."""
    identifier: str
    type: TouchBarItemType
    label: str = ""
    text: str = ""
    callback: Optional[Callable[[], None]] = None
    icon: Optional[bytes] = None
    children: List["TouchBarItem"] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    @property
    def type_name(self) -> str:
        return self.type.value


@dataclass
class TouchBarLayout:
    """Defines a Touch Bar layout configuration."""
    identifier: str
    items: List[TouchBarItem]
    default_identifier: Optional[str] = None


class TouchBarManager:
    """Manages Touch Bar interactions."""
    
    def __init__(self):
        """Initialize Touch Bar manager."""
        self._layouts: Dict[str, TouchBarLayout] = {}
        self._callbacks: Dict[str, Callable] = {}
        self._current_layout: Optional[str] = None
    
    def register_layout(self, layout: TouchBarLayout) -> None:
        """Register a Touch Bar layout.
        
        Args:
            layout: TouchBarLayout to register
        """
        self._layouts[layout.identifier] = layout
    
    def unregister_layout(self, identifier: str) -> bool:
        """Unregister a layout.
        
        Args:
            identifier: Layout identifier
            
        Returns:
            True if layout was removed
        """
        if identifier in self._layouts:
            del self._layouts[identifier]
            return True
        return False
    
    def switch_to_layout(self, identifier: str) -> bool:
        """Switch to a registered layout.
        
        Args:
            identifier: Layout identifier
            
        Returns:
            True if switch was successful
        """
        if identifier not in self._layouts:
            return False
        
        self._current_layout = identifier
        return self._apply_layout(self._layouts[identifier])
    
    def _apply_layout(self, layout: TouchBarLayout) -> bool:
        """Apply a layout to the Touch Bar.
        
        Args:
            layout: Layout to apply
            
        Returns:
            True if successful
        """
        # Note: This would require using private/undocumented APIs
        # or Accessibility controls to actually manipulate the Touch Bar
        # For now, we just track the state
        return True
    
    def get_current_layout(self) -> Optional[TouchBarLayout]:
        """Get the currently active layout.
        
        Returns:
            Current layout or None
        """
        if self._current_layout:
            return self._layouts.get(self._current_layout)
        return None
    
    def create_button(
        self,
        identifier: str,
        label: str,
        callback: Optional[Callable[[], None]] = None,
        icon: Optional[bytes] = None
    ) -> TouchBarItem:
        """Create a Touch Bar button.
        
        Args:
            identifier: Unique identifier
            label: Button label
            callback: Optional callback on tap
            icon: Optional icon data
            
        Returns:
            TouchBarItem for the button
        """
        item = TouchBarItem(
            identifier=identifier,
            type=TouchBarItemType.BUTTON,
            label=label,
            callback=callback,
            icon=icon
        )
        
        if callback:
            self._callbacks[identifier] = callback
        
        return item
    
    def create_label(
        self,
        identifier: str,
        text: str
    ) -> TouchBarItem:
        """Create a Touch Bar label.
        
        Args:
            identifier: Unique identifier
            text: Label text
            
        Returns:
            TouchBarItem for the label
        """
        return TouchBarItem(
            identifier=identifier,
            type=TouchBarItemType.LABEL,
            text=text
        )
    
    def create_group(
        self,
        identifier: str,
        items: List[TouchBarItem],
        label: str = ""
    ) -> TouchBarItem:
        """Create a grouped Touch Bar item.
        
        Args:
            identifier: Unique identifier
            items: Items to include in group
            label: Optional group label
            
        Returns:
            TouchBarItem for the group
        """
        return TouchBarItem(
            identifier=identifier,
            type=TouchBarItemType.GROUP,
            label=label,
            children=items
        )
    
    def create_scrubber(
        self,
        identifier: str,
        items: List[TouchBarItem],
        callback: Optional[Callable[[int], None]] = None
    ) -> TouchBarItem:
        """Create a scrubber (horizontal scrolling list).
        
        Args:
            identifier: Unique identifier
            items: Items to display
            callback: Optional callback with selected index
            
        Returns:
            TouchBarItem for the scrubber
        """
        item = TouchBarItem(
            identifier=identifier,
            type=TouchBarItemType.SCRUBBER,
            children=items,
            callback=callback
        )
        
        if callback:
            self._callbacks[identifier] = callback
        
        return item
    
    def trigger_callback(self, identifier: str) -> bool:
        """Trigger a callback by item identifier.
        
        Args:
            identifier: Item identifier
            
        Returns:
            True if callback was found and triggered
        """
        if identifier in self._callbacks:
            callback = self._callbacks[identifier]
            callback()
            return True
        return False
    
    def remove_callback(self, identifier: str) -> bool:
        """Remove a callback.
        
        Args:
            identifier: Item identifier
            
        Returns:
            True if callback was removed
        """
        if identifier in self._callbacks:
            del self._callbacks[identifier]
            return True
        return False
    
    def simulate_tap(self, identifier: str) -> bool:
        """Simulate a tap on a Touch Bar item.
        
        Args:
            identifier: Item identifier
            
        Returns:
            True if tap was simulated
        """
        return self.trigger_callback(identifier)
    
    def get_item_count(self) -> int:
        """Get count of registered items across all layouts.
        
        Returns:
            Total item count
        """
        return sum(
            len(layout.items) 
            for layout in self._layouts.values()
        )


def create_default_touch_bar_layout(
    items: List[Tuple[str, str, Optional[Callable]]]
) -> TouchBarLayout:
    """Create a default Touch Bar layout from simple specifications.
    
    Args:
        items: List of (identifier, label, callback) tuples
        
    Returns:
        TouchBarLayout configured with the items
    """
    manager = TouchBarManager()
    touch_bar_items = []
    
    for identifier, label, callback in items:
        item = manager.create_button(identifier, label, callback)
        touch_bar_items.append(item)
    
    return TouchBarLayout(
        identifier="default",
        items=touch_bar_items,
        default_identifier=touch_bar_items[0].identifier if touch_bar_items else None
    )


def get_touch_bar_availability() -> Dict[str, Any]:
    """Check Touch Bar availability on this Mac.
    
    Returns:
        Dictionary with availability info
    """
    import subprocess
    
    info = {
        "available": False,
        "model": None,
        "has_function_row": True,
    }
    
    # Check model identifier
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.model"],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        if result.returncode == 0:
            model = result.stdout.strip()
            
            # Touch Bar models (MacBook Pro from 2016-2020)
            touchbar_models = [
                "MacBookPro13,1", "MacBookPro13,2", "MacBookPro13,3",
                "MacBookPro14,1", "MacBookPro14,2", "MacBookPro14,3",
                "MacBookPro15,1", "MacBookPro15,2", "MacBookPro15,3",
                "MacBookPro16,1", "MacBookPro16,2", "MacBookPro16,3", "MacBookPro16,4",
                "MacBookPro17,1", "MacBookPro18,1", "MacBookPro18,2", "MacBookPro18,3",
            ]
            
            info["model"] = model
            info["available"] = any(model.startswith(m) for m in touchbar_models)
    except Exception:
        pass
    
    return info


def convert_to_touch_bar_json(layout: TouchBarLayout) -> str:
    """Convert a TouchBarLayout to JSON representation.
    
    Args:
        layout: Layout to convert
        
    Returns:
        JSON string representation
    """
    import json
    
    def item_to_dict(item: TouchBarItem) -> Dict:
        result = {
            "identifier": item.identifier,
            "type": item.type_name,
        }
        
        if item.label:
            result["label"] = item.label
        if item.text:
            result["text"] = item.text
        if item.children:
            result["children"] = [item_to_dict(c) for c in item.children]
        
        return result
    
    data = {
        "identifier": layout.identifier,
        "defaultIdentifier": layout.default_identifier,
        "items": [item_to_dict(item) for item in layout.items],
    }
    
    return json.dumps(data, indent=2)
