"""
Window State Persistence Utilities

Persist and restore window states (position, size, z-order)
for session save/restore and window configuration management.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict


@dataclass
class WindowState:
    """Persisted state of a window."""
    window_id: int
    title: str
    bundle_id: str
    x: float
    y: float
    width: float
    height: float
    z_order: int
    is_maximized: bool
    is_minimized: bool
    is_fullscreen: bool
    display_index: int
    saved_at_ms: float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class WindowLayout:
    """A saved layout containing multiple window states."""
    name: str
    windows: List[WindowState]
    created_at_ms: float = field(default_factory=lambda: time.time() * 1000)
    metadata: dict = field(default_factory=dict)


class WindowStatePersistence:
    """Persist and restore window states."""

    def __init__(self):
        self._layouts: Dict[str, WindowLayout] = {}

    def capture_window(
        self,
        window_id: int,
        title: str,
        bundle_id: str,
        x: float, y: float,
        width: float, height: float,
        z_order: int,
        is_maximized: bool = False,
        is_minimized: bool = False,
        is_fullscreen: bool = False,
        display_index: int = 0,
    ) -> WindowState:
        """Capture the current state of a window."""
        return WindowState(
            window_id=window_id,
            title=title,
            bundle_id=bundle_id,
            x=x, y=y, width=width, height=height,
            z_order=z_order,
            is_maximized=is_maximized,
            is_minimized=is_minimized,
            is_fullscreen=is_fullscreen,
            display_index=display_index,
        )

    def save_layout(
        self,
        name: str,
        windows: List[WindowState],
        metadata: Optional[dict] = None,
    ) -> WindowLayout:
        """Save a layout with the given name."""
        layout = WindowLayout(
            name=name,
            windows=windows,
            metadata=metadata or {},
        )
        self._layouts[name] = layout
        return layout

    def load_layout(self, name: str) -> Optional[WindowLayout]:
        """Load a saved layout by name."""
        return self._layouts.get(name)

    def delete_layout(self, name: str) -> bool:
        """Delete a saved layout."""
        return self._layouts.pop(name, None) is not None

    def list_layouts(self) -> List[str]:
        """List all saved layout names."""
        return list(self._layouts.keys())

    def export_layout(self, name: str) -> str:
        """Export a layout as JSON string."""
        layout = self._layouts.get(name)
        if not layout:
            return "{}"
        return json.dumps(asdict(layout), ensure_ascii=False)

    def import_layout(self, json_str: str) -> Optional[WindowLayout]:
        """Import a layout from JSON string."""
        try:
            data = json.loads(json_str)
            windows = [WindowState(**w) for w in data.get("windows", [])]
            layout = WindowLayout(
                name=data["name"],
                windows=windows,
                metadata=data.get("metadata", {}),
            )
            self._layouts[layout.name] = layout
            return layout
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    def get_layout_count(self) -> int:
        """Get the number of saved layouts."""
        return len(self._layouts)
