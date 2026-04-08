"""Window z-order utilities.

This module provides utilities for managing window z-order
(layering) relationships.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field


@dataclass
class ZOrderEntry:
    """An entry in the z-order stack."""
    window_id: int
    title: str
    level: int  # 0 = bottom, higher = on top


class ZOrderManager:
    """Manages window z-order relationships."""

    def __init__(self) -> None:
        self._entries: Dict[int, ZOrderEntry] = {}
        self._order: List[int] = []  # Bottom to top
        self._focused_id: Optional[int] = None

    def add_window(self, window_id: int, title: str = "") -> None:
        """Add a window to z-order tracking."""
        if window_id not in self._entries:
            entry = ZOrderEntry(window_id=window_id, title=title, level=len(self._order))
            self._entries[window_id] = entry
            self._order.append(window_id)

    def remove_window(self, window_id: int) -> bool:
        """Remove a window from z-order tracking."""
        if window_id not in self._entries:
            return False
        del self._entries[window_id]
        self._order.remove(window_id)
        if self._focused_id == window_id:
            self._focused_id = None
        return True

    def bring_to_front(self, window_id: int) -> bool:
        """Bring a window to the front."""
        if window_id not in self._order:
            return False
        self._order.remove(window_id)
        self._order.append(window_id)
        self._update_levels()
        return True

    def send_to_back(self, window_id: int) -> bool:
        """Send a window to the back."""
        if window_id not in self._order:
            return False
        self._order.remove(window_id)
        self._order.insert(0, window_id)
        self._update_levels()
        return True

    def set_focused(self, window_id: int) -> None:
        """Set the focused window."""
        if window_id in self._entries:
            self._focused_id = window_id

    def get_above(self, window_id: int) -> Optional[int]:
        """Get the window directly above."""
        if window_id not in self._order:
            return None
        idx = self._order.index(window_id)
        if idx < len(self._order) - 1:
            return self._order[idx + 1]
        return None

    def get_below(self, window_id: int) -> Optional[int]:
        """Get the window directly below."""
        if window_id not in self._order:
            return None
        idx = self._order.index(window_id)
        if idx > 0:
            return self._order[idx - 1]
        return None

    def get_order(self) -> List[int]:
        """Get current z-order (bottom to top)."""
        return self._order.copy()

    def get_reverse_order(self) -> List[int]:
        """Get z-order reversed (top to bottom)."""
        return self._order.copy()[::-1]

    @property
    def focused_window(self) -> Optional[int]:
        return self._focused_id

    def _update_levels(self) -> None:
        for i, wid in enumerate(self._order):
            self._entries[wid].level = i


__all__ = [
    "ZOrderEntry",
    "ZOrderManager",
]
