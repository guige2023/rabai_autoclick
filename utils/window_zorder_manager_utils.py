"""Window Z-Order Manager Utilities.

Manages window stacking order (z-order) across displays.

Example:
    >>> from window_zorder_manager_utils import ZOrderManager
    >>> mgr = ZOrderManager()
    >>> mgr.bring_to_front("Chrome")
    >>> mgr.send_to_back("Finder")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class WindowZOrder:
    """Window z-order entry."""
    window_id: str
    z_index: int
    display_id: int = 0


class ZOrderManager:
    """Manages window stacking order."""

    def __init__(self):
        """Initialize z-order manager."""
        self._orders: Dict[str, WindowZOrder] = {}
        self._max_z = 0
        self._min_z = 0

    def register(self, window_id: str, display_id: int = 0) -> None:
        """Register a window in z-order.

        Args:
            window_id: Window identifier.
            display_id: Display the window is on.
        """
        if window_id not in self._orders:
            self._max_z += 1
            self._orders[window_id] = WindowZOrder(
                window_id=window_id,
                z_index=self._max_z,
                display_id=display_id,
            )

    def bring_to_front(self, window_id: str) -> None:
        """Bring a window to the front.

        Args:
            window_id: Window to bring front.
        """
        if window_id not in self._orders:
            self.register(window_id)
        self._orders[window_id].z_index = self._max_z + 1
        self._max_z += 1

    def send_to_back(self, window_id: str) -> None:
        """Send a window to the back.

        Args:
            window_id: Window to send back.
        """
        if window_id not in self._orders:
            self.register(window_id)
        self._orders[window_id].z_index = self._min_z - 1
        self._min_z -= 1

    def get_z_order(self, window_id: str) -> int:
        """Get z-order of a window.

        Args:
            window_id: Window identifier.

        Returns:
            Z-index value (higher = more on top).
        """
        return self._orders.get(window_id, WindowZOrder(window_id, 0)).z_index

    def is_above(self, window_a: str, window_b: str) -> bool:
        """Check if window A is above window B.

        Args:
            window_a: First window ID.
            window_b: Second window ID.

        Returns:
            True if A is above B.
        """
        za = self.get_z_order(window_a)
        zb = self.get_z_order(window_b)
        return za > zb

    def get_ordered_windows(self, display_id: Optional[int] = None) -> List[str]:
        """Get windows in z-order (bottom to top).

        Args:
            display_id: Optional filter by display.

        Returns:
            List of window IDs in z-order.
        """
        entries = self._orders.values()
        if display_id is not None:
            entries = [e for e in entries if e.display_id == display_id]
        return [e.window_id for e in sorted(entries, key=lambda x: x.z_index)]
