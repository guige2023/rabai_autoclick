"""
Window Z-Order Manager.

Utilities for managing window stacking order (z-order) including
raising, lowering, and reordering windows across all applications.

Usage:
    from utils.window_zorder_manager import ZOrderManager, bring_to_front

    manager = ZOrderManager()
    manager.bring_to_front(window_id)
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class ZOrderEntry:
    """Represents a window in the z-order stack."""
    window_id: int
    app_name: str
    title: str
    z_position: int
    is_visible: bool = True


class ZOrderManager:
    """
    Manage window z-order (stacking order).

    Provides utilities for manipulating the stacking order of windows,
    including raising windows to the front, lowering to the back,
    and reordering relative to other windows.

    Example:
        manager = ZOrderManager()
        manager.bring_to_front(win_id)
        manager.lower_to_back(win_id)
    """

    def __init__(self, bridge: Optional[Any] = None) -> None:
        """
        Initialize the z-order manager.

        Args:
            bridge: Optional AccessibilityBridge instance.
        """
        self._bridge = bridge
        self._z_order_stack: List[int] = []
        self._window_info: Dict[int, ZOrderEntry] = {}

    def bring_to_front(self, window_id: int) -> bool:
        """
        Bring a window to the front of the z-order stack.

        Args:
            window_id: ID of the window to raise.

        Returns:
            True if successful.
        """
        if window_id in self._z_order_stack:
            self._z_order_stack.remove(window_id)
        self._z_order_stack.append(window_id)
        return self._apply_z_order(window_id, "raise")

    def lower_to_back(self, window_id: int) -> bool:
        """
        Lower a window to the back of the z-order stack.

        Args:
            window_id: ID of the window to lower.

        Returns:
            True if successful.
        """
        if window_id in self._z_order_stack:
            self._z_order_stack.remove(window_id)
        self._z_order_stack.insert(0, window_id)
        return self._apply_z_order(window_id, "lower")

    def reorder_relative(
        self,
        window_id: int,
        target_id: int,
        position: str = "above",
    ) -> bool:
        """
        Reorder a window relative to another window.

        Args:
            window_id: ID of the window to move.
            target_id: ID of the reference window.
            position: "above", "below", "top", or "bottom".

        Returns:
            True if successful.
        """
        if window_id == target_id:
            return False

        if window_id in self._z_order_stack:
            self._z_order_stack.remove(window_id)

        if target_id in self._z_order_stack:
            idx = self._z_order_stack.index(target_id)
            if position == "above":
                self._z_order_stack.insert(idx, window_id)
            elif position == "below":
                self._z_order_stack.insert(idx + 1, window_id)
        else:
            if position in ("top", "above"):
                self._z_order_stack.append(window_id)
            else:
                self._z_order_stack.insert(0, window_id)

        return self._apply_z_order(window_id, position)

    def _apply_z_order(
        self,
        window_id: int,
        position: str,
    ) -> bool:
        """Apply z-order change to a window."""
        if self._bridge is None:
            return False

        try:
            if position == "raise":
                self._bridge.raise_window(window_id)
            elif position == "lower":
                self._bridge.lower_window(window_id)
            return True
        except Exception:
            return False

    def get_z_order(self) -> List[int]:
        """
        Get the current z-order stack.

        Returns:
            List of window IDs from back to front.
        """
        return list(self._z_order_stack)

    def get_window_z_position(self, window_id: int) -> int:
        """
        Get the z-position of a window.

        Args:
            window_id: Window ID.

        Returns:
            Z-position (0 = bottom, higher = on top).
        """
        if window_id in self._z_order_stack:
            return self._z_order_stack.index(window_id)
        return -1


def bring_to_front(window_id: int, bridge: Optional[Any] = None) -> bool:
    """
    Convenience function to bring a window to front.

    Args:
        window_id: ID of the window.
        bridge: Optional bridge instance.

    Returns:
        True if successful.
    """
    manager = ZOrderManager(bridge)
    return manager.bring_to_front(window_id)


def lower_to_back(window_id: int, bridge: Optional[Any] = None) -> bool:
    """
    Convenience function to lower a window to back.

    Args:
        window_id: ID of the window.
        bridge: Optional bridge instance.

    Returns:
        True if successful.
    """
    manager = ZOrderManager(bridge)
    return manager.lower_to_back(window_id)
