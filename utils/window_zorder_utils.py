"""Window Z-Order and Layer Management Utilities.

Manages window stacking order and layer relationships.
Supports raising, lowering, and reordering windows across layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class ZOrderPosition(Enum):
    """Position relative to z-order."""

    TOP = auto()
    BOTTOM = auto()
    ABOVE = auto()
    BELOW = auto()


@dataclass
class ZOrderEntry:
    """Entry in the z-order stack.

    Attributes:
        window_id: Unique window identifier.
        z_index: Z-index value.
        layer: Layer name.
        title: Window title.
        is_visible: Whether window is visible.
    """

    window_id: str
    z_index: int
    layer: str = "default"
    title: str = ""
    is_visible: bool = True


class ZOrderStack:
    """Manages the z-order stack of windows.

    Example:
        stack = ZOrderStack()
        stack.add_window("win1", z_index=0)
        stack.add_window("win2", z_index=1)
        stack.raise_window("win1")
    """

    def __init__(self):
        """Initialize the z-order stack."""
        self._entries: dict[str, ZOrderEntry] = {}
        self._order: list[str] = []
        self._callbacks: list[Callable[[str, ZOrderPosition, Optional[str]], None]] = []

    def add_window(
        self,
        window_id: str,
        z_index: int = 0,
        layer: str = "default",
        title: str = "",
    ) -> None:
        """Add a window to the z-order stack.

        Args:
            window_id: Unique window identifier.
            z_index: Initial z-index.
            layer: Layer name.
            title: Window title.
        """
        if window_id in self._entries:
            self._entries[window_id].z_index = z_index
            self._entries[window_id].layer = layer
            return

        entry = ZOrderEntry(
            window_id=window_id,
            z_index=z_index,
            layer=layer,
            title=title,
        )
        self._entries[window_id] = entry
        self._rebuild_order()

    def remove_window(self, window_id: str) -> None:
        """Remove a window from the z-order stack.

        Args:
            window_id: Window to remove.
        """
        if window_id in self._entries:
            del self._entries[window_id]
            if window_id in self._order:
                self._order.remove(window_id)

    def raise_window(self, window_id: str) -> None:
        """Raise a window to the top of its layer.

        Args:
            window_id: Window to raise.
        """
        if window_id not in self._entries:
            return

        entry = self._entries[window_id]
        layer_windows = [
            wid for wid in self._order
            if self._entries[wid].layer == entry.layer
        ]

        if layer_windows and layer_windows[-1] != window_id:
            self._order.remove(window_id)
            self._order.append(window_id)
            self._notify_change(window_id, ZOrderPosition.TOP, None)

    def lower_window(self, window_id: str) -> None:
        """Lower a window to the bottom of its layer.

        Args:
            window_id: Window to lower.
        """
        if window_id not in self._entries:
            return

        entry = self._entries[window_id]
        layer_windows = [
            wid for wid in self._order
            if self._entries[wid].layer == entry.layer
        ]

        if layer_windows and layer_windows[0] != window_id:
            self._order.remove(window_id)
            insert_idx = self._order.index(layer_windows[0])
            self._order.insert(insert_idx, window_id)
            self._notify_change(window_id, ZOrderPosition.BOTTOM, None)

    def move_above(self, window_id: str, target_id: str) -> None:
        """Move a window directly above another.

        Args:
            window_id: Window to move.
            target_id: Window to position above.
        """
        if window_id not in self._entries or target_id not in self._entries:
            return

        if window_id not in self._order or target_id not in self._order:
            return

        self._order.remove(window_id)
        target_idx = self._order.index(target_id)
        self._order.insert(target_idx, window_id)
        self._notify_change(window_id, ZOrderPosition.ABOVE, target_id)

    def move_below(self, window_id: str, target_id: str) -> None:
        """Move a window directly below another.

        Args:
            window_id: Window to move.
            target_id: Window to position below.
        """
        if window_id not in self._entries or target_id not in self._entries:
            return

        if window_id not in self._order or target_id not in self._order:
            return

        self._order.remove(window_id)
        target_idx = self._order.index(target_id)
        self._order.insert(target_idx + 1, window_id)
        self._notify_change(window_id, ZOrderPosition.BELOW, target_id)

    def get_above(self, window_id: str) -> list[str]:
        """Get windows above a given window.

        Args:
            window_id: Reference window.

        Returns:
            List of window IDs above the reference.
        """
        if window_id not in self._order:
            return []

        idx = self._order.index(window_id)
        return self._order[idx + 1:]

    def get_below(self, window_id: str) -> list[str]:
        """Get windows below a given window.

        Args:
            window_id: Reference window.

        Returns:
            List of window IDs below the reference.
        """
        if window_id not in self._order:
            return []

        idx = self._order.index(window_id)
        return self._order[:idx]

    def get_top(self) -> Optional[str]:
        """Get the topmost window.

        Returns:
            Window ID or None.
        """
        return self._order[-1] if self._order else None

    def get_bottom(self) -> Optional[str]:
        """Get the bottommost window.

        Returns:
            Window ID or None.
        """
        return self._order[0] if self._order else None

    def get_order(self) -> list[str]:
        """Get the full z-order list.

        Returns:
            List of window IDs from bottom to top.
        """
        return list(self._order)

    def get_z_index(self, window_id: str) -> int:
        """Get z-index of a window.

        Args:
            window_id: Window identifier.

        Returns:
            Z-index value or 0 if not found.
        """
        entry = self._entries.get(window_id)
        return entry.z_index if entry else 0

    def set_layer(self, window_id: str, layer: str) -> None:
        """Set the layer for a window.

        Args:
            window_id: Window identifier.
            layer: New layer name.
        """
        if window_id in self._entries:
            self._entries[window_id].layer = layer
            self._rebuild_order()

    def get_layer_windows(self, layer: str) -> list[str]:
        """Get all windows in a layer.

        Args:
            layer: Layer name.

        Returns:
            List of window IDs in the layer.
        """
        return [
            wid for wid in self._order
            if self._entries[wid].layer == layer
        ]

    def _rebuild_order(self) -> None:
        """Rebuild the z-order list from entries."""
        self._order = sorted(
            self._entries.keys(),
            key=lambda wid: (
                self._entries[wid].layer,
                self._entries[wid].z_index,
            ),
        )

    def on_zorder_change(
        self,
        callback: Callable[[str, ZOrderPosition, Optional[str]], None],
    ) -> None:
        """Register a callback for z-order changes.

        Args:
            callback: Function called on change.
        """
        self._callbacks.append(callback)

    def _notify_change(
        self,
        window_id: str,
        position: ZOrderPosition,
        relative_to: Optional[str],
    ) -> None:
        """Notify callbacks of a z-order change."""
        for callback in self._callbacks:
            try:
                callback(window_id, position, relative_to)
            except Exception:
                pass


class LayerManager:
    """Manages window layers.

    Example:
        mgr = LayerManager()
        mgr.create_layer("modal")
        mgr.move_to_layer("popup1", "modal")
    """

    def __init__(self):
        """Initialize the layer manager."""
        self._layers: dict[str, int] = {}
        self._zorder_stack: Optional[ZOrderStack] = None

    def set_zorder_stack(self, stack: ZOrderStack) -> None:
        """Set the z-order stack to use.

        Args:
            stack: ZOrderStack instance.
        """
        self._zorder_stack = stack

    def create_layer(
        self,
        layer: str,
        z_index: int = 100,
    ) -> None:
        """Create a new layer.

        Args:
            layer: Layer name.
            z_index: Base z-index for the layer.
        """
        self._layers[layer] = z_index

    def delete_layer(self, layer: str) -> None:
        """Delete a layer.

        Args:
            layer: Layer name.
        """
        if layer in self._layers:
            del self._layers[layer]

    def get_layer_z_index(self, layer: str) -> int:
        """Get the base z-index for a layer.

        Args:
            layer: Layer name.

        Returns:
            Z-index value or 0.
        """
        return self._layers.get(layer, 0)

    def move_to_layer(self, window_id: str, layer: str) -> None:
        """Move a window to a layer.

        Args:
            window_id: Window to move.
            layer: Target layer.
        """
        if self._zorder_stack:
            self._zorder_stack.set_layer(window_id, layer)
