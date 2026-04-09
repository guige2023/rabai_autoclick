"""
Window Z-Order Operations Utilities

Manage window stacking order (z-order) operations: bringing windows
to front, sending to back, and querying relative z-order positions.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ZOrderInfo:
    """Information about a window's z-order position."""
    window_id: int
    window_title: str
    z_position: int  # 0 = bottom, higher = more on top
    relative_to_focused: int  # negative = below focused, positive = above


class WindowZOrderManager:
    """Manage z-order operations for windows in an automation context."""

    def __init__(self):
        self._z_cache: dict[int, int] = {}

    def bring_to_front(self, window_id: int) -> bool:
        """Bring a window to the front of the z-order stack."""
        self._z_cache[window_id] = self._get_max_z() + 1
        return True

    def send_to_back(self, window_id: int) -> bool:
        """Send a window to the back of the z-order stack."""
        min_z = self._get_min_z() - 1
        self._z_cache[window_id] = min_z
        return True

    def get_z_position(self, window_id: int) -> int:
        """Get the current z-position of a window."""
        return self._z_cache.get(window_id, 0)

    def is_above(self, window_id_a: int, window_id_b: int) -> bool:
        """Check if window A is positioned above window B."""
        return self.get_z_position(window_id_a) > self.get_z_position(window_id_b)

    def sort_by_z_order(self, window_ids: List[int]) -> List[int]:
        """Return window IDs sorted from bottom to top."""
        return sorted(window_ids, key=lambda wid: self.get_z_position(wid))

    def _get_max_z(self) -> int:
        """Get the current maximum z-order value."""
        return max(self._z_cache.values()) if self._z_cache else 0

    def _get_min_z(self) -> int:
        """Get the current minimum z-order value."""
        return min(self._z_cache.values()) if self._z_cache else 0


def compare_z_order(
    windows: List[tuple[int, str, int]],  # (id, title, z_position)
    focused_window_id: int,
) -> List[ZOrderInfo]:
    """Build a list of ZOrderInfo from raw window data."""
    results = []
    for wid, title, zpos in windows:
        results.append(ZOrderInfo(
            window_id=wid,
            window_title=title,
            z_position=zpos,
            relative_to_focused=zpos - next(
                (zp for w in windows if w[0] == focused_window_id),
                (0, "", 0)
            )[2],
        ))
    return sorted(results, key=lambda z: z.z_position)
