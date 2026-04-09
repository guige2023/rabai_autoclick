"""
Window z-order comparison utilities.

This module provides utilities for comparing window z-ordering,
detecting z-order changes, and computing z-order distance metrics.
"""

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto


class ZOrderChange(Enum):
    """Type of z-order change detected."""
    NONE = auto()
    RAISED = auto()
    LOWERED = auto()
    REORDERED = auto()


@dataclass
class WindowZInfo:
    """Z-order information for a window."""
    window_id: str
    title: str = ""
    bundle_id: str = ""
    z_index: int = 0
    is_on_screen: bool = True
    owner_pid: int = 0


@dataclass
class ZOrderDelta:
    """Change detected in z-ordering between two snapshots."""
    window_id: str
    old_z: int = 0
    new_z: int = 0
    change_type: ZOrderChange = ZOrderChange.NONE

    @property
    def z_diff(self) -> int:
        """Difference in z-index."""
        return self.new_z - self.old_z

    @property
    def moved_up(self) -> bool:
        """True if window moved higher (lower z-index number)."""
        return self.new_z < self.old_z

    @property
    def moved_down(self) -> bool:
        """True if window moved lower (higher z-index number)."""
        return self.new_z > self.old_z


@dataclass
class ZOrderComparison:
    """Full comparison result between two z-order snapshots."""
    deltas: List[ZOrderDelta] = field(default_factory=list)
    added_windows: List[str] = field(default_factory=list)
    removed_windows: List[str] = field(default_factory=list)
    reordered_count: int = 0

    def has_changes(self) -> bool:
        """Return True if any z-order changes occurred."""
        return len(self.deltas) > 0 or len(self.added_windows) > 0 or len(self.removed_windows) > 0


def build_z_order(snapshot: List[WindowZInfo]) -> Dict[str, int]:
    """
    Build a z-index map from a snapshot.

    Args:
        snapshot: List of windows with their z positions.

    Returns:
        Dictionary mapping window_id to z-index (0 = topmost).
    """
    sorted_windows = sorted(snapshot, key=lambda w: w.z_index)
    return {w.window_id: i for i, w in enumerate(sorted_windows)}


def compare_z_orders(
    old_snapshot: List[WindowZInfo],
    new_snapshot: List[WindowZInfo],
) -> ZOrderComparison:
    """
    Compare two window z-order snapshots.

    Args:
        old_snapshot: Previous window snapshot.
        new_snapshot: Current window snapshot.

    Returns:
        ZOrderComparison with all detected changes.
    """
    old_z_map = build_z_order(old_snapshot)
    new_z_map = build_z_order(new_snapshot)

    old_ids = set(old_z_map.keys())
    new_ids = set(new_z_map.keys())

    added = list(new_ids - old_ids)
    removed = list(old_ids - new_ids)
    common = old_ids & new_ids

    deltas: List[ZOrderDelta] = []
    reordered_count = 0

    for wid in common:
        old_z = old_z_map[wid]
        new_z = new_z_map[wid]
        if old_z != new_z:
            change_type = ZOrderChange.REORDERED
            if new_z < old_z:
                change_type = ZOrderChange.RAISED
            else:
                change_type = ZOrderChange.LOWERED
            deltas.append(ZOrderDelta(
                window_id=wid,
                old_z=old_z,
                new_z=new_z,
                change_type=change_type,
            ))
            reordered_count += 1

    return ZOrderComparison(
        deltas=deltas,
        added_windows=added,
        removed_windows=removed,
        reordered_count=reordered_count,
    )


def get_top_windows(snapshot: List[WindowZInfo], count: int = 5) -> List[WindowZInfo]:
    """
    Get the top N windows by z-order.

    Args:
        snapshot: Window snapshot.
        count: Number of top windows to return.

    Returns:
        List of topmost windows (topmost first).
    """
    return sorted(snapshot, key=lambda w: w.z_index)[:count]


def get_bottom_windows(snapshot: List[WindowZInfo], count: int = 5) -> List[WindowZInfo]:
    """
    Get the bottom N windows by z-order.

    Args:
        snapshot: Window snapshot.
        count: Number of bottom windows to return.

    Returns:
        List of bottommost windows (bottommost first).
    """
    return sorted(snapshot, key=lambda w: w.z_index, reverse=True)[:count]


def z_distance(window1: str, window2: str, z_map: Dict[str, int]) -> int:
    """
    Compute z-order distance between two windows.

    Args:
        window1: First window ID.
        window2: Second window ID.
        z_map: Z-index map.

    Returns:
        Absolute difference in z-indices.
    """
    z1 = z_map.get(window1, -1)
    z2 = z_map.get(window2, -1)
    return abs(z1 - z2)


def is_window_occluded(window: WindowZInfo, snapshot: List[WindowZInfo]) -> bool:
    """
    Check if a window is occluded by other windows above it.

    Args:
        window: Window to check.
        snapshot: Full window snapshot.

    Returns:
        True if window is not the topmost and has windows above it.
    """
    if not snapshot:
        return False
    sorted_snap = sorted(snapshot, key=lambda w: w.z_index)
    topmost_id = sorted_snap[0].window_id if sorted_snap else None
    if window.window_id == topmost_id:
        return False

    # Check if window bounds overlap with anything above
    # Simplified: just check if it's not on top
    return True
