"""
Window Visibility Tracker Utilities

Track window visibility states over time, detecting transitions
between visible, hidden, minimized, maximized, and occluded states.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Optional


class VisibilityState(Enum):
    """Window visibility states."""
    VISIBLE = auto()
    HIDDEN = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    OCCLUDED = auto()  # covered by other windows
    UNKNOWN = auto()


@dataclass
class VisibilityTransition:
    """A visibility state transition."""
    window_id: int
    from_state: VisibilityState
    to_state: VisibilityState
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class WindowVisibilityInfo:
    """Current visibility information for a window."""
    window_id: int
    state: VisibilityState
    visible_area_percent: float  # 0.0 to 1.0
    occlusion_source: Optional[str] = None  # name of window occluding this one
    last_transition_ms: float = field(default_factory=lambda: time.time() * 1000)


class WindowVisibilityTracker:
    """Track window visibility state transitions."""

    def __init__(self):
        self._info: dict[int, WindowVisibilityInfo] = {}
        self._history: list[VisibilityTransition] = []

    def update_visibility(
        self,
        window_id: int,
        state: VisibilityState,
        visible_area_percent: float = 1.0,
        occlusion_source: Optional[str] = None,
    ) -> Optional[VisibilityTransition]:
        """Update the visibility state of a window."""
        current = self._info.get(window_id)
        old_state = current.state if current else VisibilityState.UNKNOWN

        if state == old_state:
            # Just update area percentage, no transition
            self._info[window_id] = WindowVisibilityInfo(
                window_id=window_id,
                state=state,
                visible_area_percent=visible_area_percent,
                occlusion_source=occlusion_source,
                last_transition_ms=current.last_transition_ms if current else time.time() * 1000,
            )
            return None

        now_ms = time.time() * 1000
        transition = VisibilityTransition(
            window_id=window_id,
            from_state=old_state,
            to_state=state,
            timestamp_ms=now_ms,
        )
        self._history.append(transition)

        self._info[window_id] = WindowVisibilityInfo(
            window_id=window_id,
            state=state,
            visible_area_percent=visible_area_percent,
            occlusion_source=occlusion_source,
            last_transition_ms=now_ms,
        )
        return transition

    def get_state(self, window_id: int) -> VisibilityState:
        """Get the current visibility state of a window."""
        info = self._info.get(window_id)
        return info.state if info else VisibilityState.UNKNOWN

    def get_info(self, window_id: int) -> Optional[WindowVisibilityInfo]:
        """Get full visibility info for a window."""
        return self._info.get(window_id)

    def get_visible_windows(self) -> list[int]:
        """Get IDs of all windows currently visible."""
        return [
            wid for wid, info in self._info.items()
            if info.state == VisibilityState.VISIBLE and info.visible_area_percent > 0.5
        ]

    def get_transition_count(self, window_id: int) -> int:
        """Get the number of visibility transitions for a window."""
        return sum(1 for t in self._history if t.window_id == window_id)
