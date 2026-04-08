"""Window Visibility and State Utilities.

Utilities for checking and managing window visibility states.
Handles minimized, maximized, fullscreen, and occluded window conditions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class VisibilityState(Enum):
    """Possible visibility states for a window."""

    VISIBLE = auto()
    MINIMIZED = auto()
    HIDDEN = auto()
    OCCLUDED = auto()
    FULLSCREEN = auto()
    UNKNOWN = auto()


class WindowState(Enum):
    """Window state flags."""

    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    SNAPPED_LEFT = auto()
    SNAPPED_RIGHT = auto()


@dataclass
class VisibilityInfo:
    """Complete visibility information for a window.

    Attributes:
        state: Current VisibilityState of the window.
        is_on_screen: Whether any part of the window is visible on screen.
        occlusion_ratio: Portion of window covered by other windows (0.0 to 1.0).
        is_focused: Whether the window currently has focus.
        screen_area: Visible screen area in pixels.
        bounds: Window bounds as (x, y, width, height).
    """

    state: VisibilityState
    is_on_screen: bool
    occlusion_ratio: float
    is_focused: bool
    screen_area: tuple[int, int, int, int]
    bounds: tuple[int, int, int, int]

    @property
    def is_fully_visible(self) -> bool:
        """Check if the entire window is visible."""
        return (
            self.state == VisibilityState.VISIBLE
            and self.is_on_screen
            and self.occlusion_ratio == 0.0
        )

    @property
    def is_interactive(self) -> bool:
        """Check if the window can receive user input."""
        return (
            self.state in (VisibilityState.VISIBLE, VisibilityState.FULLSCREEN)
            and self.is_on_screen
            and self.occlusion_ratio < 0.5
        )


class VisibilityPredicate:
    """Predicate functions for window visibility checks.

    Use these as filters or conditions in window management code.

    Example:
        predicate = VisibilityPredicate()
        if predicate.is_fully_visible(info):
            perform_action()
    """

    @staticmethod
    def is_fully_visible(info: VisibilityInfo) -> bool:
        """Check if window is completely visible."""
        return info.is_fully_visible

    @staticmethod
    def is_minimized(info: VisibilityInfo) -> bool:
        """Check if window is minimized."""
        return info.state == VisibilityState.MINIMIZED

    @staticmethod
    def is_occluded(info: VisibilityInfo) -> bool:
        """Check if window is at least partially occluded."""
        return info.occlusion_ratio > 0.0

    @staticmethod
    def is_heavily_occluded(info: VisibilityInfo, threshold: float = 0.5) -> bool:
        """Check if window is occluded beyond threshold."""
        return info.occlusion_ratio >= threshold

    @staticmethod
    def is_interactive(info: VisibilityInfo) -> bool:
        """Check if window can receive user input."""
        return info.is_interactive

    @staticmethod
    def is_on_screen(info: VisibilityInfo) -> bool:
        """Check if any part of window is on screen."""
        return info.is_on_screen


class VisibilityChecker:
    """Checks window visibility states.

    Provides methods to query and validate window visibility conditions.

    Example:
        checker = VisibilityChecker()
        info = checker.get_visibility_info(window_id)
        if checker.is_ready_for_input(info):
            window.activate()
    """

    def get_visibility_info(
        self,
        window_id: int,
        bounds: tuple[int, int, int, int],
        state_flags: set[WindowState],
    ) -> VisibilityInfo:
        """Get complete visibility information for a window.

        Args:
            window_id: Platform window identifier.
            bounds: Window bounds as (x, y, width, height).
            state_flags: Set of current WindowState flags.

        Returns:
            VisibilityInfo with all visibility details.
        """
        if WindowState.MINIMIZED in state_flags:
            state = VisibilityState.MINIMIZED
        elif WindowState.FULLSCREEN in state_flags:
            state = VisibilityState.FULLSCREEN
        elif self._is_occluded_by_others(bounds):
            state = VisibilityState.OCCLUDED
        else:
            state = VisibilityState.VISIBLE

        is_on_screen = self._check_on_screen(bounds)
        occlusion_ratio = self._calculate_occlusion_ratio(bounds)
        is_focused = self._check_has_focus(window_id)

        return VisibilityInfo(
            state=state,
            is_on_screen=is_on_screen,
            occlusion_ratio=occlusion_ratio,
            is_focused=is_focused,
            screen_area=self._get_screen_bounds(),
            bounds=bounds,
        )

    def _check_on_screen(self, bounds: tuple[int, int, int, int]) -> bool:
        """Check if window has any visible area on screen."""
        x, y, w, h = bounds
        screen_x, screen_y, screen_w, screen_h = self._get_screen_bounds()

        visible_right = min(x + w, screen_x + screen_w)
        visible_bottom = min(y + h, screen_y + screen_h)
        visible_left = max(x, screen_x)
        visible_top = max(y, screen_y)

        return visible_right > visible_left and visible_bottom > visible_top

    def _calculate_occlusion_ratio(self, bounds: tuple[int, int, int, int]) -> float:
        """Calculate what portion of the window is covered."""
        x, y, w, h = bounds
        total_area = w * h
        if total_area == 0:
            return 0.0

        # Simplified occlusion estimation
        # In real implementation, would use platform APIs
        visible_area = total_area  # Placeholder
        occluded_area = total_area - visible_area
        return max(0.0, min(1.0, occluded_area / total_area))

    def _is_occluded_by_others(self, bounds: tuple[int, int, int, int]) -> bool:
        """Check if window is covered by other windows."""
        x, y, w, h = bounds
        # Simplified check - would use platform APIs
        return False

    def _check_has_focus(self, window_id: int) -> bool:
        """Check if window currently has focus."""
        # Would use platform-specific focus detection
        return True

    def _get_screen_bounds(self) -> tuple[int, int, int, int]:
        """Get the primary screen bounds."""
        # Would use platform API to get actual screen bounds
        return (0, 0, 1920, 1080)

    def is_ready_for_input(self, info: VisibilityInfo) -> bool:
        """Check if window is in a state ready to receive input.

        Args:
            info: VisibilityInfo from get_visibility_info.

        Returns:
            True if window can receive input.
        """
        return info.is_interactive

    def needs_restoration(self, info: VisibilityInfo) -> bool:
        """Check if window needs to be restored before use.

        Args:
            info: VisibilityInfo from get_visibility_info.

        Returns:
            True if window needs restoration steps.
        """
        return info.state in (VisibilityState.MINIMIZED, VisibilityState.OCCLUDED)


class WindowRestorer:
    """Restores windows to a visible, interactive state."""

    def __init__(self, platform_adapter=None):
        """Initialize restorer with optional platform adapter.

        Args:
            platform_adapter: Platform-specific window control adapter.
        """
        self._adapter = platform_adapter

    def restore(self, info: VisibilityInfo) -> bool:
        """Restore window to visible, interactive state.

        Args:
            info: Current VisibilityInfo of the window.

        Returns:
            True if restoration was successful.
        """
        if info.state == VisibilityState.MINIMIZED:
            return self._unminimize()
        elif info.state == VisibilityState.OCCLUDED:
            return self._bring_to_front()
        elif info.state == VisibilityState.HIDDEN:
            return self._show()
        return True

    def _unminimize(self) -> bool:
        """Restore from minimized state."""
        if self._adapter:
            return self._adapter.restore()
        return False

    def _bring_to_front(self) -> bool:
        """Bring occluded window to front."""
        if self._adapter:
            return self._adapter.raise_window()
        return False

    def _show(self) -> bool:
        """Show hidden window."""
        if self._adapter:
            return self._adapter.show()
        return False
