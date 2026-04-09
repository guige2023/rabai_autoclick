"""UI visibility utilities for UI automation.

Provides utilities for detecting element visibility,
tracking visibility changes, and managing visible element states.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Set


@dataclass
class VisibilityState:
    """State of an element's visibility."""
    element_id: str
    is_visible: bool
    is_displayed: bool
    is_enabled: bool
    is_in_viewport: bool
    opacity: float
    timestamp_ms: float
    metadata: Dict = field(default_factory=dict)


@dataclass
class VisibilityChange:
    """Records a visibility change event."""
    element_id: str
    from_state: VisibilityState
    to_state: VisibilityState
    timestamp_ms: float
    change_type: str


class VisibilityDetector:
    """Detects element visibility states.
    
    Analyzes elements to determine if they are
    visible, displayed, enabled, and in viewport.
    """
    
    def __init__(
        self,
        viewport_width: float = 1920.0,
        viewport_height: float = 1080.0
    ) -> None:
        """Initialize the visibility detector.
        
        Args:
            viewport_width: Viewport width.
            viewport_height: Viewport height.
        """
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self._opacity_threshold = 0.3
    
    def check_visibility(
        self,
        element_id: str,
        x: float,
        y: float,
        width: float,
        height: float,
        is_displayed: bool = True,
        is_enabled: bool = True,
        opacity: float = 1.0
    ) -> VisibilityState:
        """Check visibility state of an element.
        
        Args:
            element_id: Element identifier.
            x: Element X position.
            y: Element Y position.
            width: Element width.
            height: Element height.
            is_displayed: Whether element is displayed.
            is_enabled: Whether element is enabled.
            opacity: Element opacity.
            
        Returns:
            VisibilityState.
        """
        is_visible = (
            is_displayed and
            is_enabled and
            opacity >= self._opacity_threshold and
            self._is_in_viewport(x, y, width, height)
        )
        
        is_in_viewport = self._is_in_viewport(x, y, width, height)
        
        return VisibilityState(
            element_id=element_id,
            is_visible=is_visible,
            is_displayed=is_displayed,
            is_enabled=is_enabled,
            is_in_viewport=is_in_viewport,
            opacity=opacity,
            timestamp_ms=time.time() * 1000
        )
    
    def _is_in_viewport(
        self,
        x: float,
        y: float,
        width: float,
        height: float
    ) -> bool:
        """Check if element is in viewport.
        
        Args:
            x: Element X position.
            y: Element Y position.
            width: Element width.
            height: Element height.
            
        Returns:
            True if in viewport.
        """
        if x + width < 0 or x > self.viewport_width:
            return False
        
        if y + height < 0 or y > self.viewport_height:
            return False
        
        return True
    
    def set_opacity_threshold(self, threshold: float) -> None:
        """Set minimum opacity for visibility.
        
        Args:
            threshold: Opacity threshold (0.0 to 1.0).
        """
        self._opacity_threshold = threshold
    
    def set_viewport_size(self, width: float, height: float) -> None:
        """Set viewport dimensions.
        
        Args:
            width: Viewport width.
            height: Viewport height.
        """
        self.viewport_width = width
        self.viewport_height = height


class VisibilityTracker:
    """Tracks visibility changes over time.
    
    Maintains history of visibility states and
    detects visibility change events.
    """
    
    def __init__(self, max_history: int = 100) -> None:
        """Initialize the visibility tracker.
        
        Args:
            max_history: Maximum history entries per element.
        """
        self.max_history = max_history
        self._history: Dict[str, Deque[VisibilityState]] = {}
        self._change_listeners: List[Callable[[VisibilityChange], None]] = []
    
    def record_state(self, state: VisibilityState) -> Optional[VisibilityChange]:
        """Record a visibility state.
        
        Args:
            state: Visibility state to record.
            
        Returns:
            VisibilityChange if state changed, None otherwise.
        """
        if state.element_id not in self._history:
            self._history[state.element_id] = deque(maxlen=self.max_history)
        
        history = self._history[state.element_id]
        
        previous_state = history[-1] if history else None
        
        history.append(state)
        
        if previous_state and self._has_changed(previous_state, state):
            change = VisibilityChange(
                element_id=state.element_id,
                from_state=previous_state,
                to_state=state,
                timestamp_ms=state.timestamp_ms,
                change_type=self._get_change_type(previous_state, state)
            )
            
            for listener in self._change_listeners:
                listener(change)
            
            return change
        
        return None
    
    def _has_changed(
        self,
        old_state: VisibilityState,
        new_state: VisibilityState
    ) -> bool:
        """Check if visibility state changed.
        
        Args:
            old_state: Previous state.
            new_state: Current state.
            
        Returns:
            True if changed.
        """
        return (
            old_state.is_visible != new_state.is_visible or
            old_state.is_displayed != new_state.is_displayed or
            old_state.is_enabled != new_state.is_enabled or
            old_state.is_in_viewport != new_state.is_in_viewport or
            abs(old_state.opacity - new_state.opacity) > 0.01
        )
    
    def _get_change_type(
        self,
        old_state: VisibilityState,
        new_state: VisibilityState
    ) -> str:
        """Determine type of visibility change.
        
        Args:
            old_state: Previous state.
            new_state: Current state.
            
        Returns:
            Change type string.
        """
        if old_state.is_visible and not new_state.is_visible:
            return "hidden"
        elif not old_state.is_visible and new_state.is_visible:
            return "shown"
        elif not old_state.is_enabled and new_state.is_enabled:
            return "enabled"
        elif old_state.is_enabled and not new_state.is_enabled:
            return "disabled"
        elif old_state.is_displayed != new_state.is_displayed:
            return "display_changed"
        elif abs(old_state.opacity - new_state.opacity) > 0.01:
            return "opacity_changed"
        else:
            return "viewport_changed"
    
    def get_current_state(self, element_id: str) -> Optional[VisibilityState]:
        """Get current visibility state.
        
        Args:
            element_id: Element identifier.
            
        Returns:
            Current state or None.
        """
        history = self._history.get(element_id)
        if history:
            return history[-1]
        return None
    
    def get_state_history(
        self,
        element_id: str,
        limit: int = 10
    ) -> List[VisibilityState]:
        """Get state history for element.
        
        Args:
            element_id: Element identifier.
            limit: Maximum entries to return.
            
        Returns:
            List of historical states.
        """
        history = self._history.get(element_id)
        if not history:
            return []
        
        return list(history)[-limit:]
    
    def add_change_listener(
        self,
        listener: Callable[[VisibilityChange], None]
    ) -> None:
        """Add a visibility change listener.
        
        Args:
            listener: Function to call on change.
        """
        self._change_listeners.append(listener)
    
    def get_all_visible_elements(self) -> List[str]:
        """Get all currently visible elements.
        
        Returns:
            List of visible element IDs.
        """
        visible = []
        for element_id, history in self._history.items():
            if history:
                latest = history[-1]
                if latest.is_visible:
                    visible.append(element_id)
        return visible
    
    def clear_history(self, element_id: Optional[str] = None) -> None:
        """Clear visibility history.
        
        Args:
            element_id: Element to clear (all if None).
        """
        if element_id:
            if element_id in self._history:
                self._history[element_id].clear()
        else:
            self._history.clear()


class VisibilityWaiter:
    """Waits for element visibility conditions.
    
    Provides utilities for waiting until elements
    become visible or hidden.
    """
    
    def __init__(
        self,
        tracker: VisibilityTracker,
        timeout_ms: float = 30000.0,
        poll_interval_ms: float = 100.0
    ) -> None:
        """Initialize the visibility waiter.
        
        Args:
            tracker: Visibility tracker to use.
            timeout_ms: Default timeout.
            poll_interval_ms: Poll interval.
        """
        self.tracker = tracker
        self.default_timeout_ms = timeout_ms
        self.poll_interval_ms = poll_interval_ms
    
    def wait_until_visible(
        self,
        element_id: str,
        timeout_ms: Optional[float] = None
    ) -> bool:
        """Wait until element becomes visible.
        
        Args:
            element_id: Element identifier.
            timeout_ms: Timeout (uses default if None).
            
        Returns:
            True if visible, False if timeout.
        """
        timeout = timeout_ms if timeout_ms is not None else self.default_timeout_ms
        deadline = time.time() * 1000 + timeout
        
        while time.time() * 1000 < deadline:
            state = self.tracker.get_current_state(element_id)
            if state and state.is_visible:
                return True
            
            time.sleep(self.poll_interval_ms / 1000.0)
        
        return False
    
    def wait_until_hidden(
        self,
        element_id: str,
        timeout_ms: Optional[float] = None
    ) -> bool:
        """Wait until element becomes hidden.
        
        Args:
            element_id: Element identifier.
            timeout_ms: Timeout (uses default if None).
            
        Returns:
            True if hidden, False if timeout.
        """
        timeout = timeout_ms if timeout_ms is not None else self.default_timeout_ms
        deadline = time.time() * 1000 + timeout
        
        while time.time() * 1000 < deadline:
            state = self.tracker.get_current_state(element_id)
            if state and not state.is_visible:
                return True
            
            time.sleep(self.poll_interval_ms / 1000.0)
        
        return False
    
    def wait_for_any_visible(
        self,
        element_ids: List[str],
        timeout_ms: Optional[float] = None
    ) -> Optional[str]:
        """Wait for any element to become visible.
        
        Args:
            element_ids: Element identifiers.
            timeout_ms: Timeout.
            
        Returns:
            Element ID if visible, None if timeout.
        """
        timeout = timeout_ms if timeout_ms is not None else self.default_timeout_ms
        deadline = time.time() * 1000 + timeout
        
        while time.time() * 1000 < deadline:
            for element_id in element_ids:
                state = self.tracker.get_current_state(element_id)
                if state and state.is_visible:
                    return element_id
            
            time.sleep(self.poll_interval_ms / 1000.0)
        
        return None


def calculate_visibility_score(state: VisibilityState) -> float:
    """Calculate a visibility score for a state.
    
    Args:
        state: Visibility state.
        
    Returns:
        Score from 0.0 to 1.0.
    """
    score = 0.0
    
    if state.is_displayed:
        score += 0.3
    
    if state.is_enabled:
        score += 0.2
    
    if state.is_in_viewport:
        score += 0.3
    
    score += state.opacity * 0.2
    
    return min(1.0, score)
