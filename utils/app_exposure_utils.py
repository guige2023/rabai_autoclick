"""
Application Exposure Utilities for UI Automation.

This module provides utilities for detecting and measuring
application window exposure, visibility states, and focus events.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
import time


class ExposureState(Enum):
    """Application exposure states."""
    HIDDEN = "hidden"
    MINIMIZED = "minimized"
    PARTIALLY_VISIBLE = "partially_visible"
    FULLY_VISIBLE = "fully_visible"
    FOCUSED = "focused"


@dataclass
class ExposureEvent:
    """Event representing an exposure state change."""
    timestamp: float
    app_bundle_id: str
    previous_state: ExposureState
    new_state: ExposureState
    exposure_ratio: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExposureMetrics:
    """Metrics for application exposure."""
    app_bundle_id: str
    total_visible_time: float = 0.0
    total_hidden_time: float = 0.0
    focus_count: int = 0
    exposure_ratio: float = 0.0
    state_transitions: int = 0


class AppExposureTracker:
    """
    Track application exposure states over time.
    """

    def __init__(self, app_bundle_id: str):
        """
        Initialize tracker for an application.

        Args:
            app_bundle_id: Bundle identifier of the app
        """
        self.app_bundle_id = app_bundle_id
        self._events: List[ExposureEvent] = []
        self._current_state: ExposureState = ExposureState.HIDDEN
        self._state_start_time: float = time.time()
        self._metrics: ExposureMetrics = ExposureMetrics(app_bundle_id=app_bundle_id)
        self._listeners: List[Callable[[ExposureEvent], None]] = []

    def record_state_change(
        self,
        new_state: ExposureState,
        exposure_ratio: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ExposureEvent:
        """
        Record a state change event.

        Args:
            new_state: New exposure state
            exposure_ratio: Ratio of window that is visible (0.0-1.0)
            metadata: Additional event metadata

        Returns:
            The recorded ExposureEvent
        """
        current_time = time.time()
        event = ExposureEvent(
            timestamp=current_time,
            app_bundle_id=self.app_bundle_id,
            previous_state=self._current_state,
            new_state=new_state,
            exposure_ratio=exposure_ratio,
            metadata=metadata or {}
        )

        self._events.append(event)
        self._update_metrics(current_time, event)
        self._current_state = new_state
        self._state_start_time = current_time

        for listener in self._listeners:
            listener(event)

        return event

    def _update_metrics(self, current_time: float, event: ExposureEvent) -> None:
        """Update internal metrics based on event."""
        duration = current_time - self._state_start_time

        if self._current_state in (ExposureState.FULLY_VISIBLE, ExposureState.FOCUSED):
            self._metrics.total_visible_time += duration
        elif self._current_state == ExposureState.PARTIALLY_VISIBLE:
            self._metrics.total_visible_time += duration * event.exposure_ratio
        elif self._current_state == ExposureState.MINIMIZED:
            self._metrics.total_hidden_time += duration
        elif self._current_state == ExposureState.HIDDEN:
            self._metrics.total_hidden_time += duration

        if event.new_state == ExposureState.FOCUSED:
            self._metrics.focus_count += 1

        self._metrics.state_transitions += 1

        total_time = self._metrics.total_visible_time + self._metrics.total_hidden_time
        if total_time > 0:
            self._metrics.exposure_ratio = self._metrics.total_visible_time / total_time

    def add_listener(self, listener: Callable[[ExposureEvent], None]) -> None:
        """Add a listener for exposure events."""
        self._listeners.append(listener)

    def remove_listener(self, listener: Callable[[ExposureEvent], None]) -> None:
        """Remove an exposure event listener."""
        if listener in self._listeners:
            self._listeners.remove(listener)

    @property
    def metrics(self) -> ExposureMetrics:
        """Get current exposure metrics."""
        return self._metrics

    @property
    def events(self) -> List[ExposureEvent]:
        """Get all recorded events."""
        return self._events.copy()

    @property
    def current_state(self) -> ExposureState:
        """Get current exposure state."""
        return self._current_state

    def get_events_in_range(
        self,
        start_time: float,
        end_time: float
    ) -> List[ExposureEvent]:
        """Get events within a time range."""
        return [e for e in self._events if start_time <= e.timestamp <= end_time]


class ExposureAnalyzer:
    """
    Analyze application exposure patterns.
    """

    def __init__(self):
        """Initialize exposure analyzer."""
        self._trackers: Dict[str, AppExposureTracker] = {}

    def get_or_create_tracker(self, app_bundle_id: str) -> AppExposureTracker:
        """Get or create a tracker for an app."""
        if app_bundle_id not in self._trackers:
            self._trackers[app_bundle_id] = AppExposureTracker(app_bundle_id)
        return self._trackers[app_bundle_id]

    def get_aggregated_metrics(self) -> Dict[str, ExposureMetrics]:
        """Get metrics for all tracked applications."""
        return {bundle_id: tracker.metrics for bundle_id, tracker in self._trackers.items()}

    def get_most_exposed_app(self) -> Optional[str]:
        """Get the app with highest exposure ratio."""
        if not self._trackers:
            return None
        return max(
            self._trackers.items(),
            key=lambda x: x[1].metrics.exposure_ratio
        )[0]

    def get_most_focused_app(self) -> Optional[str]:
        """Get the app with most focus events."""
        if not self._trackers:
            return None
        return max(
            self._trackers.items(),
            key=lambda x: x[1].metrics.focus_count
        )[0]


def calculate_visibility_score(
    visible_time: float,
    total_time: float,
    focus_count: int,
    state_transitions: int
) -> float:
    """
    Calculate a composite visibility score.

    Args:
        visible_time: Total time app was visible
        total_time: Total tracking time
        focus_count: Number of focus events
        state_transitions: Number of state transitions

    Returns:
        Visibility score (0.0-100.0)
    """
    if total_time <= 0:
        return 0.0

    exposure_component = (visible_time / total_time) * 50.0
    focus_component = min(focus_count / 10.0, 1.0) * 30.0
    stability_component = max(1.0 - (state_transitions / 100.0), 0.0) * 20.0

    return exposure_component + focus_component + stability_component
