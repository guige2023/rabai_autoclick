"""
Input Noise Filter Utilities for UI Automation.

This module provides utilities for filtering noise from
input events in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class NoiseType(Enum):
    """Types of input noise."""
    MICRO_MOVEMENT = "micro_movement"
    RAPID_FLICKER = "rapid_flicker"
    GHOST_CLICK = "ghost_click"
    STUCK_KEY = "stuck_key"
    DRIFT = "drift"


@dataclass
class InputEvent:
    """Represents an input event."""
    event_type: str
    timestamp: float
    x: Optional[float] = None
    y: Optional[float] = None
    key: Optional[str] = None
    value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FilterConfig:
    """Configuration for noise filtering."""
    micro_movement_threshold: float = 2.0
    flicker_interval_ms: float = 50.0
    ghost_click_delay_ms: float = 100.0
    drift_threshold: float = 5.0
    stuck_key_timeout_ms: float = 5000.0


class InputNoiseFilter:
    """
    Filter noise from input events.
    """

    def __init__(self, config: Optional[FilterConfig] = None):
        """
        Initialize input noise filter.

        Args:
            config: Filter configuration
        """
        self.config = config or FilterConfig()
        self._last_mouse_pos: Optional[tuple] = None
        self._last_mouse_time: float = 0.0
        self._last_click_time: float = 0.0
        self._last_click_pos: Optional[tuple] = None
        self._held_keys: Dict[str, float] = {}

    def filter_mouse_move(self, event: InputEvent) -> Optional[InputEvent]:
        """
        Filter noise from mouse move events.

        Args:
            event: Mouse move event

        Returns:
            Filtered event or None if filtered out
        """
        if event.x is None or event.y is None:
            return event

        if self._last_mouse_pos is None:
            self._last_mouse_pos = (event.x, event.y)
            self._last_mouse_time = event.timestamp
            return event

        dx = event.x - self._last_mouse_pos[0]
        dy = event.y - self._last_mouse_pos[1]
        distance = (dx * dx + dy * dy) ** 0.5

        dt = event.timestamp - self._last_mouse_time

        if distance < self.config.micro_movement_threshold:
            if dt < 0.1:
                self._last_mouse_pos = (event.x, event.y)
                self._last_mouse_time = event.timestamp
                return None

        self._last_mouse_pos = (event.x, event.y)
        self._last_mouse_time = event.timestamp
        return event

    def filter_click(self, event: InputEvent) -> Optional[InputEvent]:
        """
        Filter noise from click events.

        Args:
            event: Click event

        Returns:
            Filtered event or None if filtered out
        """
        if self._last_click_pos is not None and event.x is not None and event.y is not None:
            dx = event.x - self._last_click_pos[0]
            dy = event.y - self._last_click_pos[1]
            distance = (dx * dx + dy * dy) ** 0.5

            dt_ms = (event.timestamp - self._last_click_time) * 1000

            if distance < self.config.ghost_click_delay_ms and dt_ms < self.config.ghost_click_delay_ms:
                return None

        self._last_click_pos = (event.x, event.y)
        self._last_click_time = event.timestamp
        return event

    def filter_key_event(self, event: InputEvent) -> Optional[InputEvent]:
        """
        Filter noise from keyboard events.

        Args:
            event: Key event

        Returns:
            Filtered event or None if filtered out
        """
        if event.key is None:
            return event

        if event.event_type == "key_down":
            if event.key in self._held_keys:
                prev_time = self._held_keys[event.key]
                if event.timestamp - prev_time < 0.05:
                    return None
            self._held_keys[event.key] = event.timestamp
            return event

        elif event.event_type == "key_up":
            if event.key not in self._held_keys:
                return None

            held_duration = event.timestamp - self._held_keys[event.key]
            timeout_ms = self.config.stuck_key_timeout_ms / 1000.0

            if held_duration > timeout_ms:
                return None

            del self._held_keys[event.key]
            return event

        return event

    def filter(self, event: InputEvent) -> Optional[InputEvent]:
        """
        Filter any input event.

        Args:
            event: Input event

        Returns:
            Filtered event or None
        """
        if event.event_type in ("mouse_move", "mousemove"):
            return self.filter_mouse_move(event)
        elif event.event_type in ("click", "mouse_down", "mouse_up"):
            return self.filter_click(event)
        elif event.event_type.startswith("key"):
            return self.filter_key_event(event)
        return event


class ChainedNoiseFilter:
    """
    Chain multiple noise filters together.
    """

    def __init__(self):
        """Initialize chained filter."""
        self._filters: List[Callable[[InputEvent], Optional[InputEvent]]] = []

    def add_filter(
        self,
        filter_func: Callable[[InputEvent], Optional[InputEvent]]
    ) -> 'ChainedNoiseFilter':
        """
        Add a filter to the chain.

        Args:
            filter_func: Filter function

        Returns:
            Self for chaining
        """
        self._filters.append(filter_func)
        return self

    def filter(self, event: InputEvent) -> Optional[InputEvent]:
        """
        Apply all filters in chain.

        Args:
            event: Input event

        Returns:
            Filtered event or None
        """
        result = event
        for filter_func in self._filters:
            if result is None:
                return None
            result = filter_func(result)
        return result

    def filter_many(self, events: List[InputEvent]) -> List[InputEvent]:
        """
        Filter multiple events.

        Args:
            events: List of events

        Returns:
            Filtered events
        """
        return [e for e in events if self.filter(e) is not None]


def detect_noise_type(event: InputEvent, context: Dict[str, Any]) -> List[NoiseType]:
    """
    Detect types of noise in an event.

    Args:
        event: Input event
        context: Context with history

    Returns:
        List of detected noise types
    """
    noise_types = []

    if event.event_type == "mouse_move":
        prev_pos = context.get("prev_mouse_pos")
        prev_time = context.get("prev_mouse_time", 0.0)

        if prev_pos and event.x is not None:
            dx = event.x - prev_pos[0]
            dy = event.y - prev_pos[1]
            distance = (dx * dx + dy * dy) ** 0.5

            if distance < 2.0:
                noise_types.append(NoiseType.MICRO_MOVEMENT)

            dt = event.timestamp - prev_time
            if dt < 0.05 and distance < 10.0:
                noise_types.append(NoiseType.RAPID_FLICKER)

    elif event.event_type == "key_down":
        held_keys = context.get("held_keys", {})
        if event.key in held_keys:
            noise_types.append(NoiseType.STUCK_KEY)

    return noise_types
