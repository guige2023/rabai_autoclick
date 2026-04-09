"""
Multi-Touch Pattern Utilities for UI Automation.

This module provides utilities for recognizing and matching
multi-touch patterns in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Set
from enum import Enum


class TouchPattern(Enum):
    """Recognized multi-touch patterns."""
    SINGLE_TAP = "single_tap"
    DOUBLE_TAP = "double_tap"
    TWO_FINGER_TAP = "two_finger_tap"
    THREE_FINGER_TAP = "three_finger_tap"
    SCROLL = "scroll"
    ZOOM = "zoom"
    ROTATE = "rotate"
    SWIPE_LEFT = "swipe_left"
    SWIPE_RIGHT = "swipe_right"
    SWIPE_UP = "swipe_up"
    SWIPE_DOWN = "swipe_down"
    UNKNOWN = "unknown"


@dataclass
class MultiTouchContact:
    """A single contact in a multi-touch event."""
    id: int
    x: float
    y: float
    pressure: float
    timestamp: float
    touch_type: str = "finger"


@dataclass
class MultiTouchEvent:
    """Represents a complete multi-touch event."""
    pattern: TouchPattern
    contacts: List[MultiTouchContact] = field(default_factory=list)
    center_x: float = 0.0
    center_y: float = 0.0
    duration: float = 0.0
    timestamp: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatternMatch:
    """Result of pattern matching."""
    pattern: TouchPattern
    confidence: float
    matched_points: int
    total_points: int


class MultiTouchPatternMatcher:
    """Matches multi-touch input against known patterns."""

    def __init__(self) -> None:
        self._contact_count: int = 0
        self._contact_ids: Set[int] = set()
        self._touch_history: Dict[int, List[MultiTouchContact]] = {}
        self._start_time: Optional[float] = None
        self._last_tap_time: float = 0.0

    def begin_event(self) -> None:
        """Begin a new multi-touch event."""
        self._contact_ids.clear()
        self._touch_history.clear()
        self._contact_count = 0
        self._start_time = time.time()

    def add_contact(
        self,
        contact_id: int,
        x: float,
        y: float,
        pressure: float = 0.5,
    ) -> None:
        """Add a contact to the current event."""
        self._contact_ids.add(contact_id)
        self._contact_count = len(self._contact_ids)

        contact = MultiTouchContact(
            id=contact_id,
            x=x,
            y=y,
            pressure=pressure,
            timestamp=time.time(),
        )

        if contact_id not in self._touch_history:
            self._touch_history[contact_id] = []
        self._touch_history[contact_id].append(contact)

    def remove_contact(self, contact_id: int) -> None:
        """Remove a contact from the current event."""
        self._contact_ids.discard(contact_id)
        self._contact_count = len(self._contact_ids)

    def recognize_pattern(
        self,
        end_x: Optional[float] = None,
        end_y: Optional[float] = None,
    ) -> MultiTouchEvent:
        """Recognize the pattern from the current touch data."""
        duration = 0.0
        if self._start_time is not None:
            duration = (time.time() - self._start_time) * 1000.0

        center_x, center_y = self._calculate_center()

        pattern = self._classify_pattern(duration, end_x, end_y, center_x, center_y)

        all_contacts: List[MultiTouchContact] = []
        for contacts in self._touch_history.values():
            all_contacts.extend(contacts)

        return MultiTouchEvent(
            pattern=pattern,
            contacts=all_contacts,
            center_x=center_x,
            center_y=center_y,
            duration=duration,
            timestamp=self._start_time or time.time(),
        )

    def _calculate_center(self) -> Tuple[float, float]:
        """Calculate the center point of all contacts."""
        if not self._touch_history:
            return (0.0, 0.0)

        latest_contacts = []
        for contacts in self._touch_history.values():
            if contacts:
                latest_contacts.append(contacts[-1])

        if not latest_contacts:
            return (0.0, 0.0)

        avg_x = sum(c.x for c in latest_contacts) / len(latest_contacts)
        avg_y = sum(c.y for c in latest_contacts) / len(latest_contacts)
        return (avg_x, avg_y)

    def _classify_pattern(
        self,
        duration: float,
        end_x: Optional[float],
        end_y: Optional[float],
        center_x: float,
        center_y: float,
    ) -> TouchPattern:
        """Classify the touch pattern."""
        count = self._contact_count

        if count == 1:
            if duration < 200:
                return TouchPattern.SINGLE_TAP
            return TouchPattern.UNKNOWN

        if count == 2:
            if duration < 200:
                return TouchPattern.TWO_FINGER_TAP
            return TouchPattern.ZOOM

        if count == 3:
            if duration < 200:
                return TouchPattern.THREE_FINGER_TAP
            return TouchPattern.ROTATE

        if end_x is not None and end_y is not None:
            latest = list(self._touch_history.values())
            if latest:
                first_contacts = [group[0] for group in latest if group]
                if first_contacts:
                    start_center_x = sum(c.x for c in first_contacts) / len(first_contacts)
                    start_center_y = sum(c.y for c in first_contacts) / len(first_contacts)
                    dx = end_x - start_center_x
                    dy = end_y - start_center_y

                    if abs(dx) > abs(dy):
                        if dx < 0:
                            return TouchPattern.SWIPE_LEFT
                        else:
                            return TouchPattern.SWIPE_RIGHT
                    else:
                        if dy < 0:
                            return TouchPattern.SWIPE_UP
                        else:
                            return TouchPattern.SWIPE_DOWN

        return TouchPattern.UNKNOWN

    def get_contact_count(self) -> int:
        """Get the number of active contacts."""
        return self._contact_count

    def reset(self) -> None:
        """Reset all state."""
        self._contact_ids.clear()
        self._touch_history.clear()
        self._contact_count = 0
        self._start_time = None
        self._last_tap_time = 0.0
