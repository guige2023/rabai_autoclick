"""
Input Filter Action Module

Filters, transforms, and routes input events for
selective automation processing.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Input event types."""

    MOUSE_MOVE = "mouse_move"
    MOUSE_CLICK = "mouse_click"
    MOUSE_WHEEL = "mouse_wheel"
    KEY_DOWN = "key_down"
    KEY_UP = "key_up"
    TOUCH_START = "touch_start"
    TOUCH_MOVE = "touch_move"
    TOUCH_END = "touch_end"


@dataclass
class InputEvent:
    """Represents an input event."""

    event_type: EventType
    timestamp: float
    x: Optional[float] = None
    y: Optional[float] = None
    button: Optional[int] = None
    key: Optional[str] = None
    modifiers: Set[str] = field(default_factory=set)
    delta: Optional[int] = None


@dataclass
class FilterRule:
    """Rule for filtering events."""

    name: str
    event_types: Set[EventType]
    condition: Optional[Callable[[InputEvent], bool]] = None
    action: str = "pass"  # pass, block, modify


class InputFilter:
    """
    Filters and processes input events.

    Supports event blocking, transformation,
    and conditional routing.
    """

    def __init__(self):
        self._rules: List[FilterRule] = []
        self._event_handlers: Dict[EventType, List[Callable]] = {}
        self._blocked_count: Dict[EventType, int] = {}
        self._passed_count: Dict[EventType, int] = {}

    def add_rule(
        self,
        name: str,
        event_types: List[EventType],
        condition: Optional[Callable[[InputEvent], bool]] = None,
        action: str = "pass",
    ) -> None:
        """Add a filter rule."""
        rule = FilterRule(
            name=name,
            event_types=set(event_types),
            condition=condition,
            action=action,
        )
        self._rules.append(rule)

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name."""
        for i, rule in enumerate(self._rules):
            if rule.name == name:
                self._rules.pop(i)
                return True
        return False

    def process_event(self, event: InputEvent) -> Optional[InputEvent]:
        """
        Process an input event through filters.

        Args:
            event: Input event to process

        Returns:
            Modified event, None if blocked, or original if passed
        """
        for rule in self._rules:
            if event.event_type not in rule.event_types:
                continue

            if rule.condition and not rule.condition(event):
                continue

            if rule.action == "block":
                self._blocked_count[event.event_type] = self._blocked_count.get(event.event_type, 0) + 1
                logger.debug(f"Event blocked by rule: {rule.name}")
                return None

            elif rule.action == "pass":
                self._passed_count[event.event_type] = self._passed_count.get(event.event_type, 0) + 1

        handlers = self._event_handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Event handler failed: {e}")

        return event

    def register_handler(
        self,
        event_type: EventType,
        handler: Callable[[InputEvent], None],
    ) -> None:
        """Register an event handler."""
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)

    def get_stats(self) -> Dict[str, Any]:
        """Get filter statistics."""
        return {
            "rules_count": len(self._rules),
            "blocked": self._blocked_count.copy(),
            "passed": self._passed_count.copy(),
        }


def create_input_filter() -> InputFilter:
    """Factory function."""
    return InputFilter()
