"""UI Event Synthesizer Utilities.

Synthesizes UI events from semantic descriptions for test automation.

Example:
    >>> from ui_event_synthesizer_utils import UIEventSynthesizer
    >>> synth = UIEventSynthesizer()
    >>> synth.synthesize("click the OK button in the dialog")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional


class EventType(Enum):
    """UI event types."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    RIGHT_CLICK = auto()
    HOVER = auto()
    TYPE = auto()
    PRESS = auto()
    SWIPE = auto()
    PINCH = auto()
    SCROLL = auto()
    DRAG = auto()
    CUSTOM = auto()


@dataclass
class SynthesizedEvent:
    """A synthesized UI event."""
    event_type: EventType
    target: str
    params: Dict[str, Any] = field(default_factory=dict)
    delay_after: float = 0.1
    repeat: int = 1


class UIEventSynthesizer:
    """Synthesizes UI events from natural language."""

    ACTION_PATTERNS = {
        "click": EventType.CLICK,
        "tap": EventType.CLICK,
        "double click": EventType.DOUBLE_CLICK,
        "right click": EventType.RIGHT_CLICK,
        "context click": EventType.RIGHT_CLICK,
        "hover": EventType.HOVER,
        "type": EventType.TYPE,
        "enter": EventType.TYPE,
        "press": EventType.PRESS,
        "swipe": EventType.SWIPE,
        "pinch": EventType.PINCH,
        "scroll": EventType.SCROLL,
        "drag": EventType.DRAG,
    }

    def synthesize(self, description: str) -> List[SynthesizedEvent]:
        """Synthesize events from a description.

        Args:
            description: Natural language description.

        Returns:
            List of SynthesizedEvent objects.
        """
        desc_lower = description.lower()
        events = []

        for pattern, event_type in self.ACTION_PATTERNS.items():
            if pattern in desc_lower:
                event = self._create_event(event_type, desc_lower)
                if event:
                    events.append(event)
                break

        if not events:
            events.append(SynthesizedEvent(EventType.CUSTOM, description))

        return events

    def _create_event(self, event_type: EventType, desc: str) -> Optional[SynthesizedEvent]:
        """Create event from description."""
        target = self._extract_target(desc)
        if not target:
            return None

        params = {}
        if event_type == EventType.TYPE:
            params["text"] = self._extract_text(desc)
        elif event_type == EventType.PRESS:
            params["key"] = self._extract_key(desc)

        return SynthesizedEvent(
            event_type=event_type,
            target=target,
            params=params,
            delay_after=0.1,
            repeat=1,
        )

    def _extract_target(self, desc: str) -> Optional[str]:
        """Extract target element from description."""
        markers = ["button", "field", "input", "checkbox", "radio", "menu", "item"]
        for marker in markers:
            idx = desc.find(marker)
            if idx >= 0:
                return desc[:idx].strip().split()[-1] or marker
        return None

    def _extract_text(self, desc: str) -> str:
        """Extract text to type."""
        if '"' in desc:
            start = desc.rfind('"') + 1
            end = desc.rfind('"', start)
            if start > 0 and end > start:
                return desc[start:end]
        return ""

    def _extract_key(self, desc: str) -> str:
        """Extract key name to press."""
        keys = ["enter", "escape", "tab", "space", "backspace", "delete"]
        for key in keys:
            if key in desc:
                return key
        return "enter"
