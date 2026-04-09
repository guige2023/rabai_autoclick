"""Keyboard Macro Executor Utilities.

Executes recorded keyboard macros with timing control.

Example:
    >>> from keyboard_macro_executor_utils import KeyboardMacroExecutor
    >>> exec = KeyboardMacroExecutor()
    >>> exec.load_macro([("key", "a"), ("key", "b")])
    >>> exec.execute()
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, List, Optional


class MacroEventType(Enum):
    """Keyboard macro event types."""
    KEY_PRESS = auto()
    KEY_RELEASE = auto()
    TEXT = auto()
    DELAY = auto()


@dataclass
class MacroEvent:
    """A single macro event."""
    event_type: MacroEventType
    value: Any
    duration_ms: float = 0.0


@dataclass
class MacroResult:
    """Result of macro execution."""
    success: bool
    events_executed: int
    duration: float
    error: Optional[str] = None


class KeyboardMacroExecutor:
    """Executes keyboard macros."""

    def __init__(self, on_key_event: Optional[Callable[..., None]] = None):
        """Initialize executor.

        Args:
            on_key_event: Callback for key events (key, is_press).
        """
        self.on_key_event = on_key_event
        self._macro: List[MacroEvent] = []

    def load_macro(self, events: List[Any]) -> None:
        """Load macro from event list.

        Args:
            events: List of event tuples (type, value).
        """
        self._macro = []
        for event in events:
            if isinstance(event, MacroEvent):
                self._macro.append(event)
            elif isinstance(event, tuple) and len(event) >= 2:
                etype, evalue = event[0], event[1]
                edur = event[2] if len(event) > 2 else 0.0
                try:
                    et = MacroEventType[etype.upper()]
                    self._macro.append(MacroEvent(et, evalue, edur))
                except KeyError:
                    pass

    def execute(self) -> MacroResult:
        """Execute the loaded macro.

        Returns:
            MacroResult with execution details.
        """
        start = time.perf_counter()
        try:
            for event in self._macro:
                self._execute_event(event)
            return MacroResult(
                success=True,
                events_executed=len(self._macro),
                duration=time.perf_counter() - start,
            )
        except Exception as e:
            return MacroResult(
                success=False,
                events_executed=0,
                duration=time.perf_counter() - start,
                error=str(e),
            )

    def _execute_event(self, event: MacroEvent) -> None:
        """Execute a single macro event."""
        if event.event_type == MacroEventType.DELAY:
            time.sleep(event.duration_ms / 1000.0)
        elif event.event_type == MacroEventType.KEY_PRESS:
            if self.on_key_event:
                self.on_key_event(event.value, True)
        elif event.event_type == MacroEventType.KEY_RELEASE:
            if self.on_key_event:
                self.on_key_event(event.value, False)
        elif event.event_type == MacroEventType.TEXT:
            if self.on_key_event:
                for char in str(event.value):
                    self.on_key_event(char, True)
                    self.on_key_event(char, False)
