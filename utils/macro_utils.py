"""
Macro recording and playback utilities.

Provides functionality to record user actions as macros and replay them
with support for timing, conditionals, and variable substitution.
"""

from __future__ import annotations

import time
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum, auto


class MacroEventType(Enum):
    """Types of events that can be recorded in a macro."""
    CLICK = auto()
    TYPE = auto()
    WAIT = auto()
    SCROLL = auto()
    KEYPRESS = auto()
    SCREENSHOT = auto()
    CONDITIONAL = auto()
    LOOP = auto()
    VARIABLE_SET = auto()
    VARIABLE_GET = auto()


@dataclass
class MacroEvent:
    """A single recorded event in a macro sequence."""
    event_type: MacroEventType
    timestamp: float
    data: dict[str, Any]
    target: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type.name,
            "timestamp": self.timestamp,
            "data": self.data,
            "target": self.target,
        }

    @classmethod
    def from_dict(cls, d: dict) -> MacroEvent:
        return cls(
            event_type=MacroEventType[d["event_type"]],
            timestamp=d["timestamp"],
            data=d["data"],
            target=d.get("target"),
        )


@dataclass
class Macro:
    """A recorded macro containing a sequence of events."""
    name: str
    description: str = ""
    events: list[MacroEvent] = field(default_factory=list)
    variables: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)

    def add_event(self, event_type: MacroEventType, data: dict, target: Optional[str] = None) -> None:
        """Add an event to the macro."""
        event = MacroEvent(
            event_type=event_type,
            timestamp=time.time(),
            data=data,
            target=target,
        )
        self.events.append(event)
        self.modified_at = time.time()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "events": [e.to_dict() for e in self.events],
            "variables": self.variables,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "modified_at": self.modified_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Macro:
        return cls(
            name=d["name"],
            description=d.get("description", ""),
            events=[MacroEvent.from_dict(e) for e in d.get("events", [])],
            variables=d.get("variables", {}),
            metadata=d.get("metadata", {}),
            created_at=d.get("created_at", time.time()),
            modified_at=d.get("modified_at", time.time()),
        )

    def save(self, path: str) -> None:
        """Save macro to a JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str) -> Macro:
        """Load a macro from a JSON file."""
        with open(path) as f:
            return cls.from_dict(json.load(f))

    def duration(self) -> float:
        """Get the total duration of the macro in seconds."""
        if len(self.events) < 2:
            return 0.0
        return self.events[-1].timestamp - self.events[0].timestamp


class MacroRecorder:
    """Records user actions as a macro."""

    def __init__(self, name: str, description: str = ""):
        self.macro = Macro(name=name, description=description)
        self._recording = False
        self._start_time: Optional[float] = None

    def start(self) -> None:
        """Start recording."""
        self._recording = True
        self._start_time = time.time()
        self.macro.created_at = self._start_time
        self.macro.modified_at = self._start_time

    def stop(self) -> Macro:
        """Stop recording and return the macro."""
        self._recording = False
        return self.macro

    def is_recording(self) -> bool:
        return self._recording

    def record_click(self, x: int, y: int, button: str = "left", target: Optional[str] = None) -> None:
        """Record a click event."""
        if not self._recording:
            return
        self.macro.add_event(
            MacroEventType.CLICK,
            {"x": x, "y": y, "button": button},
            target=target,
        )

    def record_type(self, text: str, target: Optional[str] = None) -> None:
        """Record a typing event."""
        if not self._recording:
            return
        self.macro.add_event(
            MacroEventType.TYPE,
            {"text": text},
            target=target,
        )

    def record_wait(self, duration: float) -> None:
        """Record a wait period."""
        if not self._recording:
            return
        self.macro.add_event(
            MacroEventType.WAIT,
            {"duration": duration},
        )

    def record_scroll(self, dx: int, dy: int, target: Optional[str] = None) -> None:
        """Record a scroll event."""
        if not self._recording:
            return
        self.macro.add_event(
            MacroEventType.SCROLL,
            {"dx": dx, "dy": dy},
            target=target,
        )

    def record_keypress(self, key: str, modifiers: Optional[list[str]] = None) -> None:
        """Record a keypress event."""
        if not self._recording:
            return
        self.macro.add_event(
            MacroEventType.KEYPRESS,
            {"key": key, "modifiers": modifiers or []},
        )

    def set_variable(self, name: str, value: Any) -> None:
        """Set a variable in the macro context."""
        if not self._recording:
            return
        self.macro.variables[name] = value
        self.macro.add_event(
            MacroEventType.VARIABLE_SET,
            {"name": name, "value": value},
        )


class MacroPlayer:
    """Plays back recorded macros."""

    def __init__(self, macro: Macro):
        self.macro = macro
        self._variables: dict[str, Any] = dict(macro.variables)
        self._handlers: dict[MacroEventType, Callable[[MacroEvent], None]] = {}
        self._speed: float = 1.0
        self._paused = False
        self._cancelled = False

    def set_variable(self, name: str, value: Any) -> None:
        """Set a playback variable (overrides macro variable)."""
        self._variables[name] = value

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get a variable value with optional default."""
        return self._variables.get(name, default)

    def set_speed(self, speed: float) -> None:
        """Set playback speed multiplier (1.0 = normal)."""
        self._speed = max(0.1, min(speed, 10.0))

    def register_handler(self, event_type: MacroEventType, handler: Callable[[MacroEvent], None]) -> None:
        """Register a handler for an event type."""
        self._handlers[event_type] = handler

    def play(self) -> None:
        """Play the macro from start to finish."""
        self._cancelled = False
        self._paused = False
        prev_timestamp = None
        for event in self.macro.events:
            if self._cancelled:
                break
            while self._paused and not self._cancelled:
                time.sleep(0.1)
            if self._cancelled:
                break
            if prev_timestamp is not None:
                elapsed = (event.timestamp - prev_timestamp) / self._speed
                if elapsed > 0:
                    time.sleep(elapsed)
            prev_timestamp = event.timestamp
            self._execute_event(event)

    def _execute_event(self, event: MacroEvent) -> None:
        """Execute a single macro event."""
        handler = self._handlers.get(event.event_type)
        if handler:
            handler(event)

    def pause(self) -> None:
        """Pause playback."""
        self._paused = True

    def resume(self) -> None:
        """Resume playback."""
        self._paused = False

    def cancel(self) -> None:
        """Cancel playback."""
        self._cancelled = True
        self._paused = False

    def is_playing(self) -> bool:
        return not self._cancelled and not self._paused


class MacroCondition:
    """Represents a condition for macro branching."""

    def __init__(
        self,
        variable: str,
        operator: str,
        value: Any,
        then_macro: Optional[Macro] = None,
        else_macro: Optional[Macro] = None,
    ):
        self.variable = variable
        self.operator = operator
        self.value = value
        self.then_macro = then_macro
        self.else_macro = else_macro

    def evaluate(self, context: dict[str, Any]) -> bool:
        """Evaluate the condition against a context."""
        actual = context.get(self.variable)
        ops = {
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            "exists": lambda a, _: a is not None,
            "not_exists": lambda a, _: a is None,
            "contains": lambda a, b: b in a if a else False,
        }
        op_func = ops.get(self.operator, ops["=="])
        return op_func(actual, self.value)


class MacroLoop:
    """Represents a loop in a macro."""

    def __init__(self, iterations: int = -1, until: Optional[MacroCondition] = None):
        self.iterations = iterations
        self.until = until
        self._count = 0

    def should_continue(self, context: dict[str, Any]) -> bool:
        """Check if loop should continue."""
        self._count += 1
        if self.iterations > 0 and self._count >= self.iterations:
            return False
        if self.until:
            return not self.until.evaluate(context)
        return True

    def reset(self) -> None:
        """Reset loop counter."""
        self._count = 0
