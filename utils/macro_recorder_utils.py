"""
Macro Recorder Utilities

Provides utilities for recording and playing back
macros in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from enum import Enum, auto


class MacroActionType(Enum):
    """Types of macro actions."""
    CLICK = auto()
    TYPE = auto()
    PRESS = auto()
    WAIT = auto()
    SCROLL = auto()
    CUSTOM = auto()


@dataclass
class MacroAction:
    """Represents a single macro action."""
    action_type: MacroActionType
    params: dict[str, Any]
    timestamp: datetime
    duration_ms: float = 0.0


@dataclass
class Macro:
    """Represents a recorded macro."""
    name: str
    actions: list[MacroAction] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    description: str = ""


class MacroRecorder:
    """
    Records and plays back macros.
    
    Captures sequences of actions for
    automated replay.
    """

    def __init__(self) -> None:
        self._macros: dict[str, Macro] = {}
        self._is_recording = False
        self._current_macro: Macro | None = None

    def start_recording(self, name: str) -> None:
        """Start recording a new macro."""
        self._is_recording = True
        self._current_macro = Macro(name=name)

    def stop_recording(self, description: str = "") -> Macro | None:
        """Stop recording and return the macro."""
        self._is_recording = False
        if self._current_macro:
            self._current_macro.description = description
            self._macros[self._current_macro.name] = self._current_macro
            macro = self._current_macro
            self._current_macro = None
            return macro
        return None

    def record_action(
        self,
        action_type: MacroActionType,
        params: dict[str, Any],
        duration_ms: float = 0.0,
    ) -> None:
        """Record an action."""
        if self._is_recording and self._current_macro:
            action = MacroAction(
                action_type=action_type,
                params=params,
                timestamp=datetime.now(),
                duration_ms=duration_ms,
            )
            self._current_macro.actions.append(action)

    def get_macro(self, name: str) -> Macro | None:
        """Get a macro by name."""
        return self._macros.get(name)

    def list_macros(self) -> list[str]:
        """List all macro names."""
        return list(self._macros.keys())

    def delete_macro(self, name: str) -> bool:
        """Delete a macro."""
        return self._macros.pop(name, None) is not None

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._is_recording
