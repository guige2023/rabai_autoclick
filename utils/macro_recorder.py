"""Macro recorder for UI automation.

Records and plays back sequences of UI actions for creating
automation macros.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class MacroActionType(Enum):
    """Types of macro actions."""
    CLICK = auto()
    RIGHT_CLICK = auto()
    DOUBLE_CLICK = auto()
    TYPE = auto()
    PRESS_KEY = auto()
    WAIT = auto()
    SCROLL = auto()
    DRAG = auto()
    CUSTOM = auto()


@dataclass
class MacroAction:
    """A single action in a macro.

    Attributes:
        action_type: Type of action.
        target: Target selector or element.
        value: Action parameter (text, keys, etc.).
        options: Additional action options.
        timestamp: Relative time from macro start.
        description: Human-readable description.
    """
    action_type: MacroActionType
    target: str = ""
    value: Any = None
    options: dict[str, Any] = field(default_factory=dict)
    timestamp: float = 0.0
    description: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class Macro:
    """A recorded macro containing a sequence of actions.

    Attributes:
        name: Macro name.
        description: Human-readable description.
        actions: Ordered list of actions.
        created_at: Creation timestamp.
        modified_at: Last modification timestamp.
        playback_speed: Speed multiplier for playback (1.0 = normal).
    """
    name: str
    description: str = ""
    actions: list[MacroAction] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    playback_speed: float = 1.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def duration(self) -> float:
        """Total macro duration in seconds."""
        if not self.actions:
            return 0.0
        return self.actions[-1].timestamp

    @property
    def action_count(self) -> int:
        """Number of actions in this macro."""
        return len(self.actions)


class MacroRecorder:
    """Records user interactions as macros."""

    def __init__(self) -> None:
        """Initialize recorder."""
        self._is_recording: bool = False
        self._start_time: float = 0.0
        self._current_macro: Optional[Macro] = None

    def start_recording(self, name: str, description: str = "") -> Macro:
        """Start recording a new macro."""
        self._is_recording = True
        self._start_time = time.time()
        self._current_macro = Macro(name=name, description=description)
        return self._current_macro

    def record_action(
        self,
        action_type: MacroActionType,
        target: str = "",
        value: Any = None,
        **options: Any,
    ) -> Optional[MacroAction]:
        """Record a single action."""
        if not self._is_recording or not self._current_macro:
            return None

        timestamp = time.time() - self._start_time
        action = MacroAction(
            action_type=action_type,
            target=target,
            value=value,
            options=options,
            timestamp=timestamp,
        )
        self._current_macro.actions.append(action)
        return action

    def stop_recording(self) -> Optional[Macro]:
        """Stop recording and return the macro."""
        self._is_recording = False
        macro = self._current_macro
        if macro:
            macro.modified_at = time.time()
        self._current_macro = None
        return macro

    def is_recording(self) -> bool:
        """Return True if currently recording."""
        return self._is_recording

    @property
    def current_macro(self) -> Optional[Macro]:
        """Get the macro being recorded."""
        return self._current_macro


class MacroPlayer:
    """Plays back recorded macros."""

    def __init__(self) -> None:
        """Initialize player."""
        self._action_handlers: dict[
            MacroActionType, Callable[[MacroAction], bool]
        ] = {}
        self._is_playing: bool = False
        self._current_macro: Optional[Macro] = None
        self._on_start_callbacks: list[Callable[[Macro], None]] = []
        self._on_complete_callbacks: list[Callable[[Macro], None]] = []
        self._on_action_callbacks: list[Callable[[MacroAction], None]] = []

    def register_handler(
        self,
        action_type: MacroActionType,
        handler: Callable[[MacroAction], bool],
    ) -> None:
        """Register a handler for an action type."""
        self._action_handlers[action_type] = handler

    def play(
        self,
        macro: Macro,
        from_step: int = 0,
    ) -> bool:
        """Play a macro from an optional starting step.

        Returns True if playback completed successfully.
        """
        self._is_playing = True
        self._current_macro = macro
        self._notify_start(macro)

        try:
            actions = macro.actions[from_step:]
            prev_timestamp = macro.actions[from_step].timestamp if from_step < len(actions) else 0.0

            for action in actions:
                if not self._is_playing:
                    break

                delay = (action.timestamp - prev_timestamp) / macro.playback_speed
                if delay > 0:
                    time.sleep(delay)

                handler = self._action_handlers.get(action.action_type)
                if handler:
                    if not handler(action):
                        return False

                self._notify_action(action)
                prev_timestamp = action.timestamp

            return True
        finally:
            self._is_playing = False
            self._notify_complete(macro)

    def stop(self) -> None:
        """Stop current playback."""
        self._is_playing = False

    def is_playing(self) -> bool:
        """Return True if currently playing."""
        return self._is_playing

    def on_start(self, callback: Callable[[Macro], None]) -> None:
        """Register callback for macro start."""
        self._on_start_callbacks.append(callback)

    def on_complete(self, callback: Callable[[Macro], None]) -> None:
        """Register callback for macro completion."""
        self._on_complete_callbacks.append(callback)

    def on_action(self, callback: Callable[[MacroAction], None]) -> None:
        """Register callback for each action."""
        self._on_action_callbacks.append(callback)

    def _notify_start(self, macro: Macro) -> None:
        for cb in self._on_start_callbacks:
            try:
                cb(macro)
            except Exception:
                pass

    def _notify_complete(self, macro: Macro) -> None:
        for cb in self._on_complete_callbacks:
            try:
                cb(macro)
            except Exception:
                pass

    def _notify_action(self, action: MacroAction) -> None:
        for cb in self._on_action_callbacks:
            try:
                cb(action)
            except Exception:
                pass
