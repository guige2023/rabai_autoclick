"""Macro recording and playback automation action module.

Records mouse/keyboard actions and plays them back.
Supports variable substitution, loops, and conditional playback.
"""

from __future__ import annotations

import time
import json
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ActionType(Enum):
    """Type of recorded action."""
    MOUSE_MOVE = "mouse_move"
    MOUSE_CLICK = "mouse_click"
    MOUSE_RIGHT_CLICK = "mouse_right_click"
    MOUSE_DOUBLE_CLICK = "mouse_double_click"
    MOUSE_DRAG = "mouse_drag"
    KEY_TYPE = "key_type"
    KEY_PRESS = "key_press"
    WAIT = "wait"
    SCREENSHOT = "screenshot"
    TEXT_INPUT = "text_input"
    COMMENT = "comment"


@dataclass
class MacroAction:
    """A single recorded or programmed action."""
    action_type: ActionType
    x: Optional[int] = None
    y: Optional[int] = None
    x2: Optional[int] = None
    y2: Optional[int] = None
    text: str = ""
    key: str = ""
    duration_ms: float = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Macro:
    """A recorded macro containing a sequence of actions."""
    name: str
    actions: List[MacroAction]
    created_at: float = field(default_factory=time.time)
    description: str = ""
    variables: Dict[str, str] = field(default_factory=dict)


class MacroActionImpl:
    """Macro recording and playback engine.

    Records user actions and replays them with variable substitution.

    Example:
        macro = MacroActionImpl()
        macro.start_recording("login_flow")
        # ... perform actions ...
        macro.stop_recording()
        macro.save_to_file("login.json")

        macro.load_from_file("login.json")
        macro.set_variable("username", "alice")
        macro.set_variable("password", "secret")
        macro.play()
    """

    def __init__(self) -> None:
        """Initialize macro action."""
        self._current_macro: Optional[Macro] = None
        self._is_recording = False
        self._is_playing = False
        self._variables: Dict[str, str] = {}
        self._playback_speed: float = 1.0
        self._action_handlers: Dict[ActionType, Callable[[MacroAction], None]] = {}

    def start_recording(self, name: str, description: str = "") -> None:
        """Start recording a new macro.

        Args:
            name: Name of the macro.
            description: Optional description.
        """
        self._current_macro = Macro(
            name=name,
            actions=[],
            description=description,
        )
        self._is_recording = True
        logger.info("Started recording macro: %s", name)

    def stop_recording(self) -> Optional[Macro]:
        """Stop recording and return the macro.

        Returns:
            The recorded Macro or None if not recording.
        """
        self._is_recording = False
        macro = self._current_macro
        logger.info("Stopped recording macro: %s (%d actions)", macro.name, len(macro.actions) if macro else 0)
        return macro

    def record_action(
        self,
        action_type: ActionType,
        x: Optional[int] = None,
        y: Optional[int] = None,
        text: str = "",
        key: str = "",
        duration_ms: float = 0,
        **kwargs,
    ) -> None:
        """Record an action (typically called by input hooks).

        Args:
            action_type: Type of action.
            x: X coordinate for mouse actions.
            y: Y coordinate for mouse actions.
            text: Text for text input actions.
            key: Key name for keyboard actions.
            duration_ms: Duration for wait actions.
        """
        if not self._is_recording or not self._current_macro:
            return

        action = MacroAction(
            action_type=action_type,
            x=x,
            y=y,
            text=text,
            key=key,
            duration_ms=duration_ms,
            metadata=kwargs,
        )
        self._current_macro.actions.append(action)

    def set_variable(self, name: str, value: str) -> None:
        """Set a variable for substitution during playback.

        Args:
            name: Variable name (use ${name} in macros).
            value: Variable value.
        """
        self._variables[name] = value

    def set_variables(self, variables: Dict[str, str]) -> None:
        """Set multiple variables at once."""
        self._variables.update(variables)

    def set_playback_speed(self, speed: float) -> None:
        """Set playback speed multiplier.

        Args:
            speed: 1.0 = normal, 2.0 = 2x faster, 0.5 = half speed.
        """
        self._playback_speed = max(0.1, speed)

    def register_handler(
        self,
        action_type: ActionType,
        handler: Callable[[MacroAction], None],
    ) -> None:
        """Register a handler for an action type during playback.

        Args:
            action_type: Action type to handle.
            handler: Callable that executes the action.
        """
        self._action_handlers[action_type] = handler

    def play(
        self,
        macro: Optional[Macro] = None,
        loops: int = 1,
        stop_on_error: bool = True,
    ) -> bool:
        """Play back a macro.

        Args:
            macro: Macro to play (uses current if None.
            loops: Number of times to repeat.
            stop_on_error: Stop playback on error.

        Returns:
            True if playback completed successfully.
        """
        if macro is None:
            macro = self._current_macro

        if not macro:
            logger.error("No macro to play")
            return False

        self._is_playing = True
        success = True

        for loop in range(loops):
            logger.info("Playback loop %d/%d", loop + 1, loops)
            for i, action in enumerate(macro.actions):
                if not self._is_playing:
                    logger.info("Playback cancelled")
                    return False

                try:
                    self._execute_action(action)
                except Exception as e:
                    logger.error("Action %d failed: %s", i, e)
                    if stop_on_error:
                        success = False
                        break
                    success = False

        self._is_playing = False
        return success

    def stop(self) -> None:
        """Stop current playback."""
        self._is_playing = False

    def _execute_action(self, action: MacroAction) -> None:
        """Execute a single macro action with variable substitution."""
        text = self._substitute_variables(action.text)
        key = self._substitute_variables(action.key)

        handler = self._action_handlers.get(action.action_type)
        if handler:
            handler(action)
            return

        delay = action.duration_ms / 1000 / self._playback_speed
        if delay > 0:
            time.sleep(delay)

    def _substitute_variables(self, text: str) -> str:
        """Replace ${var} patterns with variable values."""
        import re
        def replacer(match):
            var_name = match.group(1)
            return self._variables.get(var_name, match.group(0))
        return re.sub(r"\$\{(\w+)\}", replacer, text)

    def save_to_file(self, path: str, macro: Optional[Macro] = None) -> None:
        """Save a macro to a JSON file.

        Args:
            path: File path to save to.
            macro: Macro to save (current if None).
        """
        macro = macro or self._current_macro
        if not macro:
            raise ValueError("No macro to save")

        data = {
            "name": macro.name,
            "description": macro.description,
            "created_at": macro.created_at,
            "variables": macro.variables,
            "actions": [
                {
                    "action_type": a.action_type.value,
                    "x": a.x,
                    "y": a.y,
                    "x2": a.x2,
                    "y2": a.y2,
                    "text": a.text,
                    "key": a.key,
                    "duration_ms": a.duration_ms,
                    "metadata": a.metadata,
                }
                for a in macro.actions
            ],
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info("Saved macro to %s", path)

    def load_from_file(self, path: str) -> Macro:
        """Load a macro from a JSON file.

        Args:
            path: File path to load from.

        Returns:
            Loaded Macro object.
        """
        with open(path, "r") as f:
            data = json.load(f)

        actions = [
            MacroAction(
                action_type=ActionType(a["action_type"]),
                x=a.get("x"),
                y=a.get("y"),
                x2=a.get("x2"),
                y2=a.get("y2"),
                text=a.get("text", ""),
                key=a.get("key", ""),
                duration_ms=a.get("duration_ms", 0),
                metadata=a.get("metadata", {}),
            )
            for a in data["actions"]
        ]

        macro = Macro(
            name=data["name"],
            actions=actions,
            created_at=data.get("created_at", time.time()),
            description=data.get("description", ""),
            variables=data.get("variables", {}),
        )
        self._current_macro = macro
        logger.info("Loaded macro from %s (%d actions)", path, len(actions))
        return macro
