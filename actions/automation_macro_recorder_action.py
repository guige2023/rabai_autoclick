"""
Automation Macro Recorder Action Module

Records UI automation macros from user interactions,
supporting playback, editing, and scheduling.

Author: RabAi Team
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

import logging

logger = logging.getLogger(__name__)


class ActionType(Enum):
    """Types of actions that can be recorded."""

    CLICK = auto()
    DOUBLE_CLICK = auto()
    RIGHT_CLICK = auto()
    TYPE = auto()
    PRESS_KEY = auto()
    SCROLL = auto()
    DRAG = auto()
    WAIT = auto()
    HOVER = auto()
    SCREENSHOT = auto()
    CUSTOM = auto()


@dataclass
class RecordedAction:
    """A single recorded UI action."""

    action_id: str
    action_type: ActionType
    timestamp: float
    target: Optional[str] = None
    x: Optional[int] = None
    y: Optional[int] = None
    value: Optional[str] = None
    duration_ms: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    screenshot_path: Optional[str] = None


@dataclass
class Macro:
    """A recorded macro containing multiple actions."""

    macro_id: str
    name: str
    description: str
    actions: List[RecordedAction]
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    version: int = 1
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "macro_id": self.macro_id,
            "name": self.name,
            "description": self.description,
            "actions": [
                {
                    "action_id": a.action_id,
                    "action_type": a.action_type.name,
                    "timestamp": a.timestamp,
                    "target": a.target,
                    "x": a.x,
                    "y": a.y,
                    "value": a.value,
                    "duration_ms": a.duration_ms,
                    "metadata": a.metadata,
                    "screenshot_path": a.screenshot_path,
                }
                for a in self.actions
            ],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "version": self.version,
            "tags": self.tags,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Macro:
        return cls(
            macro_id=data["macro_id"],
            name=data["name"],
            description=data.get("description", ""),
            actions=[
                RecordedAction(
                    action_id=a["action_id"],
                    action_type=ActionType[a["action_type"]],
                    timestamp=a["timestamp"],
                    target=a.get("target"),
                    x=a.get("x"),
                    y=a.get("y"),
                    value=a.get("value"),
                    duration_ms=a.get("duration_ms"),
                    metadata=a.get("metadata", {}),
                    screenshot_path=a.get("screenshot_path"),
                )
                for a in data["actions"]
            ],
            created_at=data.get("created_at", time.time()),
            updated_at=data.get("updated_at", time.time()),
            version=data.get("version", 1),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )


class MacroRecorder:
    """Records user interactions into macros."""

    def __init__(self, name: str = "Untitled") -> None:
        self.name = name
        self._actions: List[RecordedAction] = []
        self._recording = False
        self._start_time: Optional[float] = None

    def start_recording(self) -> None:
        """Start recording a new macro."""
        self._actions = []
        self._recording = True
        self._start_time = time.time()
        logger.info(f"Macro recording started: {self.name}")

    def stop_recording(self) -> List[RecordedAction]:
        """Stop recording and return recorded actions."""
        self._recording = False
        logger.info(f"Macro recording stopped. {len(self._actions)} actions recorded.")
        return self._actions.copy()

    def record_action(
        self,
        action_type: ActionType,
        target: Optional[str] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        value: Optional[str] = None,
        duration_ms: Optional[int] = None,
        **kwargs: Any,
    ) -> str:
        """Record a single action."""
        if not self._recording:
            return ""

        action_id = str(uuid.uuid4())
        action = RecordedAction(
            action_id=action_id,
            action_type=action_type,
            timestamp=time.time() - (self._start_time or time.time()),
            target=target,
            x=x,
            y=y,
            value=value,
            duration_ms=duration_ms,
            metadata=kwargs,
        )
        self._actions.append(action)
        return action_id

    def delete_action(self, action_id: str) -> bool:
        """Delete an action by ID."""
        for i, action in enumerate(self._actions):
            if action.action_id == action_id:
                del self._actions[i]
                return True
        return False

    def insert_action(
        self,
        index: int,
        action_type: ActionType,
        **kwargs: Any,
    ) -> str:
        """Insert an action at a specific index."""
        action_id = str(uuid.uuid4())
        action = RecordedAction(
            action_id=action_id,
            action_type=action_type,
            timestamp=self._actions[index - 1].timestamp if index > 0 else 0.0,
            **kwargs,
        )
        self._actions.insert(index, action)
        return action_id

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._recording

    def get_actions(self) -> List[RecordedAction]:
        """Get all recorded actions."""
        return self._actions.copy()


class MacroPlayer:
    """Plays back recorded macros."""

    def __init__(self) -> None:
        self._handlers: Dict[ActionType, Callable[[RecordedAction], Any]] = {}
        self._playback_speed = 1.0
        self._aborted = False

    def register_handler(self, action_type: ActionType, handler: Callable[[RecordedAction], Any]) -> None:
        """Register a handler for an action type."""
        self._handlers[action_type] = handler

    def play(
        self,
        macro: Macro,
        start_index: int = 0,
        end_index: Optional[int] = None,
    ) -> List[Any]:
        """Play back a macro and return results."""
        self._aborted = False
        results = []
        actions = macro.actions[start_index:end_index or len(macro.actions)]

        for action in actions:
            if self._aborted:
                logger.info("Playback aborted")
                break

            handler = self._handlers.get(action.action_type)
            if handler:
                try:
                    result = handler(action)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Playback error at {action.action_id}: {e}")
                    results.append(e)

            if action.duration_ms:
                adjusted_wait = action.duration_ms / self._playback_speed
                time.sleep(adjusted_wait / 1000.0)

        return results

    def abort(self) -> None:
        """Abort the current playback."""
        self._aborted = True

    def set_speed(self, speed: float) -> None:
        """Set playback speed multiplier."""
        self._playback_speed = max(0.1, min(speed, 10.0))


class MacroAction:
    """Action class for macro recording and playback."""

    def __init__(self) -> None:
        self.recorder = MacroRecorder()
        self.player = MacroPlayer()
        self._macros: Dict[str, Macro] = {}

    def create_macro(
        self,
        name: str,
        description: str = "",
        tags: Optional[List[str]] = None,
    ) -> MacroRecorder:
        """Create a new macro recorder."""
        self.recorder = MacroRecorder(name)
        return self.recorder

    def save_macro(
        self,
        name: str,
        description: str = "",
        tags: Optional[List[str]] = None,
    ) -> Macro:
        """Save the current recording as a macro."""
        actions = self.recorder.stop_recording()
        macro = Macro(
            macro_id=str(uuid.uuid4()),
            name=name,
            description=description,
            actions=actions,
            tags=tags or [],
        )
        self._macros[macro.macro_id] = macro
        return macro

    def load_macro(self, macro_id: str) -> Optional[Macro]:
        """Load a macro by ID."""
        return self._macros.get(macro_id)

    def play_macro(
        self,
        macro_id: str,
        start_index: int = 0,
    ) -> List[Any]:
        """Play a saved macro."""
        macro = self._macros.get(macro_id)
        if not macro:
            logger.error(f"Macro not found: {macro_id}")
            return []
        return self.player.play(macro, start_index)

    def export_macro(self, macro_id: str) -> Optional[str]:
        """Export macro as JSON string."""
        macro = self._macros.get(macro_id)
        if macro:
            return json.dumps(macro.to_dict(), indent=2)
        return None

    def import_macro(self, json_str: str) -> Optional[Macro]:
        """Import macro from JSON string."""
        try:
            data = json.loads(json_str)
            macro = Macro.from_dict(data)
            self._macros[macro.macro_id] = macro
            return macro
        except Exception as e:
            logger.error(f"Failed to import macro: {e}")
            return None

    def list_macros(self) -> List[Dict[str, Any]]:
        """List all saved macros."""
        return [
            {
                "macro_id": m.macro_id,
                "name": m.name,
                "description": m.description,
                "action_count": len(m.actions),
                "tags": m.tags,
                "created_at": m.created_at,
            }
            for m in self._macros.values()
        ]
