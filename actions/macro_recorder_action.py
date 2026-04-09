"""Macro recorder action module for RabAI AutoClick.

Provides macro recording and playback:
- MacroRecorderAction: Record user actions as macros
- MacroPlayerAction: Play back recorded macros
- MacroEditorAction: Edit and modify recorded macros
- MacroSchedulerAction: Schedule macro execution
- MacroLibraryAction: Manage macro library
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MacroEventType(Enum):
    """Types of events that can be recorded."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    KEYBOARD_TYPE = auto()
    KEYBOARD_KEY = auto()
    SCREENSHOT = auto()
    WAIT = auto()
    CUSTOM = auto()


class RecordedEvent:
    """A single recorded event."""

    def __init__(self, event_type: MacroEventType, data: Dict[str, Any]) -> None:
        self.id = str(uuid.uuid4())
        self.event_type = event_type
        self.data = data
        self.timestamp = time.time()
        self.offset_ms = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "event_type": self.event_type.name,
            "data": self.data,
            "timestamp": self.timestamp,
            "offset_ms": self.offset_ms,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RecordedEvent":
        event = cls(MacroEventType[d["event_type"]], d["data"])
        event.id = d["id"]
        event.timestamp = d["timestamp"]
        event.offset_ms = d.get("offset_ms", 0)
        return event


class MacroRecorderAction(BaseAction):
    """Record user actions as macros."""
    action_type = "macro_recorder"
    display_name = "宏录制器"
    description = "录制用户操作序列为宏"

    def __init__(self) -> None:
        super().__init__()
        self._is_recording = False
        self._events: List[RecordedEvent] = []
        self._start_time: Optional[float] = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "start":
                return self._start_recording(params)
            elif action == "stop":
                return self._stop_recording(params)
            elif action == "pause":
                return self._pause_recording(params)
            elif action == "resume":
                return self._resume_recording(params)
            elif action == "event":
                return self._record_event(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Macro recording failed: {e}")

    def _start_recording(self, params: Dict[str, Any]) -> ActionResult:
        self._events = []
        self._is_recording = True
        self._start_time = time.time()
        return ActionResult(success=True, message="Macro recording started")

    def _stop_recording(self, params: Dict[str, Any]) -> ActionResult:
        if not self._is_recording:
            return ActionResult(success=False, message="Not currently recording")
        self._is_recording = False
        macro_id = str(uuid.uuid4())
        macro = {
            "id": macro_id,
            "name": params.get("name", f"Macro_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"),
            "events": [e.to_dict() for e in self._events],
            "event_count": len(self._events),
            "duration_ms": int((time.time() - self._start_time) * 1000) if self._start_time else 0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(
            success=True,
            message=f"Macro recording stopped: {len(self._events)} events",
            data=macro,
        )

    def _pause_recording(self, params: Dict[str, Any]) -> ActionResult:
        if not self._is_recording:
            return ActionResult(success=False, message="Not currently recording")
        self._is_recording = False
        return ActionResult(success=True, message="Recording paused")

    def _resume_recording(self, params: Dict[str, Any]) -> ActionResult:
        if self._is_recording:
            return ActionResult(success=False, message="Already recording")
        self._is_recording = True
        return ActionResult(success=True, message="Recording resumed")

    def _record_event(self, params: Dict[str, Any]) -> ActionResult:
        if not self._is_recording:
            return ActionResult(success=False, message="Not currently recording")
        event_type_str = params.get("event_type", "CUSTOM")
        try:
            event_type = MacroEventType[event_type_str.upper()]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown event type: {event_type_str}")
        event_data = params.get("data", {})
        event = RecordedEvent(event_type, event_data)
        if self._start_time:
            event.offset_ms = int((time.time() - self._start_time) * 1000)
        self._events.append(event)
        return ActionResult(success=True, message=f"Event recorded: {event_type.name}", data=event.to_dict())


class MacroPlayerAction(BaseAction):
    """Play back recorded macros."""
    action_type = "macro_player"
    display_name = "宏播放器"
    description = "回放录制的宏"

    def __init__(self) -> None:
        super().__init__()
        self._playback_speed = 1.0
        self._is_playing = False
        self._current_index = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "play")
            if action == "play":
                return self._play_macro(params)
            elif action == "stop":
                return self._stop_playback()
            elif action == "pause":
                return self._pause_playback(params)
            elif action == "speed":
                return self._set_speed(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Macro playback failed: {e}")

    def _play_macro(self, params: Dict[str, Any]) -> ActionResult:
        macro = params.get("macro", {})
        events = macro.get("events", [])
        if not events:
            return ActionResult(success=False, message="No events to play")
        self._is_playing = True
        self._current_index = 0
        return ActionResult(
            success=True,
            message=f"Playing macro: {len(events)} events",
            data={"event_count": len(events), "macro_id": macro.get("id", "")},
        )

    def _stop_playback(self) -> ActionResult:
        self._is_playing = False
        return ActionResult(success=True, message="Playback stopped")

    def _pause_playback(self, params: Dict[str, Any]) -> ActionResult:
        self._is_playing = False
        return ActionResult(success=True, message="Playback paused")

    def _set_speed(self, params: Dict[str, Any]) -> ActionResult:
        speed = params.get("speed", 1.0)
        if speed <= 0:
            return ActionResult(success=False, message="Speed must be positive")
        self._playback_speed = speed
        return ActionResult(success=True, message=f"Playback speed set to {speed}x")


class MacroEditorAction(BaseAction):
    """Edit and modify recorded macros."""
    action_type = "macro_editor"
    display_name = "宏编辑器"
    description = "编辑修改录制的宏"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            macro = params.get("macro", {})
            if not macro:
                return ActionResult(success=False, message="macro is required")
            if action == "insert":
                return self._insert_event(params)
            elif action == "delete":
                return self._delete_event(params)
            elif action == "replace":
                return self._replace_event(params)
            elif action == "trim":
                return self._trim_macro(params)
            elif action == "merge":
                return self._merge_macros(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Macro editing failed: {e}")

    def _insert_event(self, params: Dict[str, Any]) -> ActionResult:
        macro = params.get("macro", {})
        events = macro.get("events", [])
        event = params.get("event", {})
        position = params.get("position", len(events))
        events.insert(position, event)
        return ActionResult(success=True, message=f"Event inserted at position {position}", data={"events": events})

    def _delete_event(self, params: Dict[str, Any]) -> ActionResult:
        macro = params.get("macro", {})
        events = macro.get("events", [])
        position = params.get("position", -1)
        if 0 <= position < len(events):
            deleted = events.pop(position)
            return ActionResult(success=True, message=f"Event deleted at position {position}", data={"events": events})
        return ActionResult(success=False, message=f"Invalid position: {position}")

    def _replace_event(self, params: Dict[str, Any]) -> ActionResult:
        macro = params.get("macro", {})
        events = macro.get("events", [])
        event = params.get("event", {})
        position = params.get("position", -1)
        if 0 <= position < len(events):
            events[position] = event
            return ActionResult(success=True, message=f"Event replaced at position {position}", data={"events": events})
        return ActionResult(success=False, message=f"Invalid position: {position}")

    def _trim_macro(self, params: Dict[str, Any]) -> ActionResult:
        macro = params.get("macro", {})
        events = macro.get("events", [])
        start = params.get("start", 0)
        end = params.get("end", len(events))
        trimmed = events[start:end]
        return ActionResult(success=True, message=f"Macro trimmed to {len(trimmed)} events", data={"events": trimmed})

    def _merge_macros(self, params: Dict[str, Any]) -> ActionResult:
        macros = params.get("macros", [])
        merged_events: List[Dict[str, Any]] = []
        for m in macros:
            merged_events.extend(m.get("events", []))
        return ActionResult(success=True, message=f"Merged {len(macros)} macros", data={"events": merged_events})


class MacroSchedulerAction(BaseAction):
    """Schedule macro execution."""
    action_type = "macro_scheduler"
    display_name = "宏调度器"
    description = "调度宏定时执行"

    def __init__(self) -> None:
        super().__init__()
        self._schedules: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "schedule":
                return self._schedule_macro(params)
            elif action == "cancel":
                return self._cancel_schedule(params)
            elif action == "list":
                return self._list_schedules()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Macro scheduling failed: {e}")

    def _schedule_macro(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        macro_id = params.get("macro_id", "")
        cron = params.get("cron", "")
        interval_seconds = params.get("interval_seconds", 0)
        if not name:
            return ActionResult(success=False, message="name is required")
        schedule_id = str(uuid.uuid4())
        self._schedules[schedule_id] = {
            "name": name,
            "macro_id": macro_id,
            "cron": cron,
            "interval_seconds": interval_seconds,
            "next_run": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
        }
        return ActionResult(success=True, message=f"Macro scheduled: {name}", data={"schedule_id": schedule_id})

    def _cancel_schedule(self, params: Dict[str, Any]) -> ActionResult:
        schedule_id = params.get("schedule_id", "")
        if schedule_id in self._schedules:
            self._schedules[schedule_id]["active"] = False
            return ActionResult(success=True, message=f"Schedule cancelled: {schedule_id[:8]}")
        return ActionResult(success=False, message="Schedule not found")

    def _list_schedules(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._schedules)} schedules", data={"schedules": self._schedules})


class MacroLibraryAction(BaseAction):
    """Manage macro library."""
    action_type = "macro_library"
    display_name = "宏库管理"
    description = "管理宏库"

    def __init__(self) -> None:
        super().__init__()
        self._macros: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "save":
                return self._save_macro(params)
            elif action == "load":
                return self._load_macro(params)
            elif action == "delete":
                return self._delete_macro(params)
            elif action == "list":
                return self._list_macros()
            elif action == "search":
                return self._search_macros(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Macro library failed: {e}")

    def _save_macro(self, params: Dict[str, Any]) -> ActionResult:
        macro = params.get("macro", {})
        name = macro.get("name", "")
        if not name:
            return ActionResult(success=False, message="macro.name is required")
        self._macros[name] = macro
        return ActionResult(success=True, message=f"Macro saved: {name}", data={"name": name, "count": len(self._macros)})

    def _load_macro(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        if name not in self._macros:
            return ActionResult(success=False, message=f"Macro not found: {name}")
        return ActionResult(success=True, message=f"Macro loaded: {name}", data=self._macros[name])

    def _delete_macro(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        if name in self._macros:
            del self._macros[name]
            return ActionResult(success=True, message=f"Macro deleted: {name}")
        return ActionResult(success=False, message=f"Macro not found: {name}")

    def _list_macros(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._macros)} macros", data={"macros": list(self._macros.keys())})

    def _search_macros(self, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "").lower()
        results = [name for name in self._macros if query in name.lower()]
        return ActionResult(success=True, message=f"Found {len(results)} macros", data={"results": results})
