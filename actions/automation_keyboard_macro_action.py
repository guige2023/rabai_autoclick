"""
Automation Keyboard Macro Action Module

Records and replays keyboard macro sequences.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import json


class KeyEventType(Enum):
    """Keyboard event types."""
    KEY_DOWN = "key_down"
    KEY_UP = "key_up"
    KEY_PRESS = "key_press"
    TEXT_INPUT = "text_input"


@dataclass
class KeyEvent:
    """Single keyboard event."""
    event_type: KeyEventType
    key: str
    modifiers: List[str] = field(default_factory=list)
    timestamp: float = 0.0
    delay_ms: float = 0.0


@dataclass
class MacroAction:
    """Single action in a macro sequence."""
    action_type: str
    key: str
    modifiers: List[str] = field(default_factory=list)
    text: Optional[str] = None
    delay_ms: float = 0.0


@dataclass
class Macro:
    """Keyboard macro sequence."""
    name: str
    description: str = ""
    actions: List[MacroAction] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    use_count: int = 0


class KeyCodeMapper:
    """Maps between key names and codes."""

    # Common key mappings
    KEY_MAP = {
        "enter": "Return",
        "return": "Return",
        "tab": "Tab",
        "escape": "Escape",
        "esc": "Escape",
        "space": "Space",
        "backspace": "Delete",
        "delete": "Delete",
        "up": "UpArrow",
        "down": "DownArrow",
        "left": "LeftArrow",
        "right": "RightArrow",
        "home": "Home",
        "end": "End",
        "pageup": "PageUp",
        "pagedown": "PageDown",
        "f1": "F1",
        "f2": "F2",
        "f3": "F3",
        "f4": "F4",
        "f5": "F5",
        "f6": "F6",
        "f7": "F7",
        "f8": "F8",
        "f9": "F9",
        "f10": "F10",
        "f11": "F11",
        "f12": "F12",
    }

    MODIFIER_MAP = {
        "ctrl": "control",
        "control": "control",
        "alt": "alternate",
        "option": "alternate",
        "shift": "shift",
        "cmd": "command",
        "command": "command",
        "meta": "command",
    }

    @classmethod
    def normalize_key(cls, key: str) -> str:
        """Normalize key name."""
        key = key.lower().strip()
        return cls.KEY_MAP.get(key, key.title())

    @classmethod
    def normalize_modifier(cls, modifier: str) -> str:
        """Normalize modifier name."""
        modifier = modifier.lower().strip()
        return cls.MODIFIER_MAP.get(modifier, modifier)


class MacroRecorder:
    """Records keyboard macro sequences."""

    def __init__(self):
        self.recording: List[KeyEvent] = []
        self.is_recording = False
        self.start_time = 0.0

    def start_recording(self) -> None:
        """Start recording keyboard events."""
        self.recording.clear()
        self.is_recording = True
        self.start_time = time.time()

    def stop_recording(self) -> List[KeyEvent]:
        """Stop recording and return events."""
        self.is_recording = False
        return self.recording.copy()

    def record_event(
        self,
        event_type: KeyEventType,
        key: str,
        modifiers: Optional[List[str]] = None
    ) -> None:
        """Record a keyboard event."""
        if not self.is_recording:
            return

        timestamp = time.time()
        delay_ms = (timestamp - self.start_time) * 1000

        event = KeyEvent(
            event_type=event_type,
            key=key,
            modifiers=modifiers or [],
            timestamp=timestamp,
            delay_ms=delay_ms
        )
        self.recording.append(event)

    def clear(self) -> None:
        """Clear recorded events."""
        self.recording.clear()


class MacroBuilder:
    """Builds macro sequences from events."""

    @staticmethod
    def from_events(name: str, events: List[KeyEvent]) -> Macro:
        """Convert key events to macro."""
        macro = Macro(name=name)

        i = 0
        while i < len(events):
            event = events[i]

            if event.event_type == KeyEventType.KEY_PRESS:
                # Combine key down/up into single press
                action = MacroAction(
                    action_type="press",
                    key=KeyCodeMapper.normalize_key(event.key),
                    modifiers=[KeyCodeMapper.normalize_modifier(m) for m in event.modifiers],
                    delay_ms=event.delay_ms
                )
                macro.actions.append(action)
                i += 1

            elif event.event_type == KeyEventType.KEY_DOWN:
                # Look ahead for key up
                delay_ms = event.delay_ms
                if i + 1 < len(events) and events[i + 1].event_type == KeyEventType.KEY_UP:
                    action = MacroAction(
                        action_type="press",
                        key=KeyCodeMapper.normalize_key(event.key),
                        modifiers=[KeyCodeMapper.normalize_modifier(m) for m in event.modifiers],
                        delay_ms=delay_ms
                    )
                    i += 2
                else:
                    action = MacroAction(
                        action_type="down",
                        key=KeyCodeMapper.normalize_key(event.key),
                        modifiers=[KeyCodeMapper.normalize_modifier(m) for m in event.modifiers],
                        delay_ms=delay_ms
                    )
                    i += 1
                macro.actions.append(action)

            elif event.event_type == KeyEventType.TEXT_INPUT:
                action = MacroAction(
                    action_type="text",
                    key="",
                    text=event.key,
                    delay_ms=event.delay_ms
                )
                macro.actions.append(action)
                i += 1

            else:
                i += 1

        return macro

    @staticmethod
    def to_json(macro: Macro) -> str:
        """Serialize macro to JSON."""
        data = {
            "name": macro.name,
            "description": macro.description,
            "created_at": macro.created_at,
            "modified_at": macro.modified_at,
            "use_count": macro.use_count,
            "actions": [
                {
                    "type": a.action_type,
                    "key": a.key,
                    "modifiers": a.modifiers,
                    "text": a.text,
                    "delay_ms": a.delay_ms
                }
                for a in macro.actions
            ]
        }
        return json.dumps(data, indent=2)

    @staticmethod
    def from_json(data: str) -> Macro:
        """Deserialize macro from JSON."""
        obj = json.loads(data)
        macro = Macro(
            name=obj["name"],
            description=obj.get("description", ""),
            created_at=obj.get("created_at", time.time()),
            modified_at=obj.get("modified_at", time.time()),
            use_count=obj.get("use_count", 0)
        )
        for a in obj.get("actions", []):
            macro.actions.append(MacroAction(
                action_type=a["type"],
                key=a["key"],
                modifiers=a.get("modifiers", []),
                text=a.get("text"),
                delay_ms=a.get("delay_ms", 0)
            ))
        return macro


class MacroPlayer:
    """Plays back macro sequences."""

    def __init__(self, execute_fn: Optional[Callable] = None):
        self.execute_fn = execute_fn or self._default_execute
        self.is_playing = False
        self.playback_speed = 1.0

    def set_playback_speed(self, speed: float) -> None:
        """Set playback speed multiplier."""
        self.playback_speed = max(0.1, min(10.0, speed))

    async def play(
        self,
        macro: Macro,
        loop: bool = False,
        max_loops: int = 1
    ) -> Dict[str, Any]:
        """Play macro sequence."""
        self.is_playing = True
        start_time = time.time()
        loops_completed = 0

        while loops_completed < max_loops:
            for action in macro.actions:
                if not self.is_playing:
                    break

                delay = action.delay_ms / self.playback_speed
                if delay > 0:
                    time.sleep(delay / 1000)

                self.execute_fn(action)

            loops_completed += 1
            if not loop or not self.is_playing:
                break

        macro.use_count += 1
        return {
            "success": True,
            "loops_completed": loops_completed,
            "duration_ms": (time.time() - start_time) * 1000
        }

    def stop(self) -> None:
        """Stop playback."""
        self.is_playing = False

    def _default_execute(self, action: MacroAction) -> None:
        """Default execution (stub for testing)."""
        pass


class AutomationKeyboardMacroAction:
    """
    Records and replays keyboard macro sequences.

    Example:
        recorder = AutomationKeyboardMacroAction()

        # Record a macro
        recorder.start_recording()
        recorder.record_key("a", [])
        recorder.record_key("b", [])
        recorder.record_text("hello")
        macro = recorder.stop_recording("Type hello")

        # Play back
        recorder.play_macro(macro)
    """

    def __init__(self):
        self.recorder = MacroRecorder()
        self.builder = MacroBuilder()
        self.player = MacroPlayer()
        self.macros: Dict[str, Macro] = {}

    def start_recording(self) -> None:
        """Start recording a macro."""
        self.recorder.start_recording()

    def record_key(
        self,
        key: str,
        modifiers: Optional[List[str]] = None
    ) -> None:
        """Record a key event."""
        self.recorder.record_event(
            KeyEventType.KEY_PRESS,
            key,
            modifiers
        )

    def record_text(self, text: str) -> None:
        """Record text input."""
        self.recorder.record_event(
            KeyEventType.TEXT_INPUT,
            text,
            []
        )

    def stop_recording(self, name: str) -> Macro:
        """Stop recording and create macro."""
        events = self.recorder.stop_recording()
        macro = self.builder.from_events(name, events)
        self.macros[name] = macro
        return macro

    def save_macro(self, macro: Macro, path: str) -> None:
        """Save macro to file."""
        with open(path, 'w') as f:
            f.write(self.builder.to_json(macro))

    def load_macro(self, path: str) -> Macro:
        """Load macro from file."""
        with open(path, 'r') as f:
            return self.builder.from_json(f.read())

    def register_macro(self, macro: Macro) -> None:
        """Register a macro for playback."""
        self.macros[macro.name] = macro

    async def play_macro(
        self,
        name: str,
        loop: bool = False,
        speed: float = 1.0
    ) -> Dict[str, Any]:
        """Play a registered macro."""
        if name not in self.macros:
            return {"success": False, "error": "Macro not found"}

        self.player.set_playback_speed(speed)
        return await self.player.play(self.macros[name], loop)

    def stop_playback(self) -> None:
        """Stop macro playback."""
        self.player.stop()

    def get_macro_names(self) -> List[str]:
        """Get list of registered macro names."""
        return list(self.macros.keys())

    def delete_macro(self, name: str) -> bool:
        """Delete a registered macro."""
        if name in self.macros:
            del self.macros[name]
            return True
        return False
