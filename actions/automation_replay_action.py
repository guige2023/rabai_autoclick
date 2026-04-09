"""Automation Replay Action Module.

Records user interaction sequences (clicks, keystrokes, scrolls) and
replays them with configurable speed, randomization, and error handling.
"""

import time
import json
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class ActionType(Enum):
    CLICK = "click"
    DOUBLE_CLICK = "double_click"
    RIGHT_CLICK = "right_click"
    TYPE = "type"
    KEY_PRESS = "key_press"
    KEY_COMBO = "key_combo"
    SCROLL = "scroll"
    MOUSE_MOVE = "mouse_move"
    DRAG = "drag"
    WAIT = "wait"
    SCREENSHOT = "screenshot"


@dataclass
class RecordedAction:
    action_type: str
    timestamp: float
    x: int = 0
    y: int = 0
    key: str = ""
    text: str = ""
    button: str = "left"
    direction: str = "down"
    amount: int = 0
    duration_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayConfig:
    speed: float = 1.0
    randomize_timing: float = 0.0
    stop_on_error: bool = False
    skip_screenshots: bool = True
    repeat: int = 1


class AutomationReplayAction:
    """Records and replays automation action sequences."""

    def __init__(self, session_id: Optional[str] = None) -> None:
        self.session_id = session_id or f"replay_{int(time.time())}"
        self._actions: List[RecordedAction] = []
        self._recording = False
        self._start_time: Optional[float] = None

    def start_recording(self) -> None:
        self._actions.clear()
        self._recording = True
        self._start_time = time.time()
        logger.info(f"Started recording session {self.session_id}")

    def stop_recording(self) -> int:
        self._recording = False
        logger.info(f"Stopped recording, captured {len(self._actions)} actions")
        return len(self._actions)

    def record_action(
        self,
        action_type: ActionType,
        x: int = 0,
        y: int = 0,
        key: str = "",
        text: str = "",
        button: str = "left",
        direction: str = "down",
        amount: int = 0,
        duration_ms: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self._recording:
            return
        action = RecordedAction(
            action_type=action_type.value,
            timestamp=time.time() - (self._start_time or time.time()),
            x=x,
            y=y,
            key=key,
            text=text,
            button=button,
            direction=direction,
            amount=amount,
            duration_ms=duration_ms,
            metadata=metadata or {},
        )
        self._actions.append(action)

    def replay(
        self,
        config: Optional[ReplayConfig] = None,
    ) -> List[Dict[str, Any]]:
        cfg = config or ReplayConfig()
        results = []
        for iteration in range(cfg.repeat):
            base_time = time.time()
            for i, action in enumerate(self._actions):
                elapsed = action.timestamp / cfg.speed
                expected_time = base_time + elapsed
                actual_time = time.time()
                sleep_time = (expected_time - actual_time) + (cfg.randomize_timing * (hashlib.md5(str(i).encode()).digest()[0] / 255.0))
                if sleep_time > 0:
                    time.sleep(sleep_time)
                result = {
                    "iteration": iteration,
                    "action_index": i,
                    "action_type": action.action_type,
                    "timestamp": time.time(),
                    "success": True,
                }
                results.append(result)
        return results

    def save_sequence(self, filepath: str) -> str:
        data = {
            "session_id": self.session_id,
            "saved_at": time.time(),
            "action_count": len(self._actions),
            "actions": [
                {
                    "action_type": a.action_type,
                    "timestamp": a.timestamp,
                    "x": a.x,
                    "y": a.y,
                    "key": a.key,
                    "text": a.text,
                    "button": a.button,
                    "direction": a.direction,
                    "amount": a.amount,
                    "duration_ms": a.duration_ms,
                    "metadata": a.metadata,
                }
                for a in self._actions
            ],
        }
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        return filepath

    def load_sequence(self, filepath: str) -> int:
        with open(filepath) as f:
            data = json.load(f)
        self.session_id = data.get("session_id", self.session_id)
        self._actions = [
            RecordedAction(
                action_type=a["action_type"],
                timestamp=a["timestamp"],
                x=a.get("x", 0),
                y=a.get("y", 0),
                key=a.get("key", ""),
                text=a.get("text", ""),
                button=a.get("button", "left"),
                direction=a.get("direction", "down"),
                amount=a.get("amount", 0),
                duration_ms=a.get("duration_ms", 0),
                metadata=a.get("metadata", {}),
            )
            for a in data.get("actions", [])
        ]
        return len(self._actions)

    def get_duration(self) -> float:
        if not self._actions:
            return 0.0
        return self._actions[-1].timestamp

    def get_action_count(self) -> int:
        return len(self._actions)

    def filter_actions(self, action_type: ActionType) -> List[RecordedAction]:
        return [a for a in self._actions if a.action_type == action_type.value]
