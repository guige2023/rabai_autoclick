"""Workflow recording utilities for capturing and replaying actions.

This module provides utilities for recording automation workflows as
sequences of actions with metadata, and replaying them with timing
control, useful for creating reusable automation scripts.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, List, Dict, Any, Callable
import time
import json
from pathlib import Path


class ActionType(Enum):
    """Type of recorded action."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    RIGHT_CLICK = auto()
    MOVE = auto()
    DRAG = auto()
    TYPE = auto()
    PRESS = auto()
    SCROLL = auto()
    WAIT = auto()
    SCREENSHOT = auto()


@dataclass
class RecordedAction:
    """A single recorded action."""
    action_type: ActionType
    timestamp: float
    x: int = 0
    y: int = 0
    end_x: int = 0
    end_y: int = 0
    text: str = ""
    key: str = ""
    duration_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def position(self) -> Tuple[int, int]:
        return (self.x, self.y)


@dataclass
class WorkflowRecording:
    """A complete workflow recording."""
    name: str
    actions: List[RecordedAction] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000
    
    @property
    def action_count(self) -> int:
        return len(self.actions)


class WorkflowRecorder:
    """Recorder for capturing workflow actions."""
    
    def __init__(self, name: str):
        self.name = name
        self.actions: List[RecordedAction] = []
        self._recording = False
        self._start_time = 0.0
    
    def start_recording(self) -> None:
        """Start recording actions."""
        self._recording = True
        self._start_time = time.time()
        self.actions = []
    
    def stop_recording(self) -> WorkflowRecording:
        """Stop recording and return the workflow."""
        self._recording = False
        return WorkflowRecording(
            name=self.name,
            actions=self.actions.copy(),
            start_time=self._start_time,
            end_time=time.time(),
        )
    
    def record_action(self, action: RecordedAction) -> None:
        """Record a single action."""
        if self._recording:
            action.timestamp = time.time() - self._start_time
            self.actions.append(action)
    
    def record_click(
        self,
        x: int,
        y: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a click action."""
        self.record_action(RecordedAction(
            action_type=ActionType.CLICK,
            timestamp=0,
            x=x,
            y=y,
            metadata=metadata or {},
        ))
    
    def record_move(
        self,
        x: int,
        y: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a mouse move action."""
        self.record_action(RecordedAction(
            action_type=ActionType.MOVE,
            timestamp=0,
            x=x,
            y=y,
            metadata=metadata or {},
        ))
    
    def record_type(
        self,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a typing action."""
        self.record_action(RecordedAction(
            action_type=ActionType.TYPE,
            timestamp=0,
            text=text,
            metadata=metadata or {},
        ))
    
    def record_wait(
        self,
        duration_ms: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a wait action."""
        self.record_action(RecordedAction(
            action_type=ActionType.WAIT,
            timestamp=0,
            duration_ms=duration_ms,
            metadata=metadata or {},
        ))
    
    def record_screenshot(
        self,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a screenshot action."""
        self.record_action(RecordedAction(
            action_type=ActionType.SCREENSHOT,
            timestamp=0,
            metadata=metadata or {},
        ))


def save_recording(
    recording: WorkflowRecording,
    filepath: str,
) -> bool:
    """Save a recording to file.
    
    Args:
        recording: WorkflowRecording to save.
        filepath: Output file path.
    
    Returns:
        True if successful.
    """
    try:
        data = {
            "name": recording.name,
            "start_time": recording.start_time,
            "end_time": recording.end_time,
            "metadata": recording.metadata,
            "actions": [
                {
                    "action_type": a.action_type.name,
                    "timestamp": a.timestamp,
                    "x": a.x,
                    "y": a.y,
                    "end_x": a.end_x,
                    "end_y": a.end_y,
                    "text": a.text,
                    "key": a.key,
                    "duration_ms": a.duration_ms,
                    "metadata": a.metadata,
                }
                for a in recording.actions
            ],
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception:
        return False


def load_recording(filepath: str) -> Optional[WorkflowRecording]:
    """Load a recording from file.
    
    Args:
        filepath: Path to recording file.
    
    Returns:
        WorkflowRecording or None.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        actions = []
        for a in data.get("actions", []):
            action_type = ActionType[a["action_type"]]
            actions.append(RecordedAction(
                action_type=action_type,
                timestamp=a["timestamp"],
                x=a.get("x", 0),
                y=a.get("y", 0),
                end_x=a.get("end_x", 0),
                end_y=a.get("end_y", 0),
                text=a.get("text", ""),
                key=a.get("key", ""),
                duration_ms=a.get("duration_ms", 0.0),
                metadata=a.get("metadata", {}),
            ))
        
        return WorkflowRecording(
            name=data["name"],
            actions=actions,
            start_time=data.get("start_time", 0.0),
            end_time=data.get("end_time", 0.0),
            metadata=data.get("metadata", {}),
        )
    except Exception:
        return None


def replay_recording(
    recording: WorkflowRecording,
    on_action: Callable[[RecordedAction], None],
    on_complete: Optional[Callable[[], None]] = None,
    speed: float = 1.0,
) -> None:
    """Replay a workflow recording.
    
    Args:
        recording: Recording to replay.
        on_action: Callback for each action.
        on_complete: Optional callback when replay completes.
        speed: Playback speed multiplier.
    """
    if not recording.actions:
        if on_complete:
            on_complete()
        return
    
    for i, action in enumerate(recording.actions):
        if i > 0:
            prev_timestamp = recording.actions[i - 1].timestamp
            curr_timestamp = action.timestamp
            wait_time = (curr_timestamp - prev_timestamp) / speed
            if wait_time > 0:
                time.sleep(wait_time)
        
        if action.action_type == ActionType.WAIT:
            time.sleep(action.duration_ms / 1000.0 / speed)
        else:
            on_action(action)
    
    if on_complete:
        on_complete()
