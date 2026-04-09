"""
Automation workflow recording and playback.

Records user interactions as automation workflows
and provides playback with variable speed and error handling.

Author: Auto-generated
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable


class ActionType(Enum):
    """Types of recorded actions."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    MOUSE_DRAG = auto()
    MOUSE_SCROLL = auto()
    KEYBOARD_TYPE = auto()
    KEYBOARD_PRESS = auto()
    WAIT = auto()
    SCREENSHOT = auto()
    CUSTOM = auto()


@dataclass
class WorkflowAction:
    """A single action in a recorded workflow."""
    action_type: ActionType
    timestamp: float
    x: float = 0
    y: float = 0
    key: str = ""
    text: str = ""
    duration_ms: float = 0
    metadata: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "action_type": self.action_type.name,
            "timestamp": self.timestamp,
            "x": self.x,
            "y": self.y,
            "key": self.key,
            "text": self.text,
            "duration_ms": self.duration_ms,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> WorkflowAction:
        """Deserialize from dictionary."""
        return cls(
            action_type=ActionType[data["action_type"]],
            timestamp=data["timestamp"],
            x=data.get("x", 0),
            y=data.get("y", 0),
            key=data.get("key", ""),
            text=data.get("text", ""),
            duration_ms=data.get("duration_ms", 0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Workflow:
    """A recorded automation workflow."""
    name: str
    description: str = ""
    actions: list[WorkflowAction] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)
    
    def add_action(self, action: WorkflowAction) -> None:
        """Add an action to the workflow."""
        self.actions.append(action)
    
    def duration_seconds(self) -> float:
        """Total duration of the workflow."""
        if not self.actions:
            return 0.0
        return self.actions[-1].timestamp - self.actions[0].timestamp
    
    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "actions": [a.to_dict() for a in self.actions],
            "created_at": self.created_at,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> Workflow:
        """Deserialize from dictionary."""
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            actions=[WorkflowAction.from_dict(a) for a in data["actions"]],
            created_at=data.get("created_at", time.time()),
            metadata=data.get("metadata", {}),
        )
    
    def save_to_file(self, filepath: str) -> None:
        """Save workflow to JSON file."""
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load_from_file(cls, filepath: str) -> Workflow:
        """Load workflow from JSON file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)


class WorkflowRecorder:
    """
    Records user actions as a workflow.
    
    Example:
        recorder = WorkflowRecorder("My Workflow")
        recorder.start()
        # ... perform actions ...
        recorder.stop()
        workflow = recorder.get_workflow()
    """
    
    def __init__(self, name: str, description: str = ""):
        self._workflow = Workflow(name=name, description=description)
        self._recording = False
        self._start_time: float = 0
        self._action_handlers: dict[ActionType, Callable] = {}
    
    def start(self) -> None:
        """Start recording."""
        self._recording = True
        self._start_time = time.perf_counter()
        self._workflow.actions.clear()
    
    def stop(self) -> None:
        """Stop recording."""
        self._recording = False
    
    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._recording
    
    def record_mouse_click(self, x: float, y: float) -> None:
        """Record a mouse click."""
        if not self._recording:
            return
        action = WorkflowAction(
            action_type=ActionType.MOUSE_CLICK,
            timestamp=time.perf_counter() - self._start_time,
            x=x,
            y=y,
        )
        self._workflow.add_action(action)
    
    def record_mouse_move(self, x: float, y: float) -> None:
        """Record a mouse move."""
        if not self._recording:
            return
        action = WorkflowAction(
            action_type=ActionType.MOUSE_MOVE,
            timestamp=time.perf_counter() - self._start_time,
            x=x,
            y=y,
        )
        self._workflow.add_action(action)
    
    def record_keyboard_type(self, text: str) -> None:
        """Record keyboard typing."""
        if not self._recording:
            return
        action = WorkflowAction(
            action_type=ActionType.KEYBOARD_TYPE,
            timestamp=time.perf_counter() - self._start_time,
            text=text,
        )
        self._workflow.add_action(action)
    
    def record_wait(self, duration_ms: float) -> None:
        """Record a wait period."""
        if not self._recording:
            return
        action = WorkflowAction(
            action_type=ActionType.WAIT,
            timestamp=time.perf_counter() - self._start_time,
            duration_ms=duration_ms,
        )
        self._workflow.add_action(action)
    
    def get_workflow(self) -> Workflow:
        """Get the recorded workflow."""
        return self._workflow


class WorkflowPlayer:
    """
    Plays back recorded workflows.
    
    Example:
        player = WorkflowPlayer(workflow)
        player.play(speed=2.0)  # 2x speed
    """
    
    def __init__(
        self,
        workflow: Workflow,
        mouse_executor: Callable[[float, float], None] | None = None,
        keyboard_executor: Callable[[str], None] | None = None,
    ):
        self._workflow = workflow
        self._mouse_executor = mouse_executor
        self._keyboard_executor = keyboard_executor
        self._playing = False
        self._paused = False
        self._canceled = False
        self._speed = 1.0
        self._current_action = 0
    
    def play(self, speed: float = 1.0) -> None:
        """
        Play the workflow.
        
        Args:
            speed: Playback speed multiplier (1.0 = normal)
        """
        self._speed = speed
        self._playing = True
        self._canceled = False
        self._current_action = 0
        
        prev_timestamp = 0.0
        
        for action in self._workflow.actions:
            if not self._playing or self._canceled:
                break
            
            while self._paused and self._playing and not self._canceled:
                time.sleep(0.1)
            
            # Wait for the appropriate delay
            delay = (action.timestamp - prev_timestamp) / self._speed
            if delay > 0:
                time.sleep(delay)
            
            self._execute_action(action)
            prev_timestamp = action.timestamp
            self._current_action += 1
        
        self._playing = False
    
    def _execute_action(self, action: WorkflowAction) -> None:
        """Execute a single workflow action."""
        if action.action_type == ActionType.MOUSE_CLICK:
            if self._mouse_executor:
                self._mouse_executor(action.x, action.y)
        
        elif action.action_type == ActionType.MOUSE_MOVE:
            if self._mouse_executor:
                self._mouse_executor(action.x, action.y)
        
        elif action.action_type == ActionType.KEYBOARD_TYPE:
            if self._keyboard_executor:
                self._keyboard_executor(action.text)
        
        elif action.action_type == ActionType.WAIT:
            time.sleep(action.duration_ms / 1000.0 / self._speed)
    
    def pause(self) -> None:
        """Pause playback."""
        self._paused = True
    
    def resume(self) -> None:
        """Resume playback."""
        self._paused = False
    
    def stop(self) -> None:
        """Stop playback."""
        self._playing = False
        self._canceled = True
    
    def is_playing(self) -> bool:
        """Check if currently playing."""
        return self._playing
    
    def is_paused(self) -> bool:
        """Check if paused."""
        return self._paused
    
    def get_progress(self) -> float:
        """Get playback progress [0.0, 1.0]."""
        if not self._workflow.actions:
            return 0.0
        return self._current_action / len(self._workflow.actions)


def merge_workflows(
    workflows: list[Workflow],
    namesep: str = " -> ",
) -> Workflow:
    """
    Merge multiple workflows into one.
    
    Args:
        workflows: List of workflows to merge
        namesep: Separator between workflow names
        
    Returns:
        Merged workflow
    """
    merged = Workflow(
        name=namesep.join(w.name for w in workflows),
        description=f"Merged from {len(workflows)} workflows",
    )
    
    offset = 0.0
    for workflow in workflows:
        if not workflow.actions:
            continue
        
        if merged.actions:
            offset = merged.actions[-1].timestamp
        
        for action in workflow.actions:
            new_action = WorkflowAction(
                action_type=action.action_type,
                timestamp=action.timestamp + offset,
                x=action.x,
                y=action.y,
                key=action.key,
                text=action.text,
                duration_ms=action.duration_ms,
                metadata=action.metadata.copy(),
            )
            merged.add_action(new_action)
    
    return merged
