"""
Test recording utilities for GUI automation.

Provides recording, playback, and export of automation test
sequences with various output formats.
"""

from __future__ import annotations

import json
import time
import subprocess
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
import os


class ActionType(Enum):
    """Recorded action types."""
    CLICK = "click"
    RIGHT_CLICK = "right_click"
    DOUBLE_CLICK = "double_click"
    TYPE = "type"
    PRESS = "press"
    WAIT = "wait"
    SCROLL = "scroll"
    DRAG = "drag"
    MOVE = "move"
    SCREENSHOT = "screenshot"


@dataclass
class RecordedAction:
    """Single recorded action."""
    action_type: ActionType
    timestamp: float
    x: Optional[int] = None
    y: Optional[int] = None
    text: Optional[str] = None
    key: Optional[str] = None
    duration: Optional[float] = None
    screenshot_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Recording:
    """Test recording session."""
    id: str
    name: str
    start_time: float
    end_time: Optional[float] = None
    actions: List[RecordedAction] = field(default_factory=list)
    app_bundle_id: Optional[str] = None
    app_name: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class TestRecorder:
    """Records automation actions for later playback."""
    
    def __init__(self, name: str = "unnamed"):
        """
        Initialize test recorder.
        
        Args:
            name: Recording name.
        """
        self.recording = Recording(
            id=self._generate_id(),
            name=name,
            start_time=time.time()
        )
        self._is_recording = False
        self._action_handlers: List[Callable] = []
    
    def start(self) -> None:
        """Start recording."""
        self._is_recording = True
    
    def stop(self) -> Recording:
        """
        Stop recording.
        
        Returns:
            Recording with all captured actions.
        """
        self._is_recording = False
        self.recording.end_time = time.time()
        return self.recording
    
    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._is_recording
    
    def record_action(self, action: RecordedAction) -> None:
        """
        Record an action.
        
        Args:
            action: Action to record.
        """
        if self._is_recording:
            self.recording.actions.append(action)
            for handler in self._action_handlers:
                handler(action)
    
    def record_click(self, x: int, y: int, button: str = "left") -> None:
        """Record a click action."""
        action_type = ActionType.CLICK
        if button == "right":
            action_type = ActionType.RIGHT_CLICK
        elif button == "double":
            action_type = ActionType.DOUBLE_CLICK
        
        self.record_action(RecordedAction(
            action_type=action_type,
            timestamp=time.time(),
            x=x, y=y
        ))
    
    def record_type(self, text: str) -> None:
        """Record a type action."""
        self.record_action(RecordedAction(
            action_type=ActionType.TYPE,
            timestamp=time.time(),
            text=text
        ))
    
    def record_key(self, key: str) -> None:
        """Record a key press."""
        self.record_action(RecordedAction(
            action_type=ActionType.PRESS,
            timestamp=time.time(),
            key=key
        ))
    
    def record_wait(self, duration: float) -> None:
        """Record a wait action."""
        self.record_action(RecordedAction(
            action_type=ActionType.WAIT,
            timestamp=time.time(),
            duration=duration
        ))
    
    def record_scroll(self, x: int, y: int, dx: int, dy: int) -> None:
        """Record a scroll action."""
        self.record_action(RecordedAction(
            action_type=ActionType.SCROLL,
            timestamp=time.time(),
            x=x, y=y,
            metadata={"dx": dx, "dy": dy}
        ))
    
    def record_drag(self, x1: int, y1: int, x2: int, y2: int) -> None:
        """Record a drag action."""
        self.record_action(RecordedAction(
            action_type=ActionType.DRAG,
            timestamp=time.time(),
            x=x1, y=y1,
            metadata={"x2": x2, "y2": y2}
        ))
    
    def add_action_handler(self, handler: Callable[[RecordedAction], None]) -> None:
        """Add action handler callback."""
        self._action_handlers.append(handler)
    
    def _generate_id(self) -> str:
        """Generate unique recording ID."""
        return f"rec_{datetime.now().strftime('%Y%m%d%H%M%S')}"


def export_recording(recording: Recording, path: str, format: str = "json") -> bool:
    """
    Export recording to file.
    
    Args:
        recording: Recording to export.
        path: Output file path.
        format: Export format ('json', 'python', 'selenium').
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        if format == "json":
            with open(path, 'w') as f:
                json.dump(asdict(recording), f, indent=2, default=str)
            return True
        elif format == "python":
            return _export_python(recording, path)
        elif format == "selenium":
            return _export_selenium(recording, path)
    except Exception:
        pass
    return False


def _export_python(recording: Recording, path: str) -> bool:
    """Export as Python playback script."""
    lines = [
        "#!/usr/bin/env python3",
        f'"""Playback script for recording: {recording.name}"""',
        "",
        "import time",
        "import Quartz",
        "",
        "def playback():",
    ]
    
    for action in recording.actions:
        if action.action_type == ActionType.CLICK:
            lines.append(f"    click({action.x}, {action.y})")
        elif action.action_type == ActionType.TYPE:
            lines.append(f"    type_text('{action.text}')")
        elif action.action_type == ActionType.WAIT:
            lines.append(f"    time.sleep({action.duration})")
        lines.append(f"    # {action.action_type.value} at {action.timestamp}")
    
    lines.extend([
        "",
        "if __name__ == '__main__':",
        "    playback()",
    ])
    
    try:
        with open(path, 'w') as f:
            f.write('\n'.join(lines))
        os.chmod(path, 0o755)
        return True
    except Exception:
        return False


def _export_selenium(recording: Recording, path: str) -> bool:
    """Export as Selenium Python script."""
    lines = [
        "#!/usr/bin/env python3",
        f'"""Selenium playback script for: {recording.name}"""',
        "",
        "from selenium import webdriver",
        "from selenium.webdriver.common.by import By",
        "from selenium.webdriver.common.action_chains import ActionChains",
        "import time",
        "",
        "def playback(driver):",
        "    actions = ActionChains(driver)",
    ]
    
    for action in recording.actions:
        lines.append(f"    # {action.action_type.value}")
        if action.action_type == ActionType.WAIT and action.duration:
            lines.append(f"    time.sleep({action.duration})")
    
    lines.extend([
        "",
        "if __name__ == '__main__':",
        "    driver = webdriver.Chrome()",
        "    playback(driver)",
        "    driver.quit()",
    ])
    
    try:
        with open(path, 'w') as f:
            f.write('\n'.join(lines))
        return True
    except Exception:
        return False


def import_recording(path: str) -> Optional[Recording]:
    """
    Import recording from file.
    
    Args:
        path: Recording file path.
        
    Returns:
        Recording if successful, None otherwise.
    """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        actions = []
        for a in data.get('actions', []):
            actions.append(RecordedAction(
                action_type=ActionType(a['action_type']),
                timestamp=a['timestamp'],
                x=a.get('x'),
                y=a.get('y'),
                text=a.get('text'),
                key=a.get('key'),
                duration=a.get('duration'),
                screenshot_path=a.get('screenshot_path'),
                metadata=a.get('metadata', {})
            ))
        
        return Recording(
            id=data['id'],
            name=data['name'],
            start_time=data['start_time'],
            end_time=data.get('end_time'),
            actions=actions,
            app_bundle_id=data.get('app_bundle_id'),
            app_name=data.get('app_name'),
            metadata=data.get('metadata', {})
        )
    except Exception:
        return None
