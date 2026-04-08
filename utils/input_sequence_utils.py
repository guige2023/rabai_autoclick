"""Input sequence recording and playback utilities.

This module provides utilities for recording sequences of user inputs
(keyboard, mouse) and playing them back with precise timing control.
"""

from __future__ import annotations

import time
import json
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class InputEvent:
    """Base class for an input event."""
    timestamp: float
    event_type: str  # 'mouse', 'keyboard', 'wait'


@dataclass
class MouseEvent(InputEvent):
    """A mouse input event."""
    event_type: str = "mouse"
    action: str = "move"  # 'move', 'click', 'double_click', 'drag', 'scroll'
    x: int = 0
    y: int = 0
    button: str = "left"
    click_count: int = 1
    scroll_dx: int = 0
    scroll_dy: int = 0
    
    def to_dict(self) -> dict:
        return {"type": "mouse", "timestamp": self.timestamp, "action": self.action,
                "x": self.x, "y": self.y, "button": self.button,
                "click_count": self.click_count, "scroll_dx": self.scroll_dx, "scroll_dy": self.scroll_dy}


@dataclass
class KeyEvent(InputEvent):
    """A keyboard input event."""
    event_type: str = "keyboard"
    action: str = "press"  # 'press', 'release'
    key: str = ""
    modifiers: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {"type": "keyboard", "timestamp": self.timestamp, "action": self.action,
                "key": self.key, "modifiers": self.modifiers}


@dataclass
class WaitEvent(InputEvent):
    """A wait/delay event for precise timing."""
    event_type: str = "wait"
    duration: float = 0.0
    
    def to_dict(self) -> dict:
        return {"type": "wait", "timestamp": self.timestamp, "duration": self.duration}


class InputSequence:
    """Records and plays back sequences of input events."""
    
    def __init__(self, name: str = "unnamed"):
        self.name = name
        self.events: list[InputEvent] = []
        self._start_time: Optional[float] = None
    
    def start(self) -> None:
        """Start recording a new sequence."""
        self.events = []
        self._start_time = time.monotonic()
    
    def add_mouse(
        self,
        action: str,
        x: int = 0,
        y: int = 0,
        button: str = "left",
        click_count: int = 1,
        scroll_dx: int = 0,
        scroll_dy: int = 0,
    ) -> None:
        """Add a mouse event to the sequence."""
        if self._start_time is None:
            self.start()
        
        timestamp = time.monotonic() - self._start_time
        event = MouseEvent(
            timestamp=timestamp,
            event_type="mouse",
            action=action,
            x=x,
            y=y,
            button=button,
            click_count=click_count,
            scroll_dx=scroll_dx,
            scroll_dy=scroll_dy,
        )
        self.events.append(event)
    
    def add_key(
        self,
        key: str,
        action: str = "press",
        modifiers: Optional[list[str]] = None,
    ) -> None:
        """Add a keyboard event to the sequence."""
        if self._start_time is None:
            self.start()
        
        timestamp = time.monotonic() - self._start_time
        event = KeyEvent(
            timestamp=timestamp,
            event_type="keyboard",
            action=action,
            key=key,
            modifiers=modifiers or [],
        )
        self.events.append(event)
    
    def add_wait(self, duration: float) -> None:
        """Add a wait period to the sequence."""
        if self._start_time is None:
            self.start()
        
        timestamp = time.monotonic() - self._start_time
        event = WaitEvent(
            timestamp=timestamp,
            event_type="wait",
            duration=duration,
        )
        self.events.append(event)
    
    def stop(self) -> int:
        """Stop recording.
        
        Returns:
            Number of events recorded.
        """
        count = len(self.events)
        self._start_time = None
        return count
    
    def to_dict(self) -> dict:
        """Serialize the sequence to a dictionary."""
        return {
            "name": self.name,
            "event_count": len(self.events),
            "events": [e.to_dict() for e in self.events],
        }
    
    def save(self, filepath: str) -> bool:
        """Save the sequence to a JSON file.
        
        Args:
            filepath: Path to save the file.
        
        Returns:
            True if saved successfully.
        """
        try:
            with open(filepath, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
            return True
        except IOError:
            return False
    
    @classmethod
    def load(cls, filepath: str) -> Optional["InputSequence"]:
        """Load a sequence from a JSON file.
        
        Args:
            filepath: Path to the file.
        
        Returns:
            Loaded InputSequence or None if load failed.
        """
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            
            seq = cls(name=data.get("name", "loaded"))
            # Reconstruct events from dict
            for event_data in data.get("events", []):
                if event_data["type"] == "mouse":
                    event = MouseEvent(
                        timestamp=event_data["timestamp"],
                        event_type="mouse",
                        action=event_data.get("action", "move"),
                        x=event_data.get("x", 0),
                        y=event_data.get("y", 0),
                        button=event_data.get("button", "left"),
                        click_count=event_data.get("click_count", 1),
                        scroll_dx=event_data.get("scroll_dx", 0),
                        scroll_dy=event_data.get("scroll_dy", 0),
                    )
                    seq.events.append(event)
                elif event_data["type"] == "keyboard":
                    event = KeyEvent(
                        timestamp=event_data["timestamp"],
                        event_type="keyboard",
                        action=event_data.get("action", "press"),
                        key=event_data.get("key", ""),
                        modifiers=event_data.get("modifiers", []),
                    )
                    seq.events.append(event)
                elif event_data["type"] == "wait":
                    event = WaitEvent(
                        timestamp=event_data["timestamp"],
                        event_type="wait",
                        duration=event_data.get("duration", 0.0),
                    )
                    seq.events.append(event)
            return seq
        except (IOError, json.JSONDecodeError):
            return None
    
    def play(
        self,
        speed: float = 1.0,
        loop: bool = False,
    ) -> None:
        """Play back the input sequence.
        
        Args:
            speed: Playback speed multiplier (1.0 = real-time).
            loop: Whether to loop the playback.
        """
        try:
            import pyautogui
        except ImportError:
            return
        
        while True:
            last_timestamp = 0.0
            
            for event in self.events:
                # Calculate wait time
                wait_time = (event.timestamp - last_timestamp) / speed
                if wait_time > 0:
                    time.sleep(wait_time)
                
                last_timestamp = event.timestamp
                
                if isinstance(event, MouseEvent):
                    if event.action == "move":
                        pyautogui.moveTo(event.x, event.y)
                    elif event.action == "click":
                        pyautogui.click(event.x, event.y, clicks=event.click_count)
                    elif event.action == "double_click":
                        pyautogui.doubleClick(event.x, event.y)
                    elif event.action == "scroll":
                        pyautogui.scroll(event.scroll_dy, event.x, event.y)
                
                elif isinstance(event, KeyEvent):
                    key = event.key
                    if event.modifiers:
                        mods = "+".join(event.modifiers + [key])
                    else:
                        mods = key
                    if event.action == "press":
                        pyautogui.press(mods)
                
                elif isinstance(event, WaitEvent):
                    time.sleep(event.duration / speed)
            
            if not loop:
                break
