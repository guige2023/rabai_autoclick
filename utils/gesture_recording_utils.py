"""
Gesture recording and playback utilities.

Record touch and mouse gestures for replay in UI automation workflows.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional


class GestureType(Enum):
    """Types of gestures that can be recorded."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE = auto()
    DRAG = auto()
    PINCH = auto()
    CUSTOM = auto()


@dataclass
class GesturePoint:
    """A single point in a gesture recording."""
    x: float
    y: float
    timestamp: float
    pressure: float = 1.0
    size: float = 1.0


@dataclass
class GestureEvent:
    """A discrete gesture event (tap, swipe start, etc.)."""
    event_type: GestureType
    start_time: float
    end_time: float
    points: list[GesturePoint] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class GestureRecording:
    """Complete gesture recording with metadata."""
    name: str
    events: list[GestureEvent]
    duration: float
    created_at: float = field(default_factory=time.time)
    app_bundle: Optional[str] = None
    screen_size: tuple[int, int] = (0, 0)
    version: str = "1.0"


class GestureRecorder:
    """Record gestures from input events."""
    
    def __init__(self, name: str):
        self.name = name
        self._events: list[GestureEvent] = []
        self._current_event: Optional[GestureEvent] = None
        self._is_recording = False
        self._start_time: float = 0
    
    def start_recording(self) -> None:
        """Start recording a new gesture."""
        self._events.clear()
        self._is_recording = True
        self._start_time = time.time()
    
    def add_point(self, x: float, y: float, pressure: float = 1.0) -> None:
        """Add a point to the current gesture."""
        if not self._is_recording:
            return
        
        if self._current_event is None:
            self._current_event = GestureEvent(
                event_type=GestureType.CUSTOM,
                start_time=time.time() - self._start_time,
                end_time=0
            )
        
        point = GesturePoint(
            x=x,
            y=y,
            timestamp=time.time() - self._start_time,
            pressure=pressure
        )
        self._current_event.points.append(point)
    
    def end_event(self, gesture_type: GestureType = GestureType.CUSTOM) -> None:
        """End the current gesture event."""
        if self._current_event is not None:
            self._current_event.event_type = gesture_type
            self._current_event.end_time = time.time() - self._start_time
            self._events.append(self._current_event)
            self._current_event = None
    
    def stop_recording(self) -> GestureRecording:
        """Stop recording and return the completed gesture."""
        self._is_recording = False
        
        if self._current_event is not None:
            self._current_event.end_time = time.time() - self._start_time
            self._events.append(self._current_event)
            self._current_event = None
        
        total_duration = self._events[-1].end_time if self._events else 0
        
        return GestureRecording(
            name=self.name,
            events=self._events,
            duration=total_duration
        )
    
    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._is_recording


class GesturePlayer:
    """Playback recorded gestures."""
    
    def __init__(self, recording: GestureRecording):
        self.recording = recording
        self._current_event_index = 0
        self._current_point_index = 0
        self._is_playing = False
    
    def start(self) -> None:
        """Start playing the gesture."""
        self._current_event_index = 0
        self._current_point_index = 0
        self._is_playing = True
    
    def get_next_point(self) -> Optional[GesturePoint]:
        """Get the next point in the gesture."""
        if not self._is_playing:
            return None
        
        if self._current_event_index >= len(self.recording.events):
            self._is_playing = False
            return None
        
        event = self.recording.events[self._current_event_index]
        if self._current_point_index >= len(event.points):
            self._current_event_index += 1
            self._current_point_index = 0
            return self.get_next_point()
        
        point = event.points[self._current_point_index]
        self._current_point_index += 1
        return point
    
    def is_playing(self) -> bool:
        """Check if still playing."""
        return self._is_playing
    
    def reset(self) -> None:
        """Reset playback to the beginning."""
        self._current_event_index = 0
        self._current_point_index = 0
        self._is_playing = False


class GestureStorage:
    """Persist and retrieve gesture recordings."""
    
    @staticmethod
    def save(recording: GestureRecording, path: Path) -> None:
        """Save a recording to disk."""
        data = {
            "name": recording.name,
            "duration": recording.duration,
            "created_at": recording.created_at,
            "app_bundle": recording.app_bundle,
            "screen_size": recording.screen_size,
            "version": recording.version,
            "events": [
                {
                    "event_type": e.event_type.name,
                    "start_time": e.start_time,
                    "end_time": e.end_time,
                    "points": [
                        {"x": p.x, "y": p.y, "timestamp": p.timestamp, "pressure": p.pressure, "size": p.size}
                        for p in e.points
                    ],
                    "metadata": e.metadata
                }
                for e in recording.events
            ]
        }
        
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    
    @staticmethod
    def load(path: Path) -> GestureRecording:
        """Load a recording from disk."""
        with open(path) as f:
            data = json.load(f)
        
        events = []
        for e_data in data["events"]:
            points = [
                GesturePoint(
                    x=p["x"],
                    y=p["y"],
                    timestamp=p["timestamp"],
                    pressure=p.get("pressure", 1.0),
                    size=p.get("size", 1.0)
                )
                for p in e_data["points"]
            ]
            event = GestureEvent(
                event_type=GestureType[e_data["event_type"]],
                start_time=e_data["start_time"],
                end_time=e_data["end_time"],
                points=points,
                metadata=e_data.get("metadata", {})
            )
            events.append(event)
        
        return GestureRecording(
            name=data["name"],
            events=events,
            duration=data["duration"],
            created_at=data.get("created_at", time.time()),
            app_bundle=data.get("app_bundle"),
            screen_size=tuple(data.get("screen_size", [0, 0])),
            version=data.get("version", "1.0")
        )
