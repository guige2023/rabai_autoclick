"""
Animation recording utilities for GUI automation.

Provides recording and playback of UI animations
for automation testing and documentation.
"""

from __future__ import annotations

import time
import json
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum


class AnimationEventType(Enum):
    """Animation event types."""
    FRAME = "frame"
    CLICK = "click"
    DRAG = "drag"
    SCROLL = "scroll"
    KEY_FRAME = "key_frame"


@dataclass
class AnimationEvent:
    """Single animation event."""
    event_type: AnimationEventType
    timestamp: float
    x: Optional[int] = None
    y: Optional[int] = None
    end_x: Optional[int] = None
    end_y: Optional[int] = None
    frame_data: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnimationRecording:
    """Animation recording session."""
    id: str
    name: str
    fps: int
    events: List[AnimationEvent] = field(default_factory=list)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    frame_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class AnimationRecorder:
    """Records UI animations."""
    
    def __init__(self, name: str = "animation", fps: int = 30):
        """
        Initialize animation recorder.
        
        Args:
            name: Recording name.
            fps: Frames per second.
        """
        self.recording = AnimationRecording(
            id=str(time.time()),
            name=name,
            fps=fps
        )
        self._is_recording = False
        self._frame_interval = 1.0 / fps
        self._last_frame_time = 0.0
        self._frame_callbacks: List[Callable] = []
    
    def start(self) -> None:
        """Start recording."""
        self._is_recording = True
        self.recording.start_time = time.time()
        self._last_frame_time = time.time()
    
    def stop(self) -> AnimationRecording:
        """
        Stop recording.
        
        Returns:
            AnimationRecording with captured events.
        """
        self._is_recording = False
        self.recording.end_time = time.time()
        return self.recording
    
    def record_frame(self, frame_data: Optional[str] = None,
                    metadata: Optional[Dict] = None) -> None:
        """
        Record a frame.
        
        Args:
            frame_data: Optional frame data.
            metadata: Optional metadata.
        """
        if not self._is_recording:
            return
        
        now = time.time()
        if now - self._last_frame_time < self._frame_interval:
            return
        
        event = AnimationEvent(
            event_type=AnimationEventType.FRAME,
            timestamp=now,
            frame_data=frame_data,
            metadata=metadata or {}
        )
        
        self.recording.events.append(event)
        self.recording.frame_count += 1
        self._last_frame_time = now
        
        for callback in self._frame_callbacks:
            try:
                callback(event)
            except Exception:
                pass
    
    def record_click(self, x: int, y: int) -> None:
        """Record click event."""
        if not self._is_recording:
            return
        
        self.recording.events.append(AnimationEvent(
            event_type=AnimationEventType.CLICK,
            timestamp=time.time(),
            x=x, y=y
        ))
    
    def record_drag(self, x1: int, y1: int, x2: int, y2: int) -> None:
        """Record drag event."""
        if not self._is_recording:
            return
        
        self.recording.events.append(AnimationEvent(
            event_type=AnimationEventType.DRAG,
            timestamp=time.time(),
            x=x1, y=y1, end_x=x2, end_y=y2
        ))
    
    def add_frame_callback(self, callback: Callable) -> None:
        """Add frame capture callback."""
        self._frame_callbacks.append(callback)
    
    def is_recording(self) -> bool:
        """Check if recording."""
        return self._is_recording
    
    def get_duration(self) -> float:
        """Get recording duration."""
        if self.recording.start_time is None:
            return 0.0
        end = self.recording.end_time or time.time()
        return end - self.recording.start_time


def export_animation(recording: AnimationRecording, path: str) -> bool:
    """
    Export animation recording to JSON.
    
    Args:
        recording: AnimationRecording to export.
        path: Output file path.
        
    Returns:
        True if successful.
    """
    try:
        data = {
            'id': recording.id,
            'name': recording.name,
            'fps': recording.fps,
            'frame_count': recording.frame_count,
            'start_time': recording.start_time,
            'end_time': recording.end_time,
            'metadata': recording.metadata,
            'events': [
                {
                    'type': e.event_type.value,
                    'timestamp': e.timestamp,
                    'x': e.x,
                    'y': e.y,
                    'end_x': e.end_x,
                    'end_y': e.end_y,
                    'frame_data': e.frame_data,
                    'metadata': e.metadata,
                }
                for e in recording.events
            ]
        }
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False


def import_animation(path: str) -> Optional[AnimationRecording]:
    """
    Import animation recording from JSON.
    
    Args:
        path: Input file path.
        
    Returns:
        AnimationRecording or None.
    """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        events = [
            AnimationEvent(
                event_type=AnimationEventType(e['type']),
                timestamp=e['timestamp'],
                x=e.get('x'),
                y=e.get('y'),
                end_x=e.get('end_x'),
                end_y=e.get('end_y'),
                frame_data=e.get('frame_data'),
                metadata=e.get('metadata', {}),
            )
            for e in data.get('events', [])
        ]
        
        recording = AnimationRecording(
            id=data['id'],
            name=data['name'],
            fps=data.get('fps', 30),
            frame_count=data.get('frame_count', 0),
            start_time=data.get('start_time'),
            end_time=data.get('end_time'),
            metadata=data.get('metadata', {}),
            events=events
        )
        
        return recording
    except Exception:
        return None
