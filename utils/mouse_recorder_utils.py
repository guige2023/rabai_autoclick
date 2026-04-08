"""
Mouse recorder utilities for recording mouse movements.

Provides mouse tracking, path recording, and playback
for automation workflows.
"""

from __future__ import annotations

import time
import json
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum


@dataclass
class MousePoint:
    """Mouse position at a point in time."""
    x: int
    y: int
    timestamp: float
    button_state: int = 0
    wheel_delta: int = 0


@dataclass
class MouseRecording:
    """Mouse recording session."""
    id: str
    name: str
    points: List[MousePoint] = field(default_factory=list)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    total_distance: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_total_distance(self) -> float:
        """Calculate total mouse travel distance."""
        if len(self.points) < 2:
            return 0.0
        
        total = 0.0
        for i in range(1, len(self.points)):
            p1 = self.points[i - 1]
            p2 = self.points[i]
            total += ((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2) ** 0.5
        
        self.total_distance = total
        return total
    
    def get_bounding_box(self) -> Tuple[int, int, int, int]:
        """Get bounding box of all points."""
        if not self.points:
            return (0, 0, 0, 0)
        
        xs = [p.x for p in self.points]
        ys = [p.y for p in self.points]
        
        return (min(xs), min(ys), max(xs), max(ys))


class MouseRecorder:
    """Records mouse movements."""
    
    def __init__(self, name: str = "recording"):
        """
        Initialize mouse recorder.
        
        Args:
            name: Recording name.
        """
        self.recording = MouseRecording(id=str(time.time()), name=name)
        self._is_recording = False
        self._last_point: Optional[MousePoint] = None
    
    def start(self) -> None:
        """Start recording."""
        self._is_recording = True
        self.recording.start_time = time.time()
        self.recording.points = []
        self._last_point = None
    
    def stop(self) -> MouseRecording:
        """
        Stop recording.
        
        Returns:
            MouseRecording with captured points.
        """
        self._is_recording = False
        self.recording.end_time = time.time()
        self.recording.calculate_total_distance()
        return self.recording
    
    def record_point(self, x: int, y: int,
                    button_state: int = 0,
                    wheel_delta: int = 0) -> None:
        """
        Record a mouse point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button_state: Button state.
            wheel_delta: Wheel delta.
        """
        if not self._is_recording:
            return
        
        point = MousePoint(
            x=x,
            y=y,
            timestamp=time.time(),
            button_state=button_state,
            wheel_delta=wheel_delta
        )
        
        self.recording.points.append(point)
        self._last_point = point
    
    def get_current_position(self) -> Tuple[int, int]:
        """
        Get current mouse position.
        
        Returns:
            Tuple of (x, y).
        """
        try:
            import Quartz
            loc = Quartz.NSEvent.mouseLocation()
            return (int(loc.x), int(loc.y))
        except Exception:
            return (0, 0)
    
    def is_recording(self) -> bool:
        """Check if recording."""
        return self._is_recording


def export_recording(recording: MouseRecording, path: str) -> bool:
    """
    Export recording to JSON.
    
    Args:
        recording: MouseRecording to export.
        path: Output file path.
        
    Returns:
        True if successful.
    """
    try:
        data = {
            'id': recording.id,
            'name': recording.name,
            'start_time': recording.start_time,
            'end_time': recording.end_time,
            'total_distance': recording.total_distance,
            'metadata': recording.metadata,
            'points': [
                {
                    'x': p.x,
                    'y': p.y,
                    'timestamp': p.timestamp,
                    'button_state': p.button_state,
                    'wheel_delta': p.wheel_delta,
                }
                for p in recording.points
            ]
        }
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False


def import_recording(path: str) -> Optional[MouseRecording]:
    """
    Import recording from JSON.
    
    Args:
        path: Input file path.
        
    Returns:
        MouseRecording or None.
    """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        points = [
            MousePoint(
                x=p['x'],
                y=p['y'],
                timestamp=p['timestamp'],
                button_state=p.get('button_state', 0),
                wheel_delta=p.get('wheel_delta', 0),
            )
            for p in data.get('points', [])
        ]
        
        recording = MouseRecording(
            id=data['id'],
            name=data['name'],
            start_time=data.get('start_time'),
            end_time=data.get('end_time'),
            metadata=data.get('metadata', {}),
            points=points
        )
        recording.calculate_total_distance()
        
        return recording
    except Exception:
        return None


def simplify_recording(recording: MouseRecording,
                      tolerance: float = 5.0) -> List[MousePoint]:
    """
    Simplify recording by removing points within tolerance.
    
    Args:
        recording: Recording to simplify.
        tolerance: Distance tolerance.
        
    Returns:
        Simplified list of MousePoint.
    """
    if not recording.points:
        return []
    
    simplified = [recording.points[0]]
    
    for point in recording.points[1:]:
        last = simplified[-1]
        distance = ((point.x - last.x) ** 2 + (point.y - last.y) ** 2) ** 0.5
        
        if distance >= tolerance:
            simplified.append(point)
    
    return simplified
