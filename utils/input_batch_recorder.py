"""Input batch recorder for capturing and replaying input sequences."""
from typing import List, Dict, Optional, Any, Callable
from dataclasses import dataclass, field
import time
import json


@dataclass
class InputEvent:
    """A single recorded input event."""
    event_type: str
    x: float
    y: float
    timestamp: float
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InputRecording:
    """Complete input recording session."""
    name: str
    events: List[InputEvent]
    duration: float
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class InputBatchRecorder:
    """Records batches of input events for playback.
    
    Captures touch, click, and gesture events into recordings
    that can be saved and replayed later.
    
    Example:
        recorder = InputBatchRecorder()
        recorder.start_recording("test_session")
        recorder.record_event("click", 100, 200)
        recording = recorder.stop_recording()
        recorder.save(recording, "recording.json")
    """

    def __init__(self, max_events: int = 10000) -> None:
        self._max_events = max_events
        self._recordings: Dict[str, InputRecording] = {}
        self._current_name: Optional[str] = None
        self._current_events: List[InputEvent] = []
        self._recording_start: float = 0
        self._listeners: List[Callable] = []

    def start_recording(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Start a new recording session."""
        if self._current_name:
            self.stop_recording()
        
        self._current_name = name
        self._current_events = []
        self._recording_start = time.time()
        
        if metadata is None:
            metadata = {}
        if name in self._recordings:
            del self._recordings[name]

    def stop_recording(self) -> Optional[InputRecording]:
        """Stop the current recording session."""
        if not self._current_name:
            return None
        
        duration = time.time() - self._recording_start
        recording = InputRecording(
            name=self._current_name,
            events=self._current_events.copy(),
            duration=duration,
            metadata={"event_count": len(self._current_events)},
        )
        
        self._recordings[self._current_name] = recording
        self._current_name = None
        self._current_events = []
        
        return recording

    def record_event(
        self,
        event_type: str,
        x: float,
        y: float,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a single input event."""
        if not self._current_name:
            return
        
        if len(self._current_events) >= self._max_events:
            self.stop_recording()
            return
        
        event = InputEvent(
            event_type=event_type,
            x=x,
            y=y,
            timestamp=time.time() - self._recording_start,
            data=data or {},
        )
        
        self._current_events.append(event)
        
        for listener in self._listeners:
            try:
                listener(event)
            except Exception:
                pass

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._current_name is not None

    def add_listener(self, listener: Callable) -> None:
        """Add an event listener."""
        self._listeners.append(listener)

    def get_recording(self, name: str) -> Optional[InputRecording]:
        """Get a saved recording by name."""
        return self._recordings.get(name)

    def list_recordings(self) -> List[str]:
        """List all saved recording names."""
        return list(self._recordings.keys())

    def delete_recording(self, name: str) -> bool:
        """Delete a saved recording."""
        return bool(self._recordings.pop(name, None))

    def save(self, recording: InputRecording, path: str) -> None:
        """Save a recording to JSON file."""
        data = {
            "name": recording.name,
            "duration": recording.duration,
            "created_at": recording.created_at,
            "metadata": recording.metadata,
            "events": [
                {"type": e.event_type, "x": e.x, "y": e.y, "timestamp": e.timestamp, "data": e.data}
                for e in recording.events
            ],
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load(self, path: str) -> Optional[InputRecording]:
        """Load a recording from JSON file."""
        try:
            with open(path) as f:
                data = json.load(f)
            
            events = [
                InputEvent(
                    event_type=e["type"],
                    x=e["x"],
                    y=e["y"],
                    timestamp=e["timestamp"],
                    data=e.get("data", {}),
                )
                for e in data.get("events", [])
            ]
            
            recording = InputRecording(
                name=data["name"],
                events=events,
                duration=data.get("duration", 0),
                created_at=data.get("created_at", time.time()),
                metadata=data.get("metadata", {}),
            )
            
            self._recordings[recording.name] = recording
            return recording
        except Exception:
            return None

    def get_stats(self, name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a recording."""
        recording = self._recordings.get(name)
        if not recording:
            return None
        
        event_types: Dict[str, int] = {}
        for event in recording.events:
            event_types[event.event_type] = event_types.get(event.event_type, 0) + 1
        
        return {
            "name": recording.name,
            "event_count": len(recording.events),
            "duration": recording.duration,
            "event_types": event_types,
            "events_per_second": len(recording.events) / recording.duration if recording.duration > 0 else 0,
        }
