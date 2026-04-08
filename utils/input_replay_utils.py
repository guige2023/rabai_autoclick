"""
Input replay utilities for playback of recorded automation actions.

Provides replay of mouse and keyboard events with
timing control and sequencing.
"""

from __future__ import annotations

import json
import time
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum


class EventType(Enum):
    """Input event types."""
    MOUSE_MOVE = "mouse_move"
    MOUSE_DOWN = "mouse_down"
    MOUSE_UP = "mouse_up"
    MOUSE_CLICK = "mouse_click"
    MOUSE_DOUBLE_CLICK = "mouse_double_click"
    MOUSE_DRAG = "mouse_drag"
    KEY_DOWN = "key_down"
    KEY_UP = "key_up"
    KEY_PRESS = "key_press"
    WAIT = "wait"
    SCREENSHOT = "screenshot"


@dataclass
class InputEvent:
    """Single input event."""
    type: EventType
    timestamp: float
    x: Optional[int] = None
    y: Optional[int] = None
    button: int = 0
    key_code: Optional[int] = None
    key: Optional[str] = None
    duration: Optional[float] = None
    modifiers: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplaySession:
    """Replay session with events."""
    id: str
    name: str
    events: List[InputEvent]
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    playback_speed: float = 1.0


@dataclass
class ReplayResult:
    """Result of replay execution."""
    success: bool
    events_played: int
    total_duration: float
    errors: List[str] = field(default_factory=list)


class InputReplay:
    """Replays recorded input events."""
    
    def __init__(self):
        self._handlers: Dict[EventType, Callable] = {}
        self._setup_default_handlers()
    
    def _setup_default_handlers(self) -> None:
        """Setup default event handlers."""
        import Quartz
        from Quartz import CGEventCreateKeyboardEvent, CGEventCreateMouseEvent
        
        def handle_mouse_move(event: InputEvent):
            if event.x is not None and event.y is not None:
                moved = CGEventCreateMouseEvent(
                    None, Quartz.kCGEventMouseMoved, (event.x, event.y), Quartz.kCGMouseButtonLeft
                )
                CGEventPost(Quartz.kCGHIDEventTap, moved)
        
        def handle_mouse_down(event: InputEvent):
            if event.x is not None and event.y is not None:
                btn = Quartz.kCGEventLeftMouseDown
                if event.button == 1:
                    btn = Quartz.kCGEventRightMouseDown
                elif event.button == 2:
                    btn = Quartz.kCGEventOtherMouseDown
                down = CGEventCreateMouseEvent(None, btn, (event.x, event.y), Quartz.kCGMouseButton(event.button))
                CGEventPost(Quartz.kCGHIDEventTap, down)
        
        def handle_mouse_up(event: InputEvent):
            if event.x is not None and event.y is not None:
                btn = Quartz.kCGEventLeftMouseUp
                if event.button == 1:
                    btn = Quartz.kCGEventRightMouseUp
                elif event.button == 2:
                    btn = Quartz.kCGEventOtherMouseUp
                up = CGEventCreateMouseEvent(None, btn, (event.x, event.y), Quartz.kCGMouseButton(event.button))
                CGEventPost(Quartz.kCGHIDEventTap, up)
        
        def handle_key_down(event: InputEvent):
            if event.key_code is not None:
                down = CGEventCreateKeyboardEvent(None, event.key_code, True)
                CGEventPost(Quartz.kCGHIDEventTap, down)
        
        def handle_key_up(event: InputEvent):
            if event.key_code is not None:
                up = CGEventCreateKeyboardEvent(None, event.key_code, False)
                CGEventPost(Quartz.kCGHIDEventTap, up)
        
        def handle_wait(event: InputEvent):
            if event.duration:
                time.sleep(event.duration)
        
        self._handlers = {
            EventType.MOUSE_MOVE: handle_mouse_move,
            EventType.MOUSE_DOWN: handle_mouse_down,
            EventType.MOUSE_UP: handle_mouse_up,
            EventType.KEY_DOWN: handle_key_down,
            EventType.KEY_UP: handle_key_up,
            EventType.WAIT: handle_wait,
        }
    
    def replay(self, session: ReplaySession) -> ReplayResult:
        """
        Replay a session.
        
        Args:
            session: ReplaySession to replay.
            
        Returns:
            ReplayResult.
        """
        start = time.time()
        errors = []
        played = 0
        last_timestamp = session.events[0].timestamp if session.events else time.time()
        
        for event in session.events:
            try:
                handler = self._handlers.get(event.type)
                if handler:
                    handler(event)
                    played += 1
                
                if event.type != EventType.WAIT and session.playback_speed > 0:
                    delay = (event.timestamp - last_timestamp) / session.playback_speed
                    if delay > 0:
                        time.sleep(min(delay, 1.0))
                
                last_timestamp = event.timestamp
            except Exception as e:
                errors.append(f"Event {event.type}: {e}")
        
        return ReplayResult(
            success=len(errors) == 0,
            events_played=played,
            total_duration=time.time() - start,
            errors=errors
        )
    
    def replay_events(self, events: List[InputEvent],
                     speed: float = 1.0) -> ReplayResult:
        """
        Replay a list of events.
        
        Args:
            events: List of InputEvent.
            speed: Playback speed multiplier.
            
        Returns:
            ReplayResult.
        """
        session = ReplaySession(
            id="temp",
            name="temp",
            events=events,
            playback_speed=speed
        )
        return self.replay(session)


def create_click_event(x: int, y: int, button: int = 0) -> InputEvent:
    """Create mouse click event."""
    return InputEvent(
        type=EventType.MOUSE_CLICK,
        timestamp=time.time(),
        x=x, y=y, button=button
    )


def create_drag_event(x1: int, y1: int, x2: int, y2: int,
                     button: int = 0) -> InputEvent:
    """Create mouse drag event."""
    return InputEvent(
        type=EventType.MOUSE_DRAG,
        timestamp=time.time(),
        x=x1, y=y1, button=button,
        metadata={'end_x': x2, 'end_y': y2}
    )


def create_key_event(key_code: int, event_type: EventType = EventType.KEY_PRRESS) -> InputEvent:
    """Create key press event."""
    return InputEvent(
        type=event_type,
        timestamp=time.time(),
        key_code=key_code
    )


def create_wait_event(duration: float) -> InputEvent:
    """Create wait event."""
    return InputEvent(
        type=EventType.WAIT,
        timestamp=time.time(),
        duration=duration
    )


def export_events(events: List[InputEvent], path: str) -> bool:
    """
    Export events to JSON.
    
    Args:
        events: List of InputEvent.
        path: Output path.
        
    Returns:
        True if successful.
    """
    try:
        data = [
            {
                'type': e.type.value,
                'timestamp': e.timestamp,
                'x': e.x,
                'y': e.y,
                'button': e.button,
                'key_code': e.key_code,
                'key': e.key,
                'duration': e.duration,
                'modifiers': e.modifiers,
                'metadata': e.metadata,
            }
            for e in events
        ]
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False


def import_events(path: str) -> List[InputEvent]:
    """
    Import events from JSON.
    
    Args:
        path: Input path.
        
    Returns:
        List of InputEvent.
    """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        events = []
        for item in data:
            events.append(InputEvent(
                type=EventType(item['type']),
                timestamp=item['timestamp'],
                x=item.get('x'),
                y=item.get('y'),
                button=item.get('button', 0),
                key_code=item.get('key_code'),
                key=item.get('key'),
                duration=item.get('duration'),
                modifiers=item.get('modifiers', []),
                metadata=item.get('metadata', {}),
            ))
        return events
    except Exception:
        return []
