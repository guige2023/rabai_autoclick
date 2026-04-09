"""
UI element lifecycle tracking utilities.

Track UI element creation, updates, and destruction for automation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum, auto


class ElementState(Enum):
    """Lifecycle state of a UI element."""
    CREATED = auto()
    UPDATED = auto()
    VISIBLE = auto()
    HIDDEN = auto()
    DESTROYED = auto()
    REUSED = auto()


@dataclass
class ElementEvent:
    """An event in an element's lifecycle."""
    state: ElementState
    timestamp: float
    element_id: str
    metadata: dict = field(default_factory=dict)


@dataclass
class ElementSnapshot:
    """Point-in-time snapshot of an element."""
    element_id: str
    timestamp: float
    bounds: tuple[int, int, int, int]
    is_visible: bool
    text: Optional[str] = None
    attributes: dict = field(default_factory=dict)


class ElementTracker:
    """Track UI element lifecycle events."""
    
    def __init__(self, max_history: int = 1000):
        self._events: list[ElementEvent] = []
        self._snapshots: dict[str, list[ElementSnapshot]] = {}
        self._max_history = max_history
        self._handlers: dict[ElementState, list[Callable[[ElementEvent], None]]] = {
            state: [] for state in ElementState
        }
    
    def record_event(
        self,
        element_id: str,
        state: ElementState,
        metadata: Optional[dict] = None
    ) -> None:
        """Record a lifecycle event for an element."""
        event = ElementEvent(
            state=state,
            timestamp=time.time(),
            element_id=element_id,
            metadata=metadata or {}
        )
        self._events.append(event)
        
        if len(self._events) > self._max_history:
            self._events.pop(0)
        
        for handler in self._handlers[state]:
            handler(event)
    
    def take_snapshot(
        self,
        element_id: str,
        bounds: tuple[int, int, int, int],
        is_visible: bool,
        text: Optional[str] = None,
        attributes: Optional[dict] = None
    ) -> None:
        """Take a snapshot of element state."""
        snapshot = ElementSnapshot(
            element_id=element_id,
            timestamp=time.time(),
            bounds=bounds,
            is_visible=is_visible,
            text=text,
            attributes=attributes or {}
        )
        
        if element_id not in self._snapshots:
            self._snapshots[element_id] = []
        
        self._snapshots[element_id].append(snapshot)
        
        if len(self._snapshots[element_id]) > self._max_history:
            self._snapshots[element_id].pop(0)
    
    def get_events(
        self,
        element_id: Optional[str] = None,
        state: Optional[ElementState] = None,
        since: Optional[float] = None
    ) -> list[ElementEvent]:
        """Query recorded events."""
        events = self._events
        
        if element_id:
            events = [e for e in events if e.element_id == element_id]
        
        if state:
            events = [e for e in events if e.state == state]
        
        if since:
            events = [e for e in events if e.timestamp >= since]
        
        return events
    
    def get_snapshots(self, element_id: str) -> list[ElementSnapshot]:
        """Get all snapshots for an element."""
        return self._snapshots.get(element_id, [])
    
    def on_state_change(self, state: ElementState, handler: Callable[[ElementEvent], None]) -> None:
        """Register handler for state change events."""
        self._handlers[state].append(handler)
    
    def get_element_lifespan(self, element_id: str) -> Optional[tuple[float, float]]:
        """Get creation and destruction times for an element."""
        events = self.get_events(element_id=element_id)
        
        if not events:
            return None
        
        creation = None
        destruction = None
        
        for event in events:
            if event.state == ElementState.CREATED and creation is None:
                creation = event.timestamp
            elif event.state == ElementState.DESTROYED:
                destruction = event.timestamp
        
        if creation is None:
            return None
        
        return (creation, destruction if destruction else time.time())
    
    def clear(self) -> None:
        """Clear all tracked events and snapshots."""
        self._events.clear()
        self._snapshots.clear()


class ElementChangeDetector:
    """Detect changes in UI element properties."""
    
    def __init__(self, tracker: ElementTracker):
        self.tracker = tracker
        self._previous_state: dict[str, ElementSnapshot] = {}
    
    def detect_changes(
        self,
        element_id: str,
        current_bounds: tuple[int, int, int, int],
        current_visible: bool,
        current_text: Optional[str] = None
    ) -> dict[str, Any]:
        """Detect changes from previous snapshot."""
        previous = self._previous_state.get(element_id)
        
        changes = {}
        
        if previous is None:
            changes["new_element"] = True
        else:
            if previous.bounds != current_bounds:
                changes["bounds_changed"] = {
                    "from": previous.bounds,
                    "to": current_bounds
                }
            
            if previous.is_visible != current_visible:
                changes["visibility_changed"] = {
                    "from": previous.is_visible,
                    "to": current_visible
                }
            
            if previous.text != current_text:
                changes["text_changed"] = {
                    "from": previous.text,
                    "to": current_text
                }
        
        new_snapshot = ElementSnapshot(
            element_id=element_id,
            timestamp=time.time(),
            bounds=current_bounds,
            is_visible=current_visible,
            text=current_text
        )
        self._previous_state[element_id] = new_snapshot
        
        return changes
