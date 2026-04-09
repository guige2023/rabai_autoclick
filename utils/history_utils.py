"""
History tracking and state management for UI automation.

Provides utilities for tracking action history, state snapshots,
and undo/redo functionality.
"""

from __future__ import annotations

import time
import json
import threading
from typing import (
    List, Optional, Any, Callable, Dict, 
    Generic, TypeVar, Iterator, Union
)
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import deque
from copy import deepcopy


T = TypeVar('T')


class ActionType(Enum):
    """Types of tracked actions."""
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()
    NAVIGATE = auto()
    CLICK = auto()
    INPUT = auto()
    CUSTOM = auto()


@dataclass
class HistoryEntry:
    """Single entry in history."""
    id: str
    action_type: ActionType
    timestamp: float
    description: str
    state_before: Optional[Dict[str, Any]]
    state_after: Optional[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    user_data: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['action_type'] = self.action_type.name
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HistoryEntry':
        """Create from dictionary."""
        data = data.copy()
        data['action_type'] = ActionType[data['action_type']]
        return cls(**data)


class StateSnapshot:
    """Snapshot of application state at a point in time."""
    
    def __init__(
        self,
        state: Dict[str, Any],
        timestamp: Optional[float] = None,
        label: Optional[str] = None,
    ) -> None:
        """Initialize snapshot.
        
        Args:
            state: State dictionary
            timestamp: Snapshot time
            label: Optional label
        """
        self.state = deepcopy(state)
        self.timestamp = timestamp or time.time()
        self.label = label
        self._hash = hash(json.dumps(state, sort_keys=True))
    
    def __repr__(self) -> str:
        return f"StateSnapshot(timestamp={self.timestamp:.2f}, label={self.label!r})"
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StateSnapshot):
            return False
        return self._hash == other._hash


class HistoryManager(Generic[T]):
    """Manages history with undo/redo capability."""
    
    def __init__(
        self,
        max_size: int = 100,
        auto_save: bool = True,
        serializer: Optional[Callable[[T], Dict]] = None,
    ) -> None:
        """Initialize history manager.
        
        Args:
            max_size: Maximum history entries
            auto_save: Automatically save state snapshots
            serializer: Custom state serializer
        """
        self.max_size = max_size
        self.auto_save = auto_save
        self.serializer = serializer or self._default_serializer
        
        self._undo_stack: deque[HistoryEntry] = deque(maxlen=max_size)
        self._redo_stack: deque[HistoryEntry] = deque(maxlen=max_size)
        self._snapshots: deque[StateSnapshot] = deque(maxlen=max_size)
        self._lock = threading.RLock()
        self._current_state: Optional[T] = None
        self._change_listeners: List[Callable[[HistoryEntry], None]] = []
    
    def _default_serializer(self, state: T) -> Dict[str, Any]:
        """Default state serialization."""
        if hasattr(state, '__dict__'):
            return {'__class__': type(state).__name__, **vars(state)}
        return {'value': state}
    
    def push(
        self,
        action_type: ActionType,
        description: str,
        state_before: Optional[T] = None,
        state_after: Optional[T] = None,
        metadata: Optional[Dict[str, Any]] = None,
        user_data: Optional[Dict[str, Any]] = None,
    ) -> HistoryEntry:
        """Push new history entry.
        
        Args:
            action_type: Type of action
            description: Human-readable description
            state_before: State before action
            state_after: State after action
            metadata: Additional metadata
            user_data: Custom user data
        
        Returns:
            Created HistoryEntry
        """
        with self._lock:
            entry = HistoryEntry(
                id=self._generate_id(),
                action_type=action_type,
                timestamp=time.time(),
                description=description,
                state_before=self._serialize(state_before) if state_before else None,
                state_after=self._serialize(state_after) if state_after else None,
                metadata=metadata or {},
                user_data=user_data,
            )
            
            self._undo_stack.append(entry)
            self._redo_stack.clear()
            
            if self.auto_save and state_after is not None:
                snapshot = StateSnapshot(
                    self._serialize(state_after),
                    label=description,
                )
                self._snapshots.append(snapshot)
            
            self._notify_listeners(entry)
            
            return entry
    
    def _generate_id(self) -> str:
        """Generate unique entry ID."""
        return f"{time.time():.6f}_{len(self._undo_stack)}"
    
    def _serialize(self, state: T) -> Dict[str, Any]:
        """Serialize state."""
        return self.serializer(state)
    
    def undo(self) -> Optional[HistoryEntry]:
        """Undo last action.
        
        Returns:
            Undo entry or None
        """
        with self._lock:
            if not self._undo_stack:
                return None
            
            entry = self._undo_stack.pop()
            self._redo_stack.append(entry)
            
            return entry
    
    def redo(self) -> Optional[HistoryEntry]:
        """Redo last undone action.
        
        Returns:
            Redo entry or None
        """
        with self._lock:
            if not self._redo_stack:
                return None
            
            entry = self._redo_stack.pop()
            self._undo_stack.append(entry)
            
            return entry
    
    def can_undo(self) -> bool:
        """Check if undo is available."""
        return len(self._undo_stack) > 0
    
    def can_redo(self) -> bool:
        """Check if redo is available."""
        return len(self._redo_stack) > 0
    
    def clear(self) -> None:
        """Clear all history."""
        with self._lock:
            self._undo_stack.clear()
            self._redo_stack.clear()
            self._snapshots.clear()
    
    def get_history(
        self,
        limit: Optional[int] = None,
        action_type: Optional[ActionType] = None,
    ) -> List[HistoryEntry]:
        """Get history entries.
        
        Args:
            limit: Maximum entries to return
            action_type: Filter by action type
        
        Returns:
            List of history entries
        """
        with self._lock:
            entries = list(self._undo_stack)
            
            if action_type is not None:
                entries = [e for e in entries if e.action_type == action_type]
            
            if limit is not None:
                entries = entries[-limit:]
            
            return entries
    
    def get_snapshots(
        self,
        limit: Optional[int] = None,
    ) -> List[StateSnapshot]:
        """Get state snapshots.
        
        Args:
            limit: Maximum snapshots to return
        
        Returns:
            List of snapshots
        """
        with self._lock:
            snapshots = list(self._snapshots)
            if limit is not None:
                snapshots = snapshots[-limit:]
            return snapshots
    
    def on_change(self, callback: Callable[[HistoryEntry], None]) -> None:
        """Register change listener.
        
        Args:
            callback: Function to call on history change
        """
        self._change_listeners.append(callback)
    
    def _notify_listeners(self, entry: HistoryEntry) -> None:
        """Notify registered listeners."""
        for listener in self._change_listeners:
            try:
                listener(entry)
            except Exception:
                pass
    
    def export_history(self, path: str) -> bool:
        """Export history to JSON file.
        
        Args:
            path: Output file path
        
        Returns:
            True if successful
        """
        try:
            with open(path, 'w') as f:
                history_data = {
                    'entries': [e.to_dict() for e in self._undo_stack],
                    'exported_at': time.time(),
                }
                json.dump(history_data, f, indent=2)
            return True
        except (IOError, OSError):
            return False
    
    def import_history(self, path: str) -> int:
        """Import history from JSON file.
        
        Args:
            path: Input file path
        
        Returns:
            Number of entries imported
        """
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            entries = [HistoryEntry.from_dict(e) for e in data.get('entries', [])]
            
            with self._lock:
                self._undo_stack.extend(entries)
            
            return len(entries)
        except (IOError, OSError, json.JSONDecodeError):
            return 0


class ActionRecorder:
    """Record and replay action sequences."""
    
    def __init__(
        self,
        max_recording_size: int = 1000,
    ) -> None:
        """Initialize action recorder.
        
        Args:
            max_recording_size: Maximum actions to record
        """
        self.max_recording_size = max_recording_size
        self._actions: deque[HistoryEntry] = deque(maxlen=max_recording_size)
        self._recording = False
        self._start_time: Optional[float] = None
        self._lock = threading.Lock()
    
    def start_recording(self) -> None:
        """Start recording actions."""
        with self._lock:
            self._recording = True
            self._start_time = time.time()
            self._actions.clear()
    
    def stop_recording(self) -> List[HistoryEntry]:
        """Stop recording and return actions.
        
        Returns:
            List of recorded actions
        """
        with self._lock:
            self._recording = False
            return list(self._actions)
    
    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._recording
    
    def record(
        self,
        action_type: ActionType,
        description: str,
        **kwargs: Any,
    ) -> None:
        """Record an action.
        
        Args:
            action_type: Type of action
            description: Action description
            **kwargs: Additional action data
        """
        if not self._recording:
            return
        
        with self._lock:
            entry = HistoryEntry(
                id=self._generate_id(),
                action_type=action_type,
                timestamp=time.time(),
                description=description,
                state_before=None,
                state_after=None,
                metadata=kwargs,
            )
            self._actions.append(entry)
    
    def _generate_id(self) -> str:
        """Generate unique action ID."""
        return f"action_{time.time():.6f}"
    
    def get_recorded_actions(self) -> List[HistoryEntry]:
        """Get all recorded actions.
        
        Returns:
            List of recorded actions
        """
        with self._lock:
            return list(self._actions)
    
    def get_recording_duration(self) -> float:
        """Get recording duration in seconds.
        
        Returns:
            Duration or 0 if not recording
        """
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time
    
    def clear_recording(self) -> None:
        """Clear current recording."""
        with self._lock:
            self._actions.clear()
            self._start_time = None


class UndoRedoStack(Generic[T]):
    """Simple undo/redo stack for single value type."""
    
    def __init__(self, initial: Optional[T] = None) -> None:
        """Initialize stack.
        
        Args:
            initial: Initial value
        """
        self._undo: List[T] = []
        self._redo: List[T] = []
        if initial is not None:
            self._undo.append(deepcopy(initial))
    
    def push(self, value: T) -> None:
        """Push new value, clearing redo.
        
        Args:
            value: New value
        """
        if self._undo and self._undo[-1] == value:
            return
        self._undo.append(deepcopy(value))
        self._redo.clear()
    
    def undo(self, current: T) -> Optional[T]:
        """Undo to previous value.
        
        Args:
            current: Current value
        
        Returns:
            Previous value or None
        """
        if not self._undo:
            return None
        
        self._redo.append(deepcopy(current))
        return deepcopy(self._undo[-1])
    
    def redo(self, current: T) -> Optional[T]:
        """Redo to next value.
        
        Args:
            current: Current value
        
        Returns:
            Next value or None
        """
        if not self._redo:
            return None
        
        self._undo.append(deepcopy(current))
        return deepcopy(self._redo.pop())
    
    def can_undo(self) -> bool:
        """Check if undo available."""
        return len(self._undo) > 0
    
    def can_redo(self) -> bool:
        """Check if redo available."""
        return len(self._redo) > 0
    
    def clear(self) -> None:
        """Clear stacks."""
        self._undo.clear()
        self._redo.clear()
