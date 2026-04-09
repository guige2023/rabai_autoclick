"""UI state history utilities for UI automation.

Provides utilities for tracking and replaying UI state changes,
recording automation sequences, and maintaining state history.
"""

from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class StateSnapshot:
    """A snapshot of UI state at a point in time."""
    timestamp_ms: float
    state_type: str
    state_data: Dict[str, Any]
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateTransition:
    """Represents a transition between two states."""
    from_state: Optional[StateSnapshot]
    to_state: StateSnapshot
    action: str
    timestamp_ms: float
    duration_ms: float = 0.0


class StateHistory:
    """Maintains a history of UI state snapshots.
    
    Stores state snapshots and provides utilities for
    querying and analyzing state changes.
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        dedup_enabled: bool = True
    ) -> None:
        """Initialize the state history.
        
        Args:
            max_size: Maximum number of snapshots to store.
            dedup_enabled: Whether to deduplicate consecutive identical states.
        """
        self.max_size = max_size
        self.dedup_enabled = dedup_enabled
        self._history: Deque[StateSnapshot] = deque(maxlen=max_size)
        self._last_snapshot: Optional[StateSnapshot] = None
        self._change_count = 0
        self._listeners: List[Callable[[StateSnapshot], None]] = []
    
    def add_snapshot(
        self,
        state_type: str,
        state_data: Dict[str, Any],
        timestamp_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[StateSnapshot]:
        """Add a state snapshot to history.
        
        Args:
            state_type: Type of state (e.g., "element", "window").
            state_data: State data dictionary.
            timestamp_ms: Timestamp (uses current time if None).
            metadata: Additional metadata.
            
        Returns:
            Created snapshot or None if deduplicated.
        """
        timestamp = timestamp_ms if timestamp_ms is not None else time.time() * 1000
        
        snapshot = StateSnapshot(
            timestamp_ms=timestamp,
            state_type=state_type,
            state_data=state_data,
            metadata=metadata or {}
        )
        
        if self.dedup_enabled and self._last_snapshot:
            if self._is_duplicate(snapshot, self._last_snapshot):
                return None
        
        self._history.append(snapshot)
        self._last_snapshot = snapshot
        self._change_count += 1
        
        for listener in self._listeners:
            listener(snapshot)
        
        return snapshot
    
    def _is_duplicate(
        self,
        current: StateSnapshot,
        previous: StateSnapshot
    ) -> bool:
        """Check if current state is duplicate of previous.
        
        Args:
            current: Current snapshot.
            previous: Previous snapshot.
            
        Returns:
            True if duplicate.
        """
        if current.state_type != previous.state_type:
            return False
        
        return current.state_data == previous.state_data
    
    def get_snapshot_at(
        self,
        timestamp_ms: float
    ) -> Optional[StateSnapshot]:
        """Get the snapshot at or just before a timestamp.
        
        Args:
            timestamp_ms: Target timestamp.
            
        Returns:
            Snapshot or None.
        """
        result = None
        for snapshot in self._history:
            if snapshot.timestamp_ms <= timestamp_ms:
                result = snapshot
            else:
                break
        
        return result
    
    def get_snapshots_in_range(
        self,
        start_ms: float,
        end_ms: float
    ) -> List[StateSnapshot]:
        """Get all snapshots within a time range.
        
        Args:
            start_ms: Start timestamp.
            end_ms: End timestamp.
            
        Returns:
            List of snapshots.
        """
        return [
            s for s in self._history
            if start_ms <= s.timestamp_ms <= end_ms
        ]
    
    def get_snapshots_by_type(
        self,
        state_type: str
    ) -> List[StateSnapshot]:
        """Get all snapshots of a specific type.
        
        Args:
            state_type: State type to filter by.
            
        Returns:
            List of matching snapshots.
        """
        return [
            s for s in self._history
            if s.state_type == state_type
        ]
    
    def get_recent(self, count: int = 10) -> List[StateSnapshot]:
        """Get the most recent snapshots.
        
        Args:
            count: Number of snapshots to return.
            
        Returns:
            List of recent snapshots.
        """
        return list(self._history)[-count:]
    
    def get_change_count(self) -> int:
        """Get number of state changes recorded.
        
        Returns:
            Number of changes.
        """
        return self._change_count
    
    def get_total_duration_ms(self) -> float:
        """Get total duration of history.
        
        Returns:
            Duration in milliseconds.
        """
        if len(self._history) < 2:
            return 0.0
        
        first = self._history[0]
        last = self._history[-1]
        
        return last.timestamp_ms - first.timestamp_ms
    
    def add_listener(
        self,
        listener: Callable[[StateSnapshot], None]
    ) -> None:
        """Add a listener for new snapshots.
        
        Args:
            listener: Function to call on new snapshot.
        """
        self._listeners.append(listener)
    
    def clear(self) -> None:
        """Clear all history."""
        self._history.clear()
        self._last_snapshot = None
        self._change_count = 0
    
    def to_list(self) -> List[Dict[str, Any]]:
        """Export history as list of dictionaries.
        
        Returns:
            List of snapshot dictionaries.
        """
        return [
            {
                "timestamp_ms": s.timestamp_ms,
                "state_type": s.state_type,
                "state_data": s.state_data,
                "metadata": s.metadata
            }
            for s in self._history
        ]


class StateTransitionTracker:
    """Tracks transitions between UI states.
    
    Records state transitions and provides utilities
    for analyzing transition patterns.
    """
    
    def __init__(self, history: Optional[StateHistory] = None) -> None:
        """Initialize the transition tracker.
        
        Args:
            history: State history to track transitions for.
        """
        self.history = history or StateHistory()
        self._transitions: Deque[StateTransition] = deque(maxlen=1000)
        self._transition_count: Dict[str, int] = {}
    
    def record_transition(
        self,
        from_state: Optional[StateSnapshot],
        to_state: StateSnapshot,
        action: str
    ) -> StateTransition:
        """Record a state transition.
        
        Args:
            from_state: Previous state (None if first state).
            to_state: New state.
            action: Action that caused transition.
            
        Returns:
            Created transition.
        """
        duration = 0.0
        if from_state:
            duration = to_state.timestamp_ms - from_state.timestamp_ms
        
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            action=action,
            timestamp_ms=to_state.timestamp_ms,
            duration_ms=duration
        )
        
        self._transitions.append(transition)
        
        self._transition_count[action] = self._transition_count.get(action, 0) + 1
        
        return transition
    
    def get_transitions_in_range(
        self,
        start_ms: float,
        end_ms: float
    ) -> List[StateTransition]:
        """Get transitions within a time range.
        
        Args:
            start_ms: Start timestamp.
            end_ms: End timestamp.
            
        Returns:
            List of transitions.
        """
        return [
            t for t in self._transitions
            if start_ms <= t.timestamp_ms <= end_ms
        ]
    
    def get_transition_count(self, action: str) -> int:
        """Get number of transitions for an action.
        
        Args:
            action: Action to count.
            
        Returns:
            Number of transitions.
        """
        return self._transition_count.get(action, 0)
    
    def get_most_common_actions(self, limit: int = 10) -> List[Tuple[str, int]]:
        """Get most common transition actions.
        
        Args:
            limit: Maximum number of actions to return.
            
        Returns:
            List of (action, count) tuples.
        """
        sorted_actions = sorted(
            self._transition_count.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_actions[:limit]
    
    def get_average_duration(self, action: str) -> float:
        """Get average transition duration for an action.
        
        Args:
            action: Action to analyze.
            
        Returns:
            Average duration in milliseconds.
        """
        durations = [
            t.duration_ms for t in self._transitions
            if t.action == action
        ]
        
        if not durations:
            return 0.0
        
        return sum(durations) / len(durations)


class StateRecorder:
    """Records UI state for later replay.
    
    Provides utilities for recording automation sequences
    and state changes for playback.
    """
    
    def __init__(
        self,
        history: Optional[StateHistory] = None,
        transition_tracker: Optional[StateTransitionTracker] = None
    ) -> None:
        """Initialize the state recorder.
        
        Args:
            history: State history to use.
            transition_tracker: Transition tracker to use.
        """
        self.history = history or StateHistory()
        self.transition_tracker = transition_tracker or StateTransitionTracker(
            self.history
        )
        self._is_recording = False
        self._start_time_ms: Optional[float] = None
        self._recording: List[Dict[str, Any]] = []
    
    def start_recording(self) -> None:
        """Start recording state changes."""
        self._is_recording = True
        self._start_time_ms = time.time() * 1000
        self._recording = []
        self.history.clear()
    
    def stop_recording(self) -> List[Dict[str, Any]]:
        """Stop recording and return recorded data.
        
        Returns:
            List of recorded state changes.
        """
        self._is_recording = False
        return self._recording
    
    def record_state(
        self,
        state_type: str,
        state_data: Dict[str, Any],
        action: Optional[str] = None
    ) -> Optional[StateSnapshot]:
        """Record a state snapshot.
        
        Args:
            state_type: Type of state.
            state_data: State data.
            action: Action that caused this state.
            
        Returns:
            Created snapshot.
        """
        if not self._is_recording:
            return None
        
        timestamp_ms = time.time() * 1000
        relative_time = timestamp_ms - (
            self._start_time_ms if self._start_time_ms else timestamp_ms
        )
        
        snapshot = self.history.add_snapshot(
            state_type=state_type,
            state_data=state_data,
            timestamp_ms=timestamp_ms
        )
        
        if snapshot:
            self._recording.append({
                "relative_time_ms": relative_time,
                "state_type": state_type,
                "state_data": state_data,
                "action": action,
                "timestamp_ms": timestamp_ms
            })
        
        return snapshot
    
    def get_recording_duration_ms(self) -> float:
        """Get duration of current recording.
        
        Returns:
            Duration in milliseconds.
        """
        if not self._recording:
            return 0.0
        
        first = self._recording[0]
        last = self._recording[-1]
        
        return last.get("relative_time_ms", 0) - first.get("relative_time_ms", 0)
    
    def export_recording(self, filepath: str) -> None:
        """Export recording to JSON file.
        
        Args:
            filepath: Path to export to.
        """
        with open(filepath, 'w') as f:
            json.dump(self._recording, f, indent=2)
    
    def import_recording(self, filepath: str) -> List[Dict[str, Any]]:
        """Import recording from JSON file.
        
        Args:
            filepath: Path to import from.
            
        Returns:
            List of recorded state changes.
        """
        with open(filepath, 'r') as f:
            self._recording = json.load(f)
        return self._recording
    
    def is_recording(self) -> bool:
        """Check if currently recording.
        
        Returns:
            True if recording.
        """
        return self._is_recording


def calculate_state_checksum(state_data: Dict[str, Any]) -> str:
    """Calculate a checksum for state data.
    
    Args:
        state_data: State data dictionary.
        
    Returns:
        Checksum string.
    """
    data_str = json.dumps(state_data, sort_keys=True)
    import hashlib
    return hashlib.md5(data_str.encode()).hexdigest()
