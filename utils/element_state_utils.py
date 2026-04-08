"""
Element State Utilities

Provides utilities for managing UI element
states in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from enum import Enum, auto


class ElementState(Enum):
    """Possible element states."""
    UNKNOWN = auto()
    EXISTS = auto()
    VISIBLE = auto()
    ENABLED = auto()
    DISABLED = auto()
    FOCUSED = auto()
    CHECKED = auto()
    EXPANDED = auto()
    COLLAPSED = auto()
    LOADING = auto()
    ANIMATING = auto()


@dataclass
class StateTransition:
    """Represents a state change."""
    from_state: ElementState
    to_state: ElementState
    timestamp: datetime
    element_id: str | None = None


class ElementStateManager:
    """
    Manages state transitions for UI elements.
    
    Tracks state changes and provides
    history and notification capabilities.
    """

    def __init__(self) -> None:
        self._element_states: dict[str, ElementState] = {}
        self._transitions: list[StateTransition] = []
        self._listeners: list[callable] = []

    def set_state(
        self,
        element_id: str,
        state: ElementState,
    ) -> StateTransition | None:
        """Set element state and record transition."""
        old_state = self._element_states.get(element_id, ElementState.UNKNOWN)
        if old_state != state:
            self._element_states[element_id] = state
            transition = StateTransition(
                from_state=old_state,
                to_state=state,
                timestamp=datetime.now(),
                element_id=element_id,
            )
            self._transitions.append(transition)
            self._notify_listeners(transition)
            return transition
        return None

    def get_state(self, element_id: str) -> ElementState:
        """Get current state of element."""
        return self._element_states.get(element_id, ElementState.UNKNOWN)

    def get_transition_history(
        self,
        element_id: str | None = None,
        limit: int | None = None,
    ) -> list[StateTransition]:
        """Get state transition history."""
        transitions = self._transitions
        if element_id:
            transitions = [t for t in transitions if t.element_id == element_id]
        if limit:
            transitions = transitions[-limit:]
        return transitions

    def add_listener(self, listener: callable) -> None:
        """Add a state change listener."""
        self._listeners.append(listener)

    def _notify_listeners(self, transition: StateTransition) -> None:
        """Notify all listeners of state change."""
        for listener in self._listeners:
            listener(transition)

    def clear_history(self) -> None:
        """Clear transition history."""
        self._transitions.clear()


@dataclass
class StateSnapshot:
    """Snapshot of element states at a point in time."""
    timestamp: datetime
    states: dict[str, ElementState] = field(default_factory=dict)
