"""
Lifecycle Utilities

Provides utilities for managing lifecycle states
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum, auto


class LifecycleState(Enum):
    """Lifecycle states."""
    INITIAL = auto()
    STARTING = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    STOPPED = auto()
    ERROR = auto()


@dataclass
class LifecycleTransition:
    """Represents a lifecycle transition."""
    from_state: LifecycleState
    to_state: LifecycleState
    timestamp: float = 0.0


class LifecycleManager:
    """
    Manages lifecycle states for automation components.
    
    Tracks state transitions and invokes
    registered callbacks.
    """

    def __init__(self) -> None:
        self._state = LifecycleState.INITIAL
        self._transitions: list[LifecycleTransition] = []
        self._state_handlers: dict[LifecycleState, list[Callable[[], None]]] = {}
        self._transition_handlers: list[Callable[[LifecycleTransition], None]] = []

    def get_state(self) -> LifecycleState:
        """Get current lifecycle state."""
        return self._state

    def transition_to(self, new_state: LifecycleState) -> bool:
        """
        Transition to a new state.
        
        Args:
            new_state: Target state.
            
        Returns:
            True if transition succeeded.
        """
        import time
        if not self._can_transition(new_state):
            return False

        old_state = self._state
        self._state = new_state
        transition = LifecycleTransition(
            from_state=old_state,
            to_state=new_state,
            timestamp=time.time(),
        )
        self._transitions.append(transition)
        self._invoke_state_handlers(new_state)
        self._invoke_transition_handlers(transition)
        return True

    def _can_transition(self, new_state: LifecycleState) -> bool:
        """Check if transition is valid."""
        valid_transitions = {
            LifecycleState.INITIAL: [LifecycleState.STARTING],
            LifecycleState.STARTING: [LifecycleState.RUNNING, LifecycleState.ERROR],
            LifecycleState.RUNNING: [LifecycleState.PAUSED, LifecycleState.STOPPING],
            LifecycleState.PAUSED: [LifecycleState.RUNNING, LifecycleState.STOPPING],
            LifecycleState.STOPPING: [LifecycleState.STOPPED],
            LifecycleState.ERROR: [LifecycleState.STOPPING, LifecycleState.INITIAL],
            LifecycleState.STOPPED: [LifecycleState.INITIAL],
        }
        return new_state in valid_transitions.get(self._state, [])

    def on_state(
        self,
        state: LifecycleState,
        handler: Callable[[], None],
    ) -> None:
        """Register handler for state entry."""
        if state not in self._state_handlers:
            self._state_handlers[state] = []
        self._state_handlers[state].append(handler)

    def on_transition(
        self,
        handler: Callable[[LifecycleTransition], None],
    ) -> None:
        """Register handler for any transition."""
        self._transition_handlers.append(handler)

    def _invoke_state_handlers(self, state: LifecycleState) -> None:
        """Invoke handlers for a state."""
        if state in self._state_handlers:
            for handler in self._state_handlers[state]:
                handler()

    def _invoke_transition_handlers(
        self,
        transition: LifecycleTransition,
    ) -> None:
        """Invoke transition handlers."""
        for handler in self._transition_handlers:
            handler(transition)
