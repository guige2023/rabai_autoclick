"""UI state machine utilities.

This module provides utilities for modeling and managing
UI states with transitions and actions.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, TypeVar
from dataclasses import dataclass, field

T = TypeVar("T")


@dataclass
class StateTransition:
    """A transition between two states."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None


class UIStateMachine:
    """A state machine for managing UI states."""

    def __init__(
        self,
        initial_state: str,
        states: Optional[List[str]] = None,
    ) -> None:
        self._initial_state = initial_state
        self._current_state = initial_state
        self._states: Set[str] = set(states) if states else {initial_state}
        self._transitions: Dict[str, List[StateTransition]] = {}
        self._on_state_change: Optional[Callable[[str, str], None]] = None

    @property
    def current_state(self) -> str:
        return self._current_state

    @property
    def initial_state(self) -> str:
        return self._initial_state

    def add_state(self, state: str) -> None:
        self._states.add(state)

    def add_transition(self, transition: StateTransition) -> None:
        if transition.from_state not in self._transitions:
            self._transitions[transition.from_state] = []
        self._transitions[transition.from_state].append(transition)

    def set_state_change_handler(
        self,
        handler: Callable[[str, str], None],
    ) -> None:
        self._on_state_change = handler

    def can_handle(self, event: str) -> bool:
        """Check if current state can handle an event."""
        transitions = self._transitions.get(self._current_state, [])
        return any(t.event == event for t in transitions)

    def handle(self, event: str) -> bool:
        """Handle an event and transition if possible.

        Args:
            event: Event name to handle.

        Returns:
            True if transition occurred.
        """
        transitions = self._transitions.get(self._current_state, [])
        for t in transitions:
            if t.event == event:
                if t.guard and not t.guard():
                    continue
                old_state = self._current_state
                self._current_state = t.to_state
                if t.action:
                    t.action()
                if self._on_state_change:
                    self._on_state_change(old_state, self._current_state)
                return True
        return False

    def reset(self) -> None:
        """Reset to initial state."""
        old_state = self._current_state
        self._current_state = self._initial_state
        if self._on_state_change:
            self._on_state_change(old_state, self._current_state)


@dataclass
class StateSnapshot:
    """Snapshot of state machine state."""
    current_state: str
    state_history: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


def create_linear_state_machine(
    states: List[str],
    events: List[str],
) -> UIStateMachine:
    """Create a linear state machine.

    Args:
        states: Ordered list of state names.
        events: List of event names (one per transition).

    Returns:
        Configured UIStateMachine.
    """
    if len(states) < 2 or len(events) < len(states) - 1:
        raise ValueError("Invalid states or events count")

    sm = UIStateMachine(initial_state=states[0], states=states)
    for i in range(len(states) - 1):
        sm.add_transition(StateTransition(states[i], states[i + 1], events[i]))
    return sm


__all__ = [
    "StateTransition",
    "UIStateMachine",
    "StateSnapshot",
    "create_linear_state_machine",
]
