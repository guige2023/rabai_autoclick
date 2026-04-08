"""
State machine utilities with transitions.

Provides state machine builder, transition guards,
and event-driven state transitions.
"""

from __future__ import annotations

import threading
from typing import Callable, Generic, TypeVar
from dataclasses import dataclass, field


T = TypeVar("T")


@dataclass
class Transition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    event: str | None = None
    guard: Callable[[], bool] | None = None
    action: Callable[[], None] | None = None


@dataclass
class StateMachineConfig:
    """Configuration for a state machine."""
    initial_state: str
    states: set[str] = field(default_factory=set)
    transitions: list[Transition] = field(default_factory=list)
    on_enter: dict[str, Callable[[], None]] = field(default_factory=dict)
    on_exit: dict[str, Callable[[], None]] = field(default_factory=dict)


class StateMachine(Generic[T]):
    """
    Generic state machine with transitions and guards.
    """

    def __init__(self, config: StateMachineConfig):
        self.config = config
        self._current_state: str = config.initial_state
        self._lock = threading.Lock()
        self._history: list[str] = [config.initial_state]

    @property
    def current_state(self) -> str:
        with self._lock:
            return self._current_state

    def can_transition(self, event: str) -> bool:
        """Check if transition is possible for event."""
        with self._lock:
            for t in self.config.transitions:
                if t.from_state == self._current_state and t.event == event:
                    if t.guard is None or t.guard():
                        return True
        return False

    def transition(self, event: str) -> bool:
        """
        Attempt to transition on event.

        Returns:
            True if transition occurred
        """
        with self._lock:
            for t in self.config.transitions:
                if t.from_state == self._current_state and t.event == event:
                    if t.guard is not None and not t.guard():
                        return False
                    self._do_transition(t)
                    return True
        return False

    def _do_transition(self, transition: Transition) -> None:
        if self._current_state in self.config.on_exit:
            self.config.on_exit[self._current_state]()

        if transition.action:
            transition.action()

        self._current_state = transition.to_state
        self._history.append(transition.to_state)

        if transition.to_state in self.config.on_enter:
            self.config.on_enter[transition.to_state]()

    def force_state(self, state: str) -> None:
        """Force state machine to specific state."""
        with self._lock:
            if state not in self.config.states:
                raise ValueError(f"Unknown state: {state}")
            self._current_state = state
            self._history.append(state)

    def get_history(self) -> list[str]:
        """Get state transition history."""
        with self._lock:
            return list(self._history)

    def reset(self) -> None:
        """Reset to initial state."""
        with self._lock:
            self._current_state = self.config.initial_state
            self._history = [self.config.initial_state]


class StateMachineBuilder:
    """Builder for StateMachine."""

    def __init__(self, initial_state: str):
        self._initial = initial_state
        self._states: set[str] = {initial_state}
        self._transitions: list[Transition] = []
        self._on_enter: dict[str, Callable[[], None]] = {}
        self._on_exit: dict[str, Callable[[], None]] = {}

    def add_state(self, state: str) -> "StateMachineBuilder":
        self._states.add(state)
        return self

    def add_states(self, *states: str) -> "StateMachineBuilder":
        for s in states:
            self._states.add(s)
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str | None = None,
        guard: Callable[[], bool] | None = None,
        action: Callable[[], None] | None = None,
    ) -> "StateMachineBuilder":
        self._states.add(from_state)
        self._states.add(to_state)
        self._transitions.append(Transition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
        ))
        return self

    def on_enter(self, state: str, callback: Callable[[], None]) -> "StateMachineBuilder":
        self._on_enter[state] = callback
        return self

    def on_exit(self, state: str, callback: Callable[[], None]) -> "StateMachineBuilder":
        self._on_exit[state] = callback
        return self

    def build(self) -> StateMachine:
        config = StateMachineConfig(
            initial_state=self._initial,
            states=self._states,
            transitions=self._transitions,
            on_enter=self._on_enter,
            on_exit=self._on_exit,
        )
        return StateMachine(config)


class HierarchicalStateMachine(StateMachine):
    """State machine with hierarchical states."""

    def __init__(self, config: StateMachineConfig):
        super().__init__(config)
        self._substates: dict[str, StateMachine | None] = {}
        self._active_substate: dict[str, str] = {}

    def enter_substate(self, parent_state: str, child_state: str) -> None:
        """Transition into a substate."""
        with self._lock:
            self._active_substate[parent_state] = child_state

    def get_active_substate(self, parent_state: str) -> str | None:
        return self._active_substate.get(parent_state)
