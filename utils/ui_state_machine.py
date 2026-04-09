"""
UI State Machine Module.

Provides utilities for implementing state machine patterns for UI automation,
including state transitions, guards, actions, and history tracking.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


logger = logging.getLogger(__name__)


@dataclass
class State:
    """Represents a state in the state machine."""
    name: str
    entry_action: Callable[[], None] | None = None
    exit_action: Callable[[], None] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """Represents a transition between states."""
    from_state: str
    to_state: str
    event: str
    guard: Callable[[], bool] | None = None
    action: Callable[[], None] | None = None


class StateMachineError(Exception):
    """Exception raised for state machine errors."""
    pass


class InvalidTransitionError(StateMachineError):
    """Raised when an invalid transition is attempted."""
    pass


class GuardFailedError(StateMachineError):
    """Raised when a transition guard fails."""
    pass


class UIStateMachine:
    """
    A state machine for managing UI states and transitions.

    Example:
        >>> sm = UIStateMachine("Idle")
        >>> sm.add_state(State("Loading"))
        >>> sm.add_transition(Transition("Idle", "Loading", "start"))
        >>> sm.send_event("start")
    """

    def __init__(self, initial_state: str) -> None:
        """
        Initialize the state machine.

        Args:
            initial_state: Name of the initial state.
        """
        self._states: dict[str, State] = {}
        self._transitions: dict[str, list[Transition]] = {}
        self._current_state: str = initial_state
        self._history: list[str] = []
        self._max_history: int = 100
        self._listeners: list[Callable[[str, str, str], None]] = []

    def add_state(self, state: State) -> None:
        """
        Add a state to the machine.

        Args:
            state: State to add.
        """
        self._states[state.name] = state
        logger.debug(f"Added state: {state.name}")

    def add_transition(self, transition: Transition) -> None:
        """
        Add a transition to the machine.

        Args:
            transition: Transition to add.
        """
        if transition.from_state not in self._states:
            raise StateMachineError(
                f"Unknown from_state: {transition.from_state}"
            )

        if transition.to_state not in self._states:
            raise StateMachineError(
                f"Unknown to_state: {transition.to_state}"
            )

        if transition.from_state not in self._transitions:
            self._transitions[transition.from_state] = []

        self._transitions[transition.from_state].append(transition)
        logger.debug(
            f"Added transition: {transition.from_state} -> {transition.to_state}"
        )

    def send_event(self, event: str) -> bool:
        """
        Send an event to trigger a transition.

        Args:
            event: Event name.

        Returns:
            True if transition occurred.

        Raises:
            InvalidTransitionError: If no valid transition exists.
        """
        current_state = self._states[self._current_state]
        current_state.exit_action and current_state.exit_action()

        transitions = self._transitions.get(self._current_state, [])

        for transition in transitions:
            if transition.event == event:
                if transition.guard and not transition.guard():
                    logger.debug(
                        f"Guard failed for transition {self._current_state} -> "
                        f"{transition.to_state} on event {event}"
                    )
                    raise GuardFailedError(
                        f"Guard failed for {event} transition"
                    )

                old_state = self._current_state
                self._current_state = transition.to_state

                self._add_history(old_state)

                if transition.action:
                    transition.action()

                new_state = self._states[self._current_state]
                new_state.entry_action and new_state.entry_action()

                self._notify_listeners(old_state, self._current_state, event)

                logger.info(
                    f"Transition: {old_state} -> {self._current_state} [{event}]"
                )
                return True

        logger.warning(f"No transition for event '{event}' in state '{self._current_state}'")
        raise InvalidTransitionError(
            f"No transition from '{self._current_state}' on event '{event}'"
        )

    def try_send_event(self, event: str) -> bool:
        """
        Try to send an event, returning False if invalid.

        Args:
            event: Event name.

        Returns:
            True if transition occurred, False otherwise.
        """
        try:
            self.send_event(event)
            return True
        except (InvalidTransitionError, GuardFailedError):
            return False

    def _add_history(self, old_state: str) -> None:
        """Add state to history."""
        self._history.append(f"{old_state} -> {self._current_state}")
        if len(self._history) > self._max_history:
            self._history.pop(0)

    def get_current_state(self) -> str:
        """
        Get the current state name.

        Returns:
            Current state name.
        """
        return self._current_state

    def get_history(self) -> list[str]:
        """
        Get state transition history.

        Returns:
            List of transition descriptions.
        """
        return list(self._history)

    def add_listener(
        self,
        listener: Callable[[str, str, str], None]
    ) -> None:
        """
        Add a state change listener.

        Args:
            listener: Callback(old_state, new_state, event).
        """
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(
        self,
        listener: Callable[[str, str, str], None]
    ) -> None:
        """
        Remove a state change listener.

        Args:
            listener: Callback to remove.
        """
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _notify_listeners(
        self,
        old_state: str,
        new_state: str,
        event: str
    ) -> None:
        """Notify all listeners of state change."""
        for listener in self._listeners:
            try:
                listener(old_state, new_state, event)
            except Exception as e:
                logger.error(f"State listener error: {e}")


class HierarchicalStateMachine(UIStateMachine):
    """
    A hierarchical state machine supporting nested states.

    Example:
        >>> sm = HierarchicalStateMachine("Root")
        >>> sm.add_submachine("Parent", child_sm)
    """

    def __init__(self, initial_state: str) -> None:
        """Initialize the hierarchical state machine."""
        super().__init__(initial_state)
        self._submachines: dict[str, UIStateMachine] = {}
        self._parent_states: dict[str, str] = {}

    def add_submachine(
        self,
        parent_state: str,
        submachine: UIStateMachine
    ) -> None:
        """
        Add a submachine to a state.

        Args:
            parent_state: Parent state name.
            submachine: Submachine to add.
        """
        self._submachines[parent_state] = submachine

        for state_name in submachine._states:
            self._parent_states[state_name] = parent_state

        logger.debug(
            f"Added submachine to state '{parent_state}'"
        )

    def send_event(self, event: str) -> bool:
        """Send event handling hierarchical submachines."""
        try:
            return super().send_event(event)
        except InvalidTransitionError:
            current_sm = self._get_current_submachine()
            if current_sm and current_sm != self:
                return current_sm.try_send_event(event)

            raise


class StateMachineBuilder:
    """
    Builder for creating state machines fluently.

    Example:
        >>> sm = (StateMachineBuilder("Idle")
        ...     .with_state("Loading")
        ...     .with_transition("Idle", "Loading", "start")
        ...     .build())
    """

    def __init__(self, initial_state: str) -> None:
        """Initialize the builder."""
        self._initial_state = initial_state
        self._states: list[State] = []
        self._transitions: list[Transition] = []

    def with_state(
        self,
        name: str,
        entry_action: Callable[[], None] | None = None,
        exit_action: Callable[[], None] | None = None
    ) -> StateMachineBuilder:
        """Add a state."""
        self._states.append(State(name, entry_action, exit_action))
        return self

    def with_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Callable[[], bool] | None = None,
        action: Callable[[], None] | None = None
    ) -> StateMachineBuilder:
        """Add a transition."""
        self._transitions.append(
            Transition(from_state, to_state, event, guard, action)
        )
        return self

    def build(self) -> UIStateMachine:
        """Build the state machine."""
        if not self._states:
            raise StateMachineError("No states defined")

        initial = self._initial_state
        if not any(s.name == initial for s in self._states):
            raise StateMachineError(f"Initial state '{initial}' not defined")

        sm = UIStateMachine(initial)

        for state in self._states:
            sm.add_state(state)

        for transition in self._transitions:
            sm.add_transition(transition)

        return sm


@dataclass
class TransitionGuard:
    """A guard condition for state transitions."""
    name: str
    condition: Callable[[], bool]
    description: str = ""


class GuardedStateMachine(UIStateMachine):
    """
    A state machine with named transition guards.

    Example:
        >>> sm = GuardedStateMachine("Idle")
        >>> sm.add_guard(TransitionGuard("can_load", lambda: True))
        >>> sm.add_transition_with_guard("Idle", "Loading", "start", "can_load")
    """

    def __init__(self, initial_state: str) -> None:
        """Initialize the guarded state machine."""
        super().__init__(initial_state)
        self._guards: dict[str, TransitionGuard] = {}

    def add_guard(self, guard: TransitionGuard) -> None:
        """
        Add a named guard.

        Args:
            guard: TransitionGuard to add.
        """
        self._guards[guard.name] = guard

    def add_transition_with_guard(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard_name: str,
        action: Callable[[], None] | None = None
    ) -> None:
        """
        Add a transition with a named guard.

        Args:
            from_state: Source state.
            to_state: Target state.
            event: Event name.
            guard_name: Name of guard to use.
            action: Optional action.
        """
        guard = self._guards.get(guard_name)

        def wrapped_guard() -> bool:
            if not guard:
                return False
            result = guard.condition()
            logger.debug(
                f"Guard '{guard_name}': {result}"
            )
            return result

        self.add_transition(
            Transition(from_state, to_state, event, wrapped_guard, action)
        )


@dataclass
class StateMetrics:
    """Metrics for state machine operations."""
    state_name: str
    entry_count: int = 0
    exit_count: int = 0
    total_time_ms: float = 0.0


class StateMachineWithMetrics(UIStateMachine):
    """
    A state machine that collects transition and timing metrics.

    Example:
        >>> sm = StateMachineWithMetrics("Idle")
        >>> stats = sm.get_metrics()
    """

    def __init__(self, initial_state: str) -> None:
        """Initialize the state machine with metrics."""
        super().__init__(initial_state)
        self._state_metrics: dict[str, StateMetrics] = {}
        self._state_entry_time: dict[str, float] = {}

    def send_event(self, event: str) -> bool:
        """Send event and update metrics."""
        old_state = self._current_state

        if old_state in self._state_entry_time:
            entry_time = self._state_entry_time[old_state]
            elapsed = (time.time() - entry_time) * 1000

            if old_state not in self._state_metrics:
                self._state_metrics[old_state] = StateMetrics(old_state)

            self._state_metrics[old_state].total_time_ms += elapsed
            self._state_metrics[old_state].exit_count += 1

        result = super().send_event(event)

        new_state = self._current_state
        self._state_entry_time[new_state] = time.time()

        if new_state not in self._state_metrics:
            self._state_metrics[new_state] = StateMetrics(new_state)

        self._state_metrics[new_state].entry_count += 1

        return result

    def get_metrics(self) -> dict[str, StateMetrics]:
        """
        Get metrics for all states.

        Returns:
            Dictionary of StateMetrics by state name.
        """
        return dict(self._state_metrics)

    def get_state_summary(self, state_name: str) -> dict[str, Any]:
        """
        Get summary for a specific state.

        Args:
            state_name: State to summarize.

        Returns:
            Summary dictionary.
        """
        metrics = self._state_metrics.get(state_name, StateMetrics(state_name))

        avg_time = (
            metrics.total_time_ms / metrics.exit_count
            if metrics.exit_count > 0
            else 0.0
        )

        return {
            "state": state_name,
            "entry_count": metrics.entry_count,
            "exit_count": metrics.exit_count,
            "total_time_ms": metrics.total_time_ms,
            "avg_time_ms": avg_time,
            "current": state_name == self._current_state
        }


import time
