"""
State Pattern Implementation

Allows an object to alter its behavior when its internal state changes.
The object will appear to change its class.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class State(ABC, Generic[T]):
    """
    Abstract base class for states.

    Type Parameters:
        T: The type of context this state operates on.
    """

    @property
    def name(self) -> str:
        """Return the state name."""
        return self.__class__.__name__

    @abstractmethod
    def handle(self, context: T) -> None:
        """Handle the state behavior."""
        pass

    @abstractmethod
    def enter(self, context: T) -> None:
        """Called when entering this state."""
        pass

    @abstractmethod
    def exit(self, context: T) -> None:
        """Called when exiting this state."""
        pass

    def __repr__(self) -> str:
        return f"<State: {self.name}>"


class StateContext(ABC, Generic[T]):
    """
    Abstract context that delegates to states.
    """

    @abstractmethod
    def set_state(self, state: State[T]) -> None:
        """Change the current state."""
        pass

    @abstractmethod
    def get_state(self) -> State[T]:
        """Get the current state."""
        pass


@dataclass
class Transition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    trigger: str = ""
    condition: Callable[[], bool] | None = None
    action: Callable[[], None] | None = None


class StateMachine(Generic[T]):
    """
    Generic state machine implementation.

    Type Parameters:
        T: The context type.
    """

    def __init__(self, initial_state: State[T]):
        self._current_state: State[T] = initial_state
        self._states: dict[str, State[T]] = {initial_state.name: initial_state}
        self._transitions: dict[str, list[Transition]] = {}
        self._history: list[dict[str, Any]] = []
        self._transition_count: int = 0
        self._time_in_states: dict[str, float] = {}

    @property
    def current_state(self) -> State[T]:
        """Get the current state."""
        return self._current_state

    @property
    def state_name(self) -> str:
        """Get the current state name."""
        return self._current_state.name

    def add_state(self, state: State[T]) -> StateMachine[T]:
        """Add a state to the machine."""
        self._states[state.name] = state
        self._time_in_states[state.name] = 0.0
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        trigger: str = "",
        condition: Callable[[], bool] | None = None,
        action: Callable[[], None] | None = None,
    ) -> StateMachine[T]:
        """Add a transition between states."""
        transition = Transition(
            from_state=from_state,
            to_state=to_state,
            trigger=trigger,
            condition=condition,
            action=action,
        )

        if from_state not in self._transitions:
            self._transitions[from_state] = []
        self._transitions[from_state].append(transition)
        return self

    def transition_to(self, context: T, state_name: str) -> bool:
        """
        Transition to a new state.

        Args:
            context: The context object.
            state_name: Name of the target state.

        Returns:
            True if transition was successful.
        """
        if state_name not in self._states:
            return False

        old_state = self._current_state
        new_state = self._states[state_name]

        # Record time in old state
        if self._history:
            elapsed = time.time() - self._history[-1]["timestamp"]
            self._time_in_states[old_state.name] = (
                self._time_in_states.get(old_state.name, 0) + elapsed
            )

        old_state.exit(context)
        self._current_state = new_state
        new_state.enter(context)

        self._transition_count += 1
        self._history.append({
            "from": old_state.name,
            "to": new_state.name,
            "timestamp": time.time(),
        })

        return True

    def trigger(self, context: T, trigger: str) -> bool:
        """
        Trigger a transition by name.

        Args:
            context: The context object.
            trigger: The trigger name.

        Returns:
            True if a transition was triggered.
        """
        transitions = self._transitions.get(self._current_state.name, [])

        for trans in transitions:
            if trans.trigger == trigger:
                if trans.condition and not trans.condition():
                    continue

                if trans.action:
                    trans.action()

                return self.transition_to(context, trans.to_state)

        return False

    def handle(self, context: T) -> None:
        """Delegate to current state's handle method."""
        self._current_state.handle(context)

    def get_history(self) -> list[dict[str, Any]]:
        """Get state transition history."""
        return copy.copy(self._history)

    def get_time_in_state(self, state_name: str) -> float:
        """Get total time spent in a state."""
        return self._time_in_states.get(state_name, 0.0)

    @property
    def transition_count(self) -> int:
        """Get total number of transitions."""
        return self._transition_count


@dataclass
class StateMetrics:
    """Metrics for state machine operations."""
    total_transitions: int = 0
    transitions_by_state: dict[str, int] = field(default_factory=dict)
    time_by_state: dict[str, float] = field(default_factory=dict)


class MeasuredStateMachine(StateMachine[T]):
    """State machine with metrics collection."""

    def transition_to(self, context: T, state_name: str) -> bool:
        """Transition with metrics collection."""
        result = super().transition_to(context, state_name)

        if result:
            state_name_from = self._history[-2]["from"] if len(self._history) > 1 else state_name
            self._metrics.total_transitions += 1
            self._metrics.transitions_by_state[state_name_from] = (
                self._metrics.transitions_by_state.get(state_name_from, 0) + 1
            )

        return result

    @property
    def metrics(self) -> StateMetrics:
        """Get metrics."""
        return self._metrics

    def __init__(self, initial_state: State[T]):
        super().__init__(initial_state)
        self._metrics = StateMetrics()


class HierarchicalState(State[T]):
    """
    State that can contain child states.
    """

    def __init__(self):
        self._child_states: dict[str, State[T]] = {}
        self._initial_child: State[T] | None = None
        self._current_child: State[T] | None = None

    def add_child(self, state: State[T], initial: bool = False) -> HierarchicalState[T]:
        """Add a child state."""
        self._child_states[state.name] = state
        if initial or self._initial_child is None:
            self._initial_child = state
        return self

    def get_current_child(self) -> State[T] | None:
        """Get the current child state."""
        return self._current_child

    def set_current_child(self, state: State[T]) -> None:
        """Set the current child state."""
        self._current_child = state


def create_simple_state_machine(
    states: list[str],
    initial: str,
    transitions: list[tuple[str, str]],
) -> tuple[dict[str, State], Callable[[str], bool]]:
    """
    Create a simple state machine from names.

    Args:
        states: List of state names.
        initial: Initial state name.
        transitions: List of (from, to) transition tuples.

    Returns:
        Tuple of (states dict, transition function).
    """
    class SimpleState(State):
        def handle(self, context: Any) -> None:
            pass

        def enter(self, context: Any) -> None:
            pass

        def exit(self, context: Any) -> None:
            pass

    state_objects = {name: SimpleState() for name in states}
    sm = StateMachine(state_objects[initial])

    for from_state, to_state in transitions:
        if from_state in state_objects and to_state in state_objects:
            sm.add_state(state_objects[from_state])
            sm.add_state(state_objects[to_state])
            sm.add_transition(from_state, to_state)

    return state_objects, lambda trigger: False  # Placeholder
