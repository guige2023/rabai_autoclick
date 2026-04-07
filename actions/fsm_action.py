"""fsm_action module for rabai_autoclick.

Provides finite state machine implementation: states, transitions,
guards, actions, and hierarchical FSM support.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

__all__ = [
    "State",
    "Transition",
    "FSM",
    "StateMachine",
    "HierarchicalFSM",
    "FSMError",
    "InvalidTransitionError",
    "Guard",
    "Action",
    "create_fsm",
]


class FSMError(Exception):
    """Base FSM exception."""
    pass


class InvalidTransitionError(FSMError):
    """Raised when transition is not allowed."""
    pass


@dataclass
class State:
    """Represents a state in the FSM."""
    name: str
    on_enter: Optional[Callable] = None
    on_exit: Optional[Callable] = None
    is_initial: bool = False
    is_final: bool = False


@dataclass
class Transition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable] = None


@dataclass
class Guard:
    """Condition that must be true for transition to occur."""
    name: str
    condition: Callable[[], bool]


@dataclass
class Action:
    """Action to execute during transition."""
    name: str
    execute: Callable


class FSM:
    """Finite State Machine implementation."""

    def __init__(
        self,
        states: Optional[List[State]] = None,
        transitions: Optional[List[Transition]] = None,
        initial_state: Optional[str] = None,
    ) -> None:
        self._states: Dict[str, State] = {}
        self._transitions: Dict[str, List[Transition]] = defaultdict(list)
        self._current_state: Optional[str] = None
        self._lock = threading.RLock()
        self._history: List[str] = []

        if states:
            for state in states:
                self.add_state(state)

        if transitions:
            for transition in transitions:
                self.add_transition(transition)

        if initial_state:
            self.set_state(initial_state)

    def add_state(self, state: State) -> None:
        """Add a state to the FSM."""
        with self._lock:
            self._states[state.name] = state
            if state.is_initial and self._current_state is None:
                self._current_state = state.name

    def add_transition(self, transition: Transition) -> None:
        """Add a transition to the FSM."""
        with self._lock:
            self._transitions[transition.event].append(transition)

    def set_state(self, state_name: str) -> None:
        """Set current state (bypasses transition)."""
        with self._lock:
            if state_name not in self._states:
                raise FSMError(f"Unknown state: {state_name}")
            self._current_state = state_name
            self._history.append(state_name)

    def get_state(self) -> Optional[str]:
        """Get current state name."""
        with self._lock:
            return self._current_state

    def trigger(self, event: str, **context: Any) -> bool:
        """Trigger an event and attempt transition.

        Args:
            event: Event name.
            **context: Context passed to guards and actions.

        Returns:
            True if transition occurred.

        Raises:
            InvalidTransitionError: If no valid transition exists.
        """
        with self._lock:
            if self._current_state is None:
                raise FSMError("FSM not initialized")

            transitions = self._transitions.get(event, [])
            for transition in transitions:
                if transition.from_state != self._current_state:
                    continue
                if transition.guard and not transition.guard():
                    continue
                self._do_transition(transition, context)
                return True

            raise InvalidTransitionError(
                f"No valid transition for event '{event}' from state '{self._current_state}'"
            )

    def try_trigger(self, event: str, **context: Any) -> bool:
        """Try to trigger event, return False if not valid.

        Args:
            event: Event name.
            **context: Context passed to guards and actions.

        Returns:
            True if transition occurred, False otherwise.
        """
        try:
            return self.trigger(event, **context)
        except InvalidTransitionError:
            return False

    def _do_transition(self, transition: Transition, context: Dict) -> None:
        """Execute a transition."""
        from_state = self._current_state

        if self._states[from_state].on_exit:
            self._states[from_state].on_exit()

        if transition.action:
            transition.action()

        self._current_state = transition.to_state
        self._history.append(transition.to_state)

        if self._states[transition.to_state].on_enter:
            self._states[transition.to_state].on_enter()

    def can_trigger(self, event: str) -> bool:
        """Check if event can be triggered from current state."""
        with self._lock:
            if self._current_state is None:
                return False
            transitions = self._transitions.get(event, [])
            for t in transitions:
                if t.from_state == self._current_state:
                    if t.guard is None or t.guard():
                        return True
            return False

    def get_available_events(self) -> List[str]:
        """Get list of events that can be triggered from current state."""
        with self._lock:
            if self._current_state is None:
                return []
            events = []
            for event, transitions in self._transitions.items():
                for t in transitions:
                    if t.from_state == self._current_state:
                        if t.guard is None or t.guard():
                            events.append(event)
                        break
            return events

    def is_final(self) -> bool:
        """Check if current state is a final state."""
        with self._lock:
            if self._current_state is None:
                return False
            return self._states[self._current_state].is_final

    def reset(self) -> None:
        """Reset FSM to initial state."""
        with self._lock:
            initial = None
            for name, state in self._states.items():
                if state.is_initial:
                    initial = name
                    break
            if initial:
                self._current_state = initial
                self._history = [initial]

    def get_history(self) -> List[str]:
        """Get state transition history."""
        with self._lock:
            return list(self._history)


class StateMachine(FSM):
    """Alias for FSM for compatibility."""
    pass


class HierarchicalFSM:
    """Hierarchical/nested state machine."""

    def __init__(self, name: str = "root") -> None:
        self.name = name
        self._states: Dict[str, Any] = {}
        self._current_state: Optional[str] = None
        self._parent: Optional["HierarchicalFSM"] = None
        self._sub_machines: Dict[str, "HierarchicalFSM"] = {}
        self._lock = threading.RLock()

    def add_state(self, state_name: str, parent: Optional[str] = None) -> None:
        """Add a state, optionally as child of parent state."""
        with self._lock:
            self._states[state_name] = {"name": state_name, "parent": parent, "children": {}}
            if parent and parent in self._states:
                self._states[parent]["children"][state_name] = True
            if self._current_state is None:
                self._current_state = state_name

    def set_state(self, state_name: str) -> None:
        """Set current state."""
        with self._lock:
            if state_name not in self._states:
                raise FSMError(f"Unknown state: {state_name}")
            self._current_state = state_name

    def get_state(self) -> Optional[str]:
        """Get current state."""
        with self._lock:
            return self._current_state

    def is_in_state(self, state_name: str) -> bool:
        """Check if FSM is in specified state (including ancestors)."""
        with self._lock:
            current = self._current_state
            while current:
                if current == state_name:
                    return True
                parent = self._states.get(current, {}).get("parent")
                current = parent
            return False


def create_fsm(
    states: List[str],
    transitions: List[tuple],
    initial: str,
) -> FSM:
    """Factory to create FSM from simple spec.

    Args:
        states: List of state names.
        transitions: List of (from_state, to_state, event) tuples.
        initial: Initial state name.

    Returns:
        Configured FSM instance.
    """
    state_objects = [State(name=s, is_initial=(s == initial)) for s in states]
    fsm = FSM(states=state_objects, initial_state=initial)
    for from_state, to_state, event in transitions:
        fsm.add_transition(Transition(from_state=from_state, to_state=to_state, event=event))
    return fsm
