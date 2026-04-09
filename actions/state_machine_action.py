"""State machine for managing workflow states.

This module provides state machine functionality:
- State definitions and transitions
- Guard conditions
- Entry/exit actions
- State history and debugging

Example:
    >>> from actions.state_machine_action import StateMachine, State, Transition
    >>> sm = StateMachine("order")
    >>> sm.add_state("pending", entry=notify_customer)
    >>> sm.add_transition("pending", "processing", "process")
    >>> sm.trigger("process")
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class State:
    """A state machine state."""
    name: str
    entry_action: Optional[Callable[[], Any]] = None
    exit_action: Optional[Callable[[], Any]] = None
    is_initial: bool = False
    is_final: bool = False


@dataclass
class Transition:
    """A state transition."""
    from_state: str
    to_state: str
    trigger: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], Any]] = None


@dataclass
class StateHistoryEntry:
    """An entry in state history."""
    from_state: Optional[str]
    to_state: str
    trigger: str
    timestamp: float
    duration_in_previous_state: float


class StateMachine:
    """A state machine for managing workflow states.

    Attributes:
        name: State machine name.
    """

    def __init__(self, name: str = "state-machine") -> None:
        self.name = name
        self._states: dict[str, State] = {}
        self._transitions: dict[str, list[Transition]] = defaultdict(list)
        self._current_state: Optional[str] = None
        self._initial_state: Optional[str] = None
        self._history: list[StateHistoryEntry] = []
        self._state_entry_time: Optional[float] = None
        self._lock = threading.RLock()
        logger.info(f"StateMachine '{name}' initialized")

    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable[[], Any]] = None,
        exit_action: Optional[Callable[[], Any]] = None,
        is_initial: bool = False,
        is_final: bool = False,
    ) -> StateMachine:
        """Add a state to the state machine.

        Args:
            name: State name.
            entry_action: Action to execute on state entry.
            exit_action: Action to execute on state exit.
            is_initial: Whether this is the initial state.
            is_final: Whether this is a final state.

        Returns:
            Self for chaining.
        """
        state = State(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            is_initial=is_initial,
            is_final=is_final,
        )
        self._states[name] = state
        if is_initial:
            self._initial_state = name
        logger.debug(f"Added state: {name}")
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        trigger: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], Any]] = None,
    ) -> StateMachine:
        """Add a transition to the state machine.

        Args:
            from_state: Source state.
            to_state: Target state.
            trigger: Trigger event name.
            guard: Optional guard condition.
            action: Optional action to execute on transition.

        Returns:
            Self for chaining.
        """
        if from_state not in self._states:
            raise ValueError(f"Unknown state: {from_state}")
        if to_state not in self._states:
            raise ValueError(f"Unknown state: {to_state}")
        transition = Transition(
            from_state=from_state,
            to_state=to_state,
            trigger=trigger,
            guard=guard,
            action=action,
        )
        self._transitions[trigger].append(transition)
        logger.debug(f"Added transition: {from_state} -> {to_state} on {trigger}")
        return self

    def initialize(self) -> None:
        """Initialize the state machine to the initial state."""
        with self._lock:
            if not self._initial_state:
                raise ValueError("No initial state defined")
            self._current_state = self._initial_state
            self._state_entry_time = time.time()
            state = self._states[self._current_state]
            if state.entry_action:
                state.entry_action()
            self._history.append(StateHistoryEntry(
                from_state=None,
                to_state=self._current_state,
                trigger="__init__",
                timestamp=time.time(),
                duration_in_previous_state=0.0,
            ))

    def trigger(self, trigger: str) -> bool:
        """Trigger a transition.

        Args:
            trigger: The trigger event name.

        Returns:
            True if transition was successful.
        """
        with self._lock:
            if not self._current_state:
                self.initialize()
            transitions = self._transitions.get(trigger, [])
            for trans in transitions:
                if trans.from_state == self._current_state:
                    if trans.guard and not trans.guard():
                        logger.debug(f"Transition guard failed: {trigger}")
                        continue
                    self._execute_transition(trans)
                    return True
            logger.warning(f"No valid transition for trigger '{trigger}' from state '{self._current_state}'")
            return False

    def _execute_transition(self, transition: Transition) -> None:
        """Execute a state transition."""
        previous_state = self._current_state
        previous_entry_time = self._state_entry_time
        exit_action = self._states[previous_state].exit_action
        if exit_action:
            exit_action()
        if transition.action:
            transition.action()
        self._current_state = transition.to_state
        self._state_entry_time = time.time()
        entry_action = self._states[self._current_state].entry_action
        if entry_action:
            entry_action()
        duration = self._state_entry_time - (previous_entry_time or 0)
        self._history.append(StateHistoryEntry(
            from_state=previous_state,
            to_state=self._current_state,
            trigger=transition.trigger,
            timestamp=time.time(),
            duration_in_previous_state=duration,
        ))
        logger.info(f"State transition: {previous_state} -> {self._current_state}")

    def get_current_state(self) -> Optional[str]:
        """Get the current state name."""
        return self._current_state

    def get_history(self, limit: int = 100) -> list[StateHistoryEntry]:
        """Get state history.

        Args:
            limit: Maximum number of entries.

        Returns:
            List of history entries.
        """
        return self._history[-limit:]

    def is_in_state(self, state: str) -> bool:
        """Check if currently in a state.

        Args:
            state: State name to check.

        Returns:
            True if in the specified state.
        """
        return self._current_state == state

    def can_transition(self, trigger: str) -> bool:
        """Check if a trigger can cause a transition.

        Args:
            trigger: Trigger event name.

        Returns:
            True if transition is possible.
        """
        with self._lock:
            transitions = self._transitions.get(trigger, [])
            for trans in transitions:
                if trans.from_state == self._current_state:
                    if not trans.guard or trans.guard():
                        return True
            return False

    def reset(self) -> None:
        """Reset the state machine."""
        with self._lock:
            self._current_state = None
            self._state_entry_time = None
            self._history.clear()


class StateMachineBuilder:
    """Builder for creating state machines."""

    def __init__(self, name: str) -> None:
        self._machine = StateMachine(name=name)

    def with_initial_state(
        self,
        name: str,
        entry_action: Optional[Callable[[], Any]] = None,
    ) -> StateMachineBuilder:
        """Set the initial state."""
        self._machine.add_state(name, is_initial=True, entry_action=entry_action)
        return self

    def with_state(
        self,
        name: str,
        entry_action: Optional[Callable[[], Any]] = None,
        exit_action: Optional[Callable[[], Any]] = None,
        is_final: bool = False,
    ) -> StateMachineBuilder:
        """Add a state."""
        self._machine.add_state(name, entry_action, exit_action, is_final=is_final)
        return self

    def with_transition(
        self,
        from_state: str,
        to_state: str,
        trigger: str,
        guard: Optional[Callable[[], bool]] = None,
    ) -> StateMachineBuilder:
        """Add a transition."""
        self._machine.add_transition(from_state, to_state, trigger, guard)
        return self

    def build(self) -> StateMachine:
        """Build the state machine."""
        return self._machine
