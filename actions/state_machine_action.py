"""State Machine Action Module.

Generic state machine implementation for workflow automation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Generic, TypeVar
import uuid

T = TypeVar("T")


class StateMachineError(Exception):
    """State machine error."""
    pass


class InvalidTransitionError(StateMachineError):
    """Raised when an invalid transition is attempted."""
    pass


@dataclass
class StateTransition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    event: str
    guard: Callable[[], bool] | None = None
    action: Callable | None = None


@dataclass
class StateMachineConfig:
    """Configuration for state machine."""
    initial_state: str
    states: set[str]
    transitions: list[StateTransition]
    final_states: set[str] | None = None
    auto_transitions: bool = True


class StateMachine:
    """Generic state machine."""

    def __init__(self, config: StateMachineConfig) -> None:
        self.config = config
        self._current_state = config.initial_state
        self._history: list[str] = [config.initial_state]
        self._transition_callbacks: dict[str, Callable] = {}
        self._entry_callbacks: dict[str, Callable] = {}
        self._exit_callbacks: dict[str, Callable] = {}

    @property
    def current_state(self) -> str:
        return self._current_state

    @property
    def is_final(self) -> bool:
        if self.config.final_states:
            return self._current_state in self.config.final_states
        return False

    def on_transition(self, from_state: str, to_state: str, callback: Callable) -> None:
        """Register callback for transition."""
        key = f"{from_state}->{to_state}"
        self._transition_callbacks[key] = callback

    def on_entry(self, state: str, callback: Callable) -> None:
        """Register callback for entering a state."""
        self._entry_callbacks[state] = callback

    def on_exit(self, state: str, callback: Callable) -> None:
        """Register callback for exiting a state."""
        self._exit_callbacks[state] = callback

    def can_transition(self, event: str) -> bool:
        """Check if event can trigger a transition."""
        for t in self.config.transitions:
            if t.from_state == self._current_state and t.event == event:
                if t.guard is None or t.guard():
                    return True
        return False

    async def send(self, event: str, **context) -> bool:
        """Send an event to the state machine."""
        for t in self.config.transitions:
            if t.from_state == self._current_state and t.event == event:
                if t.guard and not t.guard():
                    continue
                old_state = self._current_state
                exit_cb = self._exit_callbacks.get(old_state)
                if exit_cb:
                    result = exit_cb()
                    if asyncio.iscoroutine(result):
                        await result
                self._current_state = t.to_state
                self._history.append(t.to_state)
                transition_key = f"{old_state}->{t.to_state}"
                if transition_key in self._transition_callbacks:
                    result = self._transition_callbacks[transition_key](**context)
                    if asyncio.iscoroutine(result):
                        await result
                if t.action:
                    result = t.action(**context)
                    if asyncio.iscoroutine(result):
                        await result
                entry_cb = self._entry_callbacks.get(t.to_state)
                if entry_cb:
                    result = entry_cb()
                    if asyncio.iscoroutine(result):
                        await result
                return True
        raise InvalidTransitionError(
            f"No valid transition for event '{event}' from state '{self._current_state}'"
        )

    def get_history(self) -> list[str]:
        """Get state transition history."""
        return list(self._history)


class HierarchicalStateMachine:
    """Hierarchical state machine with nested states."""

    def __init__(self, name: str, initial_state: str) -> None:
        self.name = name
        self._current_state = initial_state
        self._parent_states: dict[str, str] = {}
        self._sub_machines: dict[str, StateMachine] = {}

    def add_sub_machine(self, state: str, machine: StateMachine, parent_state: str | None = None) -> None:
        """Add a sub state machine."""
        self._sub_machines[state] = machine
        if parent_state:
            self._parent_states[state] = parent_state

    async def send(self, event: str, **context) -> bool:
        """Send event to current or parent state machine."""
        if self._current_state in self._sub_machines:
            return await self._sub_machines[self._current_state].send(event, **context)
        return False

    @property
    def current_state(self) -> str:
        return self._current_state
