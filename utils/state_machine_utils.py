"""State machine utilities for managing state transitions and finite automata."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = [
    "StateMachine",
    "StateTransition",
    "InvalidTransitionError",
]


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""
    pass


@dataclass
class StateTransition:
    """A state transition definition."""
    from_state: str
    to_state: str
    event: str
    guard: Callable[[], bool] | None = None
    action: Callable[..., None] | None = None


class StateMachine:
    """Generic state machine with guards and actions."""

    def __init__(
        self,
        states: list[str],
        initial_state: str,
        transitions: list[StateTransition] | None = None,
    ) -> None:
        self.states = set(states)
        self.initial_state = initial_state
        self.current_state = initial_state
        self._transitions: dict[tuple[str, str], StateTransition] = {}
        self._event_map: dict[str, dict[str, StateTransition]] = {}
        self._history: list[str] = []

        if transitions:
            for t in transitions:
                self.add_transition(t)

    def add_transition(self, t: StateTransition) -> None:
        self._transitions[(t.from_state, t.to_state)] = t
        if t.from_state not in self._event_map:
            self._event_map[t.from_state] = {}
        self._event_map[t.from_state][t.event] = t

    def can_transition(self, event: str) -> bool:
        if self.current_state not in self._event_map:
            return False
        t = self._event_map[self.current_state].get(event)
        if t is None:
            return False
        if t.guard is not None and not t.guard():
            return False
        return True

    def trigger(self, event: str, *args: Any, **kwargs: Any) -> None:
        if self.current_state not in self._event_map:
            raise InvalidTransitionError(f"No transitions from state {self.current_state} for event {event}")

        t = self._event_map[self.current_state].get(event)
        if t is None:
            raise InvalidTransitionError(f"No transition for event {event} from state {self.current_state}")

        if t.guard is not None and not t.guard():
            raise InvalidTransitionError(f"Guard failed for transition {t.from_state} -> {t.to_state}")

        if t.action:
            t.action(*args, **kwargs)

        self._history.append(self.current_state)
        self.current_state = t.to_state

    def reset(self) -> None:
        self.current_state = self.initial_state
        self._history.clear()

    def get_history(self) -> list[str]:
        return list(self._history)

    def is_in(self, *states: str) -> bool:
        return self.current_state in states
