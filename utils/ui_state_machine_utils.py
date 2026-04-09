"""UI State Machine Utilities.

Implements a state machine for UI workflow management.

Example:
    >>> from ui_state_machine_utils import UIStateMachine, State, Event
    >>> sm = UIStateMachine()
    >>> sm.add_state("idle")
    >>> sm.add_state("loading")
    >>> sm.add_transition("idle", "start", "loading")
    >>> sm.process_event("start")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set


@dataclass
class State:
    """A state machine state."""
    name: str
    on_enter: Optional[Callable[[], None]] = None
    on_exit: Optional[Callable[[], None]] = None
    is_initial: bool = False
    is_final: bool = False


@dataclass
class Transition:
    """A state transition."""
    from_state: str
    event: str
    to_state: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None


class UIStateMachine:
    """State machine for UI workflows."""

    def __init__(self, name: str = "ui_sm"):
        """Initialize state machine.

        Args:
            name: Machine name.
        """
        self.name = name
        self._states: Dict[str, State] = {}
        self._transitions: List[Transition] = []
        self._current_state: Optional[str] = None
        self._history: List[str] = []

    def add_state(
        self,
        name: str,
        initial: bool = False,
        final: bool = False,
        on_enter: Optional[Callable[[], None]] = None,
        on_exit: Optional[Callable[[], None]] = None,
    ) -> None:
        """Add a state to the machine.

        Args:
            name: State name.
            initial: Whether this is the initial state.
            final: Whether this is a final state.
            on_enter: Callback when entering state.
            on_exit: Callback when exiting state.
        """
        self._states[name] = State(
            name=name,
            on_enter=on_enter,
            on_exit=on_exit,
            is_initial=initial,
            is_final=final,
        )
        if initial and self._current_state is None:
            self._current_state = name

    def add_transition(
        self,
        from_state: str,
        event: str,
        to_state: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], None]] = None,
    ) -> None:
        """Add a transition.

        Args:
            from_state: Source state.
            event: Event name.
            to_state: Target state.
            guard: Optional guard condition.
            action: Optional action on transition.
        """
        self._transitions.append(Transition(
            from_state=from_state,
            event=event,
            to_state=to_state,
            guard=guard,
            action=action,
        ))

    def process_event(self, event: str) -> bool:
        """Process an event.

        Args:
            event: Event name.

        Returns:
            True if transition was made.
        """
        if self._current_state is None:
            return False

        for trans in self._transitions:
            if trans.from_state == self._current_state and trans.event == event:
                if trans.guard is not None and not trans.guard():
                    continue

                self._enter_state(self._current_state, entering=False)
                if trans.action:
                    trans.action()
                self._history.append(self._current_state)
                self._current_state = trans.to_state
                self._enter_state(self._current_state, entering=True)
                return True

        return False

    def get_current_state(self) -> Optional[str]:
        """Get current state name.

        Returns:
            Current state or None.
        """
        return self._current_state

    def is_in_state(self, name: str) -> bool:
        """Check if in given state.

        Args:
            name: State name.

        Returns:
            True if in that state.
        """
        return self._current_state == name

    def _enter_state(self, name: str, entering: bool) -> None:
        """Call enter/exit callbacks.

        Args:
            name: State name.
            entering: True if entering, False if exiting.
        """
        state = self._states.get(name)
        if not state:
            return
        cb = state.on_enter if entering else state.on_exit
        if cb:
            cb()

    def get_available_events(self) -> List[str]:
        """Get events available from current state.

        Returns:
            List of event names.
        """
        if self._current_state is None:
            return []
        return [
            t.event for t in self._transitions
            if t.from_state == self._current_state
        ]
