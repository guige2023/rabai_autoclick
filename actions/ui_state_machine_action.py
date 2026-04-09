"""UI state machine action for automation workflows.

Implements a state machine for managing UI automation states:
- State transitions with guards
- Entry/exit actions
- History tracking
- Async state support
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


class StateMachineError(Exception):
    """State machine error."""
    pass


@dataclass
class State:
    """A state in the state machine."""
    name: str
    on_enter: Callable | None = None
    on_exit: Callable | None = None
    on_update: Callable | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class Transition:
    """A state transition."""
    from_state: str
    to_state: str
    event: str
    guard: Callable | None = None
    action: Callable | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class StateSnapshot:
    """Snapshot of state machine state."""
    current_state: str
    previous_state: str
    event: str
    timestamp: float
    metadata: dict


class UIStateMachine:
    """State machine for UI automation.

    Features:
    - Hierarchical states
    - Guarded transitions
    - Entry/exit actions
    - State history
    - Async support

    Example:
        sm = UIStateMachine()
        sm.add_state(State("idle"))
        sm.add_state(State("loading", on_enter=self.start_loading))
        sm.add_state(State("ready", on_exit=self.clear_loading))
        sm.add_transition(Transition("idle", "loading", "start"))
        sm.add_transition(Transition("loading", "ready", "complete"))
        sm.send("start")
    """

    def __init__(
        self,
        initial_state: str = "initial",
        final_states: set[str] | None = None,
    ):
        self.states: dict[str, State] = {}
        self.transitions: dict[str, list[Transition]] = {}  # event -> transitions
        self.initial_state = initial_state
        self.final_states = final_states or set()
        self.current_state_name = initial_state
        self.previous_state_name: str | None = None
        self.history: list[StateSnapshot] = []
        self._event_queue: asyncio.Queue[str] = asyncio.Queue()
        self._running = False
        self._async_task: asyncio.Task | None = None

    def add_state(
        self,
        state: State,
        parent: str | None = None,
    ) -> None:
        """Add a state to the machine.

        Args:
            state: State to add
            parent: Parent state for hierarchical SM (optional)
        """
        if state.name in self.states:
            raise StateMachineError(f"State already exists: {state.name}")
        self.states[state.name] = state

    def add_transition(
        self,
        transition: Transition,
    ) -> None:
        """Add a transition.

        Args:
            transition: Transition to add
        """
        if transition.from_state not in self.states:
            raise StateMachineError(f"Unknown from_state: {transition.from_state}")
        if transition.to_state not in self.states:
            raise StateMachineError(f"Unknown to_state: {transition.to_state}")

        if transition.event not in self.transitions:
            self.transitions[transition.event] = []
        self.transitions[transition.event].append(transition)

    def send(self, event: str, payload: Any = None) -> bool:
        """Send an event to the state machine (sync).

        Args:
            event: Event name
            payload: Optional event payload

        Returns:
            True if transition was taken
        """
        if event not in self.transitions:
            return False

        # Find valid transition
        for trans in self.transitions[event]:
            if trans.from_state != self.current_state_name:
                continue

            # Check guard
            if trans.guard is not None and not trans.guard(payload):
                continue

            # Execute transition
            self._execute_transition(trans, event, payload)
            return True

        return False

    async def send_async(self, event: str, payload: Any = None) -> bool:
        """Send an event asynchronously.

        Args:
            event: Event name
            payload: Optional event payload

        Returns:
            True if transition was taken
        """
        return self.send(event, payload)

    def _execute_transition(
        self,
        transition: Transition,
        event: str,
        payload: Any,
    ) -> None:
        """Execute a state transition."""
        from_state = self.states[self.current_state_name]

        # Call exit action
        if from_state.on_exit is not None:
            try:
                from_state.on_exit(payload)
            except Exception as e:
                raise StateMachineError(f"on_exit failed: {e}") from e

        # Call transition action
        if transition.action is not None:
            try:
                transition.action(payload)
            except Exception as e:
                raise StateMachineError(f"Transition action failed: {e}") from e

        # Update state
        self.previous_state_name = self.current_state_name
        self.current_state_name = transition.to_state

        # Record history
        self.history.append(StateSnapshot(
            current_state=self.current_state_name,
            previous_state=self.previous_state_name,
            event=event,
            timestamp=time.time(),
            metadata=transition.metadata,
        ))

        # Keep history bounded
        if len(self.history) > 100:
            self.history = self.history[-50:]

        # Call enter action
        to_state = self.states[self.current_state_name]
        if to_state.on_enter is not None:
            try:
                to_state.on_enter(payload)
            except Exception as e:
                raise StateMachineError(f"on_enter failed: {e}") from e

    @property
    def current_state(self) -> State | None:
        """Get current state object."""
        return self.states.get(self.current_state_name)

    @property
    def is_in_final_state(self) -> bool:
        """Check if in a final state."""
        return self.current_state_name in self.final_states

    def can_handle(self, event: str) -> bool:
        """Check if event can be handled in current state."""
        if event not in self.transitions:
            return False
        for trans in self.transitions[event]:
            if trans.from_state == self.current_state_name:
                if trans.guard is None or trans.guard(None):
                    return True
        return False

    def get_available_events(self) -> list[str]:
        """Get events available in current state."""
        if self.current_state_name not in self.current_state.metadata.get("subsub_states", {}):
            # For flat state machine
            events = []
            for event, trans_list in self.transitions.items():
                for trans in trans_list:
                    if trans.from_state == self.current_state_name:
                        if trans.guard is None or trans.guard(None):
                            events.append(event)
            return events
        return []

    def start_event_loop(self) -> None:
        """Start async event processing loop."""
        if self._running:
            return
        self._running = True
        self._async_task = asyncio.create_task(self._event_loop())

    def stop_event_loop(self) -> None:
        """Stop async event processing loop."""
        self._running = False
        if self._async_task:
            self._async_task.cancel()
            self._async_task = None

    async def _event_loop(self) -> None:
        """Async event processing loop."""
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=0.1,
                )
                self.send(event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                raise StateMachineError(f"Event loop error: {e}") from e

    def queue_event(self, event: str) -> None:
        """Queue an event for async processing."""
        self._event_queue.put_nowait(event)

    def reset(self) -> None:
        """Reset state machine to initial state."""
        self.current_state_name = self.initial_state
        self.previous_state_name = None
        self.history.clear()

    def get_history(self, limit: int = 10) -> list[StateSnapshot]:
        """Get recent state history."""
        return self.history[-limit:]

    def __repr__(self) -> str:
        return f"UIStateMachine(current={self.current_state_name}, states={len(self.states)})"


def create_ui_state_machine(
    states: list[str],
    transitions: list[tuple[str, str, str]],  # from, to, event
    initial: str = "initial",
) -> UIStateMachine:
    """Create a state machine from simple spec.

    Args:
        states: List of state names
        transitions: List of (from, to, event) tuples
        initial: Initial state name

    Returns:
        Configured state machine
    """
    sm = UIStateMachine(initial_state=initial)

    for name in states:
        sm.add_state(State(name))

    for from_state, to_state, event in transitions:
        sm.add_transition(Transition(from_state, to_state, event))

    return sm
