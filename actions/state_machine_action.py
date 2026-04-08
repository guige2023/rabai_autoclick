"""
State machine module for modeling and executing finite state machines.

Supports states, transitions, guards, actions, and hierarchical state machines.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class TransitionType(Enum):
    """Type of state transition."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    LOCAL = "local"


@dataclass
class State:
    """A state in the state machine."""
    id: str
    name: str
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    do_action: Optional[Callable] = None
    is_initial: bool = False
    is_final: bool = False
    metadata: dict = field(default_factory=dict)


@dataclass
class Transition:
    """A state transition."""
    id: str
    source_state: str
    target_state: str
    event: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL
    conditions: list[str] = field(default_factory=list)


@dataclass
class StateMachineContext:
    """Context for state machine execution."""
    machine_id: str
    current_state: str
    history: list[dict] = field(default_factory=list)
    variables: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)


@dataclass
class TransitionEvent:
    """An event that triggered a transition."""
    event_name: str
    payload: Any
    timestamp: float = field(default_factory=time.time)


class StateMachine:
    """
    Finite state machine implementation.

    Supports states, transitions, guards, actions,
    and hierarchical state machines.
    """

    def __init__(self, machine_id: str, initial_state: Optional[str] = None):
        self.machine_id = machine_id
        self._states: dict[str, State] = {}
        self._transitions: list[Transition] = []
        self._initial_state = initial_state
        self._context: Optional[StateMachineContext] = None

    def add_state(
        self,
        state_id: str,
        name: str,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        do_action: Optional[Callable] = None,
        is_initial: bool = False,
        is_final: bool = False,
    ) -> State:
        """Add a state to the state machine."""
        state = State(
            id=state_id,
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            do_action=do_action,
            is_initial=is_initial,
            is_final=is_final,
        )

        self._states[state_id] = state

        if is_initial:
            self._initial_state = state_id

        return state

    def add_transition(
        self,
        source_state: str,
        target_state: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None,
        transition_type: TransitionType = TransitionType.EXTERNAL,
    ) -> Transition:
        """Add a transition to the state machine."""
        transition = Transition(
            id=str(uuid.uuid4())[:8],
            source_state=source_state,
            target_state=target_state,
            event=event,
            guard=guard,
            action=action,
            transition_type=transition_type,
        )

        self._transitions.append(transition)
        return transition

    def initialize(self, variables: Optional[dict] = None) -> StateMachineContext:
        """Initialize the state machine context."""
        if not self._initial_state:
            raise ValueError("No initial state defined")

        self._context = StateMachineContext(
            machine_id=self.machine_id,
            current_state=self._initial_state,
            variables=variables or {},
        )

        initial = self._states.get(self._initial_state)
        if initial and initial.entry_action:
            initial.entry_action(self._context)

        return self._context

    def send_event(
        self,
        event_name: str,
        payload: Optional[Any] = None,
    ) -> bool:
        """Send an event to the state machine."""
        if not self._context:
            raise ValueError("State machine not initialized")

        current_state_id = self._context.current_state
        current_state = self._states.get(current_state_id)

        matching_transitions = [
            t for t in self._transitions
            if t.source_state == current_state_id and t.event == event_name
        ]

        if not matching_transitions:
            return False

        for transition in matching_transitions:
            if transition.guard:
                guard_result = transition.guard(self._context, payload)
                if not guard_result:
                    continue

            if current_state and current_state.exit_action:
                current_state.exit_action(self._context)

            if transition.action:
                transition.action(self._context, payload)

            self._context.current_state = transition.target_state
            self._context.last_updated = time.time()

            self._context.history.append({
                "event": event_name,
                "from_state": transition.source_state,
                "to_state": transition.target_state,
                "timestamp": time.time(),
            })

            target_state = self._states.get(transition.target_state)
            if target_state:
                if target_state.is_final:
                    return True

                if target_state.entry_action:
                    target_state.entry_action(self._context)

            return True

        return False

    def get_current_state(self) -> Optional[str]:
        """Get the current state ID."""
        return self._context.current_state if self._context else None

    def get_state_info(self, state_id: Optional[str] = None) -> Optional[dict]:
        """Get information about a state."""
        state = self._states.get(state_id or self._context.current_state)
        if not state:
            return None

        outgoing = [
            {"event": t.event, "target": t.target_state}
            for t in self._transitions if t.source_state == state.id
        ]

        incoming = [
            {"source": t.source_state, "event": t.event}
            for t in self._transitions if t.target_state == state.id
        ]

        return {
            "id": state.id,
            "name": state.name,
            "is_initial": state.is_initial,
            "is_final": state.is_final,
            "outgoing_transitions": outgoing,
            "incoming_transitions": incoming,
        }

    def get_available_events(self) -> list[str]:
        """Get events that can be triggered from current state."""
        if not self._context:
            return []

        current_state_id = self._context.current_state

        return [
            t.event for t in self._transitions
            if t.source_state == current_state_id
        ]

    def is_in_state(self, state_id: str) -> bool:
        """Check if the state machine is in a specific state."""
        return self._context.current_state == state_id if self._context else False

    def is_final_state(self) -> bool:
        """Check if the current state is a final state."""
        if not self._context:
            return False

        state = self._states.get(self._context.current_state)
        return state.is_final if state else False

    def get_history(self, limit: int = 100) -> list[dict]:
        """Get transition history."""
        return self._context.history[-limit:] if self._context else []

    def get_context(self) -> Optional[StateMachineContext]:
        """Get the current context."""
        return self._context

    def set_variable(self, key: str, value: Any) -> None:
        """Set a context variable."""
        if self._context:
            self._context.variables[key] = value

    def get_variable(self, key: str) -> Optional[Any]:
        """Get a context variable."""
        return self._context.variables.get(key) if self._context else None

    def list_states(self) -> list[dict]:
        """List all states."""
        return [
            {
                "id": s.id,
                "name": s.name,
                "is_initial": s.is_initial,
                "is_final": s.is_final,
            }
            for s in self._states.values()
        ]

    def list_transitions(self) -> list[dict]:
        """List all transitions."""
        return [
            {
                "id": t.id,
                "source": t.source_state,
                "target": t.target_state,
                "event": t.event,
                "type": t.transition_type.value,
            }
            for t in self._transitions
        ]
