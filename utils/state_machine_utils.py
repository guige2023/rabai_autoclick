"""State machine builder for RabAI AutoClick.

Provides:
- State machine definition
- Transition management
- Event handling
- State validation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class StateMachineEvent(Enum):
    """Base event type for state machines."""

    ENTER = auto()
    EXIT = auto()
    TRANSITION = auto()
    ERROR = auto()


@dataclass
class State:
    """A state in the state machine.

    Attributes:
        name: State identifier
        data: Optional state data
        is_initial: Whether this is an initial state
        is_final: Whether this is a final state
        on_enter: Optional callback on state entry
        on_exit: Optional callback on state exit
    """

    name: str
    data: Dict[str, Any] = field(default_factory=dict)
    is_initial: bool = False
    is_final: bool = False
    on_enter: Optional[Callable[["StateMachine", "State"], None]] = None
    on_exit: Optional[Callable[["StateMachine", "State"], None]] = None


@dataclass
class Transition:
    """A transition between states.

    Attributes:
        source: Source state name
        target: Target state name
        event: Event that triggers the transition
        condition: Optional condition function
        action: Optional action to perform during transition
    """

    source: str
    target: str
    event: str
    condition: Optional[Callable[["StateMachine", str, Any], bool]] = None
    action: Optional[Callable[["StateMachine", str, Any], None]] = None


class StateMachine:
    """A state machine for managing workflow states.

    Attributes:
        name: State machine name
        states: Dictionary of states
        transitions: List of transitions
        current_state: Current state name
        history: List of previous states
    """

    def __init__(self, name: str) -> None:
        """Initialize the state machine.

        Args:
            name: State machine name
        """
        self.name = name
        self.states: Dict[str, State] = {}
        self.transitions: List[Transition] = []
        self.current_state: Optional[str] = None
        self.history: List[str] = []
        self._event_handlers: Dict[str, List[Callable[[Any], None]]] = {}
        self._is_running = False

    def add_state(
        self,
        name: str,
        data: Optional[Dict[str, Any]] = None,
        is_initial: bool = False,
        is_final: bool = False,
        on_enter: Optional[Callable[["StateMachine", State], None]] = None,
        on_exit: Optional[Callable[["StateMachine", State], None]] = None,
    ) -> "StateMachine":
        """Add a state to the state machine.

        Args:
            name: State name
            data: Optional state data
            is_initial: Whether this is an initial state
            is_final: Whether this is a final state
            on_enter: Optional enter callback
            on_exit: Optional exit callback

        Returns:
            Self for chaining
        """
        state = State(
            name=name,
            data=data or {},
            is_initial=is_initial,
            is_final=is_final,
            on_enter=on_enter,
            on_exit=on_exit,
        )
        self.states[name] = state
        if is_initial and self.current_state is None:
            self.current_state = name
        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        condition: Optional[Callable[["StateMachine", str, Any], bool]] = None,
        action: Optional[Callable[["StateMachine", str, Any], None]] = None,
    ) -> "StateMachine":
        """Add a transition.

        Args:
            source: Source state name
            target: Target state name
            event: Event name that triggers transition
            condition: Optional condition function
            action: Optional action function

        Returns:
            Self for chaining
        """
        if source not in self.states:
            raise ValueError(f"Unknown source state: {source}")
        if target not in self.states:
            raise ValueError(f"Unknown target state: {target}")
        transition = Transition(
            source=source,
            target=target,
            event=event,
            condition=condition,
            action=action,
        )
        self.transitions.append(transition)
        return self

    def set_initial(self, name: str) -> "StateMachine":
        """Set the initial state.

        Args:
            name: State name

        Returns:
            Self for chaining
        """
        if name not in self.states:
            raise ValueError(f"Unknown state: {name}")
        self.current_state = name
        for state in self.states.values():
            state.is_initial = state.name == name
        return self

    def start(self) -> None:
        """Start the state machine."""
        if self.current_state is None:
            initial_states = [s for s in self.states.values() if s.is_initial]
            if not initial_states:
                raise ValueError("No initial state defined")
            self.current_state = initial_states[0].name
        self._is_running = True
        self._enter_state(self.current_state)

    def stop(self) -> None:
        """Stop the state machine."""
        self._is_running = False

    def send(self, event: str, data: Any = None) -> bool:
        """Send an event to the state machine.

        Args:
            event: Event name
            data: Optional event data

        Returns:
            True if transition was made, False otherwise
        """
        if not self._is_running or self.current_state is None:
            return False

        matching_transitions = [
            t for t in self.transitions
            if t.source == self.current_state and t.event == event
        ]

        for transition in matching_transitions:
            if transition.condition is None or transition.condition(self, event, data):
                if transition.action:
                    transition.action(self, event, data)
                self._transition_to(transition.target)
                return True

        return False

    def _enter_state(self, state_name: str) -> None:
        """Enter a state.

        Args:
            state_name: State to enter
        """
        state = self.states.get(state_name)
        if state and state.on_enter:
            state.on_enter(self, state)
        self._trigger_handlers(StateMachineEvent.ENTER, state_name)

    def _exit_state(self, state_name: str) -> None:
        """Exit a state.

        Args:
            state_name: State to exit
        """
        state = self.states.get(state_name)
        if state and state.on_exit:
            state.on_exit(self, state)
        self._trigger_handlers(StateMachineEvent.EXIT, state_name)

    def _transition_to(self, target: str) -> None:
        """Transition to a new state.

        Args:
            target: Target state name
        """
        if self.current_state:
            self._exit_state(self.current_state)
            self.history.append(self.current_state)
        self.current_state = target
        self._enter_state(target)
        self._trigger_handlers(StateMachineEvent.TRANSITION, target)

    def _trigger_handlers(self, event: StateMachineEvent, state_name: str) -> None:
        """Trigger event handlers.

        Args:
            event: Event type
            state_name: Current state name
        """
        handlers = self._event_handlers.get(event.name, [])
        for handler in handlers:
            handler(state_name)

    def on_event(self, event: StateMachineEvent, handler: Callable[[str], None]) -> "StateMachine":
        """Register an event handler.

        Args:
            event: Event type
            handler: Handler function

        Returns:
            Self for chaining
        """
        if event.name not in self._event_handlers:
            self._event_handlers[event.name] = []
        self._event_handlers[event.name].append(handler)
        return self

    def get_current_state(self) -> Optional[State]:
        """Get the current state.

        Returns:
            Current State or None
        """
        if self.current_state is None:
            return None
        return self.states.get(self.current_state)

    def is_in_state(self, state_name: str) -> bool:
        """Check if in a specific state.

        Args:
            state_name: State to check

        Returns:
            True if in the state
        """
        return self.current_state == state_name

    def is_final_state(self) -> bool:
        """Check if in a final state.

        Returns:
            True if in a final state
        """
        current = self.get_current_state()
        return current is not None and current.is_final

    def validate(self) -> Tuple[bool, List[str]]:
        """Validate the state machine.

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors: List[str] = []

        if not self.states:
            errors.append("No states defined")

        if self.current_state is None:
            errors.append("No initial state set")

        initial_count = sum(1 for s in self.states.values() if s.is_initial)
        if initial_count > 1:
            errors.append(f"Multiple initial states defined: {initial_count}")

        for transition in self.transitions:
            if transition.source not in self.states:
                errors.append(f"Transition references unknown source: {transition.source}")
            if transition.target not in self.states:
                errors.append(f"Transition references unknown target: {transition.target}")

        reachable = self._find_reachable_states()
        unreachable = set(self.states.keys()) - reachable
        if unreachable:
            errors.append(f"Unreachable states: {unreachable}")

        return len(errors) == 0, errors

    def _find_reachable_states(self) -> Set[str]:
        """Find all reachable states from initial state.

        Returns:
            Set of reachable state names
        """
        reachable: Set[str] = set()
        queue: List[str] = []

        if self.current_state:
            reachable.add(self.current_state)
            queue.append(self.current_state)
        else:
            initial_states = [s.name for s in self.states.values() if s.is_initial]
            for name in initial_states:
                reachable.add(name)
                queue.append(name)

        while queue:
            current = queue.pop(0)
            for transition in self.transitions:
                if transition.source == current and transition.target not in reachable:
                    reachable.add(transition.target)
                    queue.append(transition.target)

        return reachable

    def get_available_transitions(self) -> List[Transition]:
        """Get transitions available from current state.

        Returns:
            List of available transitions
        """
        if self.current_state is None:
            return []
        return [t for t in self.transitions if t.source == self.current_state]

    def reset(self) -> None:
        """Reset the state machine to initial state."""
        self.history.clear()
        initial_states = [s for s in self.states.values() if s.is_initial]
        if initial_states:
            self.current_state = initial_states[0].name
        else:
            self.current_state = None
