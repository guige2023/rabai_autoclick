"""State machine utilities for RabAI AutoClick.

Provides:
- State machine implementation
- State transitions
"""

from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class State:
    """Represents a state in a state machine."""

    def __init__(
        self,
        name: str,
        on_enter: Optional[Callable] = None,
        on_exit: Optional[Callable] = None,
    ) -> None:
        """Initialize state.

        Args:
            name: State name.
            on_enter: Callback when entering state.
            on_exit: Callback when exiting state.
        """
        self.name = name
        self.on_enter = on_enter
        self.on_exit = on_exit


class Transition:
    """Represents a state transition."""

    def __init__(
        self,
        from_state: str,
        to_state: str,
        event: str,
        condition: Optional[Callable[[], bool]] = None,
        action: Optional[Callable] = None,
    ) -> None:
        """Initialize transition.

        Args:
            from_state: Source state.
            to_state: Target state.
            event: Event that triggers transition.
            condition: Optional guard condition.
            action: Optional action to execute.
        """
        self.from_state = from_state
        self.to_state = to_state
        self.event = event
        self.condition = condition
        self.action = action


class StateMachine:
    """Simple state machine implementation.

    Usage:
        sm = StateMachine("idle")
        sm.add_state("idle")
        sm.add_state("running")
        sm.add_transition("idle", "running", "start")
        sm.add_transition("running", "idle", "stop")

        sm.send_event("start")
        assert sm.current_state == "running"
    """

    def __init__(self, initial_state: str) -> None:
        """Initialize state machine.

        Args:
            initial_state: Name of initial state.
        """
        self._states: Dict[str, State] = {}
        self._transitions: Dict[str, List[Transition]] = {}
        self._current_state = initial_state
        self._initial_state = initial_state

    @property
    def current_state(self) -> str:
        """Get current state name."""
        return self._current_state

    def add_state(
        self,
        name: str,
        on_enter: Optional[Callable] = None,
        on_exit: Optional[Callable] = None,
    ) -> None:
        """Add a state.

        Args:
            name: State name.
            on_enter: Callback when entering state.
            on_exit: Callback when exiting state.
        """
        self._states[name] = State(name, on_enter, on_exit)

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        condition: Optional[Callable[[], bool]] = None,
        action: Optional[Callable] = None,
    ) -> None:
        """Add a transition.

        Args:
            from_state: Source state.
            to_state: Target state.
            event: Event that triggers transition.
            condition: Optional guard condition.
            action: Optional action to execute.
        """
        if event not in self._transitions:
            self._transitions[event] = []

        self._transitions[event].append(Transition(
            from_state, to_state, event, condition, action
        ))

    def send_event(self, event: str) -> bool:
        """Send an event to the state machine.

        Args:
            event: Event name.

        Returns:
            True if transition occurred.
        """
        if event not in self._transitions:
            return False

        for transition in self._transitions[event]:
            if transition.from_state != self._current_state:
                continue

            if transition.condition and not transition.condition():
                continue

            # Execute exit callback
            current = self._states.get(self._current_state)
            if current and current.on_exit:
                current.on_exit()

            # Execute action
            if transition.action:
                transition.action()

            # Change state
            self._current_state = transition.to_state

            # Execute enter callback
            new_state = self._states.get(self._current_state)
            if new_state and new_state.on_enter:
                new_state.on_enter()

            return True

        return False

    def reset(self) -> None:
        """Reset to initial state."""
        self._current_state = self._initial_state

    def get_available_events(self) -> List[str]:
        """Get events available from current state.

        Returns:
            List of event names.
        """
        events = []
        for event, transitions in self._transitions.items():
            for t in transitions:
                if t.from_state == self._current_state:
                    events.append(event)
                    break
        return events

    def is_in_state(self, state: str) -> bool:
        """Check if in given state.

        Args:
            state: State name.

        Returns:
            True if in state.
        """
        return self._current_state == state


class HierarchicalStateMachine(StateMachine):
    """Hierarchical state machine with nested states."""

    def __init__(self, initial_state: str) -> None:
        super().__init__(initial_state)
        self._parent_map: Dict[str, str] = {}
        self._history: Dict[str, str] = {}

    def add_substate(
        self,
        parent: str,
        child: str,
        initial: bool = False,
    ) -> None:
        """Add a substate.

        Args:
            parent: Parent state name.
            child: Child state name.
            initial: If True, child is initial substate.
        """
        self.add_state(child)
        self._parent_map[child] = parent

        if initial:
            self._history[parent] = child

    def send_event(self, event: str) -> bool:
        """Send event handling hierarchy."""
        # Try current state and ancestors
        state = self._current_state
        while state:
            result = super().send_event(event)
            if result:
                return True

            parent = self._parent_map.get(state)
            if parent is None:
                break
            state = parent

        return False

    def get_substate(self, parent: str) -> Optional[str]:
        """Get current substate of parent.

        Args:
            parent: Parent state name.

        Returns:
            Current substate or None.
        """
        return self._history.get(parent)


def create_simple_state_machine(
    states: List[str],
    transitions: Dict[str, str],
    initial: str,
) -> StateMachine:
    """Create simple state machine from configuration.

    Args:
        states: List of state names.
        transitions: Dict mapping (from_state, event) to to_state.
        initial: Initial state name.

    Returns:
        Configured StateMachine.
    """
    sm = StateMachine(initial)

    for state in states:
        sm.add_state(state)

    for key, value in transitions.items():
        if isinstance(key, tuple):
            from_state, event = key
            sm.add_transition(from_state, value, event)
        else:
            sm.add_transition(initial, value, key)

    return sm