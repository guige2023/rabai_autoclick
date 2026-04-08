"""State machine utilities.

Provides a flexible state machine implementation for
managing workflow states and transitions.
"""

from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar


T = TypeVar("T", bound=Enum)


class StateMachineError(Exception):
    """Raised on invalid state machine operations."""
    pass


class Transition:
    """Represents a state transition."""

    def __init__(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[..., None]] = None,
    ) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.event = event
        self.guard = guard
        self.action = action

    def can_execute(self) -> bool:
        """Check if transition guard passes."""
        if self.guard is None:
            return True
        return self.guard()

    def execute(self, *args: Any, **kwargs: Any) -> None:
        """Execute transition action."""
        if self.action:
            self.action(*args, **kwargs)


class StateMachine(Generic[T]):
    """Generic state machine with event-driven transitions.

    Example:
        class TrafficLight(Enum):
            RED = auto()
            GREEN = auto()
            YELLOW = auto()

        sm = StateMachine[TrafficLight](TrafficLight.RED)
        sm.add_transition(TrafficLight.RED, TrafficLight.GREEN, "go")
        sm.add_transition(TrafficLight.GREEN, TrafficLight.YELLOW, "slow")
        sm.add_transition(TrafficLight.YELLOW, TrafficLight.RED, "stop")
        sm.trigger("go")
        print(sm.current)  # TrafficLight.GREEN
    """

    def __init__(self, initial_state: T) -> None:
        self._initial = initial_state
        self._current: T = initial_state
        self._transitions: Dict[str, List[Transition]] = {}
        self._on_enter: Dict[T, Callable[[], None]] = {}
        self._on_exit: Dict[T, Callable[[], None]] = {}
        self._history: List[T] = []

    @property
    def current(self) -> T:
        """Get current state."""
        return self._current

    @property
    def initial(self) -> T:
        """Get initial state."""
        return self._initial

    @property
    def history(self) -> List[T]:
        """Get state history."""
        return list(self._history)

    def add_transition(
        self,
        from_state: T,
        to_state: T,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[..., None]] = None,
    ) -> None:
        """Add a transition.

        Args:
            from_state: Source state.
            to_state: Target state.
            event: Event name that triggers transition.
            guard: Optional guard condition.
            action: Optional action to execute on transition.
        """
        key = self._state_key(from_state)
        if key not in self._transitions:
            self._transitions[key] = []
        self._transitions[key].append(Transition(
            from_state.value,
            to_state.value,
            event,
            guard,
            action,
        ))

    def on_enter(self, state: T, handler: Callable[[], None]) -> None:
        """Register enter handler for state.

        Args:
            state: State to handle.
            handler: Callback on entering state.
        """
        self._on_enter[state] = handler

    def on_exit(self, state: T, handler: Callable[[], None]) -> None:
        """Register exit handler for state.

        Args:
            state: State to handle.
            handler: Callback on exiting state.
        """
        self._on_exit[state] = handler

    def can_transition(self, event: str) -> bool:
        """Check if event can trigger a transition.

        Args:
            event: Event name.

        Returns:
            True if transition is possible.
        """
        key = self._state_key(self._current)
        for t in self._transitions.get(key, []):
            if t.event == event and t.can_execute():
                return True
        return False

    def trigger(self, event: str, *args: Any, **kwargs: Any) -> bool:
        """Trigger an event and execute transition.

        Args:
            event: Event name.
            *args: Arguments for transition action.
            **kwargs: Keyword arguments for transition action.

        Returns:
            True if transition was executed.

        Raises:
            StateMachineError: If no valid transition exists.
        """
        key = self._state_key(self._current)
        for t in self._transitions.get(key, []):
            if t.event == event and t.can_execute():
                return self._execute_transition(t, *args, **kwargs)
        return False

    def _execute_transition(self, t: Transition, *args: Any, **kwargs: Any) -> bool:
        old_state = self._current

        if old_state in self._on_exit:
            self._on_exit[old_state]()

        t.execute(*args, **kwargs)

        self._history.append(self._current)
        self._current = self._state_from_key(t.to_state)

        if self._current in self._on_enter:
            self._on_enter[self._current]()

        return True

    def _state_key(self, state: T) -> str:
        return state.value if isinstance(state, Enum) else str(state)

    def _state_from_key(self, key: str) -> T:
        for state in self._initial.__class__.__members__.values():
            if state.value == key:
                return state  # type: ignore
        raise StateMachineError(f"Unknown state: {key}")

    def reset(self) -> None:
        """Reset to initial state."""
        self._current = self._initial
        self._history.clear()

    def get_available_events(self) -> List[str]:
        """Get list of events valid from current state."""
        key = self._state_key(self._current)
        return [t.event for t in self._transitions.get(key, []) if t.can_execute()]
