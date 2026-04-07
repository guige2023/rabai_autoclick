"""Finite State Machine (FSM) utilities for RabAI AutoClick.

Provides:
- Deterministic FSM implementation
- State transition validation
- FSM builder/fluent API
- Async FSM support
- Event-driven state transitions
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generic,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)


T = TypeVar("T", bound=Enum)
E = TypeVar("E", bound=Enum)


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""

    def __init__(self, from_state: Any, to_state: Any, event: Any) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.event = event
        super().__init__(
            f"Invalid transition: cannot go from {from_state} to {to_state} on event {event}"
        )


class FSMConfigurationError(Exception):
    """Raised when FSM is configured incorrectly."""

    pass


@dataclass(frozen=True)
class Transition:
    """Represents a state transition.

    Attributes:
        from_state: Source state.
        to_state: Destination state.
        event: Event that triggers the transition.
        condition: Optional condition for guarded transitions.
        action: Optional action to execute during transition.
    """

    from_state: Any
    to_state: Any
    event: Any
    condition: Optional[Callable[[], bool]] = None
    action: Optional[Callable[..., None]] = None


class StateMachine(Generic[T, E]):
    """Generic Finite State Machine implementation.

    Supports:
    - Deterministic state transitions
    - Guarded transitions (conditions)
    - Transition actions
    - Entry/exit callbacks
    - Initial state configuration

    Type Parameters:
        T: State enum type.
        E: Event enum type.

    Example:
        class State(Enum):
            IDLE = auto()
            RUNNING = auto()
            PAUSED = auto()
            STOPPED = auto()

        class Event(Enum):
            START = auto()
            PAUSE = auto()
            RESUME = auto()
            STOP = auto()

        fsm = StateMachine[State, Event](initial=State.IDLE)
        fsm.add_transition(State.IDLE, State.RUNNING, Event.START)
        fsm.add_transition(State.RUNNING, State.PAUSED, Event.PAUSE)
        fsm.trigger(Event.START)
    """

    def __init__(
        self,
        initial: T,
        final_states: Optional[Set[T]] = None,
        allow_undefined_transitions: bool = False,
    ) -> None:
        """Initialize the FSM.

        Args:
            initial: The initial state.
            final_states: Set of terminal/final states.
            allow_undefined_transitions: If True, undefined transitions are silently ignored.
        """
        self._initial = initial
        self._current = initial
        self._final_states: Set[T] = final_states or set()
        self._allow_undefined = allow_undefined_transitions
        self._transitions: Dict[T, Dict[E, Transition]] = {}
        self._on_entry: Dict[T, List[Callable[[], None]]] = {}
        self._on_exit: Dict[T, List[Callable[[], None]]] = {}
        self._on_transition: List[Callable[[T, T, E], None]] = []
        self._lock = threading.RLock()

    @property
    def current_state(self) -> T:
        """Get current state."""
        with self._lock:
            return self._current

    @property
    def initial_state(self) -> T:
        """Get initial state."""
        return self._initial

    @property
    def is_final(self) -> bool:
        """Check if FSM is in a final state."""
        with self._lock:
            return self._current in self._final_states

    def add_transition(
        self,
        from_state: T,
        to_state: T,
        event: E,
        condition: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[..., None]] = None,
    ) -> StateMachine[T, E]:
        """Add a state transition.

        Args:
            from_state: Source state.
            to_state: Destination state.
            event: Event that triggers the transition.
            condition: Optional guard condition (must return True to transition).
            action: Optional action to execute on transition.

        Returns:
            Self for method chaining.
        """
        with self._lock:
            if from_state not in self._transitions:
                self._transitions[from_state] = {}
            self._transitions[from_state][event] = Transition(
                from_state=from_state,
                to_state=to_state,
                event=event,
                condition=condition,
                action=action,
            )
        return self

    def add_transitions(self, *transitions: Transition) -> StateMachine[T, E]:
        """Add multiple transitions at once.

        Args:
            transitions: Variable number of Transition objects.

        Returns:
            Self for method chaining.
        """
        for t in transitions:
            self.add_transition(t.from_state, t.to_state, t.event, t.condition, t.action)
        return self

    def add_observer(
        self,
        on_entry: Optional[Callable[[T], None]] = None,
        on_exit: Optional[Callable[[T], None]] = None,
        on_transition: Optional[Callable[[T, T, E], None]] = None,
    ) -> None:
        """Add state change observers.

        Args:
            on_entry: Called when entering a state (receives new state).
            on_exit: Called when exiting a state (receives old state).
            on_transition: Called on any transition (from, to, event).
        """
        with self._lock:
            if on_entry:
                for state in self._get_all_states():
                    if state not in self._on_entry:
                        self._on_entry[state] = []
                    self._on_entry[state].append(lambda s=state, cb=on_entry: cb(s))
            if on_exit:
                for state in self._get_all_states():
                    if state not in self._on_exit:
                        self._on_exit[state] = []
                    self._on_exit[state].append(lambda s=state, cb=on_exit: cb(s))
            if on_transition:
                self._on_transition.append(on_transition)

    def trigger(self, event: E, *args: Any, **kwargs: Any) -> bool:
        """Trigger an event and attempt state transition.

        Args:
            event: The event to trigger.
            *args: Positional arguments passed to transition action.
            **kwargs: Keyword arguments passed to transition action.

        Returns:
            True if transition occurred, False otherwise.

        Raises:
            InvalidTransitionError: If transition is not allowed and
                allow_undefined_transitions is False.
        """
        with self._lock:
            from_state = self._current

            if from_state not in self._transitions:
                if self._allow_undefined:
                    return False
                raise InvalidTransitionError(from_state, None, event)

            if event not in self._transitions[from_state]:
                if self._allow_undefined:
                    return False
                raise InvalidTransitionError(from_state, None, event)

            transition = self._transitions[from_state][event]

            if transition.condition is not None and not transition.condition():
                return False

            self._execute_callbacks(self._on_exit.get(from_state, []))

            to_state = transition.to_state
            self._current = to_state

            if transition.action:
                transition.action(*args, **kwargs)

            self._execute_callbacks(self._on_entry.get(to_state, []))
            self._execute_callbacks(
                [cb for cb in self._on_transition],
                from_state,
                to_state,
                event,
            )

            return True

    def _execute_callbacks(
        self, callbacks: List[Callable[..., None]], *args: Any
    ) -> None:
        for cb in callbacks:
            try:
                cb(*args)
            except Exception:
                pass

    def _get_all_states(self) -> Set[T]:
        states: Set[T] = set()
        for state_map in self._transitions.values():
            states.add(state_map[list(state_map.keys())[0]].from_state)
            states.update(t.to_state for t in state_map.values())
        states.add(self._initial)
        return states

    def can_trigger(self, event: E) -> bool:
        """Check if an event can be triggered in current state."""
        with self._lock:
            if self._current not in self._transitions:
                return False
            transition = self._transitions[self._current].get(event)
            if transition is None:
                return False
            if transition.condition is not None:
                return transition.condition()
            return True

    def get_available_events(self) -> Set[E]:
        """Get set of events that can be triggered in current state."""
        with self._lock:
            if self._current not in self._transitions:
                return set()
            return set(self._transitions[self._current].keys())

    def reset(self) -> None:
        """Reset FSM to initial state."""
        with self._lock:
            self._current = self._initial

    def __repr__(self) -> str:
        return f"StateMachine(current={self._current!r}, initial={self._initial!r})"


class AsyncStateMachine(StateMachine[T, E]):
    """Async version of StateMachine with awaitable transitions."""

    def __init__(
        self,
        initial: T,
        final_states: Optional[Set[T]] = None,
        allow_undefined_transitions: bool = False,
    ) -> None:
        super().__init__(initial, final_states, allow_undefined_transitions)
        self._async_on_entry: Dict[T, List[Callable[[T], Any]]] = {}
        self._async_on_exit: Dict[T, List[Callable[[T], Any]]] = {}
        self._async_lock = asyncio.Lock()

    async def trigger(self, event: E, *args: Any, **kwargs: Any) -> bool:
        async with self._async_lock:
            from_state = self._current

            if from_state not in self._transitions:
                if self._allow_undefined:
                    return False
                raise InvalidTransitionError(from_state, None, event)

            if event not in self._transitions[from_state]:
                if self._allow_undefined:
                    return False
                raise InvalidTransitionError(from_state, None, event)

            transition = self._transitions[from_state][event]

            if transition.condition is not None and not transition.condition():
                return False

            await self._execute_async_callbacks(self._async_on_exit.get(from_state, []))
            await self._execute_async_callbacks(self._on_exit.get(from_state, []))

            to_state = transition.to_state
            self._current = to_state

            if transition.action:
                if asyncio.iscoroutinefunction(transition.action):
                    await transition.action(*args, **kwargs)
                else:
                    transition.action(*args, **kwargs)

            await self._execute_async_callbacks(self._async_on_entry.get(to_state, []))
            self._execute_callbacks(self._on_entry.get(to_state, []))
            await self._execute_async_callbacks(
                [cb for cb in self._on_transition],
                from_state,
                to_state,
                event,
            )

            return True

    async def _execute_async_callbacks(
        self, callbacks: List[Callable[..., Any]], *args: Any
    ) -> None:
        for cb in callbacks:
            try:
                result = cb(*args)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass

    def add_async_observer(
        self,
        on_entry: Optional[Callable[[T], Any]] = None,
        on_exit: Optional[Callable[[T], Any]] = None,
    ) -> None:
        if on_entry:
            for state in self._get_all_states():
                if state not in self._async_on_entry:
                    self._async_on_entry[state] = []
                self._async_on_entry[state].append(on_entry)
        if on_exit:
            for state in self._get_all_states():
                if state not in self._async_on_exit:
                    self._async_on_exit[state] = []
                self._async_on_exit[state].append(on_exit)


class FSMBuilder(Generic[T, E]):
    """Fluent builder for StateMachine.

    Example:
        fsm = (FSMBuilder[State, Event](State.IDLE)
               .transition(State.IDLE, State.RUNNING, Event.START)
               .transition(State.RUNNING, State.PAUSED, Event.PAUSE)
               .transition(State.RUNNING, State.STOPPED, Event.STOP)
               .transition(State.PAUSED, State.RUNNING, Event.RESUME)
               .transition(State.PAUSED, State.STOPPED, Event.STOP)
               .final_states({State.STOPPED})
               .build())
    """

    def __init__(self, initial: T) -> None:
        self._initial = initial
        self._transitions: List[Transition] = []
        self._final_states: Set[T] = set()
        self._allow_undefined = False

    def transition(
        self,
        from_state: T,
        to_state: T,
        event: E,
        condition: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[..., None]] = None,
    ) -> FSMBuilder[T, E]:
        self._transitions.append(
            Transition(from_state, to_state, event, condition, action)
        )
        return self

    def final_states(self, states: Set[T]) -> FSMBuilder[T, E]:
        self._final_states = states
        return self

    def allow_undefined(self) -> FSMBuilder[T, E]:
        self._allow_undefined = True
        return self

    def build(self) -> StateMachine[T, E]:
        fsm = StateMachine[T, E](
            initial=self._initial,
            final_states=self._final_states,
            allow_undefined_transitions=self._allow_undefined,
        )
        fsm.add_transitions(*self._transitions)
        return fsm
