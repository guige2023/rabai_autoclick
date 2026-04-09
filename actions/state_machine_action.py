"""
State Machine Action Module.

Provides state machine implementation for managing
workflow states, transitions, and guards.

Author: rabai_autoclick team
"""

import logging
from typing import (
    Optional, Dict, Any, List, Callable, Set,
    Union, TypeVar
)
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TransitionType(Enum):
    """Types of state transitions."""
    INTERNAL = "internal"
    EXTERNAL = "external"
    LOCAL = "local"


@dataclass
class State:
    """Represents a state in the state machine."""
    name: str
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    do_action: Optional[Callable] = None
    is_initial: bool = False
    is_final: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """Represents a state transition."""
    source: str
    target: str
    event: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransitionResult:
    """Result of a transition attempt."""
    success: bool
    from_state: str
    to_state: str
    event: str
    consumed: bool = True
    error: Optional[str] = None


@dataclass
class StateMachineConfig:
    """Configuration for state machine."""
    name: str = "state_machine"
    initial_state: Optional[str] = None
    parallel: bool = False
    history: bool = True
    max_history: int = 100


class StateMachineError(Exception):
    """Exception raised for state machine errors."""
    pass


class InvalidTransitionError(StateMachineError):
    """Exception raised for invalid transitions."""
    pass


class GuardFailedError(StateMachineError):
    """Exception raised when a guard condition fails."""
    pass


class StateMachineAction:
    """
    State Machine Implementation.

    Supports hierarchical states, guards, actions, and
    comprehensive transition management.

    Example:
        >>> sm = StateMachineAction(name="order")
        >>> sm.add_state("pending")
        >>> sm.add_state("processing")
        >>> sm.add_transition("pending", "processing", "start")
        >>> sm.send("start")
    """

    def __init__(self, config: Optional[StateMachineConfig] = None):
        self.config = config or StateMachineConfig()
        self._states: Dict[str, State] = {}
        self._transitions: Dict[str, List[Transition]] = {}
        self._current_state: Optional[str] = None
        self._history: List[str] = []
        self._state_data: Dict[str, Any] = {}

    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        do_action: Optional[Callable] = None,
        is_initial: bool = False,
        is_final: bool = False,
        **metadata,
    ) -> "StateMachineAction":
        """
        Add a state to the state machine.

        Args:
            name: State name
            entry_action: Action to execute on entry
            exit_action: Action to execute on exit
            do_action: Action to execute while in state
            is_initial: Whether this is the initial state
            is_final: Whether this is a final state
            **metadata: Additional metadata

        Returns:
            Self for chaining
        """
        state = State(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            do_action=do_action,
            is_initial=is_initial,
            is_final=is_final,
            metadata=metadata,
        )

        self._states[name] = state

        if is_initial:
            if self.config.initial_state and self.config.initial_state != name:
                logger.warning(f"Multiple initial states: {name}")
            self.config.initial_state = name

        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None,
        transition_type: TransitionType = TransitionType.EXTERNAL,
        **metadata,
    ) -> "StateMachineAction":
        """
        Add a transition.

        Args:
            source: Source state
            target: Target state
            event: Event triggering transition
            guard: Optional guard condition
            action: Optional transition action
            transition_type: Type of transition
            **metadata: Additional metadata

        Returns:
            Self for chaining
        """
        if source not in self._states:
            raise InvalidTransitionError(f"Unknown source state: {source}")
        if target not in self._states:
            raise InvalidTransitionError(f"Unknown target state: {target}")

        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
            transition_type=transition_type,
            metadata=metadata,
        )

        if event not in self._transitions:
            self._transitions[event] = []
        self._transitions[event].append(transition)

        return self

    def initialize(self) -> None:
        """Initialize the state machine."""
        if self.config.initial_state and self._current_state is None:
            self._current_state = self.config.initial_state
            self._enter_state(self._current_state)

    def _enter_state(self, state_name: str) -> None:
        """Execute entry action for a state."""
        state = self._states.get(state_name)
        if state and state.entry_action:
            try:
                state.entry_action(self)
            except Exception as e:
                logger.error(f"Entry action failed for state {state_name}: {e}")

    def _exit_state(self, state_name: str) -> None:
        """Execute exit action for a state."""
        state = self._states.get(state_name)
        if state and state.exit_action:
            try:
                state.exit_action(self)
            except Exception as e:
                logger.error(f"Exit action failed for state {state_name}: {e}")

    def send(
        self,
        event: str,
        payload: Optional[Any] = None,
    ) -> TransitionResult:
        """
        Send an event to the state machine.

        Args:
            event: Event name
            payload: Optional event payload

        Returns:
            TransitionResult
        """
        if self._current_state is None:
            self.initialize()

        if self._current_state not in self._transitions:
            return TransitionResult(
                success=False,
                from_state=self._current_state or "",
                to_state=self._current_state or "",
                event=event,
                consumed=False,
                error=f"No transitions for event '{event}' from state '{self._current_state}'",
            )

        transitions = self._transitions.get(event, [])

        for transition in transitions:
            if transition.source != self._current_state:
                continue

            if transition.guard:
                try:
                    guard_result = transition.guard(self, payload)
                    if not guard_result:
                        return TransitionResult(
                            success=False,
                            from_state=self._current_state or "",
                            to_state=self._current_state or "",
                            event=event,
                            consumed=True,
                            error="Guard condition failed",
                        )
                except Exception as e:
                    return TransitionResult(
                        success=False,
                        from_state=self._current_state or "",
                        to_state=self._current_state or "",
                        event=event,
                        consumed=True,
                        error=f"Guard error: {str(e)}",
                    )

            from_state = self._current_state
            to_state = transition.target

            self._exit_state(from_state)

            if transition.action:
                try:
                    transition.action(self, payload)
                except Exception as e:
                    logger.error(f"Transition action failed: {e}")

            self._current_state = to_state

            if self.config.history:
                self._history.append(to_state)
                if len(self._history) > self.config.max_history:
                    self._history.pop(0)

            self._enter_state(to_state)

            return TransitionResult(
                success=True,
                from_state=from_state,
                to_state=to_state,
                event=event,
                consumed=True,
            )

        return TransitionResult(
            success=False,
            from_state=self._current_state or "",
            to_state=self._current_state or "",
            event=event,
            consumed=False,
            error=f"No matching transition for event '{event}' from state '{self._current_state}'",
        )

    @property
    def current_state(self) -> Optional[str]:
        """Get current state."""
        return self._current_state

    @property
    def is_final(self) -> bool:
        """Check if in a final state."""
        if self._current_state:
            state = self._states.get(self._current_state)
            return state.is_final if state else False
        return False

    def get_history(self) -> List[str]:
        """Get state history."""
        return self._history.copy()

    def get_available_events(self) -> List[str]:
        """Get events available from current state."""
        if not self._current_state:
            return []

        events = []
        for event, transitions in self._transitions.items():
            for t in transitions:
                if t.source == self._current_state:
                    events.append(event)
                    break
        return events

    def get_state_data(self, key: str, default: Any = None) -> Any:
        """Get state data."""
        return self._state_data.get(key, default)

    def set_state_data(self, key: str, value: Any) -> None:
        """Set state data."""
        self._state_data[key] = value

    def reset(self) -> None:
        """Reset the state machine."""
        self._current_state = None
        self._history.clear()
        self._state_data.clear()
        if self.config.initial_state:
            self.initialize()

    def get_stats(self) -> Dict[str, Any]:
        """Get state machine statistics."""
        return {
            "name": self.config.name,
            "current_state": self._current_state,
            "total_states": len(self._states),
            "total_transitions": sum(len(t) for t in self._transitions.values()),
            "history_size": len(self._history),
            "is_final": self.is_final,
            "available_events": self.get_available_events(),
        }
