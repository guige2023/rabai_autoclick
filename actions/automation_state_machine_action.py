"""Automation state machine for complex workflow orchestration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine, Optional


class StateTransitionResult(str, Enum):
    """Result of a state transition."""

    SUCCESS = "success"
    REJECTED = "rejected"
    ERROR = "error"


@dataclass
class StateTransition:
    """A state transition definition."""

    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], Coroutine[Any, Any, Any]]] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransitionAttempt:
    """Record of a transition attempt."""

    from_state: str
    to_state: str
    event: str
    result: StateTransitionResult
    timestamp: datetime
    error: Optional[str] = None


@dataclass
class StateMachineConfig:
    """Configuration for state machine."""

    initial_state: str
    transitions: list[StateTransition]
    on_entry: Optional[Callable[[str], None]] = None
    on_exit: Optional[Callable[[str], None]] = None
    on_transition: Optional[Callable[[str, str, str], None]] = None


class AutomationStateMachineAction:
    """Manages state machine for automation workflows."""

    def __init__(self, config: Optional[StateMachineConfig] = None):
        """Initialize state machine.

        Args:
            config: State machine configuration.
        """
        self._config = config
        self._current_state: Optional[str] = None
        self._transition_history: list[TransitionAttempt] = []
        self._transition_map: dict[tuple[str, str], StateTransition] = {}
        self._event_handlers: dict[str, Callable[[], Coroutine[Any, Any, Any]]] = {}
        self._is_async: bool = False

        if config:
            self._initialize_from_config(config)

    def _initialize_from_config(self, config: StateMachineConfig) -> None:
        """Initialize state machine from configuration."""
        self._current_state = config.initial_state

        for transition in config.transitions:
            key = (transition.from_state, transition.to_state)
            self._transition_map[key] = transition

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], Coroutine[Any, Any, Any]]] = None,
    ) -> None:
        """Add a state transition.

        Args:
            from_state: Source state.
            to_state: Target state.
            event: Event that triggers transition.
            guard: Optional guard condition.
            action: Optional action to execute.
        """
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
        )

        key = (from_state, to_state)
        self._transition_map[key] = transition

        if event not in self._event_handlers:
            self._event_handlers[event] = self._create_event_handler(event)

    def _create_event_handler(self, event: str) -> Callable[[], Coroutine[Any, Any, Any]]:
        """Create async handler for an event."""

        async def handler() -> StateTransitionResult:
            if not self._current_state:
                return StateTransitionResult.REJECTED

            for (from_state, to_state), transition in self._transition_map.items():
                if from_state == self._current_state and transition.event == event:
                    if transition.guard and not transition.guard():
                        self._record_transition(
                            from_state, to_state, event, StateTransitionResult.REJECTED
                        )
                        return StateTransitionResult.REJECTED

                    try:
                        if transition.action:
                            await transition.action()

                        old_state = self._current_state
                        self._current_state = to_state
                        self._record_transition(
                            old_state, to_state, event, StateTransitionResult.SUCCESS
                        )
                        return StateTransitionResult.SUCCESS

                    except Exception as e:
                        self._record_transition(
                            from_state, to_state, event, StateTransitionResult.ERROR, str(e)
                        )
                        return StateTransitionResult.ERROR

            return StateTransitionResult.REJECTED

        return handler

    async def send_event(self, event: str) -> StateTransitionResult:
        """Send an event to the state machine.

        Args:
            event: Event name.

        Returns:
            StateTransitionResult.
        """
        handler = self._event_handlers.get(event)
        if not handler:
            return StateTransitionResult.REJECTED

        self._is_async = True
        return await handler()

    def _record_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        result: StateTransitionResult,
        error: Optional[str] = None,
    ) -> None:
        """Record a transition attempt."""
        attempt = TransitionAttempt(
            from_state=from_state,
            to_state=to_state,
            event=event,
            result=result,
            timestamp=datetime.now(),
            error=error,
        )
        self._transition_history.append(attempt)

    def get_current_state(self) -> Optional[str]:
        """Get current state."""
        return self._current_state

    def can_transition(self, to_state: str) -> bool:
        """Check if transition to state is possible."""
        if not self._current_state:
            return False

        for (from_state, state), transition in self._transition_map.items():
            if from_state == self._current_state and state == to_state:
                if transition.guard:
                    return transition.guard()
                return True

        return False

    def get_available_transitions(self) -> list[str]:
        """Get list of states we can transition to."""
        if not self._current_state:
            return []

        available = []
        for (from_state, to_state), transition in self._transition_map.items():
            if from_state == self._current_state:
                if transition.guard is None or transition.guard():
                    available.append(to_state)

        return available

    def get_history(self, limit: int = 100) -> list[TransitionAttempt]:
        """Get transition history."""
        return self._transition_history[-limit:]

    def reset(self, initial_state: Optional[str] = None) -> None:
        """Reset state machine to initial state."""
        if initial_state:
            self._current_state = initial_state
        self._transition_history.clear()
