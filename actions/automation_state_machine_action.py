"""Automation State Machine Action module.

Provides state machine implementation for workflow automation.
Supports hierarchical states, guards, actions, and
async transitions.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class StateMachineError(Exception):
    """Base exception for state machine errors."""
    pass


class InvalidTransitionError(StateMachineError):
    """Raised when an invalid transition is attempted."""
    pass


class StateNotFoundError(StateMachineError):
    """Raised when a state is not found."""
    pass


@dataclass
class State:
    """A state in the state machine."""

    name: str
    on_enter: Optional[Callable[..., Any]] = None
    on_exit: Optional[Callable[..., Any]] = None
    on_entry_async: Optional[Callable[..., Any]] = None
    on_exit_async: Optional[Callable[..., Any]] = None
    is_initial: bool = False
    is_final: bool = False


@dataclass
class Transition:
    """A transition between states."""

    source: str
    target: str
    event: str
    guard: Optional[Callable[..., bool]] = None
    action: Optional[Callable[..., Any]] = None
    action_async: Optional[Callable[..., Any]] = None


@dataclass
class TransitionResult:
    """Result of a transition attempt."""

    success: bool
    from_state: str
    to_state: Optional[str]
    event: str
    error: Optional[str] = None


class StateMachine:
    """Hierarchical state machine for automation workflows."""

    def __init__(self, name: str = "state_machine"):
        self.name = name
        self._states: dict[str, State] = {}
        self._transitions: list[Transition] = []
        self._current_state: Optional[str] = None
        self._history: list[str] = []
        self._context: dict[str, Any] = {}
        self._transition_lock = asyncio.Lock()

    def add_state(
        self,
        name: str,
        on_enter: Optional[Callable] = None,
        on_exit: Optional[Callable] = None,
        on_enter_async: Optional[Callable] = None,
        on_exit_async: Optional[Callable] = None,
        is_initial: bool = False,
        is_final: bool = False,
    ) -> "StateMachine":
        """Add a state to the machine.

        Args:
            name: State name
            on_enter: Sync callback on entering state
            on_exit: Sync callback on exiting state
            on_enter_async: Async callback on entering state
            on_exit_async: Async callback on exiting state
            is_initial: Whether this is an initial state
            is_final: Whether this is a final state

        Returns:
            Self for chaining
        """
        state = State(
            name=name,
            on_enter=on_enter,
            on_exit=on_exit,
            on_entry_async=on_enter_async,
            on_exit_async=on_exit_async,
            is_initial=is_initial,
            is_final=is_final,
        )
        self._states[name] = state

        if is_initial and self._current_state is None:
            self._current_state = name

        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable[..., bool]] = None,
        action: Optional[Callable[..., Any]] = None,
        action_async: Optional[Callable[..., Any]] = None,
    ) -> "StateMachine":
        """Add a transition.

        Args:
            source: Source state name
            target: Target state name
            event: Event that triggers transition
            guard: Optional condition that must be true
            action: Optional sync action to execute
            action_async: Optional async action to execute

        Returns:
            Self for chaining
        """
        if source not in self._states:
            raise StateNotFoundError(f"Source state '{source}' not found")
        if target not in self._states:
            raise StateNotFoundError(f"Target state '{target}' not found")

        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
            action_async=action_async,
        )
        self._transitions.append(transition)
        return self

    async def send(self, event: str, **context: Any) -> TransitionResult:
        """Send an event to the state machine.

        Args:
            event: Event name
            **context: Additional context for guards/actions

        Returns:
            TransitionResult
        """
        async with self._transition_lock:
            current = self._current_state
            if current is None:
                return TransitionResult(
                    success=False,
                    from_state="none",
                    to_state=None,
                    event=event,
                    error="No current state",
                )

            matching = [
                t for t in self._transitions
                if t.source == current and t.event == event
            ]

            if not matching:
                return TransitionResult(
                    success=False,
                    from_state=current,
                    to_state=None,
                    event=event,
                    error=f"No transition for event '{event}' from state '{current}'",
                )

            for transition in matching:
                if transition.guard and not transition.guard(**context):
                    continue

                source_state = self._states[transition.source]
                target_state = self._states[transition.target]

                if source_state.on_exit:
                    source_state.on_exit(self._context)

                if source_state.on_exit_async:
                    await source_state.on_exit_async(self._context)

                if transition.action_async:
                    await transition.action_async(self._context)
                elif transition.action:
                    transition.action(self._context)

                self._history.append(current)
                self._current_state = transition.target

                if target_state.on_enter:
                    target_state.on_enter(self._context)

                if target_state.on_entry_async:
                    await target_state.on_entry_async(self._context)

                return TransitionResult(
                    success=True,
                    from_state=current,
                    to_state=transition.target,
                    event=event,
                )

            return TransitionResult(
                success=False,
                from_state=current,
                to_state=None,
                event=event,
                error="All guards evaluated to false",
            )

    def send_sync(self, event: str, **context: Any) -> TransitionResult:
        """Synchronous version of send (for non-async contexts).

        Args:
            event: Event name
            **context: Additional context

        Returns:
            TransitionResult
        """
        current = self._current_state
        if current is None:
            return TransitionResult(
                success=False,
                from_state="none",
                to_state=None,
                event=event,
                error="No current state",
            )

        matching = [
            t for t in self._transitions
            if t.source == current and t.event == event
        ]

        if not matching:
            return TransitionResult(
                success=False,
                from_state=current,
                to_state=None,
                event=event,
                error=f"No transition for event '{event}'",
            )

        for transition in matching:
            if transition.guard and not transition.guard(**context):
                continue

            source_state = self._states[transition.source]
            target_state = self._states[transition.target]

            if source_state.on_exit:
                source_state.on_exit(self._context)

            if transition.action:
                transition.action(self._context)

            self._history.append(current)
            self._current_state = transition.target

            if target_state.on_enter:
                target_state.on_enter(self._context)

            return TransitionResult(
                success=True,
                from_state=current,
                to_state=transition.target,
                event=event,
            )

        return TransitionResult(
            success=False,
            from_state=current,
            to_state=None,
            event=event,
            error="All guards evaluated to false",
        )

    @property
    def current_state(self) -> Optional[str]:
        """Get current state name."""
        return self._current_state

    @property
    def is_final(self) -> bool:
        """Check if in a final state."""
        if self._current_state is None:
            return False
        state = self._states.get(self._current_state)
        return state is not None and state.is_final

    def get_history(self) -> list[str]:
        """Get state transition history."""
        return list(self._history)

    def get_available_events(self) -> list[str]:
        """Get events available from current state."""
        if self._current_state is None:
            return []
        return [
            t.event for t in self._transitions
            if t.source == self._current_state
        ]

    def reset(self) -> None:
        """Reset state machine to initial state."""
        initial_states = [s for s in self._states.values() if s.is_initial]
        if initial_states:
            self._current_state = initial_states[0].name
        else:
            self._current_state = None
        self._history.clear()


def create_simple_workflow(
    states: list[str],
    transitions: list[tuple[str, str, str]],
) -> StateMachine:
    """Create a simple linear workflow.

    Args:
        states: List of state names
        transitions: List of (source, target, event) tuples

    Returns:
        Configured StateMachine
    """
    sm = StateMachine()

    for i, state in enumerate(states):
        sm.add_state(
            name=state,
            is_initial=i == 0,
            is_final=i == len(states) - 1,
        )

    for source, target, event in transitions:
        sm.add_transition(source, target, event)

    return sm
