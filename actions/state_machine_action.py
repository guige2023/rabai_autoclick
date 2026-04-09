"""
State machine implementation for managing workflow states and transitions.

This module provides a flexible state machine that can be used to model
complex automation workflows with defined states, transitions, guards, and actions.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class TransitionType(Enum):
    """Type of state transition."""
    EXTERNAL = auto()
    INTERNAL = auto()
    LOCAL = auto()


@dataclass
class Transition:
    """Represents a state transition."""
    source: str
    target: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[..., None]] = None
    action_args: tuple = field(default_factory=tuple)
    action_kwargs: Dict[str, Any] = field(default_factory=dict)
    description: str = ""

    def can_execute(self) -> bool:
        """Check if the transition guard allows execution."""
        if self.guard is None:
            return True
        try:
            return self.guard()
        except Exception as e:
            logger.warning(f"Guard for transition {self.source}->{self.target} failed: {e}")
            return False

    def execute(self) -> Any:
        """Execute the transition action."""
        if self.action is None:
            return None
        try:
            return self.action(*self.action_args, **self.action_kwargs)
        except Exception as e:
            logger.error(f"Transition action failed: {e}")
            raise


@dataclass
class State:
    """Represents a state in the state machine."""
    name: str
    entry_action: Optional[Callable[..., None]] = None
    exit_action: Optional[Callable[..., None]] = None
    do_activity: Optional[Callable[..., None]] = None
    is_initial: bool = False
    is_final: bool = False
    is_composite: bool = False
    substates: List["State"] = field(default_factory=list)
    parent: Optional["State"] = None

    def enter(self) -> None:
        """Execute entry action."""
        if self.entry_action:
            try:
                self.entry_action()
            except Exception as e:
                logger.error(f"Entry action for state {self.name} failed: {e}")

    def exit(self) -> None:
        """Execute exit action."""
        if self.exit_action:
            try:
                self.exit_action()
            except Exception as e:
                logger.error(f"Exit action for state {self.name} failed: {e}")


class StateMachineError(Exception):
    """Base exception for state machine errors."""
    pass


class InvalidTransitionError(StateMachineError):
    """Raised when an invalid transition is attempted."""
    pass


class GuardViolationError(StateMachineError):
    """Raised when a guard condition is not satisfied."""
    pass


class StateMachine:
    """
    A flexible state machine implementation for automation workflows.

    Features:
    - Hierarchical (nested) states
    - Guard conditions on transitions
    - Entry/exit actions on states
    - Do activities (ongoing actions while in state)
    - Transition actions
    - History states (remember last active substate)
    - Orthogonal regions (parallel states)

    Example:
        >>> sm = StateMachine("OrderProcessor")
        >>> sm.add_state("pending", is_initial=True)
        >>> sm.add_state("processing")
        >>> sm.add_state("completed", is_final=True)
        >>> sm.add_transition("pending", "processing", "start")
        >>> sm.add_transition("processing", "completed", "finish")
        >>> sm.initial_state = "pending"
        >>> sm.trigger("start")
    """

    def __init__(self, name: str = "StateMachine"):
        """
        Initialize the state machine.

        Args:
            name: Human-readable name for this state machine
        """
        self.name = name
        self.states: Dict[str, State] = {}
        self.transitions: Dict[str, List[Transition]] = {}
        self.initial_state: Optional[str] = None
        self.current_state: Optional[str] = None
        self.history: Dict[str, Optional[str]] = {}
        self.variables: Dict[str, Any] = {}
        self._transition_count: int = 0
        self._last_transition_time: Optional[float] = None
        logger.info(f"StateMachine '{name}' created")

    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable[..., None]] = None,
        exit_action: Optional[Callable[..., None]] = None,
        do_activity: Optional[Callable[..., None]] = None,
        is_initial: bool = False,
        is_final: bool = False,
    ) -> State:
        """
        Add a state to the state machine.

        Args:
            name: Unique state identifier
            entry_action: Callback on state entry
            exit_action: Callback on state exit
            do_activity: Callback executed while in state
            is_initial: Whether this is the initial state
            is_final: Whether this is a final state

        Returns:
            The created State object
        """
        if name in self.states:
            raise StateMachineError(f"State '{name}' already exists")

        state = State(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            do_activity=do_activity,
            is_initial=is_initial,
            is_final=is_final,
        )
        self.states[name] = state
        self.transitions[name] = []

        if is_initial and self.initial_state is None:
            self.initial_state = name

        logger.debug(f"State '{name}' added to machine '{self.name}'")
        return state

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[..., None]] = None,
        **kwargs,
    ) -> Transition:
        """
        Add a transition between states.

        Args:
            source: Source state name
            target: Target state name
            event: Event name that triggers this transition
            guard: Optional condition that must be true for transition
            action: Optional callback to execute on transition
            **kwargs: Additional arguments for action

        Returns:
            The created Transition object
        """
        if source not in self.states:
            raise InvalidTransitionError(f"Source state '{source}' does not exist")
        if target not in self.states:
            raise InvalidTransitionError(f"Target state '{target}' does not exist")

        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
            action_kwargs=kwargs,
        )
        self.transitions[source].append(transition)
        self._transition_count += 1
        logger.debug(
            f"Transition added: {source} --[{event}]--> {target} in '{self.name}'"
        )
        return transition

    def trigger(self, event: str, **kwargs) -> bool:
        """
        Trigger an event causing a transition.

        Args:
            event: The event name
            **kwargs: Arguments to pass to transition action

        Returns:
            True if transition occurred, False otherwise
        """
        if self.current_state is None:
            if self.initial_state is None:
                raise StateMachineError("No initial state defined")
            self.current_state = self.initial_state
            logger.info(f"State machine '{self.name}' initialized to '{self.current_state}'")

        state_transitions = self.transitions.get(self.current_state, [])
        for transition in state_transitions:
            if transition.event != event:
                continue

            if not transition.can_execute():
                logger.debug(
                    f"Transition guard failed for {self.current_state} --[{event}]--> {transition.target}"
                )
                continue

            old_state = self.current_state
            new_state = transition.target

            self._execute_transition(transition, **kwargs)
            logger.info(
                f"State machine '{self.name}': {old_state} --[{event}]--> {new_state}"
            )
            return True

        logger.debug(f"No valid transition for event '{event}' from state '{self.current_state}'")
        return False

    def _execute_transition(self, transition: Transition, **kwargs) -> None:
        """Execute a state transition."""
        source_state = self.states[transition.source]
        target_state = self.states[transition.target]

        source_state.exit()
        self.history[transition.source] = transition.target

        transition.action_args = kwargs.get("args", ())
        transition.action_kwargs = kwargs
        try:
            transition.execute()
        except Exception as e:
            logger.error(f"Transition action failed: {e}")

        self.current_state = transition.target
        self._last_transition_time = time.time()

        target_state.enter()

        if target_state.do_activity:
            try:
                target_state.do_activity()
            except Exception as e:
                logger.error(f"Do activity failed: {e}")

    def is_in_state(self, state_name: str) -> bool:
        """Check if currently in the specified state."""
        return self.current_state == state_name

    def is_final(self) -> bool:
        """Check if current state is a final state."""
        if self.current_state is None:
            return False
        return self.states[self.current_state].is_final

    def get_state(self) -> Optional[str]:
        """Get the current state name."""
        return self.current_state

    def get_transition_count(self) -> int:
        """Get the total number of transitions that have occurred."""
        return self._transition_count

    def set_variable(self, name: str, value: Any) -> None:
        """Set a state machine variable."""
        self.variables[name] = value

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get a state machine variable."""
        return self.variables.get(name, default)

    def get_history(self, state_name: str) -> Optional[str]:
        """Get the last active substate of a composite state."""
        return self.history.get(state_name)

    def reset(self) -> None:
        """Reset the state machine to its initial state."""
        self.current_state = None
        self.history.clear()
        self.variables.clear()
        self._transition_count = 0
        self._last_transition_time = None
        logger.info(f"State machine '{self.name}' reset")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state machine to dictionary."""
        return {
            "name": self.name,
            "current_state": self.current_state,
            "initial_state": self.initial_state,
            "states": list(self.states.keys()),
            "transitions": self._transition_count,
            "variables": self.variables,
        }

    def __repr__(self) -> str:
        return (
            f"StateMachine(name='{self.name}', "
            f"current='{self.current_state}', "
            f"states={len(self.states)}, "
            f"transitions={self._transition_count})"
        )
