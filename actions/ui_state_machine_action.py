"""
UI State Machine Action Module

Implements state machine logic for managing complex
UI states and transitions in automation workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class StateTransition:
    """Represents a state transition."""

    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None


@dataclass
class StateConfig:
    """Configuration for a state."""

    name: str
    entry_action: Optional[Callable[[], None]] = None
    exit_action: Optional[Callable[[], None]] = None
    is_final: bool = False


class UIStateMachine:
    """
    Manages UI states and transitions.

    Supports hierarchical states, guards, actions,
    and transition history tracking.
    """

    def __init__(
        self,
        initial_state: str,
    ):
        self._current_state: str = initial_state
        self._states: Dict[str, StateConfig] = {}
        self._transitions: List[StateTransition] = []
        self._transition_history: List[tuple] = []
        self._state_data: Dict[str, Any] = {}

    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable[[], None]] = None,
        exit_action: Optional[Callable[[], None]] = None,
        is_final: bool = False,
    ) -> None:
        """Add a state to the machine."""
        config = StateConfig(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            is_final=is_final,
        )
        self._states[name] = config

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], None]] = None,
    ) -> None:
        """Add a state transition."""
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
        )
        self._transitions.append(transition)

    def trigger_event(self, event: str) -> bool:
        """
        Trigger an event and attempt transition.

        Args:
            event: Event name

        Returns:
            True if transition occurred
        """
        for transition in self._transitions:
            if transition.from_state == self._current_state and transition.event == event:
                if transition.guard and not transition.guard():
                    logger.debug(f"Guard failed for transition: {event}")
                    return False

                self._execute_transition(transition)
                return True

        logger.debug(f"No transition for event '{event}' from state '{self._current_state}'")
        return False

    def _execute_transition(self, transition: StateTransition) -> None:
        """Execute a state transition."""
        old_state = self._current_state

        if self._states[old_state].exit_action:
            self._states[old_state].exit_action()

        if transition.action:
            transition.action()

        self._current_state = transition.to_state

        if self._states[self._current_state].entry_action:
            self._states[self._current_state].entry_action()

        self._transition_history.append((old_state, transition.to_state, transition.event))

        logger.info(f"Transition: {old_state} -> {transition.to_state} ({transition.event})")

    def get_current_state(self) -> str:
        """Get current state name."""
        return self._current_state

    def is_in_state(self, state: str) -> bool:
        """Check if in specified state."""
        return self._current_state == state

    def get_history(self) -> List[tuple]:
        """Get transition history."""
        return self._transition_history.copy()

    def set_state_data(self, key: str, value: Any) -> None:
        """Set data associated with current state."""
        self._state_data[key] = value

    def get_state_data(self, key: str) -> Any:
        """Get data associated with current state."""
        return self._state_data.get(key)


def create_ui_state_machine(initial_state: str) -> UIStateMachine:
    """Factory function."""
    return UIStateMachine(initial_state)
