"""State machine action for managing state transitions.

Provides configurable state machine with guards,
actions, and transition history.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class TransitionType(Enum):
    EXTERNAL = "external"
    INTERNAL = "internal"


@dataclass
class State:
    name: str
    is_initial: bool = False
    is_final: bool = False
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None


@dataclass
class Transition:
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL


@dataclass
class StateHistory:
    state: str
    event: str
    timestamp: float
    metadata: dict[str, Any] = field(default_factory=dict)


class StateMachineAction:
    """Configurable state machine with guards and actions.

    Args:
        initial_state: Initial state name.
        enable_history: Enable transition history.
        max_history: Maximum history entries.
    """

    def __init__(
        self,
        initial_state: str = "initial",
        enable_history: bool = True,
        max_history: int = 1000,
    ) -> None:
        self._states: Dict[str, State] = {}
        self._transitions: List[Transition] = []
        self._current_state: Optional[str] = initial_state
        self._initial_state = initial_state
        self._enable_history = enable_history
        self._max_history = max_history
        self._history: List[StateHistory] = []
        self._transition_handlers: Dict[str, List[Callable]] = {
            "on_transition": [],
            "on_state_change": [],
        }

    def add_state(
        self,
        name: str,
        is_initial: bool = False,
        is_final: bool = False,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
    ) -> bool:
        """Add a state to the state machine.

        Args:
            name: State name.
            is_initial: Is initial state.
            is_final: Is final state.
            entry_action: Action to execute on entry.
            exit_action: Action to execute on exit.

        Returns:
            True if added successfully.
        """
        if name in self._states:
            logger.warning(f"State already exists: {name}")
            return False

        state = State(
            name=name,
            is_initial=is_initial,
            is_final=is_final,
            entry_action=entry_action,
            exit_action=exit_action,
        )

        self._states[name] = state

        if is_initial:
            self._initial_state = name
            self._current_state = name

        logger.debug(f"Added state: {name}")
        return True

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable] = None,
        transition_type: TransitionType = TransitionType.EXTERNAL,
    ) -> bool:
        """Add a state transition.

        Args:
            from_state: Source state.
            to_state: Target state.
            event: Triggering event.
            guard: Optional guard condition.
            action: Optional transition action.
            transition_type: Transition type.

        Returns:
            True if added successfully.
        """
        if from_state not in self._states:
            logger.error(f"Source state not found: {from_state}")
            return False

        if to_state not in self._states:
            logger.error(f"Target state not found: {to_state}")
            return False

        transition = Transition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
            transition_type=transition_type,
        )

        self._transitions.append(transition)
        logger.debug(f"Added transition: {from_state} --[{event}]--> {to_state}")
        return True

    def get_current_state(self) -> Optional[str]:
        """Get the current state.

        Returns:
            Current state name.
        """
        return self._current_state

    def can_handle_event(self, event: str) -> bool:
        """Check if current state can handle an event.

        Args:
            event: Event name.

        Returns:
            True if transition is possible.
        """
        if not self._current_state:
            return False

        for transition in self._transitions:
            if (transition.from_state == self._current_state and
                    transition.event == event):
                if transition.guard is None or transition.guard():
                    return True
        return False

    def send_event(
        self,
        event: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Send an event to the state machine.

        Args:
            event: Event name.
            metadata: Optional event metadata.

        Returns:
            True if transition was made.
        """
        if not self._current_state:
            logger.error("State machine not initialized")
            return False

        for transition in self._transitions:
            if (transition.from_state == self._current_state and
                    transition.event == event):
                if transition.guard and not transition.guard():
                    logger.debug(f"Guard rejected transition for event: {event}")
                    continue

                old_state = self._current_state

                if transition.transition_type == TransitionType.EXTERNAL:
                    old_state_obj = self._states.get(old_state)
                    if old_state_obj and old_state_obj.exit_action:
                        old_state_obj.exit_action()

                new_state_obj = self._states.get(transition.to_state)
                if new_state_obj and new_state_obj.entry_action:
                    new_state_obj.entry_action()

                if transition.action:
                    transition.action()

                self._current_state = transition.to_state

                if self._enable_history:
                    self._history.append(StateHistory(
                        state=self._current_state,
                        event=event,
                        timestamp=time.time(),
                        metadata=metadata or {},
                    ))
                    if len(self._history) > self._max_history:
                        self._history.pop(0)

                for handler in self._transition_handlers["on_transition"]:
                    try:
                        handler(old_state, transition.to_state, event)
                    except Exception as e:
                        logger.error(f"Transition handler error: {e}")

                for handler in self._transition_handlers["on_state_change"]:
                    try:
                        handler(self._current_state)
                    except Exception as e:
                        logger.error(f"State change handler error: {e}")

                logger.debug(f"Transition: {old_state} --[{event}]--> {transition.to_state}")
                return True

        logger.debug(f"No transition for event {event} from state {self._current_state}")
        return False

    def is_in_final_state(self) -> bool:
        """Check if state machine is in a final state.

        Returns:
            True if in final state.
        """
        if not self._current_state:
            return False

        state = self._states.get(self._current_state)
        return state.is_final if state else False

    def reset(self) -> bool:
        """Reset state machine to initial state.

        Returns:
            True if reset.
        """
        self._current_state = self._initial_state
        if self._enable_history:
            self._history.clear()
        return True

    def register_transition_handler(
        self,
        handler_type: str,
        handler: Callable,
    ) -> None:
        """Register a transition event handler.

        Args:
            handler_type: Handler type ('on_transition', 'on_state_change').
            handler: Callback function.
        """
        if handler_type in self._transition_handlers:
            self._transition_handlers[handler_type].append(handler)

    def get_available_events(self) -> List[str]:
        """Get events available from current state.

        Returns:
            List of event names.
        """
        if not self._current_state:
            return []

        return [
            t.event for t in self._transitions
            if t.from_state == self._current_state
        ]

    def get_history(self, limit: int = 100) -> List[StateHistory]:
        """Get transition history.

        Args:
            limit: Maximum entries.

        Returns:
            List of history entries (newest first).
        """
        return self._history[-limit:][::-1]

    def get_stats(self) -> dict[str, Any]:
        """Get state machine statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "current_state": self._current_state,
            "total_states": len(self._states),
            "total_transitions": len(self._transitions),
            "history_size": len(self._history),
            "is_final": self.is_in_final_state(),
        }
