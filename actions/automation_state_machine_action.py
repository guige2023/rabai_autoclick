"""
Automation State Machine Action Module.

Hierarchical state machine for complex automation workflows
with guards, actions, transitions, and history tracking.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TransitionType(Enum):
    """State transition types."""
    EXTERNAL = "external"
    INTERNAL = "internal"


@dataclass
class Transition:
    """
    State transition definition.

    Attributes:
        source: Source state identifier.
        target: Target state identifier.
        event: Event that triggers this transition.
        guard: Optional guard condition function.
        action: Optional action to execute on transition.
        transition_type: External or internal transition.
    """
    source: str
    target: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], Any]] = None
    transition_type: TransitionType = TransitionType.EXTERNAL


@dataclass
class State:
    """
    State definition.

    Attributes:
        name: State identifier.
        entry_action: Action to execute on state entry.
        exit_action: Action to execute on state exit.
        do_action: Action to execute while in state.
    """
    name: str
    entry_action: Optional[Callable[[], Any]] = None
    exit_action: Optional[Callable[[], Any]] = None
    do_action: Optional[Callable[[], Any]] = None


@dataclass
class StateHistoryEntry:
    """Record of a state change."""
    from_state: Optional[str]
    to_state: str
    event: str
    timestamp: float
    transition_duration: float = 0.0


class AutomationStateMachineAction:
    """
    Hierarchical state machine for automation workflows.

    Example:
        sm = AutomationStateMachineAction("workflow")
        sm.add_state("idle", entry_action=init_handler)
        sm.add_state("running", do_action=work_handler)
        sm.add_transition("idle", "running", "start")
        sm.send_event("start")
    """

    def __init__(self, name: str = "state_machine"):
        """
        Initialize state machine action.

        Args:
            name: State machine identifier.
        """
        self.name = name
        self.states: dict[str, State] = {}
        self.transitions: list[Transition] = []
        self.initial_state: Optional[str] = None
        self.current_state: Optional[str] = None
        self._history: list[StateHistoryEntry] = []
        self._state_enter_time: float = 0.0
        self._event_queue: list[str] = []

    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        do_action: Optional[Callable] = None,
        initial: bool = False
    ) -> "AutomationStateMachineAction":
        """
        Add a state to the state machine.

        Args:
            name: State identifier.
            entry_action: Action on state entry.
            exit_action: Action on state exit.
            do_action: Action while in state.
            initial: Set as initial state if True.

        Returns:
            Self for method chaining.
        """
        state = State(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            do_action=do_action
        )
        self.states[name] = state

        if initial or self.initial_state is None:
            self.initial_state = name

        logger.debug(f"Added state: {name}")
        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None,
        transition_type: TransitionType = TransitionType.EXTERNAL
    ) -> "AutomationStateMachineAction":
        """
        Add a transition between states.

        Args:
            source: Source state name.
            target: Target state name.
            event: Event name that triggers transition.
            guard: Optional guard condition.
            action: Optional transition action.
            transition_type: External or internal.

        Returns:
            Self for method chaining.
        """
        if source not in self.states:
            raise ValueError(f"Source state '{source}' does not exist")
        if target not in self.states:
            raise ValueError(f"Target state '{target}' does not exist")

        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
            transition_type=transition_type
        )

        self.transitions.append(transition)
        logger.debug(f"Added transition: {source} --[{event}]--> {target}")
        return self

    def send_event(self, event: str) -> bool:
        """
        Send an event to trigger a transition.

        Args:
            event: Event name.

        Returns:
            True if transition occurred, False otherwise.
        """
        import time

        if self.current_state is None:
            if self.initial_state:
                self.current_state = self.initial_state
                self._state_enter_time = time.time()
                self._execute_state_action(self.current_state, "entry")
            else:
                logger.error("No initial state defined")
                return False

        matching_transitions = [
            t for t in self.transitions
            if t.source == self.current_state and t.event == event
        ]

        for transition in matching_transitions:
            if transition.guard and not transition.guard():
                logger.debug(f"Guard failed for transition {self.current_state} --[{event}]--> {transition.target}")
                continue

            previous_state = self.current_state
            enter_time = time.time()

            self._execute_state_action(previous_state, "exit")

            if transition.action:
                try:
                    transition.action()
                except Exception as e:
                    logger.error(f"Transition action failed: {e}")

            self.current_state = transition.target
            self._state_enter_time = time.time()
            self._execute_state_action(transition.target, "entry")

            transition_duration = time.time() - enter_time

            self._history.append(StateHistoryEntry(
                from_state=previous_state,
                to_state=transition.target,
                event=event,
                timestamp=enter_time,
                transition_duration=transition_duration
            ))

            logger.info(f"State machine: {previous_state} --[{event}]--> {transition.target}")
            return True

        logger.debug(f"No matching transition for event '{event}' in state '{self.current_state}'")
        return False

    def _execute_state_action(self, state_name: str, action_type: str) -> None:
        """Execute state entry, exit, or do action."""
        state = self.states.get(state_name)
        if not state:
            return

        action = None
        if action_type == "entry":
            action = state.entry_action
        elif action_type == "exit":
            action = state.exit_action
        elif action_type == "do":
            action = state.do_action

        if action:
            try:
                action()
            except Exception as e:
                logger.error(f"State action failed ({action_type}): {e}")

    def get_current_state(self) -> Optional[str]:
        """Get current state name."""
        return self.current_state

    def get_history(self, limit: int = 100) -> list[StateHistoryEntry]:
        """Get recent state history."""
        return self._history[-limit:]

    def get_allowed_events(self) -> list[str]:
        """Get list of events that can be triggered from current state."""
        return [
            t.event for t in self.transitions
            if t.source == self.current_state
        ]

    def is_in_state(self, state_name: str) -> bool:
        """Check if currently in specified state."""
        return self.current_state == state_name

    def reset(self) -> None:
        """Reset state machine to initial state."""
        if self.initial_state:
            self.current_state = self.initial_state
            import time
            self._state_enter_time = time.time()
            self._execute_state_action(self.current_state, "entry")
            logger.info(f"State machine reset to '{self.initial_state}'")

    def get_state_info(self) -> dict:
        """Get information about all states and transitions."""
        return {
            "name": self.name,
            "current_state": self.current_state,
            "initial_state": self.initial_state,
            "states": list(self.states.keys()),
            "allowed_events": self.get_allowed_events(),
            "history_length": len(self._history)
        }

    def create_submachine(self, name: str, states: list[str], initial: str) -> "AutomationStateMachineAction":
        """
        Create a sub-state machine for hierarchical states.

        Args:
            name: Sub-machine identifier.
            states: List of state names.
            initial: Initial state for sub-machine.

        Returns:
            New sub-state machine instance.
        """
        sub_sm = AutomationStateMachineAction(f"{self.name}.{name}")

        for state in states:
            if state in self.states:
                sub_sm.add_state(
                    name=state,
                    entry_action=self.states[state].entry_action,
                    exit_action=self.states[state].exit_action,
                    do_action=self.states[state].do_action
                )

        sub_sm.initial_state = initial
        sub_sm.current_state = initial

        return sub_sm
