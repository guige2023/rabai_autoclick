"""Automation State Machine Action Module.

Provides finite state machine implementation for automation workflows with
support for hierarchical states, guards, actions, parallel states,
and event-driven transitions.
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class StateType(Enum):
    """Types of states."""
    INITIAL = "initial"
    NORMAL = "normal"
    FINAL = "final"
    CHOICE = "choice"
    HISTORY = "history"
    PARALLEL = "parallel"
    COMPOSITE = "composite"


class TransitionType(Enum):
    """Types of state transitions."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    LOCAL = "local"


@dataclass
class State:
    """A state in the state machine."""
    state_id: str
    state_type: StateType = StateType.NORMAL
    entry_actions: List[Callable] = field(default_factory=list)
    exit_actions: List[Callable] = field(default_factory=list)
    do_actions: List[Callable] = field(default_factory=list)
    substates: Dict[str, "State"] = field(default_factory=dict)
    initial_state: Optional[str] = None
    history_type: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """A transition between states."""
    transition_id: str
    source_state: str
    target_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL
    priority: int = 0


@dataclass
class StateMachineConfig:
    """Configuration for state machine."""
    name: str = "state_machine"
    initial_state: Optional[str] = None
    strict_mode: bool = True
    log_transitions: bool = True
    allow_reentry: bool = True
    parallel_execution: bool = False


@dataclass
class StateChangeEvent:
    """Event record of a state change."""
    timestamp: datetime
    from_state: Optional[str]
    to_state: str
    event: str
    transition_id: Optional[str]
    duration_ms: float


@dataclass
class StateMachineSnapshot:
    """Snapshot of state machine state."""
    current_states: Set[str]
    history: List[StateChangeEvent]
    active_timers: Dict[str, datetime]
    custom_data: Dict[str, Any]


class StateMachine:
    """Finite state machine implementation."""

    def __init__(self, config: StateMachineConfig):
        self._config = config
        self._states: Dict[str, State] = {}
        self._transitions: Dict[str, List[Transition]] = defaultdict(list)
        self._initial_state: Optional[str] = None
        self._current_states: Set[str] = set()
        self._history: List[StateChangeEvent] = []
        self._is_active = False
        self._lock = threading.RLock()
        self._custom_data: Dict[str, Any] = {}

    def add_state(self, state: State) -> ActionResult:
        """Add a state to the machine."""
        try:
            with self._lock:
                if state.state_id in self._states:
                    return ActionResult(success=False, error=f"State {state.state_id} already exists")

                self._states[state.state_id] = state

                if state.state_type == StateType.INITIAL:
                    self._initial_state = state.state_id

                return ActionResult(success=True, data={"state_id": state.state_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_transition(
        self,
        transition_id: str,
        source_state: str,
        target_state: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None
    ) -> ActionResult:
        """Add a transition to the machine."""
        try:
            with self._lock:
                if source_state not in self._states:
                    return ActionResult(success=False, error=f"Source state {source_state} not found")
                if target_state not in self._states:
                    return ActionResult(success=False, error=f"Target state {target_state} not found")

                transition = Transition(
                    transition_id=transition_id,
                    source_state=source_state,
                    target_state=target_state,
                    event=event,
                    guard=guard,
                    action=action
                )
                self._transitions[source_state].append(transition)
                self._transitions[source_state].sort(key=lambda t: t.priority, reverse=True)

                return ActionResult(success=True, data={"transition_id": transition_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def initialize(self) -> ActionResult:
        """Initialize the state machine."""
        try:
            with self._lock:
                if not self._initial_state:
                    if self._config.initial_state and self._config.initial_state in self._states:
                        self._initial_state = self._config.initial_state
                    else:
                        return ActionResult(success=False, error="No initial state defined")

                self._current_states = {self._initial_state}
                self._is_active = True

                state = self._states[self._initial_state]
                self._execute_actions(state.entry_actions)

                return ActionResult(success=True, data={
                    "initial_state": self._initial_state,
                    "current_states": list(self._current_states)
                })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def send_event(self, event: str, event_data: Optional[Dict[str, Any]] = None) -> ActionResult:
        """Send an event to trigger a transition."""
        try:
            with self._lock:
                if not self._is_active:
                    return ActionResult(success=False, error="State machine not initialized")

                event_data = event_data or {}
                transitioned = False

                for current_state in list(self._current_states):
                    if current_state not in self._transitions:
                        continue

                    for transition in self._transitions[current_state]:
                        if transition.event != event:
                            continue

                        if transition.guard and not transition.guard():
                            continue

                        start_time = time.time()

                        if transition.action:
                            try:
                                transition.action()
                            except Exception as e:
                                logger.warning(f"Transition action failed: {e}")

                        old_state = current_state
                        old_state_obj = self._states.get(old_state)
                        if old_state_obj:
                            self._execute_actions(old_state_obj.exit_actions)

                        self._current_states.discard(old_state)
                        self._current_states.add(transition.target_state)

                        new_state_obj = self._states.get(transition.target_state)
                        if new_state_obj:
                            self._execute_actions(new_state_obj.entry_actions)

                        duration_ms = (time.time() - start_time) * 1000

                        change_event = StateChangeEvent(
                            timestamp=datetime.now(),
                            from_state=old_state,
                            to_state=transition.target_state,
                            event=event,
                            transition_id=transition.transition_id,
                            duration_ms=duration_ms
                        )
                        self._history.append(change_event)

                        if self._config.log_transitions:
                            logger.info(
                                f"Transition: {old_state} -> {transition.target_state} "
                                f"(event: {event})"
                            )

                        transitioned = True

                return ActionResult(
                    success=transitioned,
                    data={
                        "event": event,
                        "current_states": list(self._current_states),
                        "transitioned": transitioned
                    }
                )
        except Exception as e:
            logger.exception("Event processing failed")
            return ActionResult(success=False, error=str(e))

    def _execute_actions(self, actions: List[Callable]):
        """Execute state actions."""
        for action in actions:
            try:
                action()
            except Exception as e:
                logger.warning(f"State action failed: {e}")

    def get_current_states(self) -> Set[str]:
        """Get current active states."""
        with self._lock:
            return self._current_states.copy()

    def get_history(self, limit: Optional[int] = None) -> List[StateChangeEvent]:
        """Get state change history."""
        with self._lock:
            if limit:
                return self._history[-limit:]
            return self._history.copy()

    def create_snapshot(self) -> StateMachineSnapshot:
        """Create a snapshot of current state."""
        with self._lock:
            return StateMachineSnapshot(
                current_states=self._current_states.copy(),
                history=self._history.copy(),
                history=[],
                active_timers={},
                custom_data=self._custom_data.copy()
            )

    def restore_snapshot(self, snapshot: StateMachineSnapshot) -> ActionResult:
        """Restore state from a snapshot."""
        try:
            with self._lock:
                self._current_states = snapshot.current_states.copy()
                self._history = snapshot.history.copy()
                self._custom_data = snapshot.custom_data.copy()
                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class AutomationStateMachineAction(BaseAction):
    """Action for state machine automation."""

    def __init__(self):
        super().__init__(name="automation_state_machine")
        self._machines: Dict[str, StateMachine] = {}

    def create_machine(
        self,
        machine_id: str,
        config: Optional[StateMachineConfig] = None
    ) -> ActionResult:
        """Create a new state machine."""
        try:
            if machine_id in self._machines:
                return ActionResult(success=False, error=f"Machine {machine_id} already exists")

            config = config or StateMachineConfig(name=machine_id)
            machine = StateMachine(config)
            self._machines[machine_id] = machine

            return ActionResult(success=True, data={"machine_id": machine_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_state(
        self,
        machine_id: str,
        state_id: str,
        state_type: StateType = StateType.NORMAL
    ) -> ActionResult:
        """Add a state to a machine."""
        try:
            if machine_id not in self._machines:
                return ActionResult(success=False, error=f"Machine {machine_id} not found")

            state = State(state_id=state_id, state_type=state_type)
            return self._machines[machine_id].add_state(state)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_transition(
        self,
        machine_id: str,
        transition_id: str,
        source_state: str,
        target_state: str,
        event: str
    ) -> ActionResult:
        """Add a transition to a machine."""
        try:
            if machine_id not in self._machines:
                return ActionResult(success=False, error=f"Machine {machine_id} not found")

            return self._machines[machine_id].add_transition(
                transition_id, source_state, target_state, event
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def initialize(self, machine_id: str) -> ActionResult:
        """Initialize a state machine."""
        try:
            if machine_id not in self._machines:
                return ActionResult(success=False, error=f"Machine {machine_id} not found")

            return self._machines[machine_id].initialize()
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def send_event(
        self,
        machine_id: str,
        event: str,
        event_data: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Send an event to a state machine."""
        try:
            if machine_id not in self._machines:
                return ActionResult(success=False, error=f"Machine {machine_id} not found")

            return self._machines[machine_id].send_event(event, event_data)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_status(self, machine_id: str) -> ActionResult:
        """Get status of a state machine."""
        try:
            if machine_id not in self._machines:
                return ActionResult(success=False, error=f"Machine {machine_id} not found")

            machine = self._machines[machine_id]
            return ActionResult(success=True, data={
                "machine_id": machine_id,
                "current_states": list(machine.get_current_states()),
                "history_count": len(machine.get_history())
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute state machine action."""
        try:
            action = params.get("action")
            machine_id = params.get("machine_id")

            if action == "create":
                return self.create_machine(params.get("machine_id", "default"))
            elif action == "add_state" and machine_id:
                return self.add_state(
                    machine_id,
                    params["state_id"],
                    StateType(params.get("state_type", "normal"))
                )
            elif action == "add_transition" and machine_id:
                return self.add_transition(
                    machine_id,
                    params["transition_id"],
                    params["source_state"],
                    params["target_state"],
                    params["event"]
                )
            elif action == "initialize" and machine_id:
                return self.initialize(machine_id)
            elif action == "send_event" and machine_id:
                return self.send_event(
                    machine_id,
                    params["event"],
                    params.get("event_data")
                )
            elif action == "status" and machine_id:
                return self.get_status(machine_id)
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
