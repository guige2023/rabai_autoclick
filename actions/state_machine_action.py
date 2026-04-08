"""State machine action module for RabAI AutoClick.

Provides state machine operations:
- StateMachine: Generic state machine
- StateTransition: State transition management
- StateValidator: Validate state transitions
- StateHistory: Track state history
- StateMonitor: Monitor state changes
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """Transition types."""
    INTERNAL = "internal"
    EXTERNAL = "external"
    LOCAL = "local"


@dataclass
class State:
    """State definition."""
    name: str
    is_initial: bool = False
    is_final: bool = False
    is_history: bool = False
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """State transition."""
    source: str
    target: str
    event: str
    guard: Optional[Callable[[Dict], bool]] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateSnapshot:
    """State snapshot."""
    state: str
    timestamp: float
    event: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


class StateMachine:
    """Generic state machine."""

    def __init__(self, name: str):
        self.name = name
        self._states: Dict[str, State] = {}
        self._transitions: Dict[Tuple[str, str], List[Transition]] = {}
        self._transitions_by_event: Dict[str, List[Transition]] = {}
        self._initial_state: Optional[str] = None
        self._current_state: Optional[str] = None
        self._history: List[StateSnapshot] = []
        self._context: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def add_state(
        self,
        name: str,
        is_initial: bool = False,
        is_final: bool = False,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        metadata: Optional[Dict] = None,
    ) -> "StateMachine":
        """Add a state."""
        state = State(
            name=name,
            is_initial=is_initial,
            is_final=is_final,
            entry_action=entry_action,
            exit_action=exit_action,
            metadata=metadata or {},
        )
        self._states[name] = state

        if is_initial:
            self._initial_state = name

        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None,
        transition_type: TransitionType = TransitionType.EXTERNAL,
    ) -> "StateMachine":
        """Add a transition."""
        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
            transition_type=transition_type,
        )

        if source not in self._transitions:
            self._transitions[source] = []
        self._transitions[source].append(transition)

        if event not in self._transitions_by_event:
            self._transitions_by_event[event] = []
        self._transitions_by_event[event].append(transition)

        return self

    def initialize(self, context: Optional[Dict] = None) -> bool:
        """Initialize state machine."""
        with self._lock:
            if not self._initial_state:
                return False

            self._current_state = self._initial_state
            self._context = context or {}
            self._history.clear()

            self._record_snapshot(self._initial_state, "init")

            state = self._states.get(self._initial_state)
            if state and state.entry_action:
                try:
                    state.entry_action(self._context)
                except Exception:
                    pass

            return True

    def send_event(self, event: str, event_data: Optional[Dict] = None) -> Tuple[bool, str]:
        """Send an event to state machine."""
        with self._lock:
            if not self._current_state:
                return False, "Not initialized"

            current = self._current_state
            transitions = self._get_possible_transitions(current, event)

            for transition in transitions:
                if transition.guard and not transition.guard(self._context):
                    continue

                if transition.action:
                    try:
                        result = transition.action(self._context, event_data or {})
                        if result is False:
                            continue
                    except Exception:
                        pass

                self._execute_transition(transition, event)
                return True, "Transition executed"

            return False, f"No valid transition for event '{event}' from state '{current}'"

    def _get_possible_transitions(self, state: str, event: str) -> List[Transition]:
        """Get possible transitions."""
        result = []

        if (state, "*") in self._transitions:
            result.extend(self._transitions[(state, "*")])

        if state in self._transitions:
            for t in self._transitions[state]:
                if t.event == event or t.event == "*":
                    result.append(t)

        return result

    def _execute_transition(self, transition: Transition, event: str):
        """Execute a transition."""
        if transition.transition_type == TransitionType.EXTERNAL:
            old_state = self._states.get(self._current_state)
            if old_state and old_state.exit_action:
                try:
                    old_state.exit_action(self._context)
                except Exception:
                    pass

        old_state_name = self._current_state
        self._current_state = transition.target

        self._record_snapshot(transition.target, event, {"from": old_state_name})

        new_state = self._states.get(transition.target)
        if new_state and new_state.entry_action:
            try:
                new_state.entry_action(self._context)
            except Exception:
                pass

    def _record_snapshot(self, state: str, event: Optional[str] = None, extra: Optional[Dict] = None):
        """Record state snapshot."""
        snapshot = StateSnapshot(
            state=state,
            timestamp=time.time(),
            event=event,
            context=dict(self._context),
        )
        if extra:
            snapshot.context.update(extra)
        self._history.append(snapshot)

    def get_current_state(self) -> Optional[str]:
        """Get current state."""
        return self._current_state

    def get_history(self, limit: Optional[int] = None) -> List[StateSnapshot]:
        """Get state history."""
        if limit:
            return self._history[-limit:]
        return list(self._history)

    def get_context(self) -> Dict[str, Any]:
        """Get state context."""
        return dict(self._context)

    def is_final(self) -> bool:
        """Check if in final state."""
        if not self._current_state:
            return False
        state = self._states.get(self._current_state)
        return state.is_final if state else False

    def can_handle_event(self, event: str) -> bool:
        """Check if event can be handled."""
        if not self._current_state:
            return False
        transitions = self._get_possible_transitions(self._current_state, event)
        for t in transitions:
            if t.guard is None or t.guard(self._context):
                return True
        return False


class StateHistory:
    """Track state history."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._history: List[StateSnapshot] = []
        self._lock = threading.RLock()

    def record(self, snapshot: StateSnapshot):
        """Record a snapshot."""
        with self._lock:
            self._history.append(snapshot)
            if len(self._history) > self.max_size:
                self._history.pop(0)

    def get_history(
        self,
        state: Optional[str] = None,
        from_time: Optional[float] = None,
        to_time: Optional[float] = None,
        limit: int = 100,
    ) -> List[StateSnapshot]:
        """Get filtered history."""
        with self._lock:
            result = list(self._history)

            if state:
                result = [s for s in result if s.state == state]

            if from_time:
                result = [s for s in result if s.timestamp >= from_time]

            if to_time:
                result = [s for s in result if s.timestamp <= to_time]

            return result[-limit:]

    def clear(self):
        """Clear history."""
        with self._lock:
            self._history.clear()

    def get_statistics(self) -> Dict[str, Any]:
        """Get history statistics."""
        with self._lock:
            if not self._history:
                return {}

            state_counts: Dict[str, int] = {}
            for snapshot in self._history:
                state_counts[snapshot.state] = state_counts.get(snapshot.state, 0) + 1

            return {
                "total_snapshots": len(self._history),
                "states": state_counts,
                "first_timestamp": self._history[0].timestamp,
                "last_timestamp": self._history[-1].timestamp,
            }


class StateMachineAction(BaseAction):
    """State machine action."""
    action_type = "state_machine"
    display_name = "状态机"
    description = "状态机和工作流"

    def __init__(self):
        super().__init__()
        self._machines: Dict[str, StateMachine] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create_machine(params)
            elif operation == "add_state":
                return self._add_state(params)
            elif operation == "add_transition":
                return self._add_transition(params)
            elif operation == "init":
                return self._initialize(params)
            elif operation == "send":
                return self._send_event(params)
            elif operation == "state":
                return self._get_current_state(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "can_handle":
                return self._can_handle(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"State machine error: {str(e)}")

    def _create_machine(self, params: Dict) -> ActionResult:
        """Create a state machine."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        machine = StateMachine(name)
        self._machines[name] = machine

        return ActionResult(success=True, message=f"State machine '{name}' created")

    def _add_state(self, params: Dict) -> ActionResult:
        """Add a state to machine."""
        machine_name = params.get("machine")
        state_name = params.get("state")
        is_initial = params.get("is_initial", False)
        is_final = params.get("is_final", False)

        if not machine_name or not state_name:
            return ActionResult(success=False, message="machine and state are required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        machine.add_state(
            name=state_name,
            is_initial=is_initial,
            is_final=is_final,
        )

        return ActionResult(success=True, message=f"State '{state_name}' added")

    def _add_transition(self, params: Dict) -> ActionResult:
        """Add a transition."""
        machine_name = params.get("machine")
        source = params.get("source")
        target = params.get("target")
        event = params.get("event")

        if not all([machine_name, source, target, event]):
            return ActionResult(success=False, message="machine, source, target, and event are required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        machine.add_transition(source, target, event)

        return ActionResult(success=True, message=f"Transition {source} -> {target} on '{event}' added")

    def _initialize(self, params: Dict) -> ActionResult:
        """Initialize state machine."""
        machine_name = params.get("machine")
        init_context = params.get("context", {})

        if not machine_name:
            return ActionResult(success=False, message="machine is required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        success = machine.initialize(init_context)
        return ActionResult(
            success=success,
            message=f"Initialized to '{machine.get_current_state()}'" if success else "Failed to initialize",
        )

    def _send_event(self, params: Dict) -> ActionResult:
        """Send event to machine."""
        machine_name = params.get("machine")
        event = params.get("event")
        event_data = params.get("data", {})

        if not machine_name or not event:
            return ActionResult(success=False, message="machine and event are required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        success, message = machine.send_event(event, event_data)
        return ActionResult(
            success=success,
            message=message,
            data={"current_state": machine.get_current_state()},
        )

    def _get_current_state(self, params: Dict) -> ActionResult:
        """Get current state."""
        machine_name = params.get("machine")

        if not machine_name:
            return ActionResult(success=False, message="machine is required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        current = machine.get_current_state()
        return ActionResult(
            success=True,
            message=f"Current state: {current}",
            data={"state": current, "is_final": machine.is_final()},
        )

    def _get_history(self, params: Dict) -> ActionResult:
        """Get state history."""
        machine_name = params.get("machine")
        limit = params.get("limit", 100)

        if not machine_name:
            return ActionResult(success=False, message="machine is required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        history = machine.get_history(limit)

        return ActionResult(
            success=True,
            message=f"{len(history)} history entries",
            data={
                "history": [
                    {"state": h.state, "timestamp": h.timestamp, "event": h.event}
                    for h in history
                ]
            },
        )

    def _can_handle(self, params: Dict) -> ActionResult:
        """Check if event can be handled."""
        machine_name = params.get("machine")
        event = params.get("event")

        if not machine_name or not event:
            return ActionResult(success=False, message="machine and event are required")

        machine = self._machines.get(machine_name)
        if not machine:
            return ActionResult(success=False, message=f"Machine '{machine_name}' not found")

        can_handle = machine.can_handle_event(event)
        return ActionResult(
            success=True,
            message=f"Can handle: {can_handle}",
            data={"can_handle": can_handle},
        )
