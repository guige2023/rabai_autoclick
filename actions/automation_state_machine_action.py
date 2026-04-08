"""
Automation State Machine Action Module.

Implements a flexible state machine for workflow orchestration
with guards, actions, history, and event-driven transitions.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """Types of state transitions."""
    NORMAL = "normal"
    GUARDED = "guarded"
    FORCED = "forced"
    TIMEOUT = "timeout"


class StateMachineEvent(Enum):
    """Events that can trigger transitions."""
    START = "start"
    NEXT = "next"
    PREV = "prev"
    RESET = "reset"
    CUSTOM = "custom"


@dataclass
class State:
    """A state in the state machine."""
    name: str
    entry_action: Optional[str] = None
    exit_action: Optional[str] = None
    do_action: Optional[str] = None
    timeout_seconds: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """A transition between states."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[str] = None
    action: Optional[str] = None
    transition_type: TransitionType = TransitionType.NORMAL
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateHistoryEntry:
    """An entry in the state history."""
    timestamp: float
    from_state: Optional[str]
    to_state: str
    event: str
    transition_type: TransitionType
    duration_seconds: float
    metadata: Dict[str, Any]


@dataclass
class StateMachineSnapshot:
    """A snapshot of the state machine state."""
    instance_id: str
    current_state: str
    previous_state: Optional[str]
    context: Dict[str, Any]
    started_at: float
    last_updated: float
    history: List[Dict[str, Any]]


class StateMachine:
    """A state machine instance."""
    
    def __init__(
        self,
        instance_id: str,
        states: Dict[str, State],
        transitions: List[Transition],
        initial_state: str,
        context: Optional[Dict[str, Any]] = None
    ):
        self.instance_id = instance_id
        self.states = states
        self.transitions = transitions
        self._current_state = initial_state
        self._previous_state: Optional[str] = None
        self._context = context or {}
        self._history: deque = deque(maxlen=1000)
        self._started_at = time.time()
        self._last_updated = time.time()
        self._transition_index: Dict[Tuple[str, str], Transition] = {}
        self._lock = threading.RLock()
        
        for t in transitions:
            key = (t.from_state, t.event)
            self._transition_index[key] = t
    
    @property
    def current_state(self) -> str:
        with self._lock:
            return self._current_state
    
    def can_transition(self, event: str) -> bool:
        """Check if a transition is possible."""
        with self._lock:
            key = (self._current_state, event)
            if key not in self._transition_index:
                return False
            transition = self._transition_index[key]
            if transition.guard:
                return self._evaluate_guard(transition.guard)
            return True
    
    def _evaluate_guard(self, guard: str) -> bool:
        """Evaluate a guard condition."""
        try:
            if guard.startswith("lambda ") or "(" in guard:
                return bool(eval(guard, {"context": self._context}))
            return bool(self._context.get(guard, True))
        except Exception:
            return False
    
    def transition(self, event: str, event_data: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str], Optional[str]]:
        """Attempt to transition state machine."""
        with self._lock:
            key = (self._current_state, event)
            if key not in self._transition_index:
                return False, None, f"No transition from {self._current_state} on event {event}"
            
            transition = self._transition_index[key]
            
            if transition.guard and not self._evaluate_guard(transition.guard):
                return False, None, f"Guard condition failed: {transition.guard}"
            
            prev_state = self._current_state
            prev_time = self._last_updated
            
            entry_action = transition.action
            
            self._previous_state = prev_state
            self._current_state = transition.to_state
            self._last_updated = time.time()
            
            duration = self._last_updated - prev_time
            
            history_entry = StateHistoryEntry(
                timestamp=self._last_updated,
                from_state=prev_state,
                to_state=transition.to_state,
                event=event,
                transition_type=transition.transition_type,
                duration_seconds=duration,
                metadata=event_data or {}
            )
            self._history.append(history_entry)
            
            return True, transition.to_state, None
    
    def get_snapshot(self) -> StateMachineSnapshot:
        """Get a snapshot of current state."""
        with self._lock:
            return StateMachineSnapshot(
                instance_id=self.instance_id,
                current_state=self._current_state,
                previous_state=self._previous_state,
                context=dict(self._context),
                started_at=self._started_at,
                last_updated=self._last_updated,
                history=[
                    {
                        "timestamp": h.timestamp,
                        "from_state": h.from_state,
                        "to_state": h.to_state,
                        "event": h.event,
                        "transition_type": h.transition_type.value,
                        "duration_seconds": h.duration_seconds
                    }
                    for h in self._history
                ]
            )


class AutomationStateMachineAction(BaseAction):
    """Automation state machine action.
    
    Implements workflow orchestration using a flexible state machine
    with guards, actions, history tracking, and event-driven transitions.
    """
    action_type = "automation_state_machine"
    display_name = "自动化状态机"
    description = "工作流状态机编排"
    
    def __init__(self):
        super().__init__()
        self._machines: Dict[str, StateMachine] = {}
        self._lock = threading.RLock()
        self._action_handlers: Dict[str, Callable] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a state machine operation.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: Operation (create/transition/snapshot/history/query/stop)
                - machine_id: State machine instance ID
                - states: State definitions (for create)
                - transitions: Transition definitions (for create)
                - initial_state: Initial state (for create)
                - event: Event to trigger
                - event_data: Event payload
                - context: Machine context
                
        Returns:
            ActionResult with operation results.
        """
        start_time = time.time()
        
        operation = params.get("operation", "create")
        
        if operation == "create":
            return self._create_machine(params, start_time)
        elif operation == "transition":
            return self._transition_machine(params, start_time)
        elif operation == "snapshot":
            return self._get_snapshot(params, start_time)
        elif operation == "history":
            return self._get_history(params, start_time)
        elif operation == "query":
            return self._query_machine(params, start_time)
        elif operation == "stop":
            return self._stop_machine(params, start_time)
        elif operation == "reset":
            return self._reset_machine(params, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )
    
    def _create_machine(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new state machine."""
        machine_id = params.get("machine_id", str(uuid.uuid4()))
        states_def = params.get("states", {})
        transitions_def = params.get("transitions", [])
        initial_state = params.get("initial_state", "initial")
        init_context = params.get("context", {})
        
        states = {}
        for name, state_def in states_def.items():
            if isinstance(state_def, dict):
                states[name] = State(
                    name=name,
                    entry_action=state_def.get("entry_action"),
                    exit_action=state_def.get("exit_action"),
                    do_action=state_def.get("do_action"),
                    timeout_seconds=state_def.get("timeout_seconds"),
                    metadata=state_def.get("metadata", {})
                )
            else:
                states[name] = State(name=name)
        
        transitions = []
        for t_def in transitions_def:
            if isinstance(t_def, dict):
                transitions.append(Transition(
                    from_state=t_def["from_state"],
                    to_state=t_def["to_state"],
                    event=t_def["event"],
                    guard=t_def.get("guard"),
                    action=t_def.get("action"),
                    transition_type=TransitionType(t_def.get("type", "normal")),
                    metadata=t_def.get("metadata", {})
                ))
        
        if initial_state not in states:
            return ActionResult(
                success=False,
                message=f"Initial state not found: {initial_state}",
                duration=time.time() - start_time
            )
        
        machine = StateMachine(
            instance_id=machine_id,
            states=states,
            transitions=transitions,
            initial_state=initial_state,
            context=init_context
        )
        
        with self._lock:
            self._machines[machine_id] = machine
        
        return ActionResult(
            success=True,
            message=f"State machine created: {machine_id}",
            data={
                "machine_id": machine_id,
                "initial_state": initial_state,
                "states": list(states.keys()),
                "transitions": len(transitions)
            },
            duration=time.time() - start_time
        )
    
    def _transition_machine(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Trigger a transition in a state machine."""
        machine_id = params.get("machine_id")
        event = params.get("event", "next")
        event_data = params.get("event_data", {})
        
        if not machine_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: machine_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine not found: {machine_id}",
                    duration=time.time() - start_time
                )
            machine = self._machines[machine_id]
        
        success, new_state, error = machine.transition(event, event_data)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Transitioned to state: {new_state}",
                data={
                    "machine_id": machine_id,
                    "previous_state": machine._previous_state,
                    "current_state": new_state,
                    "event": event
                },
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=error or "Transition failed",
                data={
                    "machine_id": machine_id,
                    "current_state": machine.current_state
                },
                duration=time.time() - start_time
            )
    
    def _get_snapshot(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a snapshot of a state machine."""
        machine_id = params.get("machine_id")
        
        if not machine_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: machine_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine not found: {machine_id}",
                    duration=time.time() - start_time
                )
            machine = self._machines[machine_id]
        
        snapshot = machine.get_snapshot()
        
        return ActionResult(
            success=True,
            message=f"Snapshot retrieved for {machine_id}",
            data={
                "instance_id": snapshot.instance_id,
                "current_state": snapshot.current_state,
                "previous_state": snapshot.previous_state,
                "context": snapshot.context,
                "started_at": snapshot.started_at,
                "last_updated": snapshot.last_updated,
                "history_length": len(snapshot.history)
            },
            duration=time.time() - start_time
        )
    
    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get transition history for a state machine."""
        machine_id = params.get("machine_id")
        limit = params.get("limit", 100)
        
        if not machine_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: machine_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine not found: {machine_id}",
                    duration=time.time() - start_time
                )
            machine = self._machines[machine_id]
        
        history = list(machine._history)[-limit:]
        
        return ActionResult(
            success=True,
            message=f"History retrieved: {len(history)} entries",
            data={
                "machine_id": machine_id,
                "current_state": machine.current_state,
                "history": [
                    {
                        "timestamp": h.timestamp,
                        "from_state": h.from_state,
                        "to_state": h.to_state,
                        "event": h.event,
                        "transition_type": h.transition_type.value,
                        "duration_seconds": h.duration_seconds
                    }
                    for h in history
                ]
            },
            duration=time.time() - start_time
        )
    
    def _query_machine(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Query state machine information."""
        machine_id = params.get("machine_id")
        
        with self._lock:
            if machine_id:
                if machine_id not in self._machines:
                    return ActionResult(
                        success=False,
                        message=f"Machine not found: {machine_id}",
                        duration=time.time() - start_time
                    )
                machines = {machine_id: self._machines[machine_id]}
            else:
                machines = self._machines
        
        result = {}
        for mid, machine in machines.items():
            result[mid] = {
                "instance_id": machine.instance_id,
                "current_state": machine.current_state,
                "previous_state": machine._previous_state,
                "states": list(machine.states.keys()),
                "context_keys": list(machine._context.keys()),
                "started_at": machine._started_at,
                "last_updated": machine._last_updated,
                "transition_count": len(machine._history)
            }
        
        return ActionResult(
            success=True,
            message=f"Found {len(result)} machines",
            data={"machines": result, "count": len(result)},
            duration=time.time() - start_time
        )
    
    def _stop_machine(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Stop and remove a state machine."""
        machine_id = params.get("machine_id")
        
        if not machine_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: machine_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine not found: {machine_id}",
                    duration=time.time() - start_time
                )
            del self._machines[machine_id]
        
        return ActionResult(
            success=True,
            message=f"Machine stopped: {machine_id}",
            duration=time.time() - start_time
        )
    
    def _reset_machine(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reset a state machine to initial state."""
        machine_id = params.get("machine_id")
        initial_state = params.get("initial_state", "initial")
        
        if not machine_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: machine_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine not found: {machine_id}",
                    duration=time.time() - start_time
                )
            machine = self._machines[machine_id]
            
            if initial_state not in machine.states:
                return ActionResult(
                    success=False,
                    message=f"State not found: {initial_state}",
                    duration=time.time() - start_time
                )
            
            machine._current_state = initial_state
            machine._previous_state = None
            machine._last_updated = time.time()
            machine._history.clear()
        
        return ActionResult(
            success=True,
            message=f"Machine reset to: {initial_state}",
            data={"machine_id": machine_id, "current_state": initial_state},
            duration=time.time() - start_time
        )
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate state machine parameters."""
        operation = params.get("operation", "create")
        if operation == "create":
            if "states" not in params:
                return False, "Missing required parameter: states"
            if "transitions" not in params:
                return False, "Missing required parameter: transitions"
        elif operation in ("transition", "snapshot", "history", "stop", "reset"):
            if "machine_id" not in params:
                return False, f"Missing required parameter: machine_id"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["operation"]
