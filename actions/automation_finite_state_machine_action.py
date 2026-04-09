"""Automation Finite State Machine Action Module.

Provides a Finite State Machine (FSM) implementation for
modeling deterministic state transitions.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class FSMTransitionType(Enum):
    """FSM transition types."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    GUARDED = "guarded"


@dataclass
class FSMTransition:
    """A transition between states."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[str] = None  # Guard condition name
    action: Optional[str] = None  # Action to execute
    transition_type: FSMTransitionType = FSMTransitionType.EXTERNAL


@dataclass
class FSMState:
    """A state in the FSM."""
    name: str
    is_initial: bool = False
    is_final: bool = False
    is_history: bool = False  # History state (remembers last active sub-state)
    entry_action: Optional[str] = None
    exit_action: Optional[str] = None
    sub_states: Set[str] = field(default_factory=set)


@dataclass
class FSMInstance:
    """An active FSM instance."""
    instance_id: str
    fsm_name: str
    current_state: str
    previous_state: str = ""
    context: Dict[str, Any] = field(default_factory=dict)
    history: List[str] = field(default_factory=list)
    is_active: bool = True


class AutomationFiniteStateMachineAction(BaseAction):
    """Finite State Machine action.

    Implements a deterministic FSM with states, transitions,
    guards, and actions for workflow modeling.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (define, create_instance, send_event, get_state, reset, destroy)
            - fsm: FSM definition dict
            - instance_id: Instance identifier
            - event: Event name to send
            - event_data: Data to attach to event
            - fsm_name: Name of FSM definition
    """
    action_type = "automation_finite_state_machine"
    display_name = "有限状态机"
    description = "确定性有限状态机实现"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "fsm": None,
            "fsm_name": "default",
            "instance_id": None,
            "event": None,
            "event_data": None,
            "context": {},
        }

    def __init__(self) -> None:
        super().__init__()
        self._definitions: Dict[str, Dict[str, Any]] = {}
        self._instances: Dict[str, FSMInstance] = {}
        self._transition_handlers: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute FSM operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        fsm_def = params.get("fsm")
        fsm_name = params.get("fsm_name", "default")
        instance_id = params.get("instance_id")
        event = params.get("event")
        event_data = params.get("event_data")
        ctx = params.get("context", {})

        if operation == "define":
            return self._define_fsm(fsm_def, fsm_name, start_time)
        elif operation == "create_instance":
            return self._create_instance(fsm_name, instance_id, ctx, start_time)
        elif operation == "send_event":
            return self._send_event(fsm_name, instance_id, event, event_data, start_time)
        elif operation == "get_state":
            return self._get_state(instance_id, start_time)
        elif operation == "reset":
            return self._reset_instance(instance_id, start_time)
        elif operation == "destroy":
            return self._destroy_instance(instance_id, start_time)
        elif operation == "status":
            return self._get_fsm_status(fsm_name, instance_id, start_time)
        elif operation == "list_instances":
            return self._list_instances(fsm_name, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _define_fsm(self, fsm_def: Optional[Dict], fsm_name: str, start_time: float) -> ActionResult:
        """Define a new FSM."""
        if not fsm_def:
            return ActionResult(success=False, message="FSM definition required", duration=time.time() - start_time)

        states = {}
        for s_data in fsm_def.get("states", []):
            s_name = s_data.get("name")
            if s_name:
                states[s_name] = FSMState(
                    name=s_name,
                    is_initial=s_data.get("is_initial", False),
                    is_final=s_data.get("is_final", False),
                    is_history=s_data.get("is_history", False),
                    entry_action=s_data.get("entry_action"),
                    exit_action=s_data.get("exit_action"),
                    sub_states=set(s_data.get("sub_states", [])),
                )

        transitions = []
        for t_data in fsm_def.get("transitions", []):
            t_type_str = t_data.get("type", "external")
            try:
                t_type = FSMTransitionType(t_type_str)
            except ValueError:
                t_type = FSMTransitionType.EXTERNAL

            transitions.append(FSMTransition(
                from_state=t_data["from"],
                to_state=t_data["to"],
                event=t_data["event"],
                guard=t_data.get("guard"),
                action=t_data.get("action"),
                transition_type=t_type,
            ))

        definition = {
            "name": fsm_name,
            "states": states,
            "transitions": transitions,
            "initial_state": fsm_def.get("initial_state", ""),
        }
        self._definitions[fsm_name] = definition

        return ActionResult(
            success=True,
            message=f"FSM '{fsm_name}' defined with {len(states)} states and {len(transitions)} transitions",
            data={
                "fsm_name": fsm_name,
                "states_count": len(states),
                "transitions_count": len(transitions),
                "initial_state": definition["initial_state"],
            },
            duration=time.time() - start_time
        )

    def _create_instance(
        self,
        fsm_name: str,
        instance_id: Optional[str],
        ctx: Dict[str, Any],
        start_time: float
    ) -> ActionResult:
        """Create a new FSM instance."""
        if fsm_name not in self._definitions:
            return ActionResult(success=False, message=f"FSM '{fsm_name}' not defined", duration=time.time() - start_time)

        definition = self._definitions[fsm_name]
        instance_id = instance_id or f"instance_{int(time.time() * 1000)}"

        if instance_id in self._instances:
            return ActionResult(success=False, message=f"Instance '{instance_id}' already exists", duration=time.time() - start_time)

        initial_state = definition["initial_state"]
        instance = FSMInstance(
            instance_id=instance_id,
            fsm_name=fsm_name,
            current_state=initial_state,
            context=dict(ctx),
            history=[initial_state],
        )
        self._instances[instance_id] = instance

        return ActionResult(
            success=True,
            message=f"FSM instance '{instance_id}' created in state '{initial_state}'",
            data={
                "instance_id": instance_id,
                "fsm_name": fsm_name,
                "current_state": initial_state,
            },
            duration=time.time() - start_time
        )

    def _send_event(
        self,
        fsm_name: str,
        instance_id: Optional[str],
        event: Optional[str],
        event_data: Any,
        start_time: float
    ) -> ActionResult:
        """Send an event to an FSM instance."""
        if not event:
            return ActionResult(success=False, message="event required", duration=time.time() - start_time)

        instance = self._instances.get(instance_id) if instance_id else None
        if not instance:
            # Try to find by fsm_name if single instance exists
            if instance_id:
                candidates = [i for i in self._instances.values() if i.fsm_name == fsm_name and i.is_active]
                if len(candidates) == 1:
                    instance = candidates[0]
            if not instance:
                return ActionResult(success=False, message=f"Instance '{instance_id}' not found", duration=time.time() - start_time)

        definition = self._definitions.get(instance.fsm_name)
        if not definition:
            return ActionResult(success=False, message=f"FSM '{instance.fsm_name}' definition not found", duration=time.time() - start_time)

        # Find valid transition
        valid_transition = None
        for t in definition["transitions"]:
            if t.from_state == instance.current_state and t.event == event:
                if t.guard:
                    # Evaluate guard (simplified)
                    guard_result = self._evaluate_guard(t.guard, instance, event_data)
                    if not guard_result:
                        continue
                valid_transition = t
                break

        if not valid_transition:
            return ActionResult(
                success=True,
                message=f"No transition for event '{event}' from state '{instance.current_state}'",
                data={
                    "instance_id": instance.instance_id,
                    "event": event,
                    "current_state": instance.current_state,
                    "transitioned": False,
                },
                duration=time.time() - start_time
            )

        # Execute transition
        previous_state = instance.current_state
        instance.previous_state = previous_state
        instance.current_state = valid_transition.to_state
        instance.history.append(valid_transition.to_state)

        # Execute action if present
        action_result = None
        if valid_transition.action:
            action_result = self._execute_action(valid_transition.action, instance, event_data)

        return ActionResult(
            success=True,
            message=f"Transitioned from '{previous_state}' to '{valid_transition.to_state}' on event '{event}'",
            data={
                "instance_id": instance.instance_id,
                "event": event,
                "from_state": previous_state,
                "to_state": valid_transition.to_state,
                "transitioned": True,
                "action_result": action_result,
            },
            duration=time.time() - start_time
        )

    def _evaluate_guard(self, guard: str, instance: FSMInstance, event_data: Any) -> bool:
        """Evaluate a guard condition."""
        # Simplified guard evaluation
        ctx = instance.context
        try:
            # Very basic expression evaluation
            if guard == "true":
                return True
            if guard.startswith("ctx."):
                key = guard[4:]
                return bool(ctx.get(key))
            return True
        except Exception:
            return True

    def _execute_action(self, action: str, instance: FSMInstance, event_data: Any) -> Any:
        """Execute an action callback."""
        handler = self._transition_handlers.get(action)
        if handler:
            return handler(instance, event_data)
        return None

    def _get_state(self, instance_id: Optional[str], start_time: float) -> ActionResult:
        """Get current state of an instance."""
        if not instance_id or instance_id not in self._instances:
            return ActionResult(success=False, message=f"Instance '{instance_id}' not found", duration=time.time() - start_time)

        instance = self._instances[instance_id]
        return ActionResult(
            success=True,
            message=f"Instance '{instance_id}' is in state '{instance.current_state}'",
            data={
                "instance_id": instance_id,
                "fsm_name": instance.fsm_name,
                "current_state": instance.current_state,
                "previous_state": instance.previous_state,
                "history": instance.history,
                "context": instance.context,
                "is_active": instance.is_active,
            },
            duration=time.time() - start_time
        )

    def _reset_instance(self, instance_id: Optional[str], start_time: float) -> ActionResult:
        """Reset an instance to initial state."""
        if not instance_id or instance_id not in self._instances:
            return ActionResult(success=False, message=f"Instance '{instance_id}' not found", duration=time.time() - start_time)

        instance = self._instances[instance_id]
        definition = self._definitions.get(instance.fsm_name)
        if not definition:
            return ActionResult(success=False, message=f"FSM definition not found", duration=time.time() - start_time)

        instance.current_state = definition["initial_state"]
        instance.previous_state = ""
        instance.history = [definition["initial_state"]]
        instance.context.clear()

        return ActionResult(
            success=True,
            message=f"Instance '{instance_id}' reset to '{instance.current_state}'",
            data={"instance_id": instance_id, "current_state": instance.current_state},
            duration=time.time() - start_time
        )

    def _destroy_instance(self, instance_id: Optional[str], start_time: float) -> ActionResult:
        """Destroy an FSM instance."""
        if not instance_id or instance_id not in self._instances:
            return ActionResult(success=False, message=f"Instance '{instance_id}' not found", duration=time.time() - start_time)

        instance = self._instances[instance_id]
        instance.is_active = False
        del self._instances[instance_id]

        return ActionResult(
            success=True,
            message=f"Instance '{instance_id}' destroyed",
            data={"instance_id": instance_id},
            duration=time.time() - start_time
        )

    def _get_fsm_status(self, fsm_name: str, instance_id: Optional[str], start_time: float) -> ActionResult:
        """Get FSM status."""
        definition = self._definitions.get(fsm_name)
        instances = [i for i in self._instances.values() if i.fsm_name == fsm_name and i.is_active]

        return ActionResult(
            success=True,
            message=f"FSM '{fsm_name}' status",
            data={
                "fsm_name": fsm_name,
                "defined": definition is not None,
                "states_count": len(definition.get("states", {})) if definition else 0,
                "transitions_count": len(definition.get("transitions", [])) if definition else 0,
                "active_instances": len(instances),
                "instances": [
                    {"instance_id": i.instance_id, "current_state": i.current_state}
                    for i in instances[:20]
                ],
            },
            duration=time.time() - start_time
        )

    def _list_instances(self, fsm_name: str, start_time: float) -> ActionResult:
        """List all FSM instances."""
        instances = [i for i in self._instances.values() if i.fsm_name == fsm_name and i.is_active]
        return ActionResult(
            success=True,
            message=f"{len(instances)} active instances for FSM '{fsm_name}'",
            data={
                "fsm_name": fsm_name,
                "instances": [
                    {
                        "instance_id": i.instance_id,
                        "current_state": i.current_state,
                        "history_length": len(i.history),
                    }
                    for i in instances
                ]
            },
            duration=time.time() - start_time
        )
