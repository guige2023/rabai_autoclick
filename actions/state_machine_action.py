"""State Machine action module for RabAI AutoClick.

Provides state machine definition and transition management
with guards, actions, and history tracking.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """State transition types."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    LOCAL = "local"


@dataclass
class State:
    """Represents a state in the machine."""
    state_id: str
    name: str
    is_initial: bool = False
    is_final: bool = False
    entry_action: Optional[str] = None
    exit_action: Optional[str] = None
    description: str = ""


@dataclass
class Transition:
    """Represents a state transition."""
    transition_id: str
    source_state: str
    target_state: str
    event: str
    guard: Optional[str] = None
    action: Optional[str] = None
    transition_type: TransitionType = TransitionType.EXTERNAL
    description: str = ""


@dataclass
class StateMachineInstance:
    """Instance of a state machine for a specific entity."""
    instance_id: str
    machine_id: str
    current_state: str
    history: List[Dict[str, Any]] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class StateMachine:
    """State machine engine with guards and actions."""
    
    def __init__(self):
        self._machines: Dict[str, Dict[str, Any]] = {}  # machine_id -> {states, transitions, actions}
        self._instances: Dict[str, StateMachineInstance] = {}
        self._action_functions: Dict[str, Callable] = {}
    
    def define_machine(
        self,
        machine_id: str,
        states: List[State],
        transitions: List[Transition],
        initial_state: str
    ) -> None:
        """Define a state machine."""
        self._machines[machine_id] = {
            "states": {s.state_id: s for s in states},
            "transitions": {t.transition_id: t for t in transitions},
            "transitions_by_event": self._index_transitions(transitions),
            "initial_state": initial_state
        }
    
    def _index_transitions(self, transitions: List[Transition]) -> Dict[str, List[Transition]]:
        """Index transitions by event name."""
        index = {}
        for t in transitions:
            if t.event not in index:
                index[t.event] = []
            index[t.event].append(t)
        return index
    
    def create_instance(self, machine_id: str, 
                        instance_id: Optional[str] = None,
                        context: Optional[Dict[str, Any]] = None) -> str:
        """Create a new state machine instance."""
        if machine_id not in self._machines:
            raise ValueError(f"Machine '{machine_id}' not found")
        
        instance_id = instance_id or str(uuid.uuid4())
        machine = self._machines[machine_id]
        initial = machine["initial_state"]
        
        instance = StateMachineInstance(
            instance_id=instance_id,
            machine_id=machine_id,
            current_state=initial,
            context=context or {}
        )
        self._instances[instance_id] = instance
        return instance_id
    
    def trigger_event(self, instance_id: str, event: str,
                      event_data: Optional[Dict[str, Any]] = None) -> tuple[bool, str]:
        """Trigger an event on a state machine instance.
        
        Returns: (success, new_state_or_error)
        """
        instance = self._instances.get(instance_id)
        if not instance:
            return False, f"Instance '{instance_id}' not found"
        
        machine = self._machines.get(instance.machine_id)
        if not machine:
            return False, f"Machine '{instance.machine_id}' not found"
        
        current = instance.current_state
        transitions = machine["transitions_by_event"].get(event, [])
        
        # Find matching transition
        for t in transitions:
            if t.source_state != current:
                continue
            
            # Evaluate guard
            if t.guard and not self._evaluate_guard(t.guard, instance):
                continue
            
            # Execute transition
            old_state = instance.current_state
            
            # Exit action
            old_state_obj = machine["states"].get(old_state)
            if old_state_obj and old_state_obj.exit_action:
                self._execute_action(old_state_obj.exit_action, instance)
            
            # Transition action
            if t.action:
                self._execute_action(t.action, instance)
            
            # Change state
            instance.current_state = t.target_state
            instance.updated_at = time.time()
            
            # Entry action
            new_state_obj = machine["states"].get(t.target_state)
            if new_state_obj and new_state_obj.entry_action:
                self._execute_action(new_state_obj.entry_action, instance)
            
            # Record history
            instance.history.append({
                "timestamp": time.time(),
                "event": event,
                "from_state": old_state,
                "to_state": t.target_state,
                "transition_id": t.transition_id,
                "event_data": event_data
            })
            
            return True, t.target_state
        
        return False, f"No valid transition for event '{event}' from state '{current}'"
    
    def _evaluate_guard(self, guard: str, instance: StateMachineInstance) -> bool:
        """Evaluate a guard condition."""
        try:
            ctx = {"state": instance.current_state, "context": instance.context}
            return bool(eval(guard, {"__builtins__": {}}, ctx))
        except Exception:
            return True
    
    def _execute_action(self, action: str, instance: StateMachineInstance) -> None:
        """Execute an action function."""
        func = self._action_functions.get(action)
        if func:
            func(instance)
    
    def register_action(self, action_name: str, func: Callable) -> None:
        """Register an action function."""
        self._action_functions[action_name] = func
    
    def get_instance(self, instance_id: str) -> Optional[StateMachineInstance]:
        """Get a state machine instance."""
        return self._instances.get(instance_id)
    
    def get_state(self, instance_id: str) -> Optional[str]:
        """Get current state of an instance."""
        instance = self._instances.get(instance_id)
        return instance.current_state if instance else None


class StateMachineAction(BaseAction):
    """Execute state machine logic with transitions and guards.
    
    Supports state machine definition, event triggering, guards,
    entry/exit actions, and history tracking.
    """
    action_type = "state_machine"
    display_name = "状态机"
    description = "状态机执行引擎，支持状态转换和守卫条件"
    
    def __init__(self):
        super().__init__()
        self._machine = StateMachine()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute state machine operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "define":
                return self._define_machine(params)
            elif operation == "create":
                return self._create_instance(params)
            elif operation == "trigger":
                return self._trigger_event(params)
            elif operation == "get_state":
                return self._get_state(params)
            elif operation == "get_history":
                return self._get_history(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _define_machine(self, params: Dict[str, Any]) -> ActionResult:
        """Define a state machine."""
        machine_id = params.get("machine_id", "")
        states_data = params.get("states", [])
        transitions_data = params.get("transitions", [])
        initial = params.get("initial_state", "")
        
        states = [State(**s) for s in states_data]
        transitions = [Transition(**t) for t in transitions_data]
        
        self._machine.define_machine(machine_id, states, transitions, initial)
        return ActionResult(success=True, message=f"Machine '{machine_id}' defined")
    
    def _create_instance(self, params: Dict[str, Any]) -> ActionResult:
        """Create a state machine instance."""
        machine_id = params.get("machine_id", "")
        instance_id = self._machine.create_instance(machine_id, context=params.get("context"))
        return ActionResult(success=True, message=f"Instance '{instance_id}' created", 
                          data={"instance_id": instance_id})
    
    def _trigger_event(self, params: Dict[str, Any]) -> ActionResult:
        """Trigger an event."""
        instance_id = params.get("instance_id", "")
        event = params.get("event", "")
        
        success, result = self._machine.trigger_event(instance_id, event, params.get("event_data"))
        return ActionResult(success=success, message=f"Transition: {result}",
                          data={"new_state": result if success else None})
    
    def _get_state(self, params: Dict[str, Any]) -> ActionResult:
        """Get current state."""
        instance_id = params.get("instance_id", "")
        state = self._machine.get_state(instance_id)
        return ActionResult(success=True, message=f"State: {state}",
                          data={"state": state, "instance_id": instance_id})
    
    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get state history."""
        instance_id = params.get("instance_id", "")
        instance = self._machine.get_instance(instance_id)
        if not instance:
            return ActionResult(success=False, message="Instance not found")
        return ActionResult(success=True, message=f"{len(instance.history)} transitions",
                          data={"history": instance.history})
