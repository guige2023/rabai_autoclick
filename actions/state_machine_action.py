"""State machine action module for RabAI AutoClick.

Implements a configurable state machine for workflow modeling
with support for guards, actions, and transition callbacks.
"""

import time
import sys
import os
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """Transition types."""
    EXTERNAL = "external"
    INTERNAL = "internal"


@dataclass
class State:
    """A state in the state machine."""
    id: str
    name: str
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    is_initial: bool = False
    is_final: bool = False


@dataclass
class Transition:
    """A transition between states."""
    source: str
    target: str
    event: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL


@dataclass
class StateMachineContext:
    """Runtime context for state machine."""
    current_state: str
    history: List[str]
    variables: Dict[str, Any]
    last_event: Optional[str] = None
    last_transition_time: float = 0


class StateMachineAction(BaseAction):
    """State machine action for workflow modeling.
    
    Supports states, transitions, guards, and actions with
    initial/final states and history tracking.
    """
    action_type = "state_machine"
    display_name = "状态机"
    description = "工作流状态机"
    
    def __init__(self):
        super().__init__()
        self._machines: Dict[str, Dict[str, Any]] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state machine operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: create|send|get|list
                machine_id: Machine identifier
                states: List of state definitions (for create)
                transitions: List of transition definitions (for create)
                event: Event to send (for send)
                payload: Event payload (for send).
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'get')
        
        if operation == 'create':
            return self._create(params)
        elif operation == 'send':
            return self._send(params)
        elif operation == 'get':
            return self._get(params)
        elif operation == 'list':
            return self._list()
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new state machine."""
        machine_id = params.get('machine_id')
        state_defs = params.get('states', [])
        transition_defs = params.get('transitions', [])
        initial_state = params.get('initial_state')
        context_vars = params.get('variables', {})
        
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        states = {}
        for s in state_defs:
            state = State(
                id=s['id'],
                name=s.get('name', s['id']),
                is_initial=s.get('is_initial', False),
                is_final=s.get('is_final', False)
            )
            states[state.id] = state
        
        if not initial_state:
            for state in states.values():
                if state.is_initial:
                    initial_state = state.id
                    break
        
        if not initial_state or initial_state not in states:
            return ActionResult(success=False, message="Invalid initial_state")
        
        transitions: Dict[str, List[Transition]] = {}
        for t in transition_defs:
            trans = Transition(
                source=t['source'],
                target=t['target'],
                event=t['event'],
                transition_type=TransitionType(t.get('type', 'external'))
            )
            if trans.source not in transitions:
                transitions[trans.source] = []
            transitions[trans.source].append(trans)
        
        sm_context = StateMachineContext(
            current_state=initial_state,
            history=[initial_state],
            variables=dict(context_vars)
        )
        
        self._machines[machine_id] = {
            'states': states,
            'transitions': transitions,
            'context': sm_context
        }
        
        return ActionResult(
            success=True,
            message=f"State machine {machine_id} created",
            data={
                'machine_id': machine_id,
                'initial_state': initial_state,
                'state_count': len(states),
                'transition_count': sum(len(t) for t in transitions.values())
            }
        )
    
    def _send(self, params: Dict[str, Any]) -> ActionResult:
        """Send an event to a state machine."""
        machine_id = params.get('machine_id')
        event = params.get('event')
        payload = params.get('payload')
        
        if not machine_id or not event:
            return ActionResult(success=False, message="machine_id and event are required")
        
        if machine_id not in self._machines:
            return ActionResult(success=False, message=f"Machine {machine_id} not found")
        
        machine = self._machines[machine_id]
        ctx = machine['context']
        
        current_state = ctx.current_state
        transitions = machine['transitions'].get(current_state, [])
        
        matched_transition = None
        for trans in transitions:
            if trans.event == event:
                if trans.guard:
                    try:
                        if not trans.guard(ctx, payload):
                            continue
                    except Exception:
                        continue
                matched_transition = trans
                break
        
        if not matched_transition:
            return ActionResult(
                success=True,
                message=f"No transition for event '{event}' from state '{current_state}'",
                data={
                    'current_state': current_state,
                    'event': event,
                    'transitioned': False
                }
            )
        
        trans = matched_transition
        
        target_state = machine['states'].get(trans.target)
        if not target_state:
            return ActionResult(success=False, message=f"Target state '{trans.target}' not found")
        
        if target_state.is_final:
            ctx.current_state = trans.target
            ctx.history.append(trans.target)
            ctx.last_event = event
            ctx.last_transition_time = time.time()
            
            return ActionResult(
                success=True,
                message=f"Transitioned to final state '{trans.target}'",
                data={
                    'current_state': trans.target,
                    'event': event,
                    'transitioned': True,
                    'is_final': True
                }
            )
        
        source_state = machine['states'].get(current_state)
        if source_state and source_state.exit_action:
            try:
                source_state.exit_action(ctx)
            except Exception:
                pass
        
        if trans.action:
            try:
                trans.action(ctx, payload)
            except Exception:
                pass
        
        ctx.current_state = trans.target
        ctx.history.append(trans.target)
        ctx.last_event = event
        ctx.last_transition_time = time.time()
        
        target_state = machine['states'].get(trans.target)
        if target_state and target_state.entry_action:
            try:
                target_state.entry_action(ctx)
            except Exception:
                pass
        
        return ActionResult(
            success=True,
            message=f"Transitioned from '{source_state.id if source_state else current_state}' to '{trans.target}'",
            data={
                'current_state': trans.target,
                'previous_state': current_state,
                'event': event,
                'transitioned': True,
                'history': ctx.history
            }
        )
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get state machine status."""
        machine_id = params.get('machine_id')
        
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        if machine_id not in self._machines:
            return ActionResult(success=False, message=f"Machine {machine_id} not found")
        
        machine = self._machines[machine_id]
        ctx = machine['context']
        
        return ActionResult(
            success=True,
            message=f"Machine {machine_id} in state '{ctx.current_state}'",
            data={
                'machine_id': machine_id,
                'current_state': ctx.current_state,
                'history': ctx.history,
                'variables': ctx.variables,
                'last_event': ctx.last_event,
                'last_transition_time': ctx.last_transition_time
            }
        )
    
    def _list(self) -> ActionResult:
        """List all state machines."""
        machines = []
        
        for machine_id, machine in self._machines.items():
            ctx = machine['context']
            machines.append({
                'machine_id': machine_id,
                'current_state': ctx.current_state,
                'state_count': len(machine['states']),
                'transition_count': sum(len(t) for t in machine['transitions'].values())
            })
        
        return ActionResult(
            success=True,
            message=f"{len(machines)} state machines",
            data={'machines': machines}
        )
