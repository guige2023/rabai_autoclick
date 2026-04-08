"""State machine action module for RabAI AutoClick.

Provides state machine with transitions,
guards, actions, and history tracking.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from enum import Enum
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StateMachineAction(BaseAction):
    """State machine for managing state transitions.
    
    Supports defined states, transitions, guards,
    entry/exit actions, and state history.
    """
    action_type = "state_machine"
    display_name = "状态机"
    description = "状态机和转换管理"
    
    def __init__(self):
        super().__init__()
        self._machines: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state machine operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (create, transition,
                   get_state, reset, get_history), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'create')
        
        if action == 'create':
            return self._create_machine(params)
        elif action == 'transition':
            return self._transition(params)
        elif action == 'get_state':
            return self._get_state(params)
        elif action == 'reset':
            return self._reset_machine(params)
        elif action == 'get_history':
            return self._get_history(params)
        elif action == 'add_transition':
            return self._add_transition(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_machine(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a new state machine."""
        machine_id = params.get('machine_id')
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        states = params.get('states', [])
        if not states:
            return ActionResult(success=False, message="states are required")
        
        initial_state = params.get('initial_state', states[0] if states else None)
        transitions = params.get('transitions', [])
        
        with self._lock:
            if machine_id in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine '{machine_id}' already exists"
                )
            
            self._machines[machine_id] = {
                'machine_id': machine_id,
                'states': states,
                'initial_state': initial_state,
                'current_state': initial_state,
                'transitions': {t.get('from'): t for t in transitions},
                'history': [{
                    'from': None,
                    'to': initial_state,
                    'event': 'init',
                    'timestamp': time.time()
                }],
                'created_at': time.time()
            }
        
        return ActionResult(
            success=True,
            message=f"Created state machine '{machine_id}'",
            data={
                'machine_id': machine_id,
                'initial_state': initial_state
            }
        )
    
    def _transition(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform a state transition."""
        machine_id = params.get('machine_id')
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        event = params.get('event')
        if not event:
            return ActionResult(success=False, message="event is required")
        
        context = params.get('context', {})
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine '{machine_id}' not found"
                )
            
            machine = self._machines[machine_id]
            current_state = machine['current_state']
            transitions = machine['transitions']
            
            if current_state not in transitions:
                return ActionResult(
                    success=False,
                    message=f"No transitions defined from state '{current_state}'"
                )
            
            transition = transitions[current_state]
            
            if transition.get('event') != event:
                return ActionResult(
                    success=False,
                    message=f"Event '{event}' not valid for transition from '{current_state}'"
                )
            
            guard = transition.get('guard')
            if guard:
                guard_result = self._evaluate_guard(guard, context)
                if not guard_result:
                    return ActionResult(
                        success=False,
                        message="Transition guard condition not met"
                    )
            
            to_state = transition.get('to')
            
            machine['current_state'] = to_state
            machine['history'].append({
                'from': current_state,
                'to': to_state,
                'event': event,
                'timestamp': time.time(),
                'context': context
            })
        
        return ActionResult(
            success=True,
            message=f"Transitioned from '{current_state}' to '{to_state}'",
            data={
                'from_state': current_state,
                'to_state': to_state,
                'event': event
            }
        )
    
    def _evaluate_guard(
        self,
        guard: Dict[str, Any],
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate a guard condition."""
        condition = guard.get('condition')
        field = guard.get('field')
        operator = guard.get('operator', 'eq')
        value = guard.get('value')
        
        if condition == 'field_check':
            context_value = context.get(field)
            
            if operator == 'eq':
                return context_value == value
            elif operator == 'ne':
                return context_value != value
            elif operator == 'gt':
                return isinstance(context_value, (int, float)) and context_value > value
            elif operator == 'gte':
                return isinstance(context_value, (int, float)) and context_value >= value
            elif operator == 'lt':
                return isinstance(context_value, (int, float)) and context_value < value
            elif operator == 'lte':
                return isinstance(context_value, (int, float)) and context_value <= value
        
        return True
    
    def _get_state(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get current state of a machine."""
        machine_id = params.get('machine_id')
        
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine '{machine_id}' not found"
                )
            
            machine = self._machines[machine_id]
        
        return ActionResult(
            success=True,
            message=f"Current state: '{machine['current_state']}'",
            data={
                'machine_id': machine_id,
                'current_state': machine['current_state'],
                'initial_state': machine['initial_state'],
                'available_states': machine['states']
            }
        )
    
    def _reset_machine(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reset a machine to its initial state."""
        machine_id = params.get('machine_id')
        
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine '{machine_id}' not found"
                )
            
            machine = self._machines[machine_id]
            machine['current_state'] = machine['initial_state']
            machine['history'].append({
                'from': None,
                'to': machine['initial_state'],
                'event': 'reset',
                'timestamp': time.time()
            })
        
        return ActionResult(
            success=True,
            message=f"Reset machine '{machine_id}' to '{machine['initial_state']}'",
            data={'machine_id': machine_id}
        )
    
    def _get_history(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get transition history for a machine."""
        machine_id = params.get('machine_id')
        limit = params.get('limit', 100)
        
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine '{machine_id}' not found"
                )
            
            machine = self._machines[machine_id]
            history = machine['history'][-limit:]
        
        return ActionResult(
            success=True,
            message=f"Retrieved {len(history)} history entries",
            data={
                'machine_id': machine_id,
                'history': history,
                'count': len(history)
            }
        )
    
    def _add_transition(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Add a transition to a machine."""
        machine_id = params.get('machine_id')
        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")
        
        from_state = params.get('from')
        to_state = params.get('to')
        event = params.get('event')
        
        if not all([from_state, to_state, event]):
            return ActionResult(
                success=False,
                message="from, to, and event are required"
            )
        
        with self._lock:
            if machine_id not in self._machines:
                return ActionResult(
                    success=False,
                    message=f"Machine '{machine_id}' not found"
                )
            
            machine = self._machines[machine_id]
            
            if from_state not in machine['states']:
                return ActionResult(
                    success=False,
                    message=f"State '{from_state}' not in machine states"
                )
            if to_state not in machine['states']:
                return ActionResult(
                    success=False,
                    message=f"State '{to_state}' not in machine states"
                )
            
            machine['transitions'][from_state] = {
                'from': from_state,
                'to': to_state,
                'event': event,
                'guard': params.get('guard'),
                'action': params.get('action')
            }
        
        return ActionResult(
            success=True,
            message=f"Added transition: {from_state} -> {to_state} on '{event}'",
            data={'machine_id': machine_id}
        )
