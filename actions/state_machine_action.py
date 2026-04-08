"""State machine action module for RabAI AutoClick.

Provides state machine implementation for workflow automation
with transition handling and state validation.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StateMachineAction(BaseAction):
    """Execute state machine transitions.
    
    Manages state and transitions for workflow automation.
    """
    action_type = "state_machine"
    display_name = "状态机"
    description = "状态机转换管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state machine operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, state, event,
                   machine_id, transitions.
        
        Returns:
            ActionResult with state machine result.
        """
        operation = params.get('operation', 'get_state')
        state = params.get('state', '')
        event = params.get('event', '')
        machine_id = params.get('machine_id', 'default')
        transitions = params.get('transitions', {})

        try:
            machines = getattr(context, '_state_machines', None)
            if machines is None:
                context._state_machines = {}

            if machine_id not in machines:
                machines[machine_id] = {
                    'current_state': state or 'initial',
                    'transitions': transitions or {},
                    'history': [],
                    'created_at': time.strftime('%Y-%m-%d %H:%M:%S')
                }

            machine = machines[machine_id]

            if operation == 'get_state':
                return ActionResult(
                    success=True,
                    message=f"Current state: {machine['current_state']}",
                    data={
                        'machine_id': machine_id,
                        'current_state': machine['current_state'],
                        'history': machine['history']
                    }
                )

            elif operation == 'transition':
                if not event:
                    return ActionResult(success=False, message="event required for transition")

                current = machine['current_state']
                transition_key = f"{current}:{event}"
                
                if transition_key not in machine['transitions']:
                    return ActionResult(
                        success=False,
                        message=f"No transition for {event} from {current}",
                        data={
                            'machine_id': machine_id,
                            'current_state': current,
                            'event': event,
                            'available_transitions': self._get_available_transitions(machine, current)
                        }
                    )

                new_state = machine['transitions'][transition_key]
                old_state = machine['current_state']
                machine['current_state'] = new_state
                
                machine['history'].append({
                    'from': old_state,
                    'to': new_state,
                    'event': event,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })

                return ActionResult(
                    success=True,
                    message=f"Transitioned: {old_state} -> {new_state} via {event}",
                    data={
                        'machine_id': machine_id,
                        'from_state': old_state,
                        'to_state': new_state,
                        'event': event
                    }
                )

            elif operation == 'set_state':
                machine['current_state'] = state
                machine['history'].append({
                    'action': 'set',
                    'to': state,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })
                return ActionResult(
                    success=True,
                    message=f"State set to: {state}",
                    data={'machine_id': machine_id, 'current_state': state}
                )

            elif operation == 'reset':
                machine['current_state'] = state or 'initial'
                machine['history'].append({
                    'action': 'reset',
                    'to': machine['current_state'],
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })
                return ActionResult(
                    success=True,
                    message=f"State machine reset to: {machine['current_state']}",
                    data={'machine_id': machine_id, 'current_state': machine['current_state']}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"State machine error: {str(e)}")

    def _get_available_transitions(self, machine: Dict, state: str) -> List[str]:
        """Get available transitions from state."""
        return [k for k in machine['transitions'].keys() if k.startswith(f"{state}:")]


class StateMachineValidateAction(BaseAction):
    """Validate state machine configuration.
    
    Checks transitions and state definitions.
    """
    action_type = "state_machine_validate"
    display_name = "状态机验证"
    description = "验证状态机配置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate state machine.
        
        Args:
            context: Execution context.
            params: Dict with keys: states, initial_state,
                   transitions, final_states.
        
        Returns:
            ActionResult with validation result.
        """
        states = params.get('states', [])
        initial_state = params.get('initial_state', '')
        transitions = params.get('transitions', {})
        final_states = params.get('final_states', [])

        errors = []
        warnings = []

        if not states:
            errors.append("No states defined")
        
        if initial_state and initial_state not in states:
            errors.append(f"Initial state '{initial_state}' not in states")

        for key, value in transitions.items():
            if ':' not in key:
                errors.append(f"Invalid transition key format: {key}")
                continue
            
            from_state, event = key.split(':', 1)
            if from_state not in states:
                errors.append(f"Transition from unknown state: {from_state}")
            if value not in states:
                errors.append(f"Transition to unknown state: {value}")

        if final_states:
            reachable = self._find_reachable_states(initial_state, transitions, states)
            unreachable_final = [s for s in final_states if s not in reachable and s != initial_state]
            if unreachable_final:
                warnings.append(f"Unreachable final states: {unreachable_final}")

        valid = len(errors) == 0

        return ActionResult(
            success=valid,
            message=f"Validation {'passed' if valid else 'failed'}: {len(errors)} errors, {len(warnings)} warnings",
            data={
                'valid': valid,
                'errors': errors,
                'warnings': warnings,
                'states': states,
                'transitions': transitions
            }
        )

    def _find_reachable_states(self, start: str, transitions: Dict, all_states: List) -> set:
        """Find all states reachable from start."""
        reachable = {start}
        queue = [start]
        
        while queue:
            current = queue.pop(0)
            for key, target in transitions.items():
                if key.startswith(f"{current}:"):
                    if target not in reachable:
                        reachable.add(target)
                        queue.append(target)
        
        return reachable


class StateMachineHistoryAction(BaseAction):
    """Get state machine transition history.
    
    Retrieves history of state transitions.
    """
    action_type = "state_machine_history"
    display_name = "状态历史"
    description = "状态机转换历史"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get history.
        
        Args:
            context: Execution context.
            params: Dict with keys: machine_id, limit, since,
                   filter_event.
        
        Returns:
            ActionResult with history.
        """
        machine_id = params.get('machine_id', 'default')
        limit = params.get('limit', 100)
        since = params.get('since', None)
        filter_event = params.get('filter_event', None)

        try:
            machines = getattr(context, '_state_machines', None)
            if machines is None or machine_id not in machines:
                return ActionResult(
                    success=False,
                    message=f"State machine not found: {machine_id}"
                )

            machine = machines[machine_id]
            history = machine.get('history', [])

            filtered = []
            for entry in history:
                if since:
                    entry_time = entry.get('timestamp', '')
                    if entry_time < since:
                        continue
                if filter_event and entry.get('event') != filter_event:
                    continue
                filtered.append(entry)

            paginated = filtered[-limit:]

            return ActionResult(
                success=True,
                message=f"History: {len(paginated)} entries",
                data={
                    'machine_id': machine_id,
                    'current_state': machine['current_state'],
                    'history': paginated,
                    'total': len(filtered)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"History retrieval failed: {str(e)}")


class StateMachineGuardAction(BaseAction):
    """Evaluate guard conditions for transitions.
    
    Checks if transition should be allowed.
    """
    action_type = "state_machine_guard"
    display_name = "状态守卫"
    description = "状态转换守卫条件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate guard.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition, context_data,
                   machine_id.
        
        Returns:
            ActionResult with guard evaluation.
        """
        condition = params.get('condition', '')
        context_data = params.get('context_data', {})
        machine_id = params.get('machine_id', 'default')

        if not condition:
            return ActionResult(success=False, message="condition required")

        try:
            result = self._evaluate_condition(condition, context_data)

            return ActionResult(
                success=True,
                message=f"Guard condition '{condition}' = {result}",
                data={
                    'condition': condition,
                    'result': result,
                    'allowed': result
                }
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Guard evaluation failed: {str(e)}",
                data={'condition': condition, 'error': str(e)}
            )

    def _evaluate_condition(self, condition: str, context: Dict) -> bool:
        """Evaluate condition expression."""
        try:
            import re
            
            match = re.match(r'(\w+)\s*(==|!=|>|<|>=|<=|in|not in)\s*(.+)', condition)
            if not match:
                return bool(condition)

            var, op, val = match.groups()
            left = context.get(var, '')
            right = val.strip().strip('"\'')
            
            if op == '==':
                return str(left) == right
            elif op == '!=':
                return str(left) != right
            elif op == '>':
                return float(left) > float(right)
            elif op == '<':
                return float(left) < float(right)
            elif op == '>=':
                return float(left) >= float(right)
            elif op == '<=':
                return float(left) <= float(right)
            elif op == 'in':
                return right in str(left)
            elif op == 'not in':
                return right not in str(left)
            
            return False
        except:
            return False
