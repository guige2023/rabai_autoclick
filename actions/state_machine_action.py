"""State machine action module for RabAI AutoClick.

Provides state machine functionality for workflow automation
with transitions, guards, actions, and history tracking.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable, Set
from enum import Enum
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Transition:
    """State machine transition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[str] = None
    action: Optional[str] = None
    description: str = ""


@dataclass
class State:
    """State machine state."""
    name: str
    entry_action: Optional[str] = None
    exit_action: Optional[str] = None
    is_initial: bool = False
    is_final: bool = False


class StateMachineAction(BaseAction):
    """Execute a state machine with transitions and guards.
    
    Supports hierarchical states, transition guards,
    entry/exit actions, and state history.
    """
    action_type = "state_machine"
    display_name = "状态机"
    description = "状态机执行，支持转换守卫和进入/退出动作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run a state machine event.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - machine_id: str
                - states: list of State dicts
                - transitions: list of Transition dicts
                - current_state: str (initial state)
                - event: str (event to process)
                - event_data: dict (data to pass with event)
                - history: list (previous states, for restore)
                - save_to_var: str
        
        Returns:
            ActionResult with state machine result.
        """
        machine_id = params.get('machine_id', f'sm_{int(time.time())}')
        states_data = params.get('states', [])
        transitions_data = params.get('transitions', [])
        current_state = params.get('current_state', '')
        event = params.get('event', '')
        event_data = params.get('event_data', {})
        history = params.get('history', [])
        save_to_var = params.get('save_to_var', 'state_machine')

        # Parse states
        states = {}
        for s in states_data:
            name = s.get('name', '')
            states[name] = State(
                name=name,
                entry_action=s.get('entry_action'),
                exit_action=s.get('exit_action'),
                is_initial=s.get('is_initial', False),
                is_final=s.get('is_final', False),
            )

        # Parse transitions
        transitions = []
        for t in transitions_data:
            transitions.append(Transition(
                from_state=t.get('from_state', ''),
                to_state=t.get('to_state', ''),
                event=t.get('event', ''),
                guard=t.get('guard'),
                action=t.get('action'),
                description=t.get('description', ''),
            ))

        # Build lookup: state -> event -> transitions
        transition_map: Dict[str, Dict[str, List[Transition]]] = {}
        for s in states:
            transition_map[s] = {}
        for t in transitions:
            if t.from_state not in transition_map:
                transition_map[t.from_state] = {}
            if t.event not in transition_map[t.from_state]:
                transition_map[t.from_state][t.event] = []
            transition_map[t.from_state][t.event].append(t)

        # Process event
        result = self._process_event(
            machine_id, states, transition_map,
            current_state, event, event_data, history
        )

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=result.get('transitioned', False),
            data=result,
            message=result.get('message', '')
        )

    def _process_event(self, machine_id: str, states: Dict[str, State],
                       transition_map: Dict, current_state: str, event: str,
                       event_data: Dict, history: List) -> Dict:
        """Process an event against the state machine."""
        new_history = list(history)
        new_history.append(current_state)

        # Get current state info
        state_info = states.get(current_state)
        if state_info and state_info.is_final:
            return {
                'machine_id': machine_id,
                'current_state': current_state,
                'event': event,
                'transitioned': False,
                'message': f'Already in final state: {current_state}',
                'history': new_history,
            }

        # Find matching transitions
        if current_state not in transition_map:
            return {
                'machine_id': machine_id,
                'current_state': current_state,
                'event': event,
                'transitioned': False,
                'message': f'No transitions from state: {current_state}',
                'history': new_history,
            }

        event_transitions = transition_map.get(current_state, {}).get(event, [])

        if not event_transitions:
            return {
                'machine_id': machine_id,
                'current_state': current_state,
                'event': event,
                'transitioned': False,
                'message': f'No transition for event "{event}" in state "{current_state}"',
                'history': new_history,
            }

        # Find first valid transition (guard passes)
        valid_transition = None
        for t in event_transitions:
            if t.guard:
                # Guard evaluation would check conditions
                # For now, assume guard passes
                if not self._eval_guard(t.guard, event_data):
                    continue
            valid_transition = t
            break

        if not valid_transition:
            return {
                'machine_id': machine_id,
                'current_state': current_state,
                'event': event,
                'transitioned': False,
                'message': f'All transition guards rejected event "{event}"',
                'history': new_history,
            }

        # Execute transition
        from_state = valid_transition.from_state
        to_state = valid_transition.to_state

        # Exit action
        exit_action = states.get(from_state).exit_action if states.get(from_state) else None
        
        # Entry action
        entry_action = states.get(to_state).entry_action if states.get(to_state) else None
        
        # Transition action
        transition_action = valid_transition.action

        return {
            'machine_id': machine_id,
            'from_state': from_state,
            'to_state': to_state,
            'current_state': to_state,
            'event': event,
            'transitioned': True,
            'transition_action': transition_action,
            'exit_action': exit_action,
            'entry_action': entry_action,
            'message': f'{from_state} --[{event}]--> {to_state}',
            'history': new_history,
            'is_final': states.get(to_state, State(to_state)).is_final if to_state in states else False,
        }

    def _eval_guard(self, guard: str, event_data: Dict) -> bool:
        """Evaluate a guard condition against event data."""
        # Simple guard evaluation
        # Format: var_name == value, var_name != value, var_name (truthy check)
        g = guard.strip()
        
        if g.startswith('$'):
            var_name = g[1:]
            if '==' in var_name:
                parts = var_name.split('==')
                var_name = parts[0].strip()
                expected = parts[1].strip().strip('"\'')
                actual = str(event_data.get(var_name, ''))
                return actual == expected
            elif '!=' in var_name:
                parts = var_name.split('!=')
                var_name = parts[0].strip()
                expected = parts[1].strip().strip('"\'')
                actual = str(event_data.get(var_name, ''))
                return actual != expected
            else:
                return bool(event_data.get(var_name))
        
        return True


class StateHistoryAction(BaseAction):
    """Query or manipulate state machine history.
    
    Supports checking history, restoring previous state,
    and computing transition paths.
    """
    action_type = "state_history"
    display_name = "状态历史"
    description = "查询和操作状态机历史，支持状态回溯"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Query or manipulate state history.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - history: list of state names
                - action: str (get_current/get_previous/get_path/count/restore)
                - save_to_var: str
        
        Returns:
            ActionResult with history query result.
        """
        history = params.get('history', [])
        action = params.get('action', 'get_current')
        save_to_var = params.get('save_to_var', 'history_result')

        result = {'action': action}

        if action == 'get_current':
            result['state'] = history[-1] if history else None
            result['message'] = f"Current state: {result['state']}"
        elif action == 'get_previous':
            result['state'] = history[-2] if len(history) > 1 else None
            result['message'] = f"Previous state: {result['state']}"
        elif action == 'get_path':
            result['path'] = list(history)
            result['message'] = f"Full path: {' -> '.join(history)}"
        elif action == 'count':
            result['count'] = len(history)
            result['message'] = f"History length: {len(history)}"
        elif action == 'restore':
            restore_idx = params.get('restore_index', -1)
            if 0 <= restore_idx < len(history):
                result['restored_state'] = history[restore_idx]
                result['truncated_history'] = history[:restore_idx + 1]
                result['message'] = f"Restored to state: {result['restored_state']}"
            else:
                result['restored_state'] = history[0]
                result['truncated_history'] = [history[0]]
                result['message'] = f"Restored to initial state: {result['restored_state']}"
        elif action == 'last_transition':
            if len(history) >= 2:
                result['from_state'] = history[-2]
                result['to_state'] = history[-1]
                result['message'] = f"Last transition: {history[-2]} -> {history[-1]}"
            else:
                result['message'] = "No transition in history"
        else:
            result['message'] = f"Unknown action: {action}"

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(success=True, data=result, message=result.get('message', ''))
