"""Automation Debugger Action.

Provides debugging capabilities for automation workflows including breakpoint
management, variable inspection, step tracing, and execution replay.
"""

import sys
import os
import time
import traceback
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationDebuggerAction(BaseAction):
    """Debug automation workflows with breakpoints and tracing.
    
    Supports breakpoint management, variable inspection, step tracing,
    execution replay, and detailed error reporting.
    """
    action_type = "automation_debugger"
    display_name = "自动化调试器"
    description = "调试自动化工作流，支持断点、变量检查、步骤追踪"

    class DebugMode(Enum):
        """Debug execution modes."""
        DISABLED = "disabled"
        STEP = "step"
        CONTINUOUS = "continuous"
        REPLAY = "replay"

    def __init__(self):
        super().__init__()
        self._breakpoints: Dict[str, int] = {}  # step_id -> line
        self._watch_vars: List[str] = []
        self._trace: List[Dict] = []
        self._mode = self.DebugMode.DISABLED
        self._break_on_error = True
        self._step_count = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Debug automation execution.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'start', 'breakpoint', 'watch', 'step', 'continue', 
                         'inspect', 'trace', 'replay', 'stop'.
                - step_id: Step identifier for breakpoint.
                - line: Line number for breakpoint.
                - variables: List of variable names to watch.
                - steps: List of steps to debug (for start).
                - breakpoint_condition: Condition lambda for conditional breakpoint.
                - save_to_var: Variable name for debug info.
        
        Returns:
            ActionResult with debug information.
        """
        try:
            action = params.get('action', 'start')
            save_to_var = params.get('save_to_var', 'debug_info')

            if action == 'start':
                return self._start_debugging(context, params, save_to_var)
            elif action == 'breakpoint':
                return self._set_breakpoint(params, save_to_var)
            elif action == 'watch':
                return self._add_watch(params, save_to_var)
            elif action == 'step':
                return self._step_debugging(context, params, save_to_var)
            elif action == 'continue':
                return self._continue_debugging(context, params, save_to_var)
            elif action == 'inspect':
                return self._inspect_context(context, params, save_to_var)
            elif action == 'trace':
                return self._get_trace(save_to_var)
            elif action == 'stop':
                return self._stop_debugging(save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown debug action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Debugger error: {e}")

    def _start_debugging(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Start debug mode for workflow."""
        steps = params.get('steps', [])
        mode = params.get('mode', 'step').lower()
        break_on_error = params.get('break_on_error', True)
        
        self._mode = self.DebugMode.STEP if mode == 'step' else self.DebugMode.CONTINUOUS
        self._break_on_error = break_on_error
        self._trace = []
        self._step_count = 0

        status = {
            'mode': self._mode.value,
            'breakpoints': list(self._breakpoints.keys()),
            'watch_vars': self._watch_vars.copy(),
            'steps_loaded': len(steps),
            'started_at': time.time()
        }

        context.set_variable(save_to_var, status)
        return ActionResult(success=True, data=status, message=f"Debugger started in {mode} mode")

    def _set_breakpoint(self, params: Dict, save_to_var: str) -> ActionResult:
        """Set a breakpoint at a step."""
        step_id = params.get('step_id')
        line = params.get('line', 0)
        condition = params.get('breakpoint_condition', None)

        if not step_id:
            return ActionResult(success=False, message="step_id is required")

        self._breakpoints[step_id] = {'line': line, 'condition': condition}
        
        status = {
            'breakpoint_set': step_id,
            'line': line,
            'condition': condition,
            'total_breakpoints': len(self._breakpoints)
        }
        return ActionResult(success=True, data=status, message=f"Breakpoint set at {step_id}:{line}")

    def _add_watch(self, params: Dict, save_to_var: str) -> ActionResult:
        """Add variable to watch list."""
        variables = params.get('variables', [])
        
        if isinstance(variables, str):
            variables = [variables]
        
        self._watch_vars.extend(v for v in variables if v not in self._watch_vars)
        
        status = {
            'watch_vars': self._watch_vars.copy(),
            'count': len(self._watch_vars)
        }
        return ActionResult(success=True, data=status, message=f"Now watching {len(self._watch_vars)} variables")

    def _step_debugging(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Execute single step with debug info."""
        self._step_count += 1
        step_id = params.get('step_id', f'step_{self._step_count}')
        action_name = params.get('action')
        action_params = params.get('params', {})

        # Capture state before execution
        before_state = {}
        for var in self._watch_vars:
            val = context.get_variable(var)
            before_state[var] = val

        # Check if breakpoint hit
        if step_id in self._breakpoints:
            bp = self._breakpoints[step_id]
            if bp.get('condition'):
                try:
                    if not eval(bp['condition']):
                        pass  # Don't break
                    else:
                        return ActionResult(success=True, data={'breakpoint_hit': step_id}, 
                                         message=f"Breakpoint hit at {step_id}")
                except Exception:
                    pass
            else:
                return ActionResult(success=True, data={'breakpoint_hit': step_id}, 
                                 message=f"Breakpoint hit at {step_id}")

        # Execute step
        trace_entry = {
            'step': step_id,
            'action': action_name,
            'timestamp': time.time(),
            'before_state': before_state.copy()
        }

        error = None
        result_data = None

        try:
            action = self._get_action(action_name)
            if action:
                result = action.execute(context, action_params)
                result_data = result.data if result else None
                trace_entry['success'] = result.success if result else False
                trace_entry['after_state'] = {var: context.get_variable(var) for var in self._watch_vars}
                trace_entry['result'] = result.data if result else None
            else:
                error = f"Unknown action: {action_name}"
                trace_entry['error'] = error
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            trace_entry['error'] = error
            trace_entry['traceback'] = traceback.format_exc()

        self._trace.append(trace_entry)

        # Capture state after execution
        after_state = {var: context.get_variable(var) for var in self._watch_vars}

        status = {
            'step': step_id,
            'action': action_name,
            'before_state': before_state,
            'after_state': after_state,
            'trace': trace_entry,
            'total_steps': self._step_count,
            'mode': self._mode.value
        }

        context.set_variable(save_to_var, status)

        if error:
            if self._break_on_error:
                self._mode = self.DebugMode.STEP
                return ActionResult(success=False, data=status, 
                                 message=f"Error at {step_id}: {error}")
            return ActionResult(success=False, data=status, message=f"Error at {step_id}: {error}")

        return ActionResult(success=True, data=status, message=f"Step {step_id} completed")

    def _continue_debugging(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Continue execution until next breakpoint or error."""
        self._mode = self.DebugMode.CONTINUOUS
        status = {'mode': self._mode.value, 'continued_at': time.time()}
        context.set_variable(save_to_var, status)
        return ActionResult(success=True, data=status, message="Continuous mode")

    def _inspect_context(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Inspect context variables."""
        variables = params.get('variables', self._watch_vars)
        
        if isinstance(variables, str):
            variables = [variables]

        inspection = {}
        for var in variables:
            inspection[var] = {
                'value': context.get_variable(var),
                'type': type(context.get_variable(var)).__name__ if context.get_variable(var) is not None else None
            }

        context.set_variable(save_to_var, inspection)
        return ActionResult(success=True, data=inspection, message=f"Inspected {len(inspection)} variables")

    def _get_trace(self, save_to_var: str) -> ActionResult:
        """Get execution trace."""
        trace = {
            'total_steps': len(self._trace),
            'steps': self._trace,
            'mode': self._mode.value,
            'breakpoints': list(self._breakpoints.keys()),
            'watch_vars': self._watch_vars.copy()
        }
        return ActionResult(success=True, data=trace, message=f"Trace: {len(self._trace)} steps")

    def _stop_debugging(self, save_to_var: str) -> ActionResult:
        """Stop debug mode."""
        self._mode = self.DebugMode.DISABLED
        status = {'stopped': True, 'total_steps': self._step_count, 'trace_entries': len(self._trace)}
        self._breakpoints.clear()
        self._watch_vars.clear()
        return ActionResult(success=True, data=status, message="Debugger stopped")

    def _get_action(self, action_name: str):
        """Get action from registry."""
        from core.action_registry import ActionRegistry
        registry = ActionRegistry.get_instance()
        return registry.get_action(action_name)
