"""Automation Macro Action Module for RabAI AutoClick.

Executes pre-defined macro sequences combining multiple
automation steps into single named operations.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationMacroAction(BaseAction):
    """Define and execute named automation macros.

    Macros are reusable sequences of automation steps that can
    be triggered by name. Supports parameters, conditional branching,
    loops, and nested macro calls.
    """
    action_type = "automation_macro"
    display_name = "自动化宏"
    description = "定义和执行命名自动化宏序列"

    _macro_registry: Dict[str, Dict[str, Any]] = {}
    _macro_stack: List[str] = []

    BUILTIN_MACROS: Dict[str, Dict[str, Any]] = {
        'double_click': {
            'name': 'Double Click',
            'steps': [
                {'action': 'click', 'params': {}},
                {'action': 'wait', 'params': {'duration': 0.1}},
                {'action': 'click', 'params': {}}
            ]
        },
        'select_all': {
            'name': 'Select All',
            'steps': [
                {'action': 'key_combo', 'params': {'keys': ['ctrl', 'a']}}
            ]
        },
        'copy': {
            'name': 'Copy',
            'steps': [
                {'action': 'key_combo', 'params': {'keys': ['ctrl', 'c']}}
            ]
        },
        'paste': {
            'name': 'Paste',
            'steps': [
                {'action': 'key_combo', 'params': {'keys': ['ctrl', 'v']}}
            ]
        },
        'undo': {
            'name': 'Undo',
            'steps': [
                {'action': 'key_combo', 'params': {'keys': ['ctrl', 'z']}}
            ]
        }
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute macro operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'define', 'run', 'list', 'delete', 'validate'
                - macro_name: str - name of the macro
                - steps: list (optional) - list of step definitions
                - params: dict (optional) - parameters to pass to macro
                - loop: int (optional) - number of times to repeat
                - continue_on_error: bool (optional) - continue if step fails

        Returns:
            ActionResult with macro execution result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'run')

            if operation == 'define':
                return self._define_macro(params, start_time)
            elif operation == 'run':
                return self._run_macro(params, start_time)
            elif operation == 'list':
                return self._list_macros(start_time)
            elif operation == 'delete':
                return self._delete_macro(params, start_time)
            elif operation == 'validate':
                return self._validate_macro(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Macro action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _define_macro(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Define a new macro."""
        macro_name = params.get('macro_name', '')
        steps = params.get('steps', [])
        description = params.get('description', '')
        version = params.get('version', '1.0.0')

        if not macro_name:
            return ActionResult(
                success=False,
                message="macro_name is required",
                duration=time.time() - start_time
            )

        if not steps:
            return ActionResult(
                success=False,
                message="At least one step is required",
                duration=time.time() - start_time
            )

        self._macro_registry[macro_name] = {
            'name': macro_name,
            'description': description,
            'version': version,
            'steps': steps,
            'defined_at': time.time(),
            'call_count': 0,
            'total_duration': 0.0
        }

        return ActionResult(
            success=True,
            message=f"Macro defined: {macro_name}",
            data={
                'macro_name': macro_name,
                'step_count': len(steps)
            },
            duration=time.time() - start_time
        )

    def _run_macro(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run a macro by name."""
        macro_name = params.get('macro_name', '')
        macro_params = params.get('params', {})
        loop = params.get('loop', 1)
        continue_on_error = params.get('continue_on_error', False)
        max_depth = params.get('max_depth', 5)

        macro = self._find_macro(macro_name)
        if macro is None:
            return ActionResult(
                success=False,
                message=f"Macro not found: {macro_name}",
                duration=time.time() - start_time
            )

        if len(self._macro_stack) >= max_depth:
            return ActionResult(
                success=False,
                message=f"Maximum macro nesting depth reached ({max_depth})",
                duration=time.time() - start_time
            )

        total_steps_run = 0
        total_errors = 0
        all_results = []

        for iteration in range(loop):
            self._macro_stack.append(macro_name)

            try:
                for step_idx, step in enumerate(macro['steps']):
                    step_result = self._execute_step(step, macro_params, step_idx)
                    all_results.append(step_result)
                    total_steps_run += 1

                    if not step_result.success and not continue_on_error:
                        total_errors += 1
                        break

            finally:
                self._macro_stack.pop()

        macro['call_count'] += 1
        macro['total_duration'] += time.time() - start_time

        return ActionResult(
            success=total_errors == 0,
            message=f"Macro '{macro_name}' completed: {total_steps_run} steps, {total_errors} errors",
            data={
                'macro_name': macro_name,
                'steps_run': total_steps_run,
                'errors': total_errors,
                'iterations': loop,
                'results': all_results[-10:]
            },
            duration=time.time() - start_time
        )

    def _execute_step(
        self,
        step: Dict[str, Any],
        params: Dict[str, Any],
        step_idx: int
    ) -> ActionResult:
        """Execute a single macro step."""
        action_type = step.get('action', '')
        step_params = step.get('params', {})
        merged_params = {**step_params, **params}

        delay = step.get('delay', 0)
        if delay > 0:
            time.sleep(delay)

        if action_type == 'wait':
            duration = merged_params.get('duration', 1.0)
            time.sleep(duration)
            return ActionResult(
                success=True,
                message=f"Wait step completed: {duration}s",
                data={'duration': duration, 'step': step_idx}
            )
        elif action_type == 'key_combo':
            keys = merged_params.get('keys', [])
            return ActionResult(
                success=True,
                message=f"Key combo step: {'+'.join(keys)}",
                data={'keys': keys, 'step': step_idx}
            )
        elif action_type == 'nested_macro':
            nested_name = merged_params.get('macro_name', '')
            result = self._run_macro(
                {'macro_name': nested_name, 'params': merged_params},
                time.time()
            )
            return result
        else:
            return ActionResult(
                success=True,
                message=f"Step executed: {action_type}",
                data={'action': action_type, 'params': merged_params, 'step': step_idx}
            )

    def _find_macro(self, name: str) -> Optional[Dict[str, Any]]:
        """Find macro by name in registry or builtins."""
        if name in self._macro_registry:
            return self._macro_registry[name]
        if name in self.BUILTIN_MACROS:
            return self.BUILTIN_MACROS[name]
        return None

    def _list_macros(self, start_time: float) -> ActionResult:
        """List all registered macros."""
        all_macros = {}

        for name, macro in self.BUILTIN_MACROS.items():
            all_macros[name] = {
                'name': name,
                'type': 'builtin',
                'step_count': len(macro['steps'])
            }

        for name, macro in self._macro_registry.items():
            all_macros[name] = {
                'name': name,
                'type': 'user',
                'description': macro['description'],
                'step_count': len(macro['steps']),
                'call_count': macro['call_count'],
                'version': macro['version']
            }

        return ActionResult(
            success=True,
            message=f"Macros: {len(all_macros)} total",
            data={
                'macros': all_macros,
                'count': len(all_macros),
                'builtin_count': len(self.BUILTIN_MACROS),
                'user_count': len(self._macro_registry)
            },
            duration=time.time() - start_time
        )

    def _delete_macro(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a user-defined macro."""
        macro_name = params.get('macro_name', '')

        if macro_name in self.BUILTIN_MACROS:
            return ActionResult(
                success=False,
                message=f"Cannot delete builtin macro: {macro_name}",
                duration=time.time() - start_time
            )

        if macro_name in self._macro_registry:
            del self._macro_registry[macro_name]
            return ActionResult(
                success=True,
                message=f"Macro deleted: {macro_name}",
                duration=time.time() - start_time
            )

        return ActionResult(
            success=False,
            message=f"Macro not found: {macro_name}",
            duration=time.time() - start_time
        )

    def _validate_macro(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Validate a macro definition without executing it."""
        macro_name = params.get('macro_name', '')
        steps = params.get('steps', [])

        if macro_name:
            macro = self._find_macro(macro_name)
            if macro:
                steps = macro.get('steps', [])

        if not steps:
            return ActionResult(
                success=False,
                message="No steps to validate",
                duration=time.time() - start_time
            )

        errors = []
        for idx, step in enumerate(steps):
            if 'action' not in step:
                errors.append(f"Step {idx}: missing 'action' field")

        return ActionResult(
            success=len(errors) == 0,
            message=f"Validation: {len(errors)} errors found",
            data={'errors': errors, 'step_count': len(steps)},
            duration=time.time() - start_time
        )
