"""Automation Guard Action.

Pre and post condition guards for automation steps with validation,
assertion reporting, and recovery suggestions.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationGuardAction(BaseAction):
    """Guard automation steps with pre/post conditions.
    
    Validates preconditions before step execution and verifies
    postconditions after completion with detailed assertion reports.
    """
    action_type = "automation_guard"
    display_name = "自动化守卫"
    description = "自动化步骤前置/后置条件守卫，支持断言验证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute guarded action with condition validation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: Action to guard ('check_pre', 'check_post', 'execute_guarded').
                - preconditions: List of {var, op, value, message} dicts.
                - postconditions: List of {var, op, value, message} dicts.
                - target_action: Action name to execute (for execute_guarded).
                - target_params: Action parameters (for execute_guarded).
                - save_to_var: Variable name for results.
                - halt_on_failure: Halt execution on precondition failure.
        
        Returns:
            ActionResult with guard validation results.
        """
        try:
            action = params.get('action', 'execute_guarded')
            preconditions = params.get('preconditions', [])
            postconditions = params.get('postconditions', [])
            target_action = params.get('target_action')
            target_params = params.get('target_params', {})
            save_to_var = params.get('save_to_var', 'guard_results')
            halt_on_failure = params.get('halt_on_failure', True)

            if action == 'check_pre':
                return self._check_conditions(context, preconditions, 'pre', save_to_var)
            elif action == 'check_post':
                return self._check_conditions(context, postconditions, 'post', save_to_var)
            elif action == 'execute_guarded':
                return self._execute_with_guards(context, preconditions, postconditions, 
                                                 target_action, target_params, save_to_var, halt_on_failure)
            else:
                return ActionResult(success=False, message=f"Unknown guard action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Guard error: {e}")

    def _check_conditions(self, context: Any, conditions: List[Dict], 
                         cond_type: str, save_to_var: str) -> ActionResult:
        """Check conditions and return results."""
        results = []
        all_passed = True

        for i, cond in enumerate(conditions):
            var = cond.get('var')
            op = cond.get('op', 'eq')
            expected = cond.get('value')
            message = cond.get('message', f'{cond_type}condition[{i}]')
            
            actual = context.get_variable(var) if var else None
            
            passed, details = self._evaluate_condition(actual, op, expected)
            
            results.append({
                'index': i,
                'var': var,
                'op': op,
                'expected': expected,
                'actual': actual,
                'passed': passed,
                'message': message,
                'details': details
            })
            
            if not passed:
                all_passed = False

        summary = {
            'type': cond_type,
            'total': len(results),
            'passed': sum(1 for r in results if r['passed']),
            'failed': sum(1 for r in results if not r['passed']),
            'all_passed': all_passed,
            'results': results
        }

        context.set_variable(save_to_var, summary)
        return ActionResult(success=all_passed, data=summary,
                          message=f"{cond_type}conditions: {sum(1 for r in results if r['passed'])}/{len(results)} passed")

    def _execute_with_guards(self, context: Any, preconditions: List[Dict],
                            postconditions: List[Dict], target_action: str,
                            target_params: Dict, save_to_var: str,
                            halt_on_failure: bool) -> ActionResult:
        """Execute action with pre and post condition guards."""
        # Check preconditions
        pre_result = self._check_conditions(context, preconditions, 'pre', f'{save_to_var}_pre')
        
        if not pre_result.success and halt_on_failure:
            context.set_variable(save_to_var, {
                'success': False,
                'phase': 'precondition',
                'pre_result': pre_result.data
            })
            return ActionResult(success=False, data=pre_result.data,
                              message="Preconditions failed, halting")

        # Execute target action
        if target_action:
            action = self._get_action(target_action)
            if action is None:
                return ActionResult(success=False, message=f"Unknown action: {target_action}")
            
            exec_result = action.execute(context, target_params)
            
            if not exec_result.success:
                context.set_variable(save_to_var, {
                    'success': False,
                    'phase': 'execution',
                    'result': exec_result
                })
                return exec_result
        else:
            exec_result = ActionResult(success=True, message="No target action")

        # Check postconditions
        post_result = self._check_conditions(context, postconditions, 'post', f'{save_to_var}_post')
        
        summary = {
            'success': pre_result.success and exec_result.success and post_result.success,
            'preconditions': pre_result.data,
            'execution': exec_result.data if exec_result else None,
            'postconditions': post_result.data,
            'all_passed': pre_result.success and post_result.success
        }

        context.set_variable(save_to_var, summary)
        
        if not post_result.success:
            return ActionResult(success=False, data=summary,
                              message="Postconditions failed")
        
        return ActionResult(success=True, data=summary, message="Guarded execution completed")

    def _evaluate_condition(self, actual: Any, op: str, expected: Any) -> tuple:
        """Evaluate a single condition."""
        try:
            if op == 'eq' or op == '==':
                passed = actual == expected
                details = f"{actual} == {expected}"
            elif op == 'ne' or op == '!=':
                passed = actual != expected
                details = f"{actual} != {expected}"
            elif op == 'gt' or op == '>':
                passed = actual > expected
                details = f"{actual} > {expected}"
            elif op == 'ge' or op == '>=':
                passed = actual >= expected
                details = f"{actual} >= {expected}"
            elif op == 'lt' or op == '<':
                passed = actual < expected
                details = f"{actual} < {expected}"
            elif op == 'le' or op == '<=':
                passed = actual <= expected
                details = f"{actual} <= {expected}"
            elif op == 'contains':
                passed = expected in actual if actual else False
                details = f"{expected} in {actual}"
            elif op == 'startswith':
                passed = str(actual).startswith(str(expected))
                details = f"{actual} starts with {expected}"
            elif op == 'endswith':
                passed = str(actual).endswith(str(expected))
                details = f"{actual} ends with {expected}"
            elif op == 'regex':
                import re
                passed = bool(re.match(str(expected), str(actual)))
                details = f"regex {expected} matches {actual}"
            elif op == 'is_none':
                passed = actual is None
                details = f"{actual} is None"
            elif op == 'is_not_none':
                passed = actual is not None
                details = f"{actual} is not None"
            elif op == 'in':
                passed = actual in expected
                details = f"{actual} in {expected}"
            elif op == 'type_is':
                passed = type(actual).__name__ == expected
                details = f"type({actual}) == {expected}"
            elif op == 'len_gt':
                passed = len(actual) > expected
                details = f"len({actual}) > {expected}"
            elif op == 'len_eq':
                passed = len(actual) == expected
                details = f"len({actual}) == {expected}"
            else:
                passed = False
                details = f"Unknown operator: {op}"
            
            return passed, details
        except Exception as e:
            return False, f"Evaluation error: {e}"

    def _get_action(self, action_name: str):
        """Get action from registry."""
        from core.action_registry import ActionRegistry
        registry = ActionRegistry.get_instance()
        return registry.get_action(action_name)
