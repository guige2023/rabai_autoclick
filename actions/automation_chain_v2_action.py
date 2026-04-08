"""Automation Chain V2 action module for RabAI AutoClick.

Chain automation actions with conditional branching,
error recovery, and result passing.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationChainV2Action(BaseAction):
    """Chain automation actions with advanced control flow.

    Supports conditional branching, parallel chains,
    error recovery, and result passing between steps.
    """
    action_type = "automation_chain_v2"
    display_name = "自动化链V2"
    description = "带条件分支和错误恢复的自动化链"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute action chain.

        Args:
            context: Execution context.
            params: Dict with keys: steps (list), shared_data,
                   stop_on_error, pass_results.

        Returns:
            ActionResult with chain execution result.
        """
        start_time = time.time()
        try:
            steps = params.get('steps', [])
            shared_data = params.get('shared_data', {})
            stop_on_error = params.get('stop_on_error', True)
            pass_results = params.get('pass_results', True)

            if not steps:
                return ActionResult(success=False, message="No steps provided", duration=time.time() - start_time)

            results = []
            chain_data = dict(shared_data)

            for i, step in enumerate(steps):
                step_name = step.get('name', f'step_{i}')
                action = step.get('action')
                step_params = step.get('params', {})
                condition = step.get('condition')
                on_error = step.get('on_error')
                transform = step.get('transform_result')

                if condition:
                    try:
                        if callable(condition):
                            if not condition(chain_data, context):
                                results.append({'name': step_name, 'skipped': True, 'reason': 'condition_false'})
                                continue
                        elif isinstance(condition, dict):
                            key = condition.get('key')
                            expected = condition.get('equals')
                            if key and chain_data.get(key) != expected:
                                results.append({'name': step_name, 'skipped': True, 'reason': 'condition_false'})
                                continue
                    except Exception as e:
                        results.append({'name': step_name, 'skipped': True, 'error': str(e)})
                        if stop_on_error:
                            break
                        continue

                step_start = time.time()
                try:
                    if callable(action):
                        result = action(chain_data, step_params, context)
                    elif hasattr(context, 'execute_action'):
                        result = context.execute_action(action, step_params)
                    else:
                        result = ActionResult(success=False, message=f"Unknown action: {action}")

                    if isinstance(result, ActionResult):
                        step_result = {
                            'name': step_name,
                            'success': result.success,
                            'data': result.data,
                            'message': result.message,
                            'duration': result.duration,
                        }
                        if pass_results and result.data is not None:
                            if isinstance(result.data, dict):
                                chain_data.update(result.data)
                            chain_data[f'_last_result'] = result.data
                    else:
                        step_result = {'name': step_name, 'success': True, 'data': result, 'duration': time.time() - step_start}

                    results.append(step_result)

                    if not step_result['success'] and stop_on_error:
                        if on_error == 'recover' and i + 1 < len(steps):
                            recovery_step = steps[i + 1]
                            recovery_result = context.execute_action(recovery_step.get('action'), recovery_step.get('params', {}))
                            results.append({'name': 'recovery', 'success': recovery_result.success if isinstance(recovery_result, ActionResult) else False, 'recovered': True})
                        break

                except Exception as e:
                    results.append({'name': step_name, 'success': False, 'error': str(e), 'traceback': traceback.format_exc(), 'duration': time.time() - step_start})
                    if stop_on_error:
                        if on_error == 'continue':
                            continue
                        break

            all_success = all(r.get('success', False) for r in results)
            duration = time.time() - start_time
            return ActionResult(
                success=all_success,
                message=f"Chain: {len(results)} steps, {'OK' if all_success else 'errors'}",
                data={'results': results, 'chain_data': chain_data, 'total_steps': len(results)},
                duration=duration,
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Chain error: {str(e)}", duration=time.time() - start_time)
