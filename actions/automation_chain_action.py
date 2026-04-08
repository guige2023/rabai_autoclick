"""Automation Chain Action.

Chains multiple automation steps with dependency management, conditional
branching, data passing between steps, and error recovery with rollback.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationChainAction(BaseAction):
    """Chain multiple automation steps with dependencies.
    
    Supports sequential and parallel steps, conditional branching,
    data passing via variables, rollback on failure, and step skipping.
    """
    action_type = "automation_chain"
    display_name = "自动化链"
    description = "链接多个自动化步骤，支持依赖管理、条件分支、数据传递"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute chained automation steps.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - steps: List of step definitions.
                - data_flow: Dict mapping step outputs to downstream inputs.
                - conditions: Dict of step_id -> condition_lambda.
                - rollback_steps: List of rollback step definitions.
                - save_to_var: Variable name for results.
                - stop_on_failure: Stop chain on first failure (default: True).
                - parallel_groups: List of step groups to run in parallel.
        
        Returns:
            ActionResult with chain execution results.
        """
        try:
            steps = params.get('steps', [])
            data_flow = params.get('data_flow', {})
            conditions = params.get('conditions', {})
            rollback_steps = params.get('rollback_steps', [])
            save_to_var = params.get('save_to_var', 'chain_results')
            stop_on_failure = params.get('stop_on_failure', True)
            parallel_groups = params.get('parallel_groups', [])

            if not steps:
                return ActionResult(success=False, message="steps list is empty")

            results = {}
            step_outputs = {}
            executed = set()

            # Build parallel groups map
            parallel_map = {}
            for group in parallel_groups:
                for step_id in group:
                    parallel_map[step_id] = group

            def get_ready_steps() -> List[Dict]:
                """Get steps that are ready to execute (dependencies met)."""
                ready = []
                for step in steps:
                    step_id = step.get('id')
                    if step_id in executed:
                        continue
                    deps = step.get('depends_on', [])
                    if all(d in executed for d in deps):
                        # Check condition
                        cond = conditions.get(step_id)
                        if cond:
                            try:
                                if not eval(cond):
                                    executed.add(step_id)
                                    results[step_id] = {'skipped': True, 'reason': 'condition_false'}
                                    continue
                            except Exception:
                                pass
                        ready.append(step)
                return ready

            # Execute steps
            max_iterations = len(steps) * 2
            iteration = 0

            while len(executed) < len(steps) and iteration < max_iterations:
                iteration += 1
                ready = get_ready_steps()
                
                if not ready:
                    if len(executed) < len(steps):
                        # Deadlock - remaining steps have unmet dependencies
                        for step in steps:
                            step_id = step.get('id')
                            if step_id not in executed:
                                results[step_id] = {'skipped': True, 'reason': 'deadlock'}
                                executed.add(step_id)
                    break

                # Check if steps are parallel group
                first_step = ready[0]
                step_id = first_step.get('id')
                group = parallel_map.get(step_id, [step_id])
                
                # Filter ready to only include group members
                group_ready = [s for s in ready if s.get('id') in group]

                for step in group_ready:
                    step_id = step.get('id')
                    action_name = step.get('action')
                    action_params = step.get('params', {}).copy()

                    # Inject data from upstream
                    for target, source_info in data_flow.items():
                        if target in action_params:
                            source_step = source_info.get('step')
                            source_var = source_info.get('variable')
                            if source_step in step_outputs:
                                action_params[target] = step_outputs[source_step].get(source_var)

                    # Execute step
                    action = self._get_action(action_name)
                    if action is None:
                        results[step_id] = {'success': False, 'error': f'Unknown action: {action_name}'}
                        if stop_on_failure:
                            break
                        executed.add(step_id)
                        continue

                    result = action.execute(context, action_params)
                    step_outputs[step_id] = {'result': result}
                    results[step_id] = {'success': result.success, 'result': result}

                    if not result.success:
                        if stop_on_failure:
                            break
                    
                    executed.add(step_id)

                if not all(s.get('id') in executed for s in group_ready):
                    continue

            # Check if any step failed
            failed_steps = [s for s, r in results.items() if not r.get('success', False)]
            
            if failed_steps and rollback_steps:
                # Execute rollback
                rollback_results = {}
                for rb_step in reversed(rollback_steps):
                    rb_action = self._get_action(rb_step.get('action'))
                    if rb_action:
                        rb_result = rb_action.execute(context, rb_step.get('params', {}))
                        rollback_results[rb_step.get('id')] = {'success': rb_result.success, 'result': rb_result}

                context.set_variable(f'{save_to_var}_rollback', rollback_results)

            summary = {
                'total_steps': len(steps),
                'executed_steps': len(executed),
                'skipped_steps': len(steps) - len(executed),
                'failed_steps': len(failed_steps),
                'results': results
            }

            context.set_variable(save_to_var, summary)
            return ActionResult(success=len(failed_steps) == 0, data=summary,
                             message=f"Chain: {len(executed)}/{len(steps)} steps, {len(failed_steps)} failed")

        except Exception as e:
            return ActionResult(success=False, message=f"Chain execution error: {e}")

    def _get_action(self, action_name: str) -> Optional['BaseAction']:
        """Get action instance from registry."""
        from core.action_registry import ActionRegistry
        registry = ActionRegistry.get_instance()
        return registry.get_action(action_name)
