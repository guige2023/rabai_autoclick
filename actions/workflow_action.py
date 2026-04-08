"""Workflow action module for RabAI AutoClick.

Provides workflow orchestration capabilities including
sequential execution, parallel branches, conditional paths,
and error recovery.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    id: str
    name: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    condition: Optional[str] = None
    retry: int = 0
    timeout: float = 60.0
    on_error: str = "fail"  # fail, skip, continue, fallback
    fallback_step: Optional[str] = None


@dataclass
class WorkflowContext:
    """Execution context for a workflow."""
    workflow_id: str
    variables: Dict[str, Any] = field(default_factory=dict)
    step_results: Dict[str, Any] = field(default_factory=dict)
    status: WorkflowStatus = WorkflowStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None


class WorkflowRunnerAction(BaseAction):
    """Run a multi-step workflow with branching and error handling.
    
    Supports sequential steps, parallel branches, conditional
    execution, retry logic, and fallback steps.
    """
    action_type = "workflow_run"
    display_name = "工作流执行"
    description = "执行多步骤工作流，支持分支、条件判断和错误处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run a workflow.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - workflow_id: str
                - steps: list of WorkflowStep dicts or serialized steps
                - variables: dict (initial workflow variables)
                - max_parallel: int (default 3)
                - save_to_var: str
        
        Returns:
            ActionResult with workflow execution result.
        """
        workflow_id = params.get('workflow_id', f'wf_{int(time.time())}')
        steps_data = params.get('steps', [])
        init_vars = params.get('variables', {})
        max_parallel = params.get('max_parallel', 3)
        save_to_var = params.get('save_to_var', 'workflow_result')

        # Parse steps
        steps = []
        for s in steps_data:
            if isinstance(s, WorkflowStep):
                steps.append(s)
            else:
                steps.append(WorkflowStep(
                    id=s.get('id', f'step_{len(steps)}'),
                    name=s.get('name', s.get('id', 'unknown')),
                    action=s.get('action', ''),
                    params=s.get('params', {}),
                    condition=s.get('condition'),
                    retry=s.get('retry', 0),
                    timeout=s.get('timeout', 60.0),
                    on_error=s.get('on_error', 'fail'),
                    fallback_step=s.get('fallback_step'),
                ))

        if not steps:
            return ActionResult(success=False, message="No steps in workflow")

        wf_context = WorkflowContext(
            workflow_id=workflow_id,
            variables=dict(init_vars),
        )

        wf_context.status = WorkflowStatus.RUNNING
        wf_context.started_at = time.time()

        try:
            result = self._execute_workflow(steps, wf_context, context, max_parallel)
            
            wf_context.completed_at = time.time()
            duration = wf_context.completed_at - wf_context.started_at

            if result.get('status') == 'failed':
                wf_context.status = WorkflowStatus.FAILED
            else:
                wf_context.status = WorkflowStatus.COMPLETED

            summary = {
                'workflow_id': workflow_id,
                'status': wf_context.status.value,
                'duration_ms': int(duration * 1000),
                'steps_completed': len(wf_context.step_results),
                'variables': wf_context.variables,
                'step_results': wf_context.step_results,
            }

            if context and save_to_var:
                context.variables[save_to_var] = summary

            return ActionResult(
                success=wf_context.status == WorkflowStatus.COMPLETED,
                data=summary,
                message=f"Workflow {wf_context.status.value}: {len(wf_context.step_results)}/{len(steps)} steps"
            )

        except Exception as e:
            wf_context.status = WorkflowStatus.FAILED
            wf_context.error = str(e)
            return ActionResult(success=False, message=f"Workflow error: {e}")

    def _execute_workflow(self, steps: List[WorkflowStep], wf_ctx: WorkflowContext,
                          outer_context: Any, max_parallel: int) -> Dict:
        """Execute the workflow steps."""
        step_map = {s.id: s for s in steps}
        completed = set()
        pending = list(steps)

        while pending:
            # Find steps ready to execute (all dependencies met)
            ready = []
            for step in pending:
                deps = step.params.get('depends_on', [])
                if all(d in completed for d in deps):
                    # Check condition
                    if step.condition:
                        if not self._eval_condition(step.condition, wf_ctx):
                            completed.add(step.id)
                            wf_ctx.step_results[step.id] = {'skipped': True, 'condition': step.condition}
                            continue
                    ready.append(step)

            if not ready:
                # No ready steps but pending remain -> dependency deadlock
                return {'status': 'failed', 'error': 'Dependency deadlock detected'}

            # Execute ready steps
            if len(ready) > 1 and all(s.params.get('parallel', False) for s in ready):
                # Parallel execution
                self._execute_parallel(ready, wf_ctx, outer_context, max_parallel)
                for step in ready:
                    completed.add(step.id)
            else:
                # Sequential
                for step in ready[:1]:
                    self._execute_step(step, wf_ctx, outer_context)
                    completed.add(step.id)

            # Remove completed from pending
            pending = [s for s in pending if s.id not in completed]

        return {'status': 'completed'}

    def _execute_parallel(self, steps: List[WorkflowStep], wf_ctx: WorkflowContext,
                          outer_context: Any, max_workers: int):
        """Execute multiple steps in parallel."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=min(len(steps), max_workers)) as executor:
            futures = {
                executor.submit(self._execute_step, step, wf_ctx, outer_context): step
                for step in steps
            }
            for future in as_completed(futures):
                pass  # Results already stored in wf_ctx

    def _execute_step(self, step: WorkflowStep, wf_ctx: WorkflowContext, outer_context: Any):
        """Execute a single workflow step."""
        # Merge workflow variables into params
        params = dict(step.params)
        for k, v in params.items():
            if isinstance(v, str):
                for wk, wv in wf_ctx.variables.items():
                    v = v.replace(f'${{{wk}}}', str(wv))
                params[k] = v

        attempt = 0
        last_error = None

        while attempt <= step.retry:
            try:
                # In a real implementation, this would dispatch to the action system
                # For now, simulate execution
                result = self._dispatch_action(step.action, params, outer_context)
                
                wf_ctx.step_results[step.id] = {
                    'step': step.name,
                    'result': result,
                    'attempt': attempt + 1,
                }
                
                # Extract variables if result contains them
                if isinstance(result, dict):
                    for k, v in result.items():
                        if k.startswith('var_'):
                            wf_ctx.variables[k[4:]] = v
                
                return
                
            except Exception as e:
                last_error = str(e)
                attempt += 1
                if attempt <= step.retry:
                    time.sleep(1.0 * attempt)
        
        # Handle error
        if step.on_error == 'fail':
            raise Exception(f"Step {step.id} failed after {step.retry + 1} attempts: {last_error}")
        elif step.on_error == 'skip':
            wf_ctx.step_results[step.id] = {'skipped': True, 'error': last_error}
        elif step.on_error == 'fallback' and step.fallback_step:
            wf_ctx.step_results[step.id] = {'fallback_triggered': True, 'error': last_error}
        elif step.on_error == 'continue':
            wf_ctx.step_results[step.id] = {'continued': True, 'error': last_error}

    def _dispatch_action(self, action_name: str, params: Dict, context: Any) -> Any:
        """Dispatch to an action by name."""
        # This would normally look up the action in the registry
        # For now, return a mock result
        return {'executed': action_name, 'params': params}

    def _eval_condition(self, condition: str, wf_ctx: WorkflowContext) -> bool:
        """Evaluate a condition expression."""
        # Simple condition evaluation
        # Supports: $var_name == value, $var_name != value, $var_name
        cond = condition.strip()
        
        if cond.startswith('$') and '==' not in cond and '!=' not in cond:
            # Boolean check: $var_name means "is truthy"
            var_name = cond[1:]
            val = wf_ctx.variables.get(var_name)
            return bool(val)
        
        if '==' in cond:
            var_name, expected = cond.split('==', 1)
            var_name = var_name.strip()[1:]  # Remove $
            expected = expected.strip().strip('"\'')
            actual = str(wf_ctx.variables.get(var_name, ''))
            return actual == expected
        
        if '!=' in cond:
            var_name, unexpected = cond.split('!=', 1)
            var_name = var_name.strip()[1:]
            unexpected = unexpected.strip().strip('"\'')
            actual = str(wf_ctx.variables.get(var_name, ''))
            return actual != unexpected
        
        return False


class WorkflowBranchAction(BaseAction):
    """Execute one of multiple branches based on a condition.
    
    Similar to if/elif/else but for workflow orchestration.
    """
    action_type = "workflow_branch"
    display_name = "工作流分支"
    description = "根据条件选择执行分支，支持if/elif/else结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Evaluate conditions and return matching branch.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - branches: list of {condition, action, params}
                - default: dict (fallback branch)
                - variables: dict (context variables for condition eval)
                - save_to_var: str
        
        Returns:
            ActionResult with selected branch info.
        """
        branches = params.get('branches', [])
        default = params.get('default', None)
        variables = params.get('variables', {})
        save_to_var = params.get('save_to_var', 'branch_result')

        selected = None
        selected_branch = None

        for branch in branches:
            condition = branch.get('condition', '')
            if self._eval_condition(condition, variables):
                selected = branch
                selected_branch = branch.get('action', 'matched')
                break

        if selected is None and default:
            selected = default
            selected_branch = default.get('action', 'default')

        if selected is None:
            return ActionResult(success=True, message="No branch matched, no default")

        result = {
            'branch': selected_branch,
            'params': selected.get('params', {}),
            'condition_matched': condition if selected != default else 'default',
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Branch selected: {selected_branch}"
        )

    def _eval_condition(self, condition: str, variables: Dict) -> bool:
        """Evaluate a condition against variables."""
        if not condition:
            return True
        
        cond = condition.strip()
        
        if cond.startswith('$') and '==' not in cond and '!=' not in cond and '>' not in cond and '<' not in cond:
            var_name = cond[1:]
            return bool(variables.get(var_name))
        
        for op in ['==', '!=', '>=', '<=', '>', '<']:
            if op in cond:
                parts = cond.split(op, 1)
                var_name = parts[0].strip()
                if var_name.startswith('$'):
                    var_name = var_name[1:]
                expected = parts[1].strip().strip('"\'')
                actual = str(variables.get(var_name, ''))
                
                if op == '==':
                    return actual == expected
                elif op == '!=':
                    return actual != expected
                elif op == '>':
                    try:
                        return float(actual) > float(expected)
                    except (ValueError, TypeError):
                        return False
                elif op == '<':
                    try:
                        return float(actual) < float(expected)
                    except (ValueError, TypeError):
                        return False
                elif op == '>=':
                    try:
                        return float(actual) >= float(expected)
                    except (ValueError, TypeError):
                        return False
                elif op == '<=':
                    try:
                        return float(actual) <= float(expected)
                    except (ValueError, TypeError):
                        return False
        
        return False
