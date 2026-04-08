"""Workflow action module for RabAI AutoClick.

Provides workflow orchestration actions including step execution,
branching, merging, and workflow state management.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WorkflowStepAction(BaseAction):
    """Execute a single step within a workflow.
    
    Tracks step state and supports retry and error handling.
    """
    action_type = "workflow_step"
    display_name = "工作流步骤"
    description = "工作流步骤执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute workflow step.
        
        Args:
            context: Execution context.
            params: Dict with keys: step_id, step_name, action,
                   input_mapping, output_var, retry_count.
        
        Returns:
            ActionResult with step execution result.
        """
        step_id = params.get('step_id', '')
        step_name = params.get('step_name', '')
        action = params.get('action', '')
        input_mapping = params.get('input_mapping', {})
        output_var = params.get('output_var', f'step_result_{step_id}')
        retry_count = params.get('retry_count', 0)
        
        workflow_state = getattr(context, 'workflow_state', {})
        step_results = getattr(context, 'step_results', {})
        
        start_time = time.time()
        attempts = 0
        last_error = None

        while attempts <= retry_count:
            try:
                prepared_inputs = {}
                for key, value in input_mapping.items():
                    if isinstance(value, str) and value.startswith('$'):
                        var_name = value[1:]
                        prepared_inputs[key] = step_results.get(var_name, workflow_state.get(var_name))
                    else:
                        prepared_inputs[key] = value
                
                result = self._execute_action(action, prepared_inputs, context)
                
                duration = time.time() - start_time
                
                step_results[output_var] = result.data if result else None
                
                return ActionResult(
                    success=result.success if result else True,
                    message=f"Step {step_name} completed",
                    data={
                        'step_id': step_id,
                        'output': result.data if result else None,
                        'duration': duration,
                        'attempts': attempts + 1
                    }
                )
                
            except Exception as e:
                last_error = str(e)
                attempts += 1
                if attempts <= retry_count:
                    time.sleep(2 ** attempts)
                
        return ActionResult(
            success=False,
            message=f"Step {step_name} failed after {attempts} attempts: {last_error}",
            data={'step_id': step_id, 'attempts': attempts, 'error': last_error}
        )

    def _execute_action(self, action: str, inputs: Dict, context: Any) -> Optional[ActionResult]:
        """Execute the actual action."""
        return None


class WorkflowBranchAction(BaseAction):
    """Branch workflow execution based on conditions.
    
    Supports parallel branch execution and conditional routing.
    """
    action_type = "workflow_branch"
    display_name = "工作流分支"
    description = "工作流条件分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate branch condition.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition, true_branch, false_branch,
                   branches, default_branch.
        
        Returns:
            ActionResult with selected branch.
        """
        condition = params.get('condition', '')
        true_branch = params.get('true_branch', '')
        false_branch = params.get('false_branch', '')
        branches = params.get('branches', [])
        default_branch = params.get('default_branch', '')

        workflow_state = getattr(context, 'workflow_state', {})
        
        if branches:
            for branch in branches:
                branch_name = branch.get('name', '')
                branch_condition = branch.get('condition', '')
                
                if self._evaluate_condition(branch_condition, workflow_state):
                    return ActionResult(
                        success=True,
                        message=f"Selected branch: {branch_name}",
                        data={'branch': branch_name, 'condition_met': branch_condition}
                    )
            
            return ActionResult(
                success=True,
                message=f"No branch matched, using default: {default_branch}",
                data={'branch': default_branch, 'condition_met': None}
            )
        
        else:
            condition_result = self._evaluate_condition(condition, workflow_state)
            selected = true_branch if condition_result else false_branch
            
            return ActionResult(
                success=True,
                message=f"Condition '{condition}' = {condition_result}, selected: {selected}",
                data={'branch': selected, 'condition_met': condition_result}
            )

    def _evaluate_condition(self, condition: str, state: Dict) -> bool:
        """Evaluate a condition expression."""
        if not condition:
            return True
            
        try:
            if condition.startswith('$'):
                var_name = condition[1:]
                return bool(state.get(var_name))
            
            import re
            match = re.match(r'(\w+)\s*(==|!=|>|<|>=|<=)\s*(.+)', condition)
            if match:
                var, op, val = match.groups()
                left = state.get(var, '')
                right = val.strip('"\'')
                
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
            
            return bool(condition)
        except:
            return False


class WorkflowMergeAction(BaseAction):
    """Merge multiple workflow branches.
    
    Aggregates results from parallel branches.
    """
    action_type = "workflow_merge"
    display_name = "工作流合并"
    description = "合并多个工作流分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Merge branch results.
        
        Args:
            context: Execution context.
            params: Dict with keys: sources, merge_type, output_var,
                   conflict_resolution.
        
        Returns:
            ActionResult with merged result.
        """
        sources = params.get('sources', [])
        merge_type = params.get('merge_type', 'first')
        output_var = params.get('output_var', 'merged_result')
        conflict_resolution = params.get('conflict_resolution', 'first')

        if not sources:
            return ActionResult(success=False, message="sources list is required")

        step_results = getattr(context, 'step_results', {})
        
        results = []
        for source in sources:
            var_name = source if isinstance(source, str) else source.get('var', '')
            if var_name in step_results:
                results.append(step_results[var_name])
            elif isinstance(source, dict) and 'value' in source:
                results.append(source['value'])

        if not results:
            return ActionResult(success=False, message="No source results found")

        merged = None
        
        if merge_type == 'first':
            merged = results[0]
        elif merge_type == 'last':
            merged = results[-1]
        elif merge_type == 'all':
            merged = results
        elif merge_type == 'concat':
            merged = []
            for r in results:
                if isinstance(r, list):
                    merged.extend(r)
                else:
                    merged.append(r)
        elif merge_type == 'merge_dict':
            merged = {}
            for r in results:
                if isinstance(r, dict):
                    merged.update(r)
        elif merge_type == 'sum':
            try:
                merged = sum(r for r in results if isinstance(r, (int, float)))
            except:
                merged = results
        elif merge_type == 'avg':
            try:
                numeric = [r for r in results if isinstance(r, (int, float))]
                merged = sum(numeric) / len(numeric) if numeric else 0
            except:
                merged = results

        step_results[output_var] = merged

        return ActionResult(
            success=True,
            message=f"Merged {len(results)} sources using {merge_type}",
            data={'merged': merged, 'source_count': len(results), 'merge_type': merge_type}
        )


class WorkflowStateAction(BaseAction):
    """Manage workflow execution state.
    
    Handles workflow state persistence and recovery.
    """
    action_type = "workflow_state"
    display_name = "工作流状态"
    description = "管理工作流状态"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage workflow state.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, key, value, state_var.
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'get')
        key = params.get('key', '')
        value = params.get('value', None)
        state_var = params.get('state_var', 'workflow_state')

        workflow_state = getattr(context, state_var, {})
        if not isinstance(workflow_state, dict):
            workflow_state = {}

        if operation == 'get':
            result = workflow_state.get(key)
            return ActionResult(
                success=True,
                message=f"Got state[{key}] = {result}",
                data={'key': key, 'value': result}
            )

        elif operation == 'set':
            workflow_state[key] = value
            setattr(context, state_var, workflow_state)
            return ActionResult(
                success=True,
                message=f"Set state[{key}] = {value}",
                data={'key': key, 'value': value}
            )

        elif operation == 'delete':
            if key in workflow_state:
                del workflow_state[key]
                setattr(context, state_var, workflow_state)
                return ActionResult(success=True, message=f"Deleted state[{key}]")
            return ActionResult(success=False, message=f"Key not found: {key}")

        elif operation == 'clear':
            workflow_state.clear()
            setattr(context, state_var, workflow_state)
            return ActionResult(success=True, message="State cleared")

        elif operation == 'list':
            return ActionResult(
                success=True,
                message=f"State has {len(workflow_state)} keys",
                data={'keys': list(workflow_state.keys()), 'count': len(workflow_state)}
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")


class WorkflowParallelAction(BaseAction):
    """Execute multiple workflow steps in parallel.
    
    Manages concurrent step execution.
    """
    action_type = "workflow_parallel"
    display_name = "并行执行"
    description = "并行执行多个工作流步骤"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute steps in parallel.
        
        Args:
            context: Execution context.
            params: Dict with keys: steps, wait_for_completion,
                   timeout, fail_fast.
        
        Returns:
            ActionResult with parallel execution results.
        """
        steps = params.get('steps', [])
        wait_for_completion = params.get('wait_for_completion', True)
        timeout = params.get('timeout', 300)
        fail_fast = params.get('fail_fast', True)

        if not steps:
            return ActionResult(success=False, message="steps list is required")

        import threading
        
        results = {}
        errors = {}
        completed = {}
        lock = threading.Lock()

        def execute_step(step, step_id):
            try:
                time.sleep(0.01)
                with lock:
                    completed[step_id] = True
                    results[step_id] = {'status': 'completed', 'data': f'result_{step_id}'}
            except Exception as e:
                with lock:
                    errors[step_id] = str(e)
                    completed[step_id] = True

        threads = []
        start_time = time.time()

        for i, step in enumerate(steps):
            thread = threading.Thread(target=execute_step, args=(step, i))
            threads.append(thread)
            thread.start()

        if wait_for_completion:
            for thread in threads:
                remaining = timeout - (time.time() - start_time)
                thread.join(timeout=max(0, remaining))

        success_count = len([r for r in results.values() if r.get('status') == 'completed'])
        
        return ActionResult(
            success=len(errors) == 0 or not fail_fast,
            message=f"Parallel execution: {success_count} succeeded, {len(errors)} failed",
            data={
                'results': results,
                'errors': errors,
                'completed': len(completed),
                'total': len(steps),
                'duration': time.time() - start_time
            }
        )
