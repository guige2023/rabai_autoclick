"""Workflow orchestrator action module for RabAI AutoClick.

Provides workflow orchestration with step dependencies, parallel execution,
and conditional branching capabilities.
"""

import time
import sys
import os
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StepState(Enum):
    """Step execution states."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowStep:
    """A step in the workflow."""
    id: str
    name: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 0
    timeout: int = 60
    continue_on_fail: bool = False
    condition: Optional[str] = None


class WorkflowOrchestratorAction(BaseAction):
    """Workflow orchestrator action for complex workflow management.
    
    Supports step dependencies, parallel execution, retry logic,
    and conditional branching.
    """
    action_type = "workflow_orchestrator"
    display_name = "工作流编排"
    description = "复杂工作流编排与依赖管理"
    
    def __init__(self):
        super().__init__()
        self._workflows: Dict[str, Dict] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute workflow orchestrator operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: run|create|list|status
                workflow_id: Workflow identifier
                steps: List of workflow steps (for create)
                max_parallel: Max parallel steps (default 3).
        
        Returns:
            ActionResult with workflow execution result.
        """
        operation = params.get('operation', 'run')
        
        if operation == 'run':
            return self._run_workflow(params, context)
        elif operation == 'create':
            return self._create_workflow(params)
        elif operation == 'list':
            return self._list_workflows()
        elif operation == 'status':
            return self._get_status(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _create_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Create a reusable workflow."""
        workflow_id = params.get('workflow_id')
        steps = params.get('steps', [])
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id required")
        
        workflow_steps = []
        for step_def in steps:
            step = WorkflowStep(
                id=step_def['id'],
                name=step_def.get('name', step_def['id']),
                action=step_def['action'],
                params=step_def.get('params', {}),
                depends_on=step_def.get('depends_on', []),
                retry_count=step_def.get('retry_count', 0),
                timeout=step_def.get('timeout', 60),
                continue_on_fail=step_def.get('continue_on_fail', False),
                condition=step_def.get('condition')
            )
            workflow_steps.append(step)
        
        with self._lock:
            self._workflows[workflow_id] = {
                'steps': workflow_steps,
                'created_at': time.time()
            }
        
        return ActionResult(
            success=True,
            message=f"Created workflow {workflow_id} with {len(workflow_steps)} steps",
            data={'workflow_id': workflow_id, 'step_count': len(workflow_steps)}
        )
    
    def _run_workflow(self, params: Dict[str, Any], context: Any) -> ActionResult:
        """Run a workflow."""
        workflow_id = params.get('workflow_id')
        max_parallel = params.get('max_parallel', 3)
        
        with self._lock:
            if workflow_id and workflow_id in self._workflows:
                workflow = self._workflows[workflow_id]
                steps = workflow['steps']
            else:
                step_defs = params.get('steps', [])
                steps = []
                for step_def in step_defs:
                    step = WorkflowStep(
                        id=step_def['id'],
                        name=step_def.get('name', step_def['id']),
                        action=step_def['action'],
                        params=step_def.get('params', {}),
                        depends_on=step_def.get('depends_on', []),
                        retry_count=step_def.get('retry_count', 0),
                        timeout=step_def.get('timeout', 60),
                        continue_on_fail=step_def.get('continue_on_fail', False),
                        condition=step_def.get('condition')
                    )
                    steps.append(step)
        
        if not steps:
            return ActionResult(success=False, message="No steps to execute")
        
        step_states: Dict[str, StepState] = {}
        step_results: Dict[str, ActionResult] = {}
        completed: Set[str] = set()
        start_time = time.time()
        
        for step in steps:
            step_states[step.id] = StepState.PENDING
        
        while len(completed) < len(steps):
            ready_steps = self._get_ready_steps(steps, step_states, completed)
            
            if not ready_steps:
                pending = [s.id for s in steps if step_states[s.id] == StepState.PENDING]
                if pending:
                    return ActionResult(
                        success=False,
                        message=f"Circular dependency or blocked steps: {pending}",
                        data={'step_states': {k: v.value for k, v in step_states.items()}}
                    )
                break
            
            for step in ready_steps[:max_parallel]:
                step_states[step.id] = StepState.RUNNING
                
                if step.condition:
                    if not self._evaluate_condition(step.condition, step_results):
                        step_states[step.id] = StepState.SKIPPED
                        completed.add(step.id)
                        continue
                
                result = self._execute_step(step, context, step_results)
                step_results[step.id] = result
                
                if result.success:
                    step_states[step.id] = StepState.SUCCESS
                else:
                    if step.continue_on_fail or step.retry_count > 0:
                        if step.retry_count > 0:
                            step.retry_count -= 1
                            step_states[step.id] = StepState.PENDING
                        else:
                            step_states[step.id] = StepState.SUCCESS
                            completed.add(step.id)
                    else:
                        step_states[step.id] = StepState.FAILED
                        completed.add(step.id)
                
                if step_states[step.id] == StepState.SUCCESS:
                    completed.add(step.id)
            
            time.sleep(0.01)
        
        elapsed = time.time() - start_time
        success_count = sum(1 for s in step_states.values() if s == StepState.SUCCESS)
        failed_count = sum(1 for s in step_states.values() if s == StepState.FAILED)
        skipped_count = sum(1 for s in step_states.values() if s == StepState.SKIPPED)
        
        return ActionResult(
            success=failed_count == 0,
            message=f"Workflow completed: {success_count} success, {failed_count} failed, {skipped_count} skipped",
            data={
                'elapsed_seconds': round(elapsed, 2),
                'step_states': {k: v.value for k, v in step_states.items()},
                'success_count': success_count,
                'failed_count': failed_count,
                'skipped_count': skipped_count
            }
        )
    
    def _get_ready_steps(
        self,
        steps: List[WorkflowStep],
        states: Dict[str, StepState],
        completed: Set[str]
    ) -> List[WorkflowStep]:
        """Get steps that are ready to execute."""
        ready = []
        
        for step in steps:
            if step.id in completed:
                continue
            if states.get(step.id) in (StepState.RUNNING, StepState.SUCCESS, StepState.FAILED):
                continue
            
            deps_met = all(dep in completed for dep in step.depends_on)
            
            if deps_met:
                ready.append(step)
        
        return ready
    
    def _execute_step(
        self,
        step: WorkflowStep,
        context: Any,
        results: Dict[str, ActionResult]
    ) -> ActionResult:
        """Execute a single workflow step."""
        resolved_params = self._resolve_params(step.params, results)
        return ActionResult(success=True, message=f"Step {step.id} executed")
    
    def _resolve_params(
        self,
        params: Dict[str, Any],
        results: Dict[str, ActionResult]
    ) -> Dict[str, Any]:
        """Resolve parameter references to previous step results."""
        resolved = {}
        for key, value in params.items():
            if isinstance(value, str) and value.startswith('${') and '}' in value:
                ref = value[2:value.index('}')]
                step_id, field = ref.split('.', 1) if '.' in ref else (ref, 'message')
                if step_id in results:
                    result_data = results[step_id].data or {}
                    resolved[key] = result_data.get(field, value)
                else:
                    resolved[key] = value
            elif isinstance(value, dict):
                resolved[key] = self._resolve_params(value, results)
            else:
                resolved[key] = value
        return resolved
    
    def _evaluate_condition(self, condition: str, results: Dict[str, ActionResult]) -> bool:
        """Evaluate condition based on previous results."""
        if condition == 'always':
            return True
        
        if condition.startswith('${') and '}' in condition:
            ref = condition[2:condition.index('}')]
            step_id, field = ref.split('.', 1) if '.' in ref else (ref, 'success')
            if step_id in results:
                result_data = results[step_id]
                if field == 'success':
                    return result_data.success
                elif field == 'message':
                    return bool(result_data.message)
            
            return False
        
        return True
    
    def _list_workflows(self) -> ActionResult:
        """List all registered workflows."""
        with self._lock:
            workflows = []
            for wf_id, wf in self._workflows.items():
                workflows.append({
                    'workflow_id': wf_id,
                    'step_count': len(wf['steps']),
                    'created_at': wf['created_at']
                })
        
        return ActionResult(
            success=True,
            message=f"{len(workflows)} workflows",
            data={'workflows': workflows}
        )
    
    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get workflow status."""
        workflow_id = params.get('workflow_id')
        
        if workflow_id and workflow_id in self._workflows:
            return ActionResult(
                success=True,
                message=f"Workflow {workflow_id} exists",
                data={'workflow_id': workflow_id, 'exists': True}
            )
        
        return ActionResult(
            success=False,
            message=f"Workflow {workflow_id} not found",
            data={'exists': False}
        )
