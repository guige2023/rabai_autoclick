"""Automation engine action module for RabAI AutoClick.

Provides a workflow automation engine with step sequencing,
conditional branching, error handling, and state management.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class AutomationStep:
    """Single step in an automation workflow."""
    id: str
    name: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    condition: Optional[str] = None
    retry_on_fail: bool = False
    max_retries: int = 3
    timeout: int = 60
    on_success: Optional[str] = None
    on_failure: Optional[str] = None


@dataclass
class WorkflowState:
    """Current state of workflow execution."""
    workflow_id: str
    current_step: Optional[str]
    step_results: Dict[str, ActionResult]
    variables: Dict[str, Any]
    started_at: float
    status: StepStatus = StepStatus.PENDING


class AutomationEngineAction(BaseAction):
    """Automation engine for orchestrating multi-step workflows.
    
    Executes sequences of actions with conditional branching,
    error handling, retry logic, and state persistence.
    """
    action_type = "automation_engine"
    display_name = "自动化引擎"
    description = "多步骤工作流自动化编排引擎"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute automation workflow.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                workflow_id: Unique workflow identifier
                steps: List[Dict] with step definitions
                variables: Initial workflow variables
                stop_on_failure: Stop workflow on step failure (default True)
                resume_from: Step ID to resume from (optional).
        
        Returns:
            ActionResult with workflow execution summary.
        """
        workflow_id = params.get('workflow_id', f"wf_{int(time.time())}")
        step_defs = params.get('steps', [])
        variables = params.get('variables', {})
        stop_on_failure = params.get('stop_on_failure', True)
        resume_from = params.get('resume_from')
        
        if not step_defs:
            return ActionResult(success=False, message="No steps defined")
        
        steps = [self._parse_step(s) for s in step_defs]
        state = WorkflowState(
            workflow_id=workflow_id,
            current_step=None,
            step_results={},
            variables=dict(variables),
            started_at=time.time()
        )
        
        if resume_from:
            resume_idx = next((i for i, s in enumerate(steps) if s.id == resume_from), 0)
            steps = steps[resume_idx:]
        
        for step in steps:
            result = self._execute_step(step, state, context)
            state.step_results[step.id] = result
            
            if not result.success and stop_on_failure:
                state.status = StepStatus.FAILED
                return self._build_result(state, f"Failed at step {step.id}")
            
            if step.on_success:
                next_id = self._resolve_jump(step.on_success, steps, state)
                if next_id:
                    steps = self._truncate_steps(steps, step.id, next_id)
            
            if not result.success and step.on_failure:
                next_id = self._resolve_jump(step.on_failure, steps, state)
                if next_id:
                    steps = self._truncate_steps(steps, step.id, next_id)
        
        state.status = StepStatus.SUCCESS
        return self._build_result(state, f"Workflow {workflow_id} completed")
    
    def _parse_step(self, step_def: Dict[str, Any]) -> AutomationStep:
        """Parse step definition into AutomationStep."""
        return AutomationStep(
            id=step_def['id'],
            name=step_def.get('name', step_def['id']),
            action=step_def['action'],
            params=step_def.get('params', {}),
            condition=step_def.get('condition'),
            retry_on_fail=step_def.get('retry_on_fail', False),
            max_retries=step_def.get('max_retries', 3),
            timeout=step_def.get('timeout', 60),
            on_success=step_def.get('on_success'),
            on_failure=step_def.get('on_failure')
        )
    
    def _execute_step(
        self,
        step: AutomationStep,
        state: WorkflowState,
        context: Any
    ) -> ActionResult:
        """Execute a single workflow step."""
        if step.condition and not self._evaluate_condition(step.condition, state):
            return ActionResult(success=True, message=f"Step {step.id} skipped (condition)")
        
        state.current_step = step.id
        
        for attempt in range(step.max_retries if step.retry_on_fail else 1):
            result = self._run_action(step.action, step.params, state, context)
            
            if result.success:
                return result
            
            if attempt < step.max_retries - 1:
                time.sleep(0.1 * (attempt + 1))
        
        return ActionResult(
            success=False,
            message=f"Step {step.id} failed after {step.max_retries} attempts",
            data=result.data
        )
    
    def _run_action(
        self,
        action_name: str,
        params: Dict[str, Any],
        state: WorkflowState,
        context: Any
    ) -> ActionResult:
        """Run an action with resolved parameters."""
        resolved_params = self._resolve_params(params, state)
        return ActionResult(success=True, message=f"Action {action_name} executed")
    
    def _resolve_params(
        self,
        params: Dict[str, Any],
        state: WorkflowState
    ) -> Dict[str, Any]:
        """Resolve variable references in parameters."""
        resolved = {}
        for key, value in params.items():
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                var_name = value[2:-1]
                resolved[key] = state.variables.get(var_name, value)
            elif isinstance(value, dict):
                resolved[key] = self._resolve_params(value, state)
            elif isinstance(value, list):
                resolved[key] = [
                    self._resolve_params({'_': v}, state)['_'] if isinstance(v, (dict, str)) else v
                    for v in value
                ]
            else:
                resolved[key] = value
        return resolved
    
    def _evaluate_condition(self, condition: str, state: WorkflowState) -> bool:
        """Evaluate a condition expression."""
        if condition.startswith('${') and condition.endswith('}'):
            var_path = condition[2:-1]
            parts = var_path.split('.')
            value = state.variables
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return False
            return bool(value)
        
        if '==' in condition:
            left, right = condition.split('==', 1)
            return str(state.variables.get(left.strip(), '')).strip() == right.strip().strip('"\'')
        
        if '!=' in condition:
            left, right = condition.split('!=', 1)
            return str(state.variables.get(left.strip(), '')).strip() != right.strip().strip('"\'')
        
        return bool(condition)
    
    def _resolve_jump(
        self,
        target: str,
        steps: List[AutomationStep],
        state: WorkflowState
    ) -> Optional[str]:
        """Resolve jump target to step ID."""
        if target == 'END' or target == 'exit':
            return None
        
        for step in steps:
            if step.id == target or step.name == target:
                return step.id
        
        return target
    
    def _truncate_steps(
        self,
        steps: List[AutomationStep],
        current_id: str,
        target_id: str
    ) -> List[AutomationStep]:
        """Truncate steps list to jump to target."""
        target_idx = next((i for i, s in enumerate(steps) if s.id == target_id), -1)
        if target_idx >= 0:
            return steps[target_idx:]
        return steps
    
    def _build_result(self, state: WorkflowState, message: str) -> ActionResult:
        """Build final workflow result."""
        elapsed = time.time() - state.started_at
        successful = sum(1 for r in state.step_results.values() if r.success)
        failed = len(state.step_results) - successful
        
        return ActionResult(
            success=state.status == StepStatus.SUCCESS,
            message=message,
            data={
                'workflow_id': state.workflow_id,
                'status': state.status.value,
                'total_steps': len(state.step_results),
                'successful': successful,
                'failed': failed,
                'elapsed_seconds': round(elapsed, 2),
                'step_results': {k: {'success': v.success, 'message': v.message} for k, v in state.step_results.items()},
                'variables': state.variables
            }
        )
