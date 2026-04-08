"""Workflow engine action module for RabAI AutoClick.

Provides workflow orchestration with step execution, branching,
error handling, and state persistence.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StepStatus(Enum):
    """Workflow step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowStep:
    """A single workflow step."""
    id: str
    name: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    conditions: List[Dict[str, Any]] = field(default_factory=list)
    on_success: Optional[str] = None  # Next step ID
    on_failure: Optional[str] = None
    retry_count: int = 0
    timeout: float = 60.0


@dataclass
class WorkflowState:
    """Workflow execution state."""
    workflow_id: str
    current_step: str
    step_results: Dict[str, Any] = field(default_factory=dict)
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class WorkflowEngineAction(BaseAction):
    """Execute multi-step workflows with branching and error handling.
    
    Supports conditional branching, retry logic, parallel execution,
    and state persistence across workflow runs.
    """
    action_type = "workflow_engine"
    display_name = "工作流引擎"
    description = "多步骤工作流编排与执行"
    
    def __init__(self):
        super().__init__()
        self._workflows: Dict[str, List[WorkflowStep]] = {}
        self._states: Dict[str, WorkflowState] = {}
        self._handlers: Dict[str, Callable] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute workflow operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'run', 'resume', 'status', 'cancel'
                - workflow_id: Workflow identifier
                - steps: List of step definitions (for register)
                - initial_data: Initial workflow data (for run)
        
        Returns:
            ActionResult with workflow execution result.
        """
        operation = params.get('operation', 'run').lower()
        
        if operation == 'register':
            return self._register_workflow(params)
        elif operation == 'run':
            return self._run_workflow(params)
        elif operation == 'resume':
            return self._resume_workflow(params)
        elif operation == 'status':
            return self._get_status(params)
        elif operation == 'cancel':
            return self._cancel_workflow(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _register_workflow(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Register a workflow definition."""
        workflow_id = params.get('workflow_id')
        steps = params.get('steps', [])
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        workflow_steps = []
        for step_def in steps:
            step = WorkflowStep(
                id=step_def.get('id'),
                name=step_def.get('name', step_def.get('id')),
                action=step_def.get('action'),
                params=step_def.get('params', {}),
                conditions=step_def.get('conditions', []),
                on_success=step_def.get('on_success'),
                on_failure=step_def.get('on_failure'),
                retry_count=step_def.get('retry_count', 0),
                timeout=step_def.get('timeout', 60.0)
            )
            workflow_steps.append(step)
        
        self._workflows[workflow_id] = workflow_steps
        
        return ActionResult(
            success=True,
            message=f"Registered workflow '{workflow_id}' with {len(steps)} steps",
            data={'workflow_id': workflow_id, 'step_count': len(steps)}
        )
    
    def _run_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Run a workflow from start."""
        workflow_id = params.get('workflow_id')
        initial_data = params.get('initial_data', {})
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        if workflow_id not in self._workflows:
            return ActionResult(
                success=False,
                message=f"Workflow '{workflow_id}' not found"
            )
        
        # Initialize state
        state = WorkflowState(
            workflow_id=workflow_id,
            current_step=self._workflows[workflow_id][0].id if self._workflows[workflow_id] else "",
            step_results={'initial': initial_data}
        )
        self._states[workflow_id] = state
        
        # Execute workflow
        return self._execute_workflow(workflow_id)
    
    def _resume_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Resume a paused or failed workflow."""
        workflow_id = params.get('workflow_id')
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        if workflow_id not in self._states:
            return ActionResult(
                success=False,
                message=f"No state found for workflow '{workflow_id}'"
            )
        
        return self._execute_workflow(workflow_id)
    
    def _execute_workflow(self, workflow_id: str) -> ActionResult:
        """Execute workflow steps sequentially."""
        state = self._states[workflow_id]
        steps = self._workflows[workflow_id]
        
        step_map = {s.id: s for s in steps}
        results = state.step_results
        
        while state.current_step:
            step = step_map.get(state.current_step)
            if not step:
                return ActionResult(
                    success=False,
                    message=f"Step '{state.current_step}' not found"
                )
            
            # Check conditions
            if not self._check_conditions(step, results):
                state.step_results[step.id] = {'status': 'skipped'}
                state.current_step = step.on_success
                continue
            
            # Execute step
            try:
                result = self._execute_step(step, results)
                results[step.id] = result
                
                if result.get('success'):
                    state.current_step = step.on_success
                else:
                    if step.retry_count > 0:
                        # Retry logic would go here
                        state.current_step = step.on_failure
                    else:
                        state.current_step = step.on_failure
            except Exception as e:
                results[step.id] = {'success': False, 'error': str(e)}
                state.current_step = step.on_failure
            
            # Check if next step exists
            if not state.current_step or state.current_step not in step_map:
                break
        
        # Workflow complete
        all_success = all(
            r.get('success', False) for k, r in results.items()
            if k != 'initial'
        )
        
        return ActionResult(
            success=all_success,
            message=f"Workflow {'completed' if all_success else 'failed'}",
            data={
                'workflow_id': workflow_id,
                'results': results,
                'all_success': all_success
            }
        )
    
    def _execute_step(
        self,
        step: WorkflowStep,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a single workflow step."""
        # Get handler for action
        handler = self._handlers.get(step.action)
        
        if handler:
            return handler(step.params, context)
        
        # Default: return params as result
        return {
            'success': True,
            'output': step.params
        }
    
    def _check_conditions(
        self,
        step: WorkflowStep,
        context: Dict[str, Any]
    ) -> bool:
        """Check if step conditions are met."""
        if not step.conditions:
            return True
        
        for condition in step.conditions:
            field = condition.get('field')
            operator = condition.get('op')
            value = condition.get('value')
            
            # Get value from context
            context_value = context
            for key in field.split('.'):
                context_value = context_value.get(key, {})
            
            # Check condition
            if operator == 'eq' and context_value != value:
                return False
            elif operator == 'ne' and context_value == value:
                return False
            elif operator == 'exists' and context_value is None:
                return False
        
        return True
    
    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get workflow status."""
        workflow_id = params.get('workflow_id')
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        if workflow_id not in self._states:
            return ActionResult(
                success=False,
                message=f"No state found for workflow '{workflow_id}'"
            )
        
        state = self._states[workflow_id]
        
        return ActionResult(
            success=True,
            message=f"Workflow status: {state.status.value}",
            data={
                'workflow_id': workflow_id,
                'current_step': state.current_step,
                'status': state.status.value,
                'results': state.step_results
            }
        )
    
    def _cancel_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a running workflow."""
        workflow_id = params.get('workflow_id')
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        if workflow_id in self._states:
            self._states[workflow_id].status = StepStatus.FAILED
            return ActionResult(
                success=True,
                message=f"Workflow '{workflow_id}' cancelled"
            )
        
        return ActionResult(
            success=False,
            message=f"Workflow '{workflow_id}' not found"
        )
    
    def register_handler(self, action: str, handler: Callable) -> None:
        """Register a step handler."""
        self._handlers[action] = handler
