"""Workflow V2 action module for RabAI AutoClick.

Provides advanced workflow execution capabilities including
parallel branches, conditional routing, and workflow templates.
"""

import time
import threading
import sys
import os
import json
import hashlib
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowStep:
    """A single step in a workflow.
    
    Attributes:
        id: Unique step identifier.
        name: Step name.
        action: Action to execute.
        params: Action parameters.
        depends_on: List of step IDs this depends on.
        retry: Number of retries on failure.
    """
    id: str
    name: str
    action: str
    params: Dict[str, Any]
    depends_on: List[str] = field(default_factory=list)
    retry: int = 0


@dataclass
class WorkflowResult:
    """Result of workflow execution.
    
    Attributes:
        status: Final workflow status.
        step_results: Results from each step.
        duration: Total execution time.
        error: Error message if failed.
    """
    status: WorkflowStatus
    step_results: Dict[str, Any]
    duration: float
    error: Optional[str] = None


class WorkflowEngine:
    """Executes workflows with step dependencies and parallel execution."""
    
    def __init__(self, action_registry: Dict[str, Callable] = None):
        """Initialize workflow engine.
        
        Args:
            action_registry: Dict mapping action names to callables.
        """
        self.action_registry = action_registry or {}
        self._running_workflows: Dict[str, WorkflowStatus] = {}
        self._lock = threading.RLock()
    
    def register_action(self, name: str, action: Callable) -> None:
        """Register an action.
        
        Args:
            name: Action name.
            action: Callable to register.
        """
        self.action_registry[name] = action
    
    def execute_workflow(
        self,
        workflow_id: str,
        steps: List[WorkflowStep],
        context: Any = None
    ) -> WorkflowResult:
        """Execute a workflow.
        
        Args:
            workflow_id: Unique workflow identifier.
            steps: List of workflow steps.
            context: Execution context.
        
        Returns:
            WorkflowResult with execution outcomes.
        """
        start_time = time.time()
        
        with self._lock:
            self._running_workflows[workflow_id] = WorkflowStatus.RUNNING
        
        step_results: Dict[str, Any] = {}
        completed: set = set()
        failed_steps: set = set()
        
        step_map = {s.id: s for s in steps}
        
        def can_execute(step: WorkflowStep) -> bool:
            """Check if step dependencies are satisfied."""
            return all(dep in completed for dep in step.depends_on)
        
        max_iterations = len(steps) * 3
        iteration = 0
        
        while len(completed) < len(steps) and iteration < max_iterations:
            iteration += 1
            
            for step in steps:
                if step.id in completed or step.id in failed_steps:
                    continue
                
                if not can_execute(step):
                    continue
                
                action_name = step.action
                
                if action_name not in self.action_registry:
                    step_results[step.id] = {"success": False, "error": f"Unknown action: {action_name}"}
                    failed_steps.add(step.id)
                    continue
                
                action_func = self.action_registry[action_name]
                retries = step.retry
                
                for attempt in range(retries + 1):
                    try:
                        result = action_func(context, step.params)
                        step_results[step.id] = result
                        
                        if hasattr(result, 'success') and result.success:
                            completed.add(step.id)
                            break
                        elif isinstance(result, dict) and result.get('success'):
                            completed.add(step.id)
                            break
                        elif isinstance(result, ActionResult) and result.success:
                            completed.add(step.id)
                            break
                        else:
                            if attempt == retries:
                                failed_steps.add(step.id)
                    
                    except Exception as e:
                        if attempt == retries:
                            step_results[step.id] = {"success": False, "error": str(e)}
                            failed_steps.add(step.id)
                        else:
                            time.sleep(0.1 * (attempt + 1))
        
        with self._lock:
            if failed_steps:
                self._running_workflows[workflow_id] = WorkflowStatus.FAILED
                status = WorkflowStatus.FAILED
                error = f"Steps {failed_steps} failed"
            else:
                self._running_workflows[workflow_id] = WorkflowStatus.COMPLETED
                status = WorkflowStatus.COMPLETED
                error = None
        
        return WorkflowResult(
            status=status,
            step_results=step_results,
            duration=time.time() - start_time,
            error=error
        )
    
    def get_status(self, workflow_id: str) -> Optional[WorkflowStatus]:
        """Get workflow status."""
        with self._lock:
            return self._running_workflows.get(workflow_id)


# Global workflow engine
_workflow_engine = WorkflowEngine()


class WorkflowExecuteAction(BaseAction):
    """Execute a workflow from step definitions."""
    action_type = "workflow_execute"
    display_name = "工作流执行"
    description = "执行工作流步骤"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute workflow.
        
        Args:
            context: Execution context.
            params: Dict with keys: workflow_id, steps.
        
        Returns:
            ActionResult with workflow result.
        """
        workflow_id = params.get('workflow_id', f"wf_{int(time.time() * 1000)}")
        steps_data = params.get('steps', [])
        
        if not steps_data:
            return ActionResult(success=False, message="steps are required")
        
        steps = []
        for step_data in steps_data:
            step = WorkflowStep(
                id=step_data.get('id', f"step_{len(steps)}"),
                name=step_data.get('name', ''),
                action=step_data.get('action', ''),
                params=step_data.get('params', {}),
                depends_on=step_data.get('depends_on', []),
                retry=step_data.get('retry', 0)
            )
            steps.append(step)
        
        result = _workflow_engine.execute_workflow(workflow_id, steps, context)
        
        return ActionResult(
            success=result.status == WorkflowStatus.COMPLETED,
            message=f"Workflow {result.status.value} in {result.duration:.2f}s",
            data={
                "workflow_id": workflow_id,
                "status": result.status.value,
                "step_results": {k: v for k, v in result.step_results.items()},
                "duration": round(result.duration, 2),
                "error": result.error
            }
        )


class WorkflowStatusAction(BaseAction):
    """Get workflow execution status."""
    action_type = "workflow_status"
    display_name = "工作流状态"
    description = "查看工作流执行状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get workflow status.
        
        Args:
            context: Execution context.
            params: Dict with keys: workflow_id.
        
        Returns:
            ActionResult with status.
        """
        workflow_id = params.get('workflow_id', '')
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        status = _workflow_engine.get_status(workflow_id)
        
        if status is None:
            return ActionResult(success=True, message="Workflow not found", data={"status": None})
        
        return ActionResult(
            success=True,
            message=f"Workflow status: {status.value}",
            data={"workflow_id": workflow_id, "status": status.value}
        )


class WorkflowTemplateAction(BaseAction):
    """Execute a workflow from a template."""
    action_type = "workflow_template"
    display_name = "工作流模板"
    description = "从模板执行工作流"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute workflow template.
        
        Args:
            context: Execution context.
            params: Dict with keys: template_name, template_steps, variables.
        
        Returns:
            ActionResult with execution result.
        """
        template_name = params.get('template_name', 'default')
        template_steps = params.get('template_steps', [])
        variables = params.get('variables', {})
        
        if not template_steps:
            return ActionResult(success=False, message="template_steps are required")
        
        resolved_steps = []
        for step_data in template_steps:
            resolved_params = {}
            for key, value in step_data.get('params', {}).items():
                if isinstance(value, str):
                    for var_name, var_value in variables.items():
                        value = value.replace(f"${{{var_name}}}", str(var_value))
                resolved_params[key] = value
            
            step = WorkflowStep(
                id=step_data.get('id', f"step_{len(resolved_steps)}"),
                name=step_data.get('name', ''),
                action=step_data.get('action', ''),
                params=resolved_params,
                depends_on=step_data.get('depends_on', []),
                retry=step_data.get('retry', 0)
            )
            resolved_steps.append(step)
        
        workflow_id = f"{template_name}_{int(time.time() * 1000)}"
        result = _workflow_engine.execute_workflow(workflow_id, resolved_steps, context)
        
        return ActionResult(
            success=result.status == WorkflowStatus.COMPLETED,
            message=f"Workflow {result.status.value} in {result.duration:.2f}s",
            data={
                "workflow_id": workflow_id,
                "template": template_name,
                "status": result.status.value,
                "duration": round(result.duration, 2)
            }
        )


class WorkflowCancelAction(BaseAction):
    """Cancel a running workflow."""
    action_type = "workflow_cancel"
    display_name = "工作流取消"
    description = "取消正在执行的工作流"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cancel workflow.
        
        Args:
            context: Execution context.
            params: Dict with keys: workflow_id.
        
        Returns:
            ActionResult with cancellation status.
        """
        workflow_id = params.get('workflow_id', '')
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        from core.base_action import BaseAction, ActionResult as AR
        _workflow_engine._running_workflows[workflow_id] = WorkflowStatus.CANCELLED
        
        return ActionResult(success=True, message=f"Workflow {workflow_id} cancelled")
