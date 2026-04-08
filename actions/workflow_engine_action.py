"""Workflow engine action module for RabAI AutoClick.

Provides workflow execution:
- WorkflowEngine: Execute workflows
- WorkflowBuilder: Build workflows
- StepExecutor: Execute workflow steps
- StateManager: Manage workflow state
- WorkflowMonitor: Monitor workflow execution
"""

import time
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(Enum):
    """Workflow step types."""
    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    WAIT = "wait"
    NOTIFY = "notify"


@dataclass
class WorkflowStep:
    """Workflow step definition."""
    id: str
    name: str
    step_type: StepType
    handler: Callable
    condition: Optional[Callable[[Dict], bool]] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: float = 60.0
    on_failure: Optional[str] = None
    next_step: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """Result of step execution."""
    step_id: str
    status: StepStatus
    started_at: float
    completed_at: Optional[float] = None
    output: Any = None
    error: Optional[str] = None
    retry_attempt: int = 0


@dataclass
class WorkflowState:
    """Workflow execution state."""
    workflow_id: str
    status: str = "running"
    current_step: Optional[str] = None
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    error: Optional[str] = None


class WorkflowBuilder:
    """Build workflows."""

    def __init__(self, name: str):
        self.name = name
        self._steps: Dict[str, WorkflowStep] = {}
        self._entry_point: Optional[str] = None
        self._step_order: List[str] = []

    def add_step(
        self,
        step_id: str,
        name: str,
        step_type: StepType,
        handler: Callable,
        condition: Optional[Callable] = None,
        retry_count: int = 0,
        retry_delay: float = 1.0,
        timeout: float = 60.0,
        on_failure: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> "WorkflowBuilder":
        """Add a step to workflow."""
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=step_type,
            handler=handler,
            condition=condition,
            retry_count=retry_count,
            retry_delay=retry_delay,
            timeout=timeout,
            on_failure=on_failure,
            metadata=metadata or {},
        )
        self._steps[step_id] = step
        self._step_order.append(step_id)
        if self._entry_point is None:
            self._entry_point = step_id
        return self

    def set_entry_point(self, step_id: str) -> "WorkflowBuilder":
        """Set workflow entry point."""
        self._entry_point = step_id
        return self

    def link_steps(self, from_step: str, to_step: str) -> "WorkflowBuilder":
        """Link two steps."""
        if from_step in self._steps:
            self._steps[from_step].next_step = to_step
        return self

    def build(self) -> "WorkflowEngine":
        """Build the workflow."""
        return WorkflowEngine(name=self.name, steps=self._steps, entry_point=self._entry_point)


class StepExecutor:
    """Execute workflow steps."""

    def __init__(self):
        self._handlers: Dict[StepType, Callable] = {}

    def execute(self, step: WorkflowStep, context: Dict[str, Any]) -> StepResult:
        """Execute a single step."""
        start_time = time.time()
        result = StepResult(
            step_id=step.id,
            status=StepStatus.RUNNING,
            started_at=start_time,
        )

        try:
            if step.condition and not step.condition(context):
                result.status = StepStatus.SKIPPED
                result.output = "Condition not met"
                return result

            handler = self._handlers.get(step.step_type, step.handler)
            if callable(handler):
                output = self._execute_with_timeout(handler, context, step.timeout)
                result.status = StepStatus.COMPLETED
                result.output = output
            else:
                raise ValueError(f"No handler for step type {step.step_type}")

        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = str(e)

            if step.retry_count > 0 and result.retry_attempt < step.retry_count:
                result.status = StepStatus.RETRYING
                time.sleep(step.retry_delay)
                result.retry_attempt += 1

        result.completed_at = time.time()
        return result

    def _execute_with_timeout(self, handler: Callable, context: Dict, timeout: float) -> Any:
        """Execute handler with timeout."""
        result = [None]
        error = [None]
        done = [False]

        def target():
            try:
                result[0] = handler(context)
            except Exception as e:
                error[0] = e
            finally:
                done[0] = True

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout=timeout)

        if not done[0]:
            raise TimeoutError(f"Step execution timed out after {timeout}s")

        if error[0]:
            raise error[0]

        return result[0]

    def register_handler(self, step_type: StepType, handler: Callable):
        """Register handler for step type."""
        self._handlers[step_type] = handler


class WorkflowEngine:
    """Workflow execution engine."""

    def __init__(self, name: str, steps: Dict[str, WorkflowStep], entry_point: Optional[str] = None):
        self.name = name
        self.steps = steps
        self.entry_point = entry_point
        self.executor = StepExecutor()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def execute(self, initial_context: Optional[Dict] = None) -> WorkflowState:
        """Execute the workflow."""
        workflow_id = str(uuid.uuid4())
        state = WorkflowState(
            workflow_id=workflow_id,
            context=initial_context or {},
        )

        current_step_id = self.entry_point
        while current_step_id:
            step = self.steps.get(current_step_id)
            if not step:
                break

            state.current_step = current_step_id
            step_result = self.executor.execute(step, state.context)
            state.step_results[current_step_id] = step_result

            if step_result.status == StepStatus.FAILED:
                state.status = "failed"
                state.error = step_result.error

                if step.on_failure and step.on_failure in self.steps:
                    current_step_id = step.on_failure
                else:
                    break

            elif step_result.status == StepStatus.COMPLETED:
                current_step_id = step.next_step

            elif step_result.status == StepStatus.SKIPPED:
                current_step_id = step.next_step

            else:
                break

        if state.status == "running":
            state.status = "completed"

        state.completed_at = time.time()
        return state

    def get_state(self, workflow_id: str) -> Optional[WorkflowState]:
        """Get workflow state (placeholder)."""
        return None


class WorkflowMonitor:
    """Monitor workflow execution."""

    def __init__(self):
        self._workflows: Dict[str, WorkflowState] = {}
        self._lock = threading.RLock()

    def track(self, state: WorkflowState):
        """Track workflow state."""
        with self._lock:
            self._workflows[state.workflow_id] = state

    def get_status(self, workflow_id: str) -> Optional[Dict]:
        """Get workflow status."""
        with self._lock:
            state = self._workflows.get(workflow_id)
            if not state:
                return None

            return {
                "workflow_id": state.workflow_id,
                "status": state.status,
                "current_step": state.current_step,
                "completed_steps": sum(1 for r in state.step_results.values() if r.status == StepStatus.COMPLETED),
                "failed_steps": sum(1 for r in state.step_results.values() if r.status == StepStatus.FAILED),
                "duration": (state.completed_at or time.time()) - state.started_at,
            }

    def list_workflows(self, status: Optional[str] = None) -> List[Dict]:
        """List workflows."""
        with self._lock:
            workflows = list(self._workflows.values())
            if status:
                workflows = [w for w in workflows if w.status == status]
            return [
                {
                    "workflow_id": w.workflow_id,
                    "status": w.status,
                    "started_at": w.started_at,
                    "duration": (w.completed_at or time.time()) - w.started_at,
                }
                for w in workflows
            ]


class WorkflowEngineAction(BaseAction):
    """Workflow engine action."""
    action_type = "workflow_engine"
    display_name = "工作流引擎"
    description = "工作流编排和执行"

    def __init__(self):
        super().__init__()
        self._workflows: Dict[str, WorkflowEngine] = {}
        self._monitor = WorkflowMonitor()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "build":
                return self._build_workflow(params)
            elif operation == "execute":
                return self._execute_workflow(params)
            elif operation == "status":
                return self._get_status(params)
            elif operation == "list":
                return self._list_workflows(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Workflow error: {str(e)}")

    def _build_workflow(self, params: Dict) -> ActionResult:
        """Build a workflow."""
        name = params.get("name", "workflow")
        steps_data = params.get("steps", [])

        builder = WorkflowBuilder(name)

        for step_data in steps_data:
            step_id = step_data.get("id")
            step_name = step_data.get("name", step_id)
            step_type_str = step_data.get("type", "ACTION").upper()
            handler = step_data.get("handler", lambda ctx: ctx)

            try:
                step_type = StepType[step_type_str]
            except KeyError:
                step_type = StepType.ACTION

            builder.add_step(
                step_id=step_id,
                name=step_name,
                step_type=step_type,
                handler=handler,
                retry_count=step_data.get("retry_count", 0),
                retry_delay=step_data.get("retry_delay", 1.0),
                timeout=step_data.get("timeout", 60.0),
            )

        workflow = builder.build()
        self._workflows[name] = workflow

        return ActionResult(success=True, message=f"Workflow '{name}' built", data={"name": name})

    def _execute_workflow(self, params: Dict) -> ActionResult:
        """Execute a workflow."""
        name = params.get("name")
        initial_context = params.get("context", {})

        if not name:
            return ActionResult(success=False, message="name is required")

        if name not in self._workflows:
            return ActionResult(success=False, message=f"Workflow '{name}' not found")

        workflow = self._workflows[name]
        state = workflow.execute(initial_context)
        self._monitor.track(state)

        return ActionResult(
            success=state.status == "completed",
            message=f"Workflow {state.status}",
            data={
                "workflow_id": state.workflow_id,
                "status": state.status,
                "duration": (state.completed_at or time.time()) - state.started_at,
                "step_results": {
                    sid: {
                        "status": r.status.value,
                        "output": str(r.output)[:100] if r.output else None,
                        "error": r.error,
                    }
                    for sid, r in state.step_results.items()
                },
            },
        )

    def _get_status(self, params: Dict) -> ActionResult:
        """Get workflow status."""
        workflow_id = params.get("workflow_id")
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")

        status = self._monitor.get_status(workflow_id)
        if not status:
            return ActionResult(success=False, message="Workflow not found")

        return ActionResult(success=True, message="Status retrieved", data=status)

    def _list_workflows(self, params: Dict) -> ActionResult:
        """List all workflows."""
        status_filter = params.get("status")
        workflows = self._monitor.list_workflows(status_filter)

        return ActionResult(
            success=True,
            message=f"{len(workflows)} workflows",
            data={"workflows": workflows},
        )
