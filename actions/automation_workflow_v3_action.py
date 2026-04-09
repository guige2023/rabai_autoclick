"""
Advanced Workflow Orchestration Module v3.

Provides sophisticated workflow orchestration with parallel execution,
conditional branching, error recovery, and state persistence
for complex automation scenarios.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Set,
    Tuple, TypeVar, Generic, Union
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import logging
import asyncio
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, Future
import threading

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WorkflowStatus(Enum):
    """Workflow execution status."""
    CREATED = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    TIMED_OUT = auto()


class StepStatus(Enum):
    """Step execution status."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    SKIPPED = auto()
    FAILED = auto()
    RETRYING = auto()


class StepType(Enum):
    """Types of workflow steps."""
    ACTION = auto()
    CONDITION = auto()
    PARALLEL = auto()
    LOOP = auto()
    WAIT = auto()
    SUBWORKFLOW = auto()
    APPROVAL = auto()


@dataclass
class WorkflowStep:
    """Represents a single workflow step."""
    step_id: str
    name: str
    step_type: StepType
    action: Callable[..., Any]
    
    # Execution config
    timeout_seconds: Optional[float] = None
    retry_count: int = 0
    retry_delay_seconds: float = 1.0
    continue_on_error: bool = False
    
    # Branching
    condition: Optional[Callable[["WorkflowContext"], bool]] = None
    on_success_next: Optional[str] = None
    on_failure_next: Optional[str] = None
    
    # Parallel execution
    parallel_steps: List["WorkflowStep"] = field(default_factory=list)
    parallel_mode: str = "all"  # all, any, first
    
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """Result of step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    
    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds() * 1000
        return None
    
    @property
    def success(self) -> bool:
        return self.status == StepStatus.COMPLETED


@dataclass
class WorkflowContext:
    """Shared workflow execution context."""
    workflow_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_result(self, step_id: str) -> Optional[StepResult]:
        return self.step_results.get(step_id)
    
    def get_output(self, step_id: str) -> Any:
        result = self.step_results.get(step_id)
        return result.output if result else None
    
    def add_error(self, step_id: str, error: Exception) -> None:
        self.errors.append({
            "step_id": step_id,
            "error": str(error),
            "type": type(error).__name__,
            "timestamp": datetime.now()
        })


@dataclass
class WorkflowResult:
    """Complete workflow execution result."""
    workflow_id: str
    status: WorkflowStatus
    context: WorkflowContext
    completed_at: Optional[datetime] = None
    total_duration_ms: Optional[float] = None
    steps_executed: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "status": self.status.name,
            "duration_ms": self.total_duration_ms,
            "steps_executed": self.steps_executed,
            "errors": len(self.context.errors)
        }


class ParallelExecutor:
    """Handles parallel step execution."""
    
    def __init__(self, max_workers: int = 4) -> None:
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def execute_all(
        self,
        steps: List[WorkflowStep],
        context: WorkflowContext
    ) -> Dict[str, StepResult]:
        """Execute steps in parallel and wait for all."""
        futures: Dict[str, Future] = {}
        
        for step in steps:
            future = self.executor.submit(self._execute_step, step, context)
            futures[step.step_id] = future
        
        results = {}
        for step_id, future in futures.items():
            try:
                results[step_id] = future.result(timeout=60)
            except Exception as e:
                logger.error(f"Parallel step {step_id} failed: {e}")
                results[step_id] = StepResult(
                    step_id=step_id,
                    status=StepStatus.FAILED,
                    error=str(e)
                )
        
        return results
    
    def execute_any(
        self,
        steps: List[WorkflowStep],
        context: WorkflowContext
    ) -> Tuple[Optional[StepResult], List[StepResult]]:
        """Execute steps, return when first completes."""
        futures: Dict[str, Future] = {}
        
        for step in steps:
            future = self.executor.submit(self._execute_step, step, context)
            futures[step.step_id] = future
        
        completed = []
        first_result = None
        
        for step_id, future in futures.items():
            try:
                result = future.result(timeout=60)
                completed.append(result)
                if first_result is None:
                    first_result = result
                    # Cancel remaining
                    for fid, f in futures.items():
                        if fid != step_id:
                            f.cancel()
                    break
            except Exception as e:
                completed.append(StepResult(
                    step_id=step_id,
                    status=StepStatus.FAILED,
                    error=str(e)
                ))
        
        return first_result, completed
    
    def _execute_step(
        self,
        step: WorkflowStep,
        context: WorkflowContext
    ) -> StepResult:
        """Execute a single step."""
        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=datetime.now()
        )
        
        try:
            if asyncio.iscoroutinefunction(step.action):
                result.output = asyncio.run(step.action(context))
            else:
                result.output = step.action(context)
            
            result.status = StepStatus.COMPLETED
        
        except Exception as e:
            logger.error(f"Step {step.step_id} failed: {e}")
            result.status = StepStatus.FAILED
            result.error = str(e)
        
        result.completed_at = datetime.now()
        return result


class WorkflowOrchestrator:
    """
    Advanced workflow orchestration engine.
    
    Supports complex workflows with parallel execution,
    conditional branching, error recovery, and state management.
    """
    
    def __init__(
        self,
        workflow_id: Optional[str] = None,
        max_parallel_workers: int = 4
    ) -> None:
        self.workflow_id = workflow_id or str(uuid.uuid4())
        self.steps: Dict[str, WorkflowStep] = {}
        self.entry_point: Optional[str] = None
        self.parallel_executor = ParallelExecutor(max_workers=max_parallel_workers)
        self._status = WorkflowStatus.CREATED
        self._lock = threading.RLock()
    
    def add_step(self, step: WorkflowStep) -> "WorkflowOrchestrator":
        """Add a step to the workflow."""
        with self._lock:
            self.steps[step.step_id] = step
            if self.entry_point is None:
                self.entry_point = step.step_id
        return self
    
    def set_entry_point(self, step_id: str) -> "WorkflowOrchestrator":
        """Set workflow entry point."""
        self.entry_point = step_id
        return self
    
    def execute(
        self,
        initial_data: Optional[Dict[str, Any]] = None
    ) -> WorkflowResult:
        """
        Execute the workflow synchronously.
        
        Args:
            initial_data: Initial workflow data
            
        Returns:
            WorkflowResult with execution details
        """
        context = WorkflowContext(
            workflow_id=self.workflow_id,
            data=initial_data or {}
        )
        
        self._status = WorkflowStatus.RUNNING
        
        try:
            if self.entry_point:
                self._execute_step_recursive(self.entry_point, context)
            
            self._status = WorkflowStatus.COMPLETED
        
        except Exception as e:
            logger.error(f"Workflow {self.workflow_id} failed: {e}")
            context.add_error("workflow", e)
            self._status = WorkflowStatus.FAILED
        
        result = WorkflowResult(
            workflow_id=self.workflow_id,
            status=self._status,
            context=context,
            completed_at=datetime.now(),
            total_duration_ms=(
                datetime.now() - context.started_at
            ).total_seconds() * 1000,
            steps_executed=len(context.step_results)
        )
        
        return result
    
    def _execute_step_recursive(
        self,
        step_id: str,
        context: WorkflowContext
    ) -> Optional[StepResult]:
        """Recursively execute workflow steps."""
        if step_id not in self.steps:
            logger.warning(f"Step {step_id} not found")
            return None
        
        step = self.steps[step_id]
        
        # Check condition
        if step.condition and not step.condition(context):
            result = StepResult(
                step_id=step_id,
                status=StepStatus.SKIPPED
            )
            context.step_results[step_id] = result
            return result
        
        # Handle parallel steps
        if step.step_type == StepType.PARALLEL and step.parallel_steps:
            result = self._execute_parallel(step, context)
            context.step_results[step_id] = result
            
            if result.success and step.on_success_next:
                self._execute_step_recursive(step.on_success_next, context)
            elif not result.success and step.on_failure_next:
                self._execute_step_recursive(step.on_failure_next, context)
            
            return result
        
        # Execute step with retries
        result = self._execute_with_retry(step, context)
        context.step_results[step_id] = result
        
        # Handle branching
        if result.success and step.on_success_next:
            self._execute_step_recursive(step.on_success_next, context)
        elif not result.success and step.on_failure_next:
            if step.continue_on_error:
                self._execute_step_recursive(step.on_failure_next, context)
            else:
                context.add_error(step_id, Exception(result.error))
        
        return result
    
    def _execute_with_retry(
        self,
        step: WorkflowStep,
        context: WorkflowContext
    ) -> StepResult:
        """Execute step with retry logic."""
        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=datetime.now()
        )
        
        for attempt in range(step.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(step.action):
                    result.output = asyncio.run(step.action(context))
                else:
                    result.output = step.action(context)
                
                result.status = StepStatus.COMPLETED
                break
            
            except Exception as e:
                logger.warning(f"Step {step.step_id} attempt {attempt + 1} failed: {e}")
                
                if attempt < step.retry_count:
                    result.status = StepStatus.RETRYING
                    import time
                    time.sleep(step.retry_delay_seconds)
                    result.retry_count = attempt + 1
                else:
                    result.status = StepStatus.FAILED
                    result.error = str(e)
        
        result.completed_at = datetime.now()
        return result
    
    def _execute_parallel(
        self,
        step: WorkflowStep,
        context: WorkflowContext
    ) -> StepResult:
        """Execute parallel steps."""
        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=datetime.now()
        )
        
        if step.parallel_mode == "all":
            results = self.parallel_executor.execute_all(
                step.parallel_steps, context
            )
            result.output = results
            
            # Check if all succeeded
            all_success = all(r.success for r in results.values())
            result.status = StepStatus.COMPLETED if all_success else StepStatus.FAILED
        
        elif step.parallel_mode == "any":
            first_result, _ = self.parallel_executor.execute_any(
                step.parallel_steps, context
            )
            result.output = first_result
            result.status = StepStatus.COMPLETED if first_result and first_result.success else StepStatus.FAILED
        
        result.completed_at = datetime.now()
        return result
    
    @property
    def status(self) -> WorkflowStatus:
        return self._status


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Define workflow steps
    def fetch_data(ctx: WorkflowContext) -> Dict:
        ctx.data["fetched"] = True
        return {"items": [1, 2, 3]}
    
    def process_data(ctx: WorkflowContext) -> int:
        items = ctx.data.get("items", [])
        return sum(items)
    
    def check_result(ctx: WorkflowContext) -> bool:
        return ctx.data.get("result", 0) > 0
    
    def notify(ctx: WorkflowContext) -> None:
        print(f"Notified: result = {ctx.data.get('result')}")
    
    # Build workflow
    workflow = WorkflowOrchestrator()
    
    workflow.add_step(WorkflowStep(
        step_id="fetch",
        name="Fetch Data",
        step_type=StepType.ACTION,
        action=fetch_data
    ))
    
    workflow.add_step(WorkflowStep(
        step_id="process",
        name="Process Data",
        step_type=StepType.ACTION,
        action=process_data,
        on_success_next="check",
        on_failure_next="fetch"  # Retry on failure
    ))
    
    workflow.add_step(WorkflowStep(
        step_id="check",
        name="Check Result",
        step_type=StepType.CONDITION,
        action=check_result,
        on_success_next="notify",
        on_failure_next="fetch"
    ))
    
    workflow.add_step(WorkflowStep(
        step_id="notify",
        name="Send Notification",
        step_type=StepType.ACTION,
        action=notify
    ))
    
    workflow.set_entry_point("fetch")
    
    # Execute
    print("=== Workflow Execution ===")
    result = workflow.execute({"initial": "data"})
    
    print(f"\nStatus: {result.status.name}")
    print(f"Duration: {result.total_duration_ms:.2f}ms")
    print(f"Steps executed: {result.steps_executed}")
    print(f"Errors: {len(result.context.errors)}")
