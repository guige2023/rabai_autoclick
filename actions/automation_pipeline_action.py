"""Automation Pipeline Action Module.

Provides pipeline orchestration: step execution, dependency management,
parallel execution, error handling, and pipeline state tracking.

Example:
    result = execute(context, {"action": "create_pipeline", "steps": [...]})
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class StepStatus(Enum):
    """Pipeline step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(Enum):
    """Pipeline step types."""
    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    WAIT = "wait"
    NOTIFY = "notify"


@dataclass
class PipelineStep:
    """A single step in a pipeline."""
    
    id: str
    name: str
    step_type: StepType = StepType.ACTION
    action: Optional[str] = None
    params: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: float = 60.0
    condition: Optional[str] = None
    
    def __post_init__(self) -> None:
        """Convert string step_type to enum."""
        if isinstance(self.step_type, str):
            self.step_type = StepType(self.step_type)


@dataclass
class StepResult:
    """Result of a step execution."""
    
    step_id: str
    status: StepStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    output: Any = None
    error: Optional[str] = None
    retry_attempt: int = 0
    
    @property
    def duration_ms(self) -> float:
        """Get execution duration in milliseconds."""
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds() * 1000


class Pipeline:
    """Pipeline executor with dependency management."""
    
    def __init__(self, name: str) -> None:
        """Initialize pipeline.
        
        Args:
            name: Pipeline name
        """
        self.name = name
        self.steps: list[PipelineStep] = []
        self._results: dict[str, StepResult] = {}
    
    def add_step(self, step: PipelineStep) -> None:
        """Add step to pipeline.
        
        Args:
            step: Step to add
        """
        self.steps.append(step)
    
    def get_execution_order(self) -> list[str]:
        """Get steps in execution order (topological sort).
        
        Returns:
            List of step IDs in execution order
        """
        visited: set[str] = set()
        order: list[str] = []
        
        def visit(step_id: str) -> None:
            if step_id in visited:
                return
            visited.add(step_id)
            
            for step in self.steps:
                if step.id == step_id:
                    for dep in step.depends_on:
                        visit(dep)
                    order.append(step_id)
                    break
        
        for step in self.steps:
            visit(step.id)
        
        return order
    
    def can_execute(self, step: PipelineStep) -> bool:
        """Check if step can be executed (dependencies complete).
        
        Args:
            step: Step to check
            
        Returns:
            True if all dependencies succeeded
        """
        for dep_id in step.depends_on:
            dep_result = self._results.get(dep_id)
            if dep_result is None or dep_result.status != StepStatus.SUCCESS:
                return False
        return True
    
    def record_result(self, result: StepResult) -> None:
        """Record step execution result.
        
        Args:
            result: Step execution result
        """
        self._results[result.step_id] = result
    
    def get_status(self) -> dict[str, Any]:
        """Get pipeline status."""
        completed = sum(
            1 for r in self._results.values()
            if r.status in (StepStatus.SUCCESS, StepStatus.FAILED, StepStatus.SKIPPED)
        )
        
        return {
            "name": self.name,
            "total_steps": len(self.steps),
            "completed_steps": completed,
            "pending_steps": len(self.steps) - completed,
            "results": {
                step_id: {
                    "status": result.status.value,
                    "duration_ms": result.duration_ms,
                    "error": result.error,
                }
                for step_id, result in self._results.items()
            },
        }


class ParallelExecutor:
    """Executes multiple steps in parallel."""
    
    def __init__(self, max_workers: int = 4) -> None:
        """Initialize parallel executor.
        
        Args:
            max_workers: Maximum concurrent executions
        """
        self.max_workers = max_workers
        self._running: set[str] = set()
        self._completed: dict[str, StepResult] = {}
    
    def start_step(self, step_id: str) -> bool:
        """Start a step.
        
        Args:
            step_id: Step to start
            
        Returns:
            True if started, False if at capacity
        """
        if len(self._running) >= self.max_workers:
            return False
        
        self._running.add(step_id)
        return True
    
    def complete_step(self, step_id: str, result: StepResult) -> None:
        """Mark step as complete.
        
        Args:
            step_id: Completed step
            result: Execution result
        """
        self._running.discard(step_id)
        self._completed[step_id] = result
    
    def is_complete(self) -> bool:
        """Check if all steps complete."""
        return len(self._running) == 0 and len(self._completed) > 0
    
    def get_status(self) -> dict[str, Any]:
        """Get executor status."""
        return {
            "running": len(self._running),
            "completed": len(self._completed),
            "max_workers": self.max_workers,
        }


class ErrorHandler:
    """Handles errors in pipeline execution."""
    
    def __init__(self, continue_on_error: bool = False) -> None:
        """Initialize error handler.
        
        Args:
            continue_on_error: Whether to continue pipeline on error
        """
        self.continue_on_error = continue_on_error
        self._errors: list[dict[str, Any]] = []
    
    def handle_error(
        self,
        step_id: str,
        error: Exception,
        context: dict[str, Any],
    ) -> StepStatus:
        """Handle step execution error.
        
        Args:
            step_id: Failed step
            error: Exception that occurred
            context: Execution context
            
        Returns:
            StepStatus determining next action
        """
        self._errors.append({
            "step_id": step_id,
            "error": str(error),
            "error_type": type(error).__name__,
            "timestamp": datetime.now().isoformat(),
            "context": context,
        })
        
        if self.continue_on_error:
            return StepStatus.SKIPPED
        
        return StepStatus.FAILED
    
    def get_errors(self) -> list[dict[str, Any]]:
        """Get all recorded errors."""
        return self._errors.copy()
    
    def clear_errors(self) -> None:
        """Clear error history."""
        self._errors.clear()


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute automation pipeline action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "create_pipeline":
        pipeline = Pipeline(name=params.get("name", "pipeline"))
        result["data"] = {"pipeline_name": pipeline.name}
    
    elif action == "add_step":
        step = PipelineStep(
            id=params.get("id", ""),
            name=params.get("name", ""),
            step_type=params.get("step_type", StepType.ACTION),
            action=params.get("action"),
            params=params.get("params", {}),
            depends_on=params.get("depends_on", []),
        )
        result["data"] = {
            "step_id": step.id,
            "step_type": step.step_type.value,
        }
    
    elif action == "execution_order":
        pipeline = Pipeline(name="temp")
        step_ids = params.get("step_ids", [])
        for sid in step_ids:
            pipeline.add_step(PipelineStep(id=sid, name=sid))
        order = pipeline.get_execution_order()
        result["data"] = {"order": order}
    
    elif action == "parallel_status":
        executor = ParallelExecutor(max_workers=params.get("max_workers", 4))
        result["data"] = executor.get_status()
    
    elif action == "handle_error":
        handler = ErrorHandler(continue_on_error=params.get("continue_on_error", False))
        status = handler.handle_error(
            params.get("step_id", ""),
            ValueError(params.get("error", "Unknown")),
            context,
        )
        result["data"] = {"status": status.value}
    
    elif action == "get_errors":
        handler = ErrorHandler()
        result["data"] = {"errors": handler.get_errors()}
    
    elif action == "step_status":
        step_result = StepResult(
            step_id=params.get("step_id", ""),
            status=StepStatus(params.get("status", "pending")),
            start_time=datetime.now(),
            end_time=datetime.now(),
        )
        result["data"] = {
            "step_id": step_result.step_id,
            "duration_ms": step_result.duration_ms,
        }
    
    elif action == "pipeline_status":
        pipeline = Pipeline(name=params.get("name", "pipeline"))
        result["data"] = pipeline.get_status()
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
