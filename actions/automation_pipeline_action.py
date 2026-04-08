"""
Automation Pipeline Action Module

Provides pipeline orchestration for complex automation workflows.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(Enum):
    """Individual step status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class PipelineStep:
    """A single step in the pipeline."""
    name: str
    action: Callable[[Any], Awaitable[Any]]
    dependencies: list[str] = field(default_factory=list)
    retry_count: int = 0
    timeout_seconds: float = 300
    condition: Optional[Callable[[Any], bool]] = None
    on_failure: Optional[str] = None  # Step name to execute on failure
    
    def __post_init__(self):
        self.status = StepStatus.PENDING
        self.result = None
        self.error = None
        self.attempts = 0


@dataclass
class PipelineContext:
    """Shared context passed through pipeline steps."""
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any):
        self.data[key] = value
    
    def add_error(self, step: str, error: str, details: Optional[dict] = None):
        self.errors.append({
            "step": step,
            "error": error,
            "details": details or {},
            "timestamp": datetime.now().isoformat()
        })


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    status: PipelineStatus
    context: PipelineContext
    start_time: datetime
    end_time: Optional[datetime] = None
    step_results: dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0
    
    @property
    def success(self) -> bool:
        return self.status == PipelineStatus.COMPLETED


class AutomationPipelineAction:
    """Main pipeline orchestration action."""
    
    def __init__(self):
        self._steps: dict[str, PipelineStep] = {}
        self._execution_graph: dict[str, list[str]] = {}
        self._parallel_groups: list[list[str]] = []
    
    def add_step(self, step: PipelineStep) -> "AutomationPipelineAction":
        """Add a step to the pipeline."""
        self._steps[step.name] = step
        self._build_graph()
        return self
    
    def add_parallel_group(self, step_names: list[str]) -> "AutomationPipelineAction":
        """Add a group of steps to run in parallel."""
        self._parallel_groups.append(step_names)
        for name in step_names:
            if name in self._steps:
                self._steps[name].dependencies = []
        return self
    
    def _build_graph(self):
        """Build execution dependency graph."""
        self._execution_graph.clear()
        
        for name, step in self._steps.items():
            if name not in self._execution_graph:
                self._execution_graph[name] = []
            for dep in step.dependencies:
                if dep in self._execution_graph:
                    self._execution_graph[dep].append(name)
    
    def _get_execution_order(self) -> list[list[str]]:
        """Get steps in execution order (topological sort with levels)."""
        in_degree = {name: len(step.dependencies) for name, step in self._steps.items()}
        levels = []
        remaining = set(self._steps.keys())
        
        while remaining:
            # Find all steps with no remaining dependencies
            current_level = [name for name in remaining if in_degree[name] == 0]
            
            if not current_level:
                raise ValueError("Circular dependency detected in pipeline")
            
            levels.append(current_level)
            
            for name in current_level:
                remaining.remove(name)
                for dependent in self._execution_graph.get(name, []):
                    in_degree[dependent] -= 1
        
        return levels
    
    async def execute(
        self,
        initial_data: Optional[dict[str, Any]] = None,
        start_step: Optional[str] = None,
        end_step: Optional[str] = None,
        skip_steps: Optional[list[str]] = None
    ) -> PipelineResult:
        """
        Execute the pipeline.
        
        Args:
            initial_data: Initial data to pass through pipeline
            start_step: Optional step to start from
            end_step: Optional step to end at
            skip_steps: Steps to skip during execution
            
        Returns:
            PipelineResult with execution details
        """
        context = PipelineContext(data=initial_data or {})
        start_time = datetime.now()
        skip_steps = set(skip_steps or [])
        
        try:
            execution_levels = self._get_execution_order()
            
            # Filter by start/end steps if specified
            if start_step or end_step:
                execution_levels = self._filter_execution_levels(
                    execution_levels, start_step, end_step
                )
            
            for level_idx, level in enumerate(execution_levels):
                # Filter out skipped steps
                level = [s for s in level if s not in skip_steps]
                
                if not level:
                    continue
                
                # Execute level (parallel or sequential)
                if len(level) > 1:
                    await self._execute_parallel(level, context)
                else:
                    await self._execute_step(level[0], context)
                
                # Check if any step failed critically
                if context.errors and context.errors[-1].get("critical", False):
                    break
            
            all_completed = all(
                self._steps[name].status == StepStatus.COMPLETED
                for name in self._steps
                if name not in skip_steps
            )
            
            status = PipelineStatus.COMPLETED if all_completed else PipelineStatus.FAILED
            
        except Exception as e:
            context.add_error("pipeline", str(e), {"stage": "execution"})
            status = PipelineStatus.FAILED
        
        return PipelineResult(
            status=status,
            context=context,
            start_time=start_time,
            end_time=datetime.now(),
            step_results={
                name: {"status": step.status.value, "result": step.result}
                for name, step in self._steps.items()
            }
        )
    
    def _filter_execution_levels(
        self,
        levels: list[list[str]],
        start_step: Optional[str],
        end_step: Optional[str]
    ) -> list[list[str]]:
        """Filter execution levels based on start/end steps."""
        # Simple implementation - can be enhanced
        return levels
    
    async def _execute_parallel(
        self,
        step_names: list[str],
        context: PipelineContext
    ):
        """Execute multiple steps in parallel."""
        tasks = [self._execute_step(name, context) for name in step_names]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_step(self, name: str, context: PipelineContext):
        """Execute a single step."""
        step = self._steps[name]
        step.status = StepStatus.RUNNING
        
        try:
            # Check condition if specified
            if step.condition and not step.condition(context.data):
                step.status = StepStatus.SKIPPED
                return
            
            # Execute with timeout
            result = await asyncio.wait_for(
                step.action(context),
                timeout=step.timeout_seconds
            )
            
            step.status = StepStatus.COMPLETED
            step.result = result
            context.set(name, result)
            
        except asyncio.TimeoutError:
            step.status = StepStatus.FAILED
            step.error = f"Step timed out after {step.timeout_seconds}s"
            context.add_error(name, step.error, {"type": "timeout"})
            
        except Exception as e:
            step.status = StepStatus.FAILED
            step.error = str(e)
            context.add_error(name, step.error, {"type": "execution_error"})
            
            # Handle retry
            if step.retry_count > 0 and step.attempts < step.retry_count:
                step.attempts += 1
                step.status = StepStatus.RETRYING
                await asyncio.sleep(2 ** step.attempts)  # Exponential backoff
                await self._execute_step(name, context)
            
            # Execute on_failure step if specified
            if step.on_failure and step.on_failure in self._steps:
                await self._execute_step(step.on_failure, context)
    
    async def validate(self) -> dict[str, Any]:
        """Validate pipeline configuration."""
        errors = []
        warnings = []
        
        # Check for missing dependencies
        for name, step in self._steps.items():
            for dep in step.dependencies:
                if dep not in self._steps:
                    errors.append(f"Step '{name}' depends on unknown step '{dep}'")
        
        # Check for circular dependencies
        try:
            self._get_execution_order()
        except ValueError as e:
            errors.append(str(e))
        
        # Check for orphaned steps
        dependents = set()
        for step in self._steps.values():
            dependents.update(step.dependencies)
        
        orphaned = set(self._steps.keys()) - dependents
        if orphaned:
            # First step is expected to have no dependencies
            if len(orphaned) > 1:
                warnings.append(f"Orphaned steps found (no dependents): {orphaned}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "step_count": len(self._steps)
        }
    
    def get_status(self) -> dict[str, Any]:
        """Get current pipeline status."""
        return {
            "steps": {
                name: {
                    "status": step.status.value,
                    "attempts": step.attempts,
                    "error": step.error
                }
                for name, step in self._steps.items()
            },
            "execution_order": self._get_execution_order()
        }
