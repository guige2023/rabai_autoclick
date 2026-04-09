"""
Automation Pipeline Orchestration Module.

Provides configurable pipeline execution with stages, branching,
error handling, and retry capabilities for complex automation workflows.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Set, TypeVar,
    Generic, Union, Protocol
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()


class StageStatus(Enum):
    """Individual stage execution status."""
    SKIPPED = auto()
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    RETRYING = auto()


@dataclass
class StageResult:
    """Result of a pipeline stage execution."""
    stage_name: str
    status: StageStatus
    output: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_ms(self) -> Optional[float]:
        """Calculate execution duration in milliseconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None


@dataclass
class PipelineContext:
    """Shared context passed through pipeline stages."""
    data: Dict[str, Any] = field(default_factory=dict)
    results: Dict[str, StageResult] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_previous_result(self, stage_name: str) -> Optional[StageResult]:
        """Get result from a previous stage."""
        return self.results.get(stage_name)
    
    def get_output(self, stage_name: str) -> Any:
        """Get output from a previous stage."""
        result = self.results.get(stage_name)
        return result.output if result else None


StageFunction = Callable[[PipelineContext], Any]
ConditionFunction = Callable[[PipelineContext], bool]


class PipelineStage:
    """Represents a single stage in the automation pipeline."""
    
    def __init__(
        self,
        name: str,
        func: StageFunction,
        retry_count: int = 0,
        retry_delay: float = 1.0,
        continue_on_error: bool = False,
        condition: Optional[ConditionFunction] = None,
        timeout: Optional[float] = None
    ) -> None:
        self.name = name
        self.func = func
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.continue_on_error = continue_on_error
        self.condition = condition
        self.timeout = timeout


class AutomationPipeline:
    """
    Configurable automation pipeline orchestrator.
    
    Supports sequential and parallel execution, branching,
    error handling, and retry logic.
    """
    
    def __init__(
        self,
        name: str,
        description: Optional[str] = None
    ) -> None:
        self.name = name
        self.description = description
        self.stages: List[PipelineStage] = []
        self.branches: Dict[str, List[str]] = {}
        self._status = PipelineStatus.PENDING
    
    def add_stage(
        self,
        name: str,
        func: StageFunction,
        retry_count: int = 0,
        retry_delay: float = 1.0,
        continue_on_error: bool = False,
        condition: Optional[ConditionFunction] = None,
        timeout: Optional[float] = None
    ) -> "AutomationPipeline":
        """
        Add a stage to the pipeline.
        
        Args:
            name: Stage name
            func: Stage execution function
            retry_count: Number of retries on failure
            retry_delay: Delay between retries in seconds
            continue_on_error: Whether to continue pipeline on stage failure
            condition: Optional condition for stage execution
            timeout: Optional stage timeout in seconds
            
        Returns:
            Self for method chaining
        """
        stage = PipelineStage(
            name=name,
            func=func,
            retry_count=retry_count,
            retry_delay=retry_delay,
            continue_on_error=continue_on_error,
            condition=condition,
            timeout=timeout
        )
        self.stages.append(stage)
        return self
    
    def add_branch(
        self,
        branch_name: str,
        stages: List[str]
    ) -> "AutomationPipeline":
        """
        Add a named branch with stage references.
        
        Args:
            branch_name: Name of the branch
            stages: List of stage names in this branch
            
        Returns:
            Self for method chaining
        """
        self.branches[branch_name] = stages
        return self
    
    async def execute_async(
        self,
        initial_context: Optional[PipelineContext] = None
    ) -> PipelineContext:
        """
        Execute pipeline asynchronously.
        
        Args:
            initial_context: Optional initial context
            
        Returns:
            Final pipeline context with all results
        """
        context = initial_context or PipelineContext()
        self._status = PipelineStatus.RUNNING
        
        for stage in self.stages:
            result = await self._execute_stage_async(stage, context)
            context.results[stage.name] = result
            
            if result.status == StageStatus.FAILED and not stage.continue_on_error:
                self._status = PipelineStatus.FAILED
                context.errors.append(
                    f"Stage '{stage.name}' failed: {result.error}"
                )
                break
        
        if self._status == PipelineStatus.RUNNING:
            self._status = PipelineStatus.COMPLETED
        
        return context
    
    async def _execute_stage_async(
        self,
        stage: PipelineStage,
        context: PipelineContext
    ) -> StageResult:
        """Execute a single stage with retries."""
        if stage.condition and not stage.condition(context):
            return StageResult(stage.name, StageStatus.SKIPPED)
        
        result = StageResult(stage.name, StageStatus.RUNNING)
        result.start_time = datetime.now()
        
        for attempt in range(stage.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(stage.func):
                    output = await asyncio.wait_for(
                        stage.func(context),
                        timeout=stage.timeout
                    )
                else:
                    output = stage.func(context)
                
                result.status = StageStatus.COMPLETED
                result.output = output
                result.retry_count = attempt
                break
            
            except Exception as e:
                logger.warning(
                    f"Stage '{stage.name}' attempt {attempt + 1} failed: {e}"
                )
                if attempt < stage.retry_count:
                    result.status = StageStatus.RETRYING
                    await asyncio.sleep(stage.retry_delay)
                else:
                    result.status = StageStatus.FAILED
                    result.error = str(e)
        
        result.end_time = datetime.now()
        return result
    
    def execute(
        self,
        initial_context: Optional[PipelineContext] = None
    ) -> PipelineContext:
        """
        Execute pipeline synchronously.
        
        Args:
            initial_context: Optional initial context
            
        Returns:
            Final pipeline context
        """
        return asyncio.run(self.execute_async(initial_context))
    
    def execute_parallel(
        self,
        stage_groups: List[List[str]],
        initial_context: Optional[PipelineContext] = None
    ) -> PipelineContext:
        """
        Execute multiple stage groups in parallel.
        
        Args:
            stage_groups: List of stage name groups to run in parallel
            initial_context: Optional initial context
            
        Returns:
            Combined pipeline context
        """
        context = initial_context or PipelineContext()
        
        def run_group(group: List[str]) -> Dict[str, StageResult]:
            results = {}
            for stage_name in group:
                stage = next(s for s in self.stages if s.name == stage_name)
                results[stage_name] = StageResult(
                    stage_name, StageStatus.COMPLETED, output=stage.func(context)
                )
            return results
        
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(run_group, g) for g in stage_groups]
            for future in futures:
                context.results.update(future.result())
        
        return context
    
    @property
    def status(self) -> PipelineStatus:
        """Get current pipeline status."""
        return self._status
    
    def get_summary(self) -> Dict[str, Any]:
        """Get pipeline execution summary."""
        return {
            "name": self.name,
            "status": self._status.name,
            "total_stages": len(self.stages),
            "stages": [
                {"name": s.name, "retry_count": s.retry_count}
                for s in self.stages
            ]
        }


class PipelineBuilder:
    """Builder for constructing complex pipelines."""
    
    def __init__(self, name: str) -> None:
        self.pipeline = AutomationPipeline(name)
    
    def stage(
        self,
        name: str,
        retry_count: int = 0,
        continue_on_error: bool = False
    ) -> "PipelineBuilder":
        """Add a basic stage."""
        def dummy_func(ctx: PipelineContext) -> Any:
            return None
        self.pipeline.add_stage(name, dummy_func, retry_count, continue_on_error=continue_on_error)
        return self
    
    def build(self) -> AutomationPipeline:
        """Build the pipeline."""
        return self.pipeline


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    def stage1(ctx: PipelineContext) -> str:
        ctx.data["stage1"] = "completed"
        return "stage1_output"
    
    def stage2(ctx: PipelineContext) -> str:
        return f"stage2_output: {ctx.data.get('stage1', 'none')}"
    
    def conditional_stage(ctx: PipelineContext) -> str:
        if ctx.data.get("skip_conditional"):
            return "skipped"
        return "executed"
    
    pipeline = (
        AutomationPipeline("ExamplePipeline")
        .add_stage("init", lambda c: c.data.update({"initialized": True}))
        .add_stage("process", stage1)
        .add_stage("finalize", stage2)
        .add_stage("conditional", conditional_stage, condition=lambda c: True)
    )
    
    result = pipeline.execute()
    print(f"Pipeline status: {result.results['init'].status.name}")
