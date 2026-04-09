"""
Automation Chain Builder Action.

Provides a fluent API for constructing automation chains with
sequential and parallel execution, error handling, and branching.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

logger = logging.getLogger(__name__)
T = TypeVar("T")


class ChainStepType(Enum):
    """Types of steps in an automation chain."""
    ACTION = auto()
    CONDITION = auto()
    BRANCH = auto()
    PARALLEL = auto()
    LOOP = auto()
    TRANSFORM = auto()
    FILTER = auto()
    SINK = auto()


class BranchStrategy(Enum):
    """How branches are selected."""
    FIRST_MATCH = auto()
    ALL_MATCHING = auto()
    PRIORITY = auto()


@dataclass
class ChainStepResult:
    """Result of executing a single chain step."""
    step_name: str
    success: bool
    output: Any
    error: Optional[str] = None
    duration_ms: float = 0.0
    attempts: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.success


@dataclass
class ChainExecutionContext:
    """Shared context passed through all chain steps."""
    data: Dict[str, Any] = field(default_factory=dict)
    results: List[ChainStepResult] = field(default_factory=list)
    errors: List[ChainStepResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_result(self, step_name: str) -> Optional[ChainStepResult]:
        """Get result of a specific step by name."""
        for r in self.results:
            if r.step_name == step_name:
                return r
        return None

    def get_output(self, step_name: str, default: Any = None) -> Any:
        """Get output of a specific step, with default if not found or failed."""
        result = self.get_result(step_name)
        if result is None or not result.success:
            return default
        return result.output


@dataclass
class ChainExecutionResult:
    """Final result of chain execution."""
    success: bool
    context: ChainExecutionContext
    duration_ms: float
    steps_executed: int
    steps_failed: int
    final_output: Any = None
    error: Optional[str] = None

    def summary(self) -> str:
        status = "SUCCESS" if self.success else "FAILURE"
        return (f"[{status}] {self.steps_executed} steps, "
                f"{self.steps_failed} failed, {self.duration_ms:.1f}ms")


@dataclass
class ChainStep:
    """A single step in an automation chain."""
    name: str
    step_type: ChainStepType
    handler: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None
    max_retries: int = 0
    timeout_seconds: Optional[float] = None
    on_error: Optional[Callable[[Exception, Any], Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0

    def __post_init__(self) -> None:
        if self.step_type == ChainStepType.ACTION and self.condition is not None:
            raise ValueError("ACTION step cannot have a condition")


class AutomationChainBuilder:
    """
    Fluent builder for automation chains.

    Example:
        chain = (AutomationChainBuilder("data-pipeline")
            .action("fetch", fetch_data)
            .condition("validate", validate_input, {
                True: "process",
                False: "log_error",
            })
            .action("process", process_data)
            .sink("store", store_result)
            .build())
        result = await chain.execute(input_data)
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._steps: List[ChainStep] = []
        self._branches: Dict[str, List[ChainStep]] = {}
        self._branch_strategy = BranchStrategy.FIRST_MATCH
        self._default_branch: Optional[str] = None
        self._max_parallel_workers = 4

    def action(
        self,
        name: str,
        handler: Callable[[Any], Any],
        *,
        retries: int = 0,
        timeout: Optional[float] = None,
        on_error: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> Self:
        """Add an action step (synchronous or async)."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.ACTION,
            handler=handler,
            max_retries=retries,
            timeout_seconds=timeout,
            on_error=on_error,
        ))
        return self

    def async_action(
        self,
        name: str,
        handler: Callable[[Any], Awaitable[Any]],
        *,
        retries: int = 0,
        timeout: Optional[float] = None,
        on_error: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> Self:
        """Add an async action step."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.ACTION,
            handler=handler,
            max_retries=retries,
            timeout_seconds=timeout,
            on_error=on_error,
        ))
        return self

    def condition(
        self,
        name: str,
        predicate: Callable[[Any], bool],
        branches: Optional[Dict[bool, str]] = None,
    ) -> Self:
        """Add a condition step that determines next branch."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.CONDITION,
            handler=lambda ctx: predicate(ctx),
            condition=None,
        ))
        self._branches[name] = []
        return self

    def branch(self, name: str, steps: List[ChainStep]) -> Self:
        """Set branch steps for a condition."""
        self._branches[name] = steps
        return self

    def transform(
        self,
        name: str,
        transformer: Callable[[Any], Any],
    ) -> Self:
        """Add a transform step that modifies context data."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.TRANSFORM,
            handler=transformer,
        ))
        return self

    def filter(
        self,
        name: str,
        predicate: Callable[[Any], bool],
    ) -> Self:
        """Add a filter step that only continues if predicate is True."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.FILTER,
            handler=predicate,
        ))
        return self

    def parallel(
        self,
        name: str,
        handlers: Dict[str, Callable[[Any], Any]],
        *,
        timeout: Optional[float] = None,
        wait_strategy: str = "all",  # all | any | first
    ) -> Self:
        """Add a parallel execution step."""
        def parallel_handler(ctx: Any) -> Dict[str, Any]:
            results: Dict[str, Any] = {}
            with ThreadPoolExecutor(max_workers=min(len(handlers), self._max_parallel_workers)) as executor:
                futures = {k: executor.submit(h, ctx) for k, h in handlers.items()}
                for k, future in futures.items():
                    try:
                        results[k] = future.result(timeout=timeout)
                    except Exception as exc:
                        logger.error("Parallel step %s.%s failed: %s", name, k, exc)
                        results[k] = {"error": str(exc)}
            return results

        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.PARALLEL,
            handler=parallel_handler,
            metadata={"wait_strategy": wait_strategy, "handlers": list(handlers.keys())},
        ))
        return self

    def sink(
        self,
        name: str,
        handler: Callable[[Any], None],
    ) -> Self:
        """Add a sink step (terminal action that doesn't pass data forward)."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.SINK,
            handler=handler,
        ))
        return self

    def loop(
        self,
        name: str,
        items_key: str,
        step: ChainStep,
        max_iterations: int = 1000,
    ) -> Self:
        """Add a loop step that iterates over items."""
        self._steps.append(ChainStep(
            name=name,
            step_type=ChainStepType.LOOP,
            handler=lambda ctx: self._execute_loop(ctx, items_key, step, max_iterations),
            metadata={"items_key": items_key, "max_iterations": max_iterations, "inner_step": step},
        ))
        return self

    def _execute_loop(
        self,
        ctx: ChainExecutionContext,
        items_key: str,
        step: ChainStep,
        max_iterations: int,
    ) -> List[Any]:
        """Execute loop logic."""
        items = ctx.data.get(items_key, [])
        if not isinstance(items, (list, tuple)):
            raise TypeError(f"Loop items must be list/tuple, got {type(items).__name__}")
        results = []
        for i, item in enumerate(items[:max_iterations]):
            ctx.data[f"{items_key}_item"] = item
            ctx.data[f"{items_key}_index"] = i
            try:
                output = self._run_step(step, ctx)
                results.append(output)
            except Exception as exc:
                logger.error("Loop iteration %d failed: %s", i, exc)
                results.append({"error": str(exc)})
        return results

    def build(self) -> AutomationChain:
        """Build the final automation chain."""
        return AutomationChain(
            name=self._name,
            steps=self._steps,
            branches=self._branches,
            branch_strategy=self._branch_strategy,
            default_branch=self._default_branch,
        )

    def set_branch_strategy(self, strategy: BranchStrategy) -> Self:
        """Set how branches are selected."""
        self._branch_strategy = strategy
        return self


class AutomationChain:
    """Executable automation chain."""

    def __init__(
        self,
        name: str,
        steps: List[ChainStep],
        branches: Dict[str, List[ChainStep]],
        branch_strategy: BranchStrategy,
        default_branch: Optional[str],
    ) -> None:
        self.name = name
        self._steps = steps
        self._branches = branches
        self._branch_strategy = branch_strategy
        self._default_branch = default_branch

    async def execute(self, initial_data: Any) -> ChainExecutionResult:
        """Execute the chain with initial data."""
        import time
        start = time.monotonic()
        context = ChainExecutionContext(data={"input": initial_data})

        try:
            current = initial_data
            for step in self._steps:
                try:
                    output = await self._execute_step(step, current, context)
                    if step.step_type != ChainStepType.SINK:
                        current = output
                        context.data[f"step_{step.name}_output"] = output
                except Exception as exc:
                    logger.error("Step %s failed: %s", step.name, exc)
                    error_result = ChainStepResult(
                        step_name=step.name,
                        success=False,
                        output=None,
                        error=str(exc),
                    )
                    context.errors.append(error_result)
                    if step.on_error:
                        current = step.on_error(exc, current)
                    else:
                        return ChainExecutionResult(
                            success=False,
                            context=context,
                            duration_ms=(time.monotonic() - start) * 1000,
                            steps_executed=len(context.results),
                            steps_failed=len(context.errors),
                            error=str(exc),
                        )

            duration = (time.monotonic() - start) * 1000
            return ChainExecutionResult(
                success=True,
                context=context,
                duration_ms=duration,
                steps_executed=len(context.results),
                steps_failed=len(context.errors),
                final_output=current,
            )
        except Exception as exc:
            return ChainExecutionResult(
                success=False,
                context=context,
                duration_ms=(time.monotonic() - start) * 1000,
                steps_executed=len(context.results),
                steps_failed=len(context.errors) + 1,
                error=str(exc),
            )

    async def _execute_step(
        self,
        step: ChainStep,
        current: Any,
        context: ChainExecutionContext,
    ) -> Any:
        """Execute a single step, handling retries."""
        import time

        for attempt in range(max(1, step.max_retries + 1)):
            attempt_start = time.monotonic()
            try:
                result = step.handler(current)
                if asyncio.iscoroutine(result):
                    result = await asyncio.wait_for(result, timeout=step.timeout_seconds)
                duration = (time.monotonic() - attempt_start) * 1000
                step_result = ChainStepResult(
                    step_name=step.name,
                    success=True,
                    output=result,
                    duration_ms=duration,
                    attempts=attempt + 1,
                )
                context.results.append(step_result)
                return result
            except Exception as exc:
                if attempt < step.max_retries:
                    logger.warning("Step %s attempt %d failed, retrying: %s", step.name, attempt + 1, exc)
                    await asyncio.sleep(0.1 * (2 ** attempt))
                else:
                    raise

    def _run_step(self, step: ChainStep, ctx: Any) -> Any:
        """Run step synchronously."""
        result = step.handler(ctx)
        if asyncio.iscoroutine(result):
            raise TypeError("Use async execute for async handlers")
        return result
