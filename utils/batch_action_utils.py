"""
Batch Action Utilities

Provides utilities for executing multiple actions
as a single batch operation in UI automation.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable
from enum import Enum, auto
import asyncio


class BatchStrategy(Enum):
    """Strategy for batch action execution."""
    SEQUENTIAL = auto()
    PARALLEL = auto()
    PRIORITY = auto()
    FAIL_FAST = auto()
    ALL_OR_NOTHING = auto()


@dataclass
class BatchAction:
    """Represents a single action in a batch."""
    id: str
    func: Callable[..., Awaitable[Any] | Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    timeout_ms: float = 30000.0


@dataclass
class BatchResult:
    """Result of a batch action execution."""
    action_id: str
    success: bool
    result: Any = None
    error: str | None = None
    duration_ms: float = 0.0


class BatchExecutor:
    """
    Executes multiple actions as a batch.
    
    Supports various execution strategies including
    sequential, parallel, and priority-based ordering.
    """

    def __init__(
        self,
        strategy: BatchStrategy = BatchStrategy.SEQUENTIAL,
    ) -> None:
        self._strategy = strategy
        self._actions: list[BatchAction] = []

    def add_action(
        self,
        action_id: str,
        func: Callable[..., Awaitable[Any] | Any],
        *args: Any,
        priority: int = 0,
        timeout_ms: float = 30000.0,
        **kwargs: Any,
    ) -> None:
        """Add an action to the batch."""
        self._actions.append(BatchAction(
            id=action_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            timeout_ms=timeout_ms,
        ))

    async def execute(self) -> list[BatchResult]:
        """
        Execute all batch actions using the configured strategy.
        
        Returns:
            List of BatchResult for each action.
        """
        if not self._actions:
            return []

        if self._strategy == BatchStrategy.SEQUENTIAL:
            return await self._execute_sequential()
        elif self._strategy == BatchStrategy.PARALLEL:
            return await self._execute_parallel()
        elif self._strategy == BatchStrategy.PRIORITY:
            return await self._execute_priority()
        elif self._strategy == BatchStrategy.FAIL_FAST:
            return await self._execute_fail_fast()
        elif self._strategy == BatchStrategy.ALL_OR_NOTHING:
            return await self._execute_all_or_nothing()
        return []

    async def _execute_sequential(self) -> list[BatchResult]:
        """Execute actions sequentially."""
        results = []
        for action in self._actions:
            result = await self._run_action(action)
            results.append(result)
        return results

    async def _execute_parallel(self) -> list[BatchResult]:
        """Execute actions in parallel."""
        tasks = [self._run_action(a) for a in self._actions]
        return await asyncio.gather(*tasks)

    async def _execute_priority(self) -> list[BatchResult]:
        """Execute actions ordered by priority."""
        sorted_actions = sorted(self._actions, key=lambda a: -a.priority)
        results = []
        for action in sorted_actions:
            result = await self._run_action(action)
            results.append(result)
        return results

    async def _execute_fail_fast(self) -> list[BatchResult]:
        """Stop on first failure."""
        results = []
        for action in self._actions:
            result = await self._run_action(action)
            results.append(result)
            if not result.success:
                break
        return results

    async def _execute_all_or_nothing(self) -> list[BatchResult]:
        """Execute all or none based on all succeeding."""
        results = []
        for action in self._actions:
            result = await self._run_action(action)
            results.append(result)
            if not result.success:
                return results
        return results

    async def _run_action(self, action: BatchAction) -> BatchResult:
        """Run a single action with timeout."""
        import time
        start = time.monotonic()
        try:
            result = action.func(*action.args, **action.kwargs)
            if asyncio.iscoroutine(result):
                result = await asyncio.wait_for(
                    result,
                    timeout=action.timeout_ms / 1000.0
                )
            duration = (time.monotonic() - start) * 1000
            return BatchResult(
                action_id=action.id,
                success=True,
                result=result,
                duration_ms=duration,
            )
        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            return BatchResult(
                action_id=action.id,
                success=False,
                error=str(e),
                duration_ms=duration,
            )

    def clear(self) -> None:
        """Clear all pending actions."""
        self._actions.clear()
