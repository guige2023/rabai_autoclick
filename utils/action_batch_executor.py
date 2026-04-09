"""
Action batch executor for grouped automation operations.

Handles execution of multiple UI actions as an atomic batch
with rollback support on failure.

Author: AutoClick Team
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class BatchStrategy(Enum):
    """Execution strategy for batch operations."""

    SEQUENTIAL = auto()
    PARALLEL = auto()
    PRIORITY_ORDERED = auto()
    DEPENDENCY_ORDERED = auto()


@dataclass
class ActionResult:
    """Result of a single action execution."""

    action_id: str
    success: bool
    data: Any = None
    error: str | None = None
    duration_ms: float = 0.0


@dataclass
class BatchAction:
    """
    Represents a single action in a batch.

    Attributes:
        action_id: Unique identifier for this action
        execute: Callable to execute this action
        rollback: Optional rollback callable on failure
        priority: Execution priority (lower = earlier)
        dependencies: List of action_ids that must complete first
        metadata: Additional context data
    """

    action_id: str
    execute: Callable[[], Any]
    rollback: Callable[[], None] | None = None
    priority: int = 0
    dependencies: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class BatchExecutor:
    """
    Executes multiple actions as a coordinated batch.

    Supports rollback on failure and various execution strategies.

    Example:
        executor = BatchExecutor(strategy=BatchStrategy.SEQUENTIAL)
        executor.add(ActionBatch(
            action_id="click",
            execute=lambda: click_button(),
            rollback=lambda: unclick_button(),
        ))
        result = executor.execute()
    """

    def __init__(
        self,
        strategy: BatchStrategy = BatchStrategy.SEQUENTIAL,
        stop_on_failure: bool = True,
    ) -> None:
        """
        Initialize batch executor.

        Args:
            strategy: How to order/parallelize execution
            stop_on_failure: Halt batch on first failure
        """
        self._actions: list[BatchAction] = []
        self._strategy = strategy
        self._stop_on_failure = stop_on_failure
        self._results: list[ActionResult] = []
        self._completed: set[str] = set()

    def add(self, action: BatchAction) -> "BatchExecutor":
        """Add an action to the batch (fluent interface)."""
        self._actions.append(action)
        return self

    def execute(self) -> BatchResult:
        """
        Execute all actions in the batch.

        Returns:
            BatchResult with success status and all action results
        """
        self._results.clear()
        self._completed.clear()

        ordered = self._order_actions()

        for action in ordered:
            result = self._execute_action(action)
            self._results.append(result)
            self._completed.add(action.action_id)

            if not result.success and self._stop_on_failure:
                self._rollback()
                return BatchResult(
                    success=False,
                    results=self._results,
                    rolled_back=True,
                )

        return BatchResult(
            success=True,
            results=self._results,
            rolled_back=False,
        )

    def _order_actions(self) -> list[BatchAction]:
        """Order actions based on strategy."""
        if self._strategy == BatchStrategy.PRIORITY_ORDERED:
            return sorted(self._actions, key=lambda a: a.priority)
        elif self._strategy == BatchStrategy.DEPENDENCY_ORDERED:
            return self._resolve_dependencies()
        return self._actions.copy()

    def _resolve_dependencies(self) -> list[BatchAction]:
        """Resolve dependencies and return ordered list."""
        ordered: list[BatchAction] = []
        remaining = {a.action_id: a for a in self._actions}

        while remaining:
            for action_id, action in list(remaining.items()):
                deps_satisfied = all(
                    dep in self._completed for dep in action.dependencies
                )
                if deps_satisfied:
                    ordered.append(action)
                    del remaining[action_id]

            if remaining and not any(
                all(dep in self._completed for dep in a.dependencies)
                for a in remaining.values()
            ):
                remaining_list = list(remaining.values())
                ordered.extend(remaining_list)
                break

        return ordered

    def _execute_action(self, action: BatchAction) -> ActionResult:
        """Execute a single action with timing."""
        import time

        start = time.time()
        try:
            data = action.execute()
            return ActionResult(
                action_id=action.action_id,
                success=True,
                data=data,
                duration_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ActionResult(
                action_id=action.action_id,
                success=False,
                error=str(e),
                duration_ms=(time.time() - start) * 1000,
            )

    def _rollback(self) -> None:
        """Execute rollback for completed actions in reverse order."""
        for action in reversed(self._results):
            if not action.success:
                break
            batch_action = next(
                (a for a in self._actions if a.action_id == action.action_id),
                None,
            )
            if batch_action and batch_action.rollback:
                try:
                    batch_action.rollback()
                except Exception:
                    pass


@dataclass
class BatchResult:
    """Result of a batch execution."""

    success: bool
    results: list[ActionResult]
    rolled_back: bool

    @property
    def failed_actions(self) -> list[ActionResult]:
        """Get all failed action results."""
        return [r for r in self.results if not r.success]

    @property
    def total_duration_ms(self) -> float:
        """Sum of all action durations."""
        return sum(r.duration_ms for r in self.results)


def execute_batch(
    actions: list[BatchAction],
    strategy: BatchStrategy = BatchStrategy.SEQUENTIAL,
    stop_on_failure: bool = True,
) -> BatchResult:
    """
    Convenience function to execute a batch of actions.

    Args:
        actions: List of actions to execute
        strategy: Execution strategy
        stop_on_failure: Stop on first failure

    Returns:
        BatchResult with overall outcome
    """
    executor = BatchExecutor(strategy=strategy, stop_on_failure=stop_on_failure)
    for action in actions:
        executor.add(action)
    return executor.execute()
