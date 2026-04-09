"""Automation Sequence Action Module.

Executes automation actions in a defined sequence with dependencies,
parallel sections, and synchronization barriers.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


@dataclass
class SequenceStep:
    step_id: str
    action_type: str
    params: Dict[str, Any]
    depends_on: Set[str] = field(default_factory=set)
    timeout_ms: int = 30000
    retry_count: int = 0
    max_retries: int = 3
    parallel_group: Optional[str] = None
    enabled: bool = True


@dataclass
class SequenceResult:
    step_id: str
    success: bool
    duration_ms: float
    output: Any = None
    error: Optional[str] = None
    attempt: int = 0


@dataclass
class SequenceConfig:
    stop_on_error: bool = True
    parallel_groups: bool = True
    max_parallel: int = 5
    shared_context: bool = True


class AutomationSequenceAction:
    """Executes automation steps in dependency-ordered sequence."""

    def __init__(self, config: Optional[SequenceConfig] = None) -> None:
        self._config = config or SequenceConfig()
        self._steps: Dict[str, SequenceStep] = {}
        self._results: Dict[str, SequenceResult] = {}
        self._context: Dict[str, Any] = {}

    def add_step(
        self,
        step_id: str,
        action_type: str,
        params: Dict[str, Any],
        depends_on: Optional[List[str]] = None,
        timeout_ms: int = 30000,
        max_retries: int = 3,
        parallel_group: Optional[str] = None,
        enabled: bool = True,
    ) -> None:
        self._steps[step_id] = SequenceStep(
            step_id=step_id,
            action_type=action_type,
            params=params,
            depends_on=set(depends_on or []),
            timeout_ms=timeout_ms,
            max_retries=max_retries,
            parallel_group=parallel_group,
            enabled=enabled,
        )

    def remove_step(self, step_id: str) -> bool:
        return self._steps.pop(step_id, None) is not None

    def execute(
        self,
        executor: Callable[[str, Dict[str, Any]], Any],
    ) -> tuple[bool, Dict[str, SequenceResult]]:
        self._results.clear()
        if self._config.shared_context:
            self._context.clear()
        sorted_order = self._topological_sort()
        if sorted_order is None:
            return False, {}
        for step_id in sorted_order:
            step = self._steps[step_id]
            if not step.enabled:
                continue
            if not self._dependencies_met(step):
                logger.error(f"Dependencies not met for {step_id}")
                return False, self._results
            success, result = self._execute_step(step, executor)
            self._results[step_id] = result
            if self._config.shared_context and result.output is not None:
                self._context[step_id] = result.output
            if not success and self._config.stop_on_error:
                return False, self._results
        return all(r.success for r in self._results.values()), self._results

    def _execute_step(
        self,
        step: SequenceStep,
        executor: Callable,
    ) -> tuple[bool, SequenceResult]:
        start = time.time()
        for attempt in range(step.max_retries + 1):
            try:
                output = executor(step.action_type, step.params)
                return True, SequenceResult(
                    step_id=step.step_id,
                    success=True,
                    duration_ms=(time.time() - start) * 1000,
                    output=output,
                    attempt=attempt,
                )
            except Exception as e:
                if attempt < step.max_retries:
                    continue
                return False, SequenceResult(
                    step_id=step.step_id,
                    success=False,
                    duration_ms=(time.time() - start) * 1000,
                    error=str(e),
                    attempt=attempt,
                )
        return False, SequenceResult(
            step_id=step.step_id,
            success=False,
            duration_ms=(time.time() - start) * 1000,
            error="Max retries exceeded",
        )

    def _dependencies_met(self, step: SequenceStep) -> bool:
        for dep_id in step.depends_on:
            if dep_id not in self._results or not self._results[dep_id].success:
                return False
        return True

    def _topological_sort(self) -> Optional[List[str]]:
        in_degree: Dict[str, int] = {sid: 0 for sid in self._steps}
        adj_list: Dict[str, List[str]] = defaultdict(list)
        for step_id, step in self._steps.items():
            for dep in step.depends_on:
                if dep not in self._steps:
                    logger.error(f"Unknown dependency {dep} for {step_id}")
                    return None
                adj_list[dep].append(step_id)
                in_degree[step_id] += 1
        queue = deque([sid for sid, deg in in_degree.items() if deg == 0])
        result = []
        while queue:
            current = queue.popleft()
            result.append(current)
            for neighbor in adj_list[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        if len(result) != len(self._steps):
            logger.error("Circular dependency detected")
            return None
        return result

    def get_context(self) -> Dict[str, Any]:
        return dict(self._context)

    def get_step_order(self) -> List[str]:
        return self._topological_sort() or []

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._results)
        successful = sum(1 for r in self._results.values() if r.success)
        return {
            "total_steps": len(self._steps),
            "completed": total,
            "successful": successful,
            "failed": total - successful,
            "success_rate": successful / total if total > 0 else 0,
        }
