"""
Sequencer Action Module.

Provides sequencing and ordering capabilities for
coordinating multi-step operations.
"""

import time
import asyncio
import threading
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque


class SequenceState(Enum):
    """Sequence execution states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class SequenceStep:
    """A single step in a sequence."""
    name: str
    func: Callable
    dependencies: Set[str] = field(default_factory=set)
    timeout: Optional[float] = None
    retry_count: int = 0
    required: bool = True


@dataclass
class SequenceResult:
    """Result of sequence execution."""
    step_name: str
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0


class DependencyGraph:
    """Manages step dependencies and execution order."""

    def __init__(self):
        self._nodes: Dict[str, SequenceStep] = {}
        self._edges: Dict[str, Set[str]] = defaultdict(set)
        self._lock = threading.RLock()

    def add_node(self, step: SequenceStep) -> None:
        """Add a step to the graph."""
        with self._lock:
            self._nodes[step.name] = step
            self._edges[step.name] = step.dependencies.copy()

    def get_execution_order(self) -> List[str]:
        """Get topologically sorted execution order."""
        in_degree = {node: 0 for node in self._nodes}
        for node in self._nodes:
            for dep in self._edges[node]:
                in_degree[node] += 1

        queue = deque([n for n, d in in_degree.items() if d == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            for neighbor in self._nodes:
                if node in self._edges[neighbor]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        if len(result) != len(self._nodes):
            raise ValueError("Circular dependency detected")

        return result

    def get_dependencies(self, step_name: str) -> Set[str]:
        """Get all dependencies for a step."""
        return self._edges.get(step_name, set())


class SequencerAction:
    """
    Action that sequences and executes steps in order.

    Example:
        sequencer = SequencerAction("workflow")
        sequencer.add_step("step1", lambda: task1())
        sequencer.add_step("step2", lambda: task2(), dependencies={"step1"})
        sequencer.add_step("step3", lambda: task3(), dependencies={"step1"})
        results = sequencer.execute()
    """

    def __init__(self, name: str):
        self.name = name
        self._graph = DependencyGraph()
        self._steps: Dict[str, SequenceStep] = {}
        self._lock = threading.RLock()
        self._state = SequenceState.PENDING
        self._results: Dict[str, SequenceResult] = {}

    def add_step(
        self,
        name: str,
        func: Callable,
        dependencies: Optional[Set[str]] = None,
        timeout: Optional[float] = None,
        retry_count: int = 0,
        required: bool = True,
    ) -> "SequencerAction":
        """Add a step to the sequence."""
        step = SequenceStep(
            name=name,
            func=func,
            dependencies=dependencies or set(),
            timeout=timeout,
            retry_count=retry_count,
            required=required,
        )

        with self._lock:
            self._steps[name] = step
            self._graph.add_node(step)

        return self

    def remove_step(self, name: str) -> bool:
        """Remove a step from the sequence."""
        with self._lock:
            if name in self._steps:
                del self._steps[name]
                return True
            return False

    def get_step(self, name: str) -> Optional[SequenceStep]:
        """Get a step by name."""
        with self._lock:
            return self._steps.get(name)

    def _execute_step(self, step: SequenceStep) -> SequenceResult:
        """Execute a single step."""
        start_time = time.time()
        result = SequenceResult(step_name=step.name, success=False)

        for attempt in range(step.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(step.func):
                    func_result = asyncio.run(step.func())
                else:
                    func_result = step.func()

                result.result = func_result
                result.success = True
                break

            except Exception as e:
                result.error = e
                if attempt == step.retry_count:
                    result.success = False
                    if step.required:
                        raise
                else:
                    time.sleep(0.1 * (attempt + 1))

        result.duration = time.time() - start_time
        return result

    def execute(self) -> Dict[str, SequenceResult]:
        """Execute all steps in sequence order."""
        with self._lock:
            self._state = SequenceState.RUNNING
            self._results.clear()

        try:
            execution_order = self._graph.get_execution_order()

            for step_name in execution_order:
                step = self._steps[step_name]

                deps = self._graph.get_dependencies(step_name)
                for dep in deps:
                    if dep in self._results and not self._results[dep].success:
                        if step.required:
                            raise RuntimeError(
                                f"Required dependency {dep} failed for {step_name}"
                            )

                step_result = self._execute_step(step)
                self._results[step_name] = step_result

                if not step_result.success and step.required:
                    self._state = SequenceState.FAILED
                    return self._results

            self._state = SequenceState.COMPLETED

        except Exception as e:
            self._state = SequenceState.FAILED
            raise

        return self._results

    async def execute_async(self) -> Dict[str, SequenceResult]:
        """Execute all steps asynchronously."""
        with self._lock:
            self._state = SequenceState.RUNNING
            self._results.clear()

        try:
            execution_order = self._graph.get_execution_order()

            for step_name in execution_order:
                step = self._steps[step_name]

                deps = self._graph.get_dependencies(step_name)
                for dep in deps:
                    if dep in self._results and not self._results[dep].success:
                        if step.required:
                            raise RuntimeError(
                                f"Required dependency {dep} failed for {step_name}"
                            )

                if asyncio.iscoroutinefunction(step.func):
                    step_result = await self._execute_step_async(step)
                else:
                    step_result = self._execute_step(step)

                self._results[step_name] = step_result

                if not step_result.success and step.required:
                    self._state = SequenceState.FAILED
                    return self._results

            self._state = SequenceState.COMPLETED

        except Exception as e:
            self._state = SequenceState.FAILED
            raise

        return self._results

    async def _execute_step_async(self, step: SequenceStep) -> SequenceResult:
        """Execute a single step asynchronously."""
        start_time = time.time()
        result = SequenceResult(step_name=step.name, success=False)

        for attempt in range(step.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(step.func):
                    func_result = await step.func()
                else:
                    func_result = step.func()

                result.result = func_result
                result.success = True
                break

            except Exception as e:
                result.error = e
                if attempt == step.retry_count:
                    result.success = False
                    if step.required:
                        raise
                else:
                    await asyncio.sleep(0.1 * (attempt + 1))

        result.duration = time.time() - start_time
        return result

    def get_result(self, step_name: str) -> Optional[SequenceResult]:
        """Get result for a specific step."""
        return self._results.get(step_name)

    def get_results(self) -> Dict[str, SequenceResult]:
        """Get all results."""
        return dict(self._results)

    @property
    def state(self) -> SequenceState:
        """Current sequence state."""
        return self._state

    def cancel(self) -> None:
        """Cancel the sequence execution."""
        self._state = SequenceState.CANCELLED

    def reset(self) -> None:
        """Reset the sequencer."""
        with self._lock:
            self._state = SequenceState.PENDING
            self._results.clear()

    def validate(self) -> List[str]:
        """Validate the sequence and return any errors."""
        errors = []
        try:
            self._graph.get_execution_order()
        except ValueError as e:
            errors.append(str(e))

        for name, step in self._steps.items():
            for dep in step.dependencies:
                if dep not in self._steps:
                    errors.append(f"Step {name} depends on unknown step {dep}")

        return errors
