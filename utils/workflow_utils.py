"""Workflow utilities: DAG-based workflows, step dependencies, and execution tracking."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "WorkflowStep",
    "Workflow",
    "WorkflowExecutor",
]


@dataclass
class WorkflowStep:
    """A step in a workflow."""

    name: str
    fn: Callable[[], Any]
    depends_on: list[str] = field(default_factory=list)
    timeout_seconds: float = 300.0
    retryable: bool = False

    def __post_init__(self) -> None:
        self.result: Any = None
        self.error: Exception | None = None
        self.completed: bool = False


class Workflow:
    """Directed Acyclic Graph (DAG) workflow."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._steps: dict[str, WorkflowStep] = {}
        self._graph: dict[str, set[str]] = defaultdict(set)

    def add_step(self, step: WorkflowStep) -> "Workflow":
        self._steps[step.name] = step
        for dep in step.depends_on:
            self._graph[dep].add(step.name)
        return self

    def get_execution_order(self) -> list[str]:
        """Return steps in topological order (dependencies first)."""
        in_degree: dict[str, int] = defaultdict(int)
        for step in self._steps:
            in_degree[step] = 0
        for step_deps in self._graph.values():
            for dep in step_deps:
                in_degree[dep] += 1

        queue = [s for s, d in in_degree.items() if d == 0]
        order: list[str] = []

        while queue:
            node = queue.pop(0)
            order.append(node)
            for neighbor in self._graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order


class WorkflowExecutor:
    """Execute workflows with parallel step support."""

    def __init__(self, workflow: Workflow) -> None:
        self.workflow = workflow
        self._results: dict[str, Any] = {}
        self._errors: dict[str, Exception] = {}
        self._completed: set[str] = set()
        self._lock = threading.RLock()

    def run(self) -> bool:
        """Execute the workflow."""
        order = self.workflow.get_execution_order()

        for step_name in order:
            step = self.workflow._steps[step_name]

            deps_satisfied = all(d in self._completed for d in step.depends_on)
            if not deps_satisfied:
                continue

            try:
                result = step.fn()
                step.result = result
                self._results[step_name] = result
                step.completed = True
                with self._lock:
                    self._completed.add(step_name)
            except Exception as e:
                step.error = e
                self._errors[step_name] = e
                with self._lock:
                    self._completed.add(step_name)
                if not step.retryable:
                    continue

        return len(self._errors) == 0

    def get_result(self, step_name: str) -> Any | None:
        return self._results.get(step_name)

    def get_error(self, step_name: str) -> Exception | None:
        return self._errors.get(step_name)

    def is_complete(self) -> bool:
        return len(self._completed) == len(self.workflow._steps)
