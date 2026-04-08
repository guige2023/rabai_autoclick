"""Workflow utilities for RabAI AutoClick.

Provides:
- Workflow step execution
- Step dependency resolution
- Workflow state management
"""

from __future__ import annotations

import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)


class WorkflowStep:
    """A step in a workflow."""

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        dependencies: Optional[List[str]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.name = name
        self.func = func
        self.dependencies = dependencies or []
        self.timeout = timeout


class Workflow:
    """A workflow consisting of ordered steps."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._steps: Dict[str, WorkflowStep] = {}
        self._results: Dict[str, Any] = {}
        self._errors: Dict[str, Exception] = {}

    def add_step(
        self,
        name: str,
        func: Callable[..., Any],
        dependencies: Optional[List[str]] = None,
        timeout: Optional[float] = None,
    ) -> "Workflow":
        """Add a step to the workflow.

        Returns:
            Self for chaining.
        """
        self._steps[name] = WorkflowStep(name, func, dependencies, timeout)
        return self

    def execute(self) -> Dict[str, Any]:
        """Execute all steps in dependency order.

        Returns:
            Dict of step names to results.
        """
        self._results = {}
        self._errors = {}
        executed: Set[str] = set()
        order = self._topological_sort()

        for step_name in order:
            step = self._steps[step_name]
            deps_met = all(d in executed for d in step.dependencies)
            if not deps_met:
                self._errors[step_name] = RuntimeError(
                    f"Dependencies not met for {step_name}"
                )
                continue

            try:
                dep_results = {d: self._results[d] for d in step.dependencies}
                result = step.func(**dep_results)
                self._results[step_name] = result
                executed.add(step_name)
            except Exception as e:
                self._errors[step_name] = e

        return self._results

    def _topological_sort(self) -> List[str]:
        """Sort steps by dependencies."""
        visited: Set[str] = set()
        order: List[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            step = self._steps.get(name)
            if step:
                for dep in step.dependencies:
                    visit(dep)
            order.append(name)

        for name in self._steps:
            visit(name)
        return order

    @property
    def results(self) -> Dict[str, Any]:
        """Get step results."""
        return self._results

    @property
    def errors(self) -> Dict[str, Exception]:
        """Get step errors."""
        return self._errors


__all__ = [
    "WorkflowStep",
    "Workflow",
]
