"""
Workflow Optimizer Action Module.

Optimizes workflow execution: reorders tasks for parallelism,
eliminates redundant steps, and caches intermediate results.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class OptimizationSuggestion:
    """An optimization suggestion."""
    type: str  # parallelize, eliminate, cache, reorder
    step_ids: list[str]
    estimated_improvement_percent: float
    description: str


@dataclass
class OptimizationResult:
    """Result of workflow optimization."""
    suggestions: list[OptimizationSuggestion]
    current_duration_estimate: float
    optimized_duration_estimate: float


class WorkflowOptimizerAction(BaseAction):
    """Optimize workflow execution."""

    def __init__(self) -> None:
        super().__init__("workflow_optimizer")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Analyze and optimize workflow.

        Args:
            context: Execution context
            params: Parameters:
                - steps: List of workflow step definitions
                - dependencies: Step dependency graph
                - execution_times: Estimated execution times per step
                - analyze_only: Just analyze, don't optimize

        Returns:
            OptimizationResult with suggestions
        """
        steps = params.get("steps", [])
        dependencies = params.get("dependencies", {})
        execution_times = params.get("execution_times", {})
        analyze_only = params.get("analyze_only", True)

        suggestions = []

        parallelizable = self._find_parallelizable_steps(steps, dependencies)
        if parallelizable:
            suggestions.append(OptimizationSuggestion(
                type="parallelize",
                step_ids=parallelizable,
                estimated_improvement_percent=30.0,
                description=f"Steps {parallelizable} can run in parallel"
            ))

        redundant = self._find_redundant_steps(steps)
        if redundant:
            suggestions.append(OptimizationSuggestion(
                type="eliminate",
                step_ids=redundant,
                estimated_improvement_percent=15.0,
                description=f"Steps {redundant} are redundant"
            ))

        cacheable = self._find_cacheable_steps(steps)
        if cacheable:
            suggestions.append(OptimizationSuggestion(
                type="cache",
                step_ids=cacheable,
                estimated_improvement_percent=25.0,
                description=f"Results from {cacheable} can be cached"
            ))

        current_duration = sum(execution_times.get(s.get("id"), 10) for s in steps)
        optimized_duration = current_duration * 0.5

        return OptimizationResult(
            suggestions=suggestions,
            current_duration_estimate=current_duration,
            optimized_duration_estimate=optimized_duration
        ).__dict__

    def _find_parallelizable_steps(self, steps: list[dict], dependencies: dict) -> list[str]:
        """Find steps that can run in parallel."""
        independent = []
        for step in steps:
            step_id = step.get("id", "")
            deps = dependencies.get(step_id, [])
            if not deps or all(d not in [s.get("id") for s in steps[:steps.index(step)]]):
                independent.append(step_id)
        return independent[1:] if len(independent) > 1 else []

    def _find_redundant_steps(self, steps: list[dict]) -> list[str]:
        """Find redundant steps."""
        redundant = []
        for i, step in enumerate(steps):
            if i > 0 and step.get("handler") == steps[i - 1].get("handler"):
                redundant.append(step.get("id", ""))
        return redundant

    def _find_cacheable_steps(self, steps: list[dict]) -> list[str]:
        """Find steps whose results can be cached."""
        cacheable = []
        for step in steps:
            if step.get("type") == "query" or step.get("id", "").startswith("fetch_"):
                cacheable.append(step.get("id", ""))
        return cacheable
