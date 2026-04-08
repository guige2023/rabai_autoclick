"""Automation Builder Action.

Provides a fluent API for building multi-step automation workflows.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class StepType(Enum):
    ACTION = "action"
    CONDITION = "condition"
    LOOP = "loop"
    PARALLEL = "parallel"
    TRANSFORM = "transform"


@dataclass
class AutomationStep:
    name: str
    step_type: StepType
    fn: Optional[Callable] = None
    condition: Optional[Callable[[], bool]] = None
    max_iterations: int = 100
    retry_count: int = 0
    timeout_sec: float = 60.0


@dataclass
class AutomationWorkflow:
    name: str
    steps: List[AutomationStep] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


class AutomationBuilderAction:
    """Fluent builder for automation workflows."""

    def __init__(self, name: str) -> None:
        self._workflow = AutomationWorkflow(name=name)

    def action(
        self,
        name: str,
        fn: Callable,
        retry: int = 0,
        timeout: float = 60.0,
    ) -> "AutomationBuilderAction":
        step = AutomationStep(
            name=name,
            step_type=StepType.ACTION,
            fn=fn,
            retry_count=retry,
            timeout_sec=timeout,
        )
        self._workflow.steps.append(step)
        return self

    def condition(
        self,
        name: str,
        condition_fn: Callable[[], bool],
    ) -> "AutomationBuilderAction":
        step = AutomationStep(
            name=name,
            step_type=StepType.CONDITION,
            condition=condition_fn,
        )
        self._workflow.steps.append(step)
        return self

    def loop(
        self,
        name: str,
        max_iterations: int = 100,
        condition_fn: Optional[Callable[[], bool]] = None,
    ) -> "AutomationBuilderAction":
        step = AutomationStep(
            name=name,
            step_type=StepType.LOOP,
            max_iterations=max_iterations,
            condition=condition_fn,
        )
        self._workflow.steps.append(step)
        return self

    def set_variable(self, key: str, value: Any) -> "AutomationBuilderAction":
        self._workflow.variables[key] = value
        return self

    def tag(self, tag: str) -> "AutomationBuilderAction":
        self._workflow.tags.append(tag)
        return self

    def build(self) -> AutomationWorkflow:
        return self._workflow

    def run(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        ctx = dict(self._workflow.variables)
        if context:
            ctx.update(context)
        results = []
        for step in self._workflow.steps:
            if step.step_type == StepType.ACTION and step.fn:
                try:
                    result = step.fn(ctx)
                    results.append({"step": step.name, "status": "ok", "result": result})
                except Exception as e:
                    results.append({"step": step.name, "status": "error", "error": str(e)})
        return {"workflow": self._workflow.name, "results": results}
