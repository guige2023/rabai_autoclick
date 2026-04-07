"""
Template Method Pattern Implementation

Defines the skeleton of an algorithm in a method, deferring some steps
to subclasses without changing the algorithm's structure.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class AbstractTemplateMethod(ABC):
    """
    Abstract template method class.
    Defines the algorithm skeleton and calls hook methods.
    """

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """
        Template method that defines the algorithm skeleton.
        """
        self._before_steps()
        result = self._execute_steps(*args, **kwargs)
        self._after_steps()
        return result

    def _before_steps(self) -> None:
        """Hook method called before steps."""
        pass

    def _after_steps(self) -> None:
        """Hook method called after steps."""
        pass

    @abstractmethod
    def _execute_steps(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the algorithm steps. Must be implemented by subclasses."""
        pass

    def get_step_names(self) -> list[str]:
        """Return names of steps in execution order."""
        return ["before_steps", "execute_steps", "after_steps"]


class DataProcessingTemplate(AbstractTemplateMethod):
    """
    Template for a data processing workflow.
    """

    def __init__(self):
        self._steps_executed: list[str] = []
        self._metrics: dict[str, Any] = {
            "total_time_ms": 0,
            "steps": {},
        }

    def _execute_steps(self, data: list[Any], config: dict[str, Any] | None = None) -> list[Any]:
        """Execute the data processing pipeline."""
        config = config or {}

        # Step 1: Validate
        validated = self._validate(data)
        self._record_step("validate", validated is not None)

        # Step 2: Transform
        transformed = self._transform(validated, config)
        self._record_step("transform", transformed is not None)

        # Step 3: Filter
        filtered = self._filter(transformed, config)
        self._record_step("filter", filtered is not None)

        # Step 4: Aggregate
        result = self._aggregate(filtered, config)
        self._record_step("aggregate", result is not None)

        return result

    @abstractmethod
    def _validate(self, data: list[Any]) -> list[Any] | None:
        """Validate input data."""
        pass

    @abstractmethod
    def _transform(self, data: list[Any], config: dict[str, Any]) -> list[Any]:
        """Transform the data."""
        pass

    @abstractmethod
    def _filter(self, data: list[Any], config: dict[str, Any]) -> list[Any]:
        """Filter the data."""
        pass

    @abstractmethod
    def _aggregate(self, data: list[Any], config: dict[str, Any]) -> list[Any]:
        """Aggregate the data."""
        pass

    def _record_step(self, name: str, success: bool) -> None:
        """Record step execution."""
        self._steps_executed.append(name)
        self._metrics["steps"][name] = {
            "success": success,
            "timestamp": time.time(),
        }

    @property
    def steps_executed(self) -> list[str]:
        """Get names of executed steps."""
        return list(self._steps_executed)

    @property
    def metrics(self) -> dict[str, Any]:
        """Get execution metrics."""
        return copy.copy(self._metrics)


class HookTemplateMethod(ABC):
    """
    Template method class with explicit hooks.
    """

    def process(self, data: Any) -> Any:
        """Main template method with hooks."""
        data = self.pre_process(data)

        if not self.should_continue(data):
            return self.get_default_result()

        data = self.pre_transform(data)
        data = self.transform(data)
        data = self.post_transform(data)

        result = self.post_process(data)
        return result

    def pre_process(self, data: Any) -> Any:
        """Hook: called before main processing."""
        return data

    def post_process(self, data: Any) -> Any:
        """Hook: called after main processing."""
        return data

    def should_continue(self, data: Any) -> bool:
        """Hook: determines if processing should continue."""
        return True

    def get_default_result(self) -> Any:
        """Hook: returns default result if processing stops."""
        return None

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Main transformation logic."""
        pass

    def pre_transform(self, data: Any) -> Any:
        """Hook: called before transform."""
        return data

    def post_transform(self, data: Any) -> Any:
        """Hook: called after transform."""
        return data


class TemplateMethodBuilder(Generic[T]):
    """
    Builder for creating template method classes.
    """

    def __init__(self):
        self._name: str = "Template"
        self._steps: list[Callable] = []
        self._hooks: dict[str, Callable] = {}

    def with_name(self, name: str) -> TemplateMethodBuilder[T]:
        """Set the template name."""
        self._name = name
        return self

    def add_step(self, step: Callable[[Any], Any]) -> TemplateMethodBuilder[T]:
        """Add a step to the template."""
        self._steps.append(step)
        return self

    def with_hook(self, name: str, hook: Callable[[Any], Any]) -> TemplateMethodBuilder[T]:
        """Add a hook method."""
        self._hooks[name] = hook
        return self

    def build(self) -> type[T]:
        """Build and return a template method class."""
        name = self._name
        steps = list(self._steps)
        hooks = dict(self._hooks)

        class BuiltTemplateMethod(HookTemplateMethod):
            pass

        template = BuiltTemplateMethod
        template.__name__ = name

        return template  # type: ignore


@dataclass
class StepMetrics:
    """Metrics for a single step."""
    name: str
    duration_ms: float
    success: bool
    timestamp: float


class MeasuredTemplateMethod(AbstractTemplateMethod):
    """
    Template method with step timing and metrics.
    """

    def __init__(self):
        self._step_metrics: list[StepMetrics] = []
        self._total_time_ms: float = 0.0

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute with metrics collection."""
        step_start = time.time()
        self._before_steps()
        step_elapsed = (time.time() - step_start) * 1000

        self._step_metrics.append(StepMetrics(
            name="before_steps",
            duration_ms=step_elapsed,
            success=True,
            timestamp=time.time(),
        ))

        start = time.time()
        result = self._execute_steps(*args, **kwargs)
        self._total_time_ms += (time.time() - start) * 1000

        step_start = time.time()
        self._after_steps()
        step_elapsed = (time.time() - step_start) * 1000

        self._step_metrics.append(StepMetrics(
            name="after_steps",
            duration_ms=step_elapsed,
            success=True,
            timestamp=time.time(),
        ))

        return result

    @property
    def total_time_ms(self) -> float:
        """Get total execution time."""
        return self._total_time_ms

    @property
    def step_metrics(self) -> list[StepMetrics]:
        """Get per-step metrics."""
        return list(self._step_metrics)

    def get_step_duration(self, step_name: str) -> float | None:
        """Get duration for a specific step."""
        for metric in self._step_metrics:
            if metric.name == step_name:
                return metric.duration_ms
        return None


def create_template_method(
    name: str,
    steps: list[Callable[[Any], Any]],
    pre_hook: Callable[[Any], Any] | None = None,
    post_hook: Callable[[Any], Any] | None = None,
) -> Callable[[Any], Any]:
    """
    Create a simple template method function.

    Args:
        name: Template name.
        steps: List of step functions to execute in order.
        pre_hook: Optional pre-processing hook.
        post_hook: Optional post-processing hook.

    Returns:
        A template method function.
    """
    def template_method(data: Any) -> Any:
        result = data

        if pre_hook:
            result = pre_hook(result)

        for step in steps:
            result = step(result)

        if post_hook:
            result = post_hook(result)

        return result

    template_method.__name__ = name
    return template_method
