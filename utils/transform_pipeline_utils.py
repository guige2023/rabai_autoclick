"""
Transform Pipeline Utilities

Provides utilities for building data transformation
pipelines in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class TransformPipeline(Generic[T]):
    """
    Builds a pipeline of transformations.
    
    Applies a series of transforms to data
    in sequence.
    """

    def __init__(self, initial: T) -> None:
        self._value = initial
        self._transforms: list[Callable[[Any], Any]] = []

    def pipe(self, func: Callable[[Any], R]) -> TransformPipeline:
        """Add a transformation to the pipeline."""
        self._transforms.append(func)
        return self

    def execute(self) -> T:
        """Execute all transformations."""
        result = self._value
        for transform in self._transforms:
            result = transform(result)
        return result

    def execute_yield(self) -> Any:
        """Yield each step of transformation."""
        result = self._value
        yield result
        for transform in self._transforms:
            result = transform(result)
            yield result
