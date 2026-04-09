"""
Operator Action Module.

Provides operator pattern implementation for action chaining
and composition.
"""

import time
import asyncio
import threading
from typing import Any, Callable, Optional, List, Dict
from dataclasses import dataclass, field
from enum import Enum
from functools import partial


class OperatorType(Enum):
    """Types of operators."""
    MAP = "map"
    FLAT_MAP = "flat_map"
    FILTER = "filter"
    REDUCE = "reduce"
    PEEK = "peek"
    TAKE = "take"
    SKIP = "skip"
    DISTINCT = "distinct"
    SORT = "sort"
    GROUP_BY = "group_by"


@dataclass
class Operator:
    """Represents an operator in a pipeline."""
    name: str
    func: Callable
    operator_type: OperatorType
    parallel: bool = False


class Pipeline:
    """Pipeline for chaining operators."""

    def __init__(self, name: str = "pipeline"):
        self.name = name
        self._operators: List[Operator] = []
        self._lock = threading.RLock()

    def add(self, operator: Operator) -> "Pipeline":
        """Add an operator to the pipeline."""
        with self._lock:
            self._operators.append(operator)
        return self

    def map(self, func: Callable, parallel: bool = False) -> "Pipeline":
        """Add a map operator."""
        return self.add(Operator("map", func, OperatorType.MAP, parallel))

    def flat_map(self, func: Callable) -> "Pipeline":
        """Add a flat_map operator."""
        return self.add(Operator("flat_map", func, OperatorType.FLAT_MAP, False))

    def filter(self, func: Callable) -> "Pipeline":
        """Add a filter operator."""
        return self.add(Operator("filter", func, OperatorType.FILTER, False))

    def peek(self, func: Callable) -> "Pipeline":
        """Add a peek operator."""
        return self.add(Operator("peek", func, OperatorType.PEEK, False))

    def take(self, count: int) -> "Pipeline":
        """Add a take operator."""
        return self.add(Operator("take", partial(lambda n, x: x[:n], count), OperatorType.TAKE, False))

    def skip(self, count: int) -> "Pipeline":
        """Add a skip operator."""
        return self.add(Operator("skip", partial(lambda n, x: x[n:], count), OperatorType.SKIP, False))

    def distinct(self) -> "Pipeline":
        """Add a distinct operator."""
        return self.add(Operator("distinct", lambda x: list(set(x)), OperatorType.DISTINCT, False))

    def sort(self, key: Optional[Callable] = None, reverse: bool = False) -> "Pipeline":
        """Add a sort operator."""
        func = partial(lambda k, r, x: sorted(x, key=k, reverse=r), key, reverse)
        return self.add(Operator("sort", func, OperatorType.SORT, False))

    def _execute_operator(self, operator: Operator, data: Any) -> Any:
        """Execute a single operator."""
        if operator.operator_type == OperatorType.MAP:
            if isinstance(data, list):
                return [operator.func(item) for item in data]
            return operator.func(data)

        elif operator.operator_type == OperatorType.FLAT_MAP:
            if isinstance(data, list):
                result = []
                for item in data:
                    mapped = operator.func(item)
                    if isinstance(mapped, list):
                        result.extend(mapped)
                    else:
                        result.append(mapped)
                return result
            return operator.func(data)

        elif operator.operator_type == OperatorType.FILTER:
            if isinstance(data, list):
                return [item for item in data if operator.func(item)]
            return data if operator.func(data) else None

        elif operator.operator_type == OperatorType.PEEK:
            if isinstance(data, list):
                for item in data:
                    operator.func(item)
            else:
                operator.func(data)
            return data

        elif operator.operator_type == OperatorType.TAKE:
            if isinstance(data, list):
                return operator.func(data)
            return data

        elif operator.operator_type == OperatorType.SKIP:
            if isinstance(data, list):
                return operator.func(data)
            return data

        elif operator.operator_type == OperatorType.DISTINCT:
            if isinstance(data, list):
                seen = set()
                result = []
                for item in data:
                    key = str(item)
                    if key not in seen:
                        seen.add(key)
                        result.append(item)
                return result
            return data

        elif operator.operator_type == OperatorType.SORT:
            if isinstance(data, list):
                return operator.func(data)
            return data

        return data

    def execute(self, data: Any) -> Any:
        """Execute the pipeline on data."""
        result = data
        with self._lock:
            operators = list(self._operators)

        for operator in operators:
            result = self._execute_operator(operator, result)
            if result is None:
                break

        return result

    async def execute_async(self, data: Any) -> Any:
        """Execute the pipeline asynchronously."""
        result = data
        with self._lock:
            operators = list(self._operators)

        for operator in operators:
            if asyncio.iscoroutinefunction(operator.func):
                if isinstance(result, list):
                    result = [await operator.func(item) for item in result]
                else:
                    result = await operator.func(result)
            else:
                result = self._execute_operator(operator, result)

            if result is None:
                break

        return result


class OperatorAction:
    """
    Action that provides operator pattern functionality.

    Example:
        action = OperatorAction("data_processor")
        result = action.pipe(
            [1, 2, 3, 4, 5],
            action.map(lambda x: x * 2),
            action.filter(lambda x: x > 4),
            action.take(2),
        )
    """

    def __init__(self, name: str):
        self.name = name
        self._lock = threading.RLock()

    def create_pipeline(self, name: str = "") -> Pipeline:
        """Create a new pipeline."""
        return Pipeline(name or f"{self.name}_pipeline")

    def pipe(self, data: Any, *operators: Callable) -> Any:
        """Execute operators in sequence on data."""
        result = data
        for operator in operators:
            if asyncio.iscoroutinefunction(operator):
                result = asyncio.run(operator(result))
            elif callable(operator):
                result = operator(result)
        return result

    def map(self, func: Callable) -> Callable:
        """Create a map operator."""
        return lambda data: (
            [func(item) for item in data] if isinstance(data, list) else func(data)
        )

    def flat_map(self, func: Callable) -> Callable:
        """Create a flat_map operator."""
        def flat_map_impl(data):
            if isinstance(data, list):
                result = []
                for item in data:
                    mapped = func(item)
                    if isinstance(mapped, list):
                        result.extend(mapped)
                    else:
                        result.append(mapped)
                return result
            return func(data)
        return flat_map_impl

    def filter(self, func: Callable) -> Callable:
        """Create a filter operator."""
        def filter_impl(data):
            if isinstance(data, list):
                return [item for item in data if func(item)]
            return data if func(data) else None
        return filter_impl

    def peek(self, func: Callable) -> Callable:
        """Create a peek operator."""
        def peek_impl(data):
            if isinstance(data, list):
                for item in data:
                    func(item)
            else:
                func(data)
            return data
        return peek_impl

    def take(self, count: int) -> Callable:
        """Create a take operator."""
        return lambda data: data[:count] if isinstance(data, list) else data

    def skip(self, count: int) -> Callable:
        """Create a skip operator."""
        return lambda data: data[count:] if isinstance(data, list) else data

    def distinct(self) -> Callable:
        """Create a distinct operator."""
        def distinct_impl(data):
            if isinstance(data, list):
                seen = set()
                result = []
                for item in data:
                    key = str(item)
                    if key not in seen:
                        seen.add(key)
                        result.append(item)
                return result
            return data
        return distinct_impl

    def sort(self, key: Optional[Callable] = None, reverse: bool = False) -> Callable:
        """Create a sort operator."""
        return lambda data: sorted(data, key=key, reverse=reverse) if isinstance(data, list) else data

    def group_by(self, key: Callable) -> Callable:
        """Create a group_by operator."""
        def group_by_impl(data):
            if isinstance(data, list):
                groups: Dict[Any, List] = {}
                for item in data:
                    group_key = key(item)
                    if group_key not in groups:
                        groups[group_key] = []
                    groups[group_key].append(item)
                return groups
            return data
        return group_by_impl

    def reduce(self, func: Callable, initial: Any = None) -> Callable:
        """Create a reduce operator."""
        def reduce_impl(data):
            if isinstance(data, list):
                if initial is not None:
                    return functools.reduce(func, data, initial)
                return functools.reduce(func, data)
            return data
        return reduce_impl
