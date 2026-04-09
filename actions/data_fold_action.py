"""
Data Fold Action Module.

Provides fold/reduce operations for data processing pipelines,
enabling aggregation of collections into single values.
"""

from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


class FoldDirection(Enum):
    """Direction for fold operation."""
    LEFT = auto()
    RIGHT = auto()


@dataclass
class FoldState(Generic[T, U]):
    """State maintained during a fold operation."""
    accumulator: U
    current_index: int = 0
    processed_count: int = 0
    errors: List[str] = []


@dataclass
class FoldResult(Generic[U]):
    """Result of a fold operation."""
    value: U
    iterations: int
    duration_ms: float
    errors: List[str]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "value": self.value,
            "iterations": self.iterations,
            "duration_ms": self.duration_ms,
            "error_count": len(self.errors),
            "metadata": self.metadata,
        }


class DataFoldAction(Generic[T, U]):
    """
    Provides fold/reduce operations for data processing.

    This action implements various fold algorithms for aggregating
    collections of data into single values, with support for error
    handling and progress tracking.

    Example:
        >>> fold = DataFoldAction()
        >>> result = fold.fold_left([1, 2, 3, 4], 0, lambda acc, x: acc + x)
        >>> print(result.value)
        10
    """

    def fold_left(
        self,
        items: List[T],
        initial: U,
        func: Callable[[U, T, int], U],
        on_error: Optional[Callable[[U, Exception, int], U]] = None,
    ) -> FoldResult[U]:
        """
        Fold a list from left to right.

        Args:
            items: Items to fold.
            initial: Initial accumulator value.
            func: Folding function (accumulator, item, index).
            on_error: Optional error handler.

        Returns:
            FoldResult with final accumulated value.
        """
        import time
        start = time.perf_counter()
        errors = []
        accumulator = initial
        state = FoldState(accumulator=accumulator)

        for i, item in enumerate(items):
            state.current_index = i
            try:
                accumulator = func(accumulator, item, i)
                state.processed_count += 1
            except Exception as e:
                errors.append(f"Index {i}: {str(e)}")
                logger.warning(f"Fold error at index {i}: {e}")
                if on_error:
                    accumulator = on_error(accumulator, e, i)
                else:
                    accumulator = self._default_error_handler(accumulator, e, i)

        duration = (time.perf_counter() - start) * 1000

        return FoldResult(
            value=accumulator,
            iterations=state.processed_count,
            duration_ms=duration,
            errors=errors,
            metadata={"direction": "left", "total_items": len(items)},
        )

    def fold_right(
        self,
        items: List[T],
        initial: U,
        func: Callable[[U, T, int], U],
        on_error: Optional[Callable[[U, Exception, int], U]] = None,
    ) -> FoldResult[U]:
        """
        Fold a list from right to left.

        Args:
            items: Items to fold.
            initial: Initial accumulator value.
            func: Folding function (accumulator, item, index).
            on_error: Optional error handler.

        Returns:
            FoldResult with final accumulated value.
        """
        import time
        start = time.perf_counter()
        errors = []
        accumulator = initial

        for i in range(len(items) - 1, -1, -1):
            try:
                accumulator = func(accumulator, items[i], i)
            except Exception as e:
                errors.append(f"Index {i}: {str(e)}")
                logger.warning(f"Fold error at index {i}: {e}")
                if on_error:
                    accumulator = on_error(accumulator, e, i)
                else:
                    accumulator = self._default_error_handler(accumulator, e, i)

        duration = (time.perf_counter() - start) * 1000

        return FoldResult(
            value=accumulator,
            iterations=len(items),
            duration_ms=duration,
            errors=errors,
            metadata={"direction": "right", "total_items": len(items)},
        )

    def _default_error_handler(self, acc: U, error: Exception, index: int) -> U:
        """Default error handler that continues with accumulator unchanged."""
        return acc

    def fold_map(
        self,
        items: List[T],
        mapper: Callable[[T], U],
        reducer: Callable[[U, U], U],
        initial: Optional[U] = None,
    ) -> FoldResult[U]:
        """
        Map items then fold the results.

        Args:
            items: Items to process.
            mapper: Transformation function.
            reducer: Reduction function.
            initial: Optional initial value.

        Returns:
            FoldResult with reduced value.
        """
        mapped = [mapper(item) for item in items]

        if initial is None:
            if not mapped:
                raise ValueError("Cannot fold empty list without initial value")
            result = mapped[0]
            start_index = 1
        else:
            result = initial
            start_index = 0

        for i, item in enumerate(mapped[start_index:], start=start_index):
            result = reducer(result, item)

        return FoldResult(
            value=result,
            iterations=len(items),
            duration_ms=0.0,
            errors=[],
            metadata={"map_reduce": True, "total_items": len(items)},
        )

    def fold_group_by(
        self,
        items: List[T],
        key_func: Callable[[T], str],
        value_func: Optional[Callable[[List[T]], U]] = None,
    ) -> FoldResult[Dict[str, U]]:
        """
        Fold items into groups by key.

        Args:
            items: Items to group.
            key_func: Function to extract group key.
            value_func: Optional function to transform grouped values.

        Returns:
            FoldResult with grouped values.
        """
        import time
        start = time.perf_counter()
        groups: Dict[str, List[T]] = {}

        for item in items:
            key = key_func(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        if value_func:
            result = {key: value_func(values) for key, values in groups.items()}
        else:
            result = groups  # type: ignore

        duration = (time.perf_counter() - start) * 1000

        return FoldResult(
            value=result,
            iterations=len(items),
            duration_ms=duration,
            errors=[],
            metadata={"group_count": len(groups), "total_items": len(items)},
        )

    def fold_partition(
        self,
        items: List[T],
        predicate: Callable[[T], bool],
    ) -> FoldResult[tuple]:
        """
        Fold items into two groups based on predicate.

        Args:
            items: Items to partition.
            predicate: Function that returns True for first group.

        Returns:
            FoldResult with (matching, non_matching) tuple.
        """
        import time
        start = time.perf_counter()

        matching = []
        non_matching = []

        for item in items:
            if predicate(item):
                matching.append(item)
            else:
                non_matching.append(item)

        duration = (time.perf_counter() - start) * 1000

        return FoldResult(
            value=(matching, non_matching),
            iterations=len(items),
            duration_ms=duration,
            errors=[],
            metadata={
                "matching_count": len(matching),
                "non_matching_count": len(non_matching),
            },
        )

    def fold_tree(
        self,
        root: T,
        children_func: Callable[[T], List[T]],
        accumulator_func: Callable[[U, T], U],
        initial: U,
    ) -> FoldResult[U]:
        """
        Fold a tree structure (depth-first).

        Args:
            root: Root node.
            children_func: Function to get children of a node.
            accumulator_func: Function to accumulate node value.
            initial: Initial accumulator value.

        Returns:
            FoldResult with accumulated value.
        """
        import time
        from collections import deque

        start = time.perf_counter()
        accumulator = initial
        queue = deque([root])
        iterations = 0

        while queue:
            node = queue.popleft()
            accumulator = accumulator_func(accumulator, node)
            iterations += 1

            children = children_func(node)
            queue.extend(children)

        duration = (time.perf_counter() - start) * 1000

        return FoldResult(
            value=accumulator,
            iterations=iterations,
            duration_ms=duration,
            errors=[],
            metadata={"tree_fold": True},
        )


def fold_left(
    items: List[T],
    initial: U,
    func: Callable[[U, T, int], U],
) -> U:
    """Convenience function for fold_left."""
    action = DataFoldAction()
    return action.fold_left(items, initial, func).value


def fold_right(
    items: List[T],
    initial: U,
    func: Callable[[U, T, int], U],
) -> U:
    """Convenience function for fold_right."""
    action = DataFoldAction()
    return action.fold_right(items, initial, func).value
