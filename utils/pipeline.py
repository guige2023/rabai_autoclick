"""Pipeline utilities for RabAI AutoClick.

Provides:
- Data pipeline processing
- Stream transformations
"""

from typing import Any, Callable, Generator, Generic, List, Optional, TypeVar, Iterator


T = TypeVar("T")
R = TypeVar("R")


class Pipeline(Generic[T]):
    """Data pipeline for processing streams.

    Usage:
        result = (
            Pipeline([1, 2, 3, 4, 5])
            .filter(lambda x: x > 2)
            .map(lambda x: x * 2)
            .collect()
        )
    """

    def __init__(self, data: Optional[List[T]] = None) -> None:
        """Initialize pipeline.

        Args:
            data: Initial data list.
        """
        self._data = data or []
        self._stages: List[Callable[[Iterator], Iterator]] = []

    def map(self, func: Callable[[T], R]) -> 'Pipeline[R]':
        """Add map stage.

        Args:
            func: Transformation function.

        Returns:
            New Pipeline with stage added.
        """
        pipeline = Pipeline()
        pipeline._data = self._data
        pipeline._stages = self._stages + [lambda it: map(func, it)]
        return pipeline

    def filter(self, predicate: Callable[[T], bool]) -> 'Pipeline[T]':
        """Add filter stage.

        Args:
            predicate: Filter predicate.

        Returns:
            New Pipeline with stage added.
        """
        pipeline = Pipeline()
        pipeline._data = self._data
        pipeline._stages = self._stages + [lambda it: filter(predicate, it)]
        return pipeline

    def flat_map(self, func: Callable[[T], List[R]]) -> 'Pipeline[R]':
        """Add flatmap stage.

        Args:
            func: Function returning list to flatten.

        Returns:
            New Pipeline with stage added.
        """
        def stage(it: Iterator) -> Generator:
            for item in it:
                for result in func(item):
                    yield result

        pipeline = Pipeline()
        pipeline._data = self._data
        pipeline._stages = self._stages + [stage]
        return pipeline

    def reduce(
        self,
        func: Callable[[Any, T], Any],
        initial: Optional[Any] = None,
    ) -> Optional[Any]:
        """Reduce pipeline to single value.

        Args:
            func: Reduction function.
            initial: Initial value.

        Returns:
            Reduced value.
        """
        result = initial
        for item in self:
            if result is None:
                result = item
            else:
                result = func(result, item)
        return result

    def collect(self) -> List[T]:
        """Collect pipeline results.

        Returns:
            List of results.
        """
        return list(self)

    def take(self, n: int) -> 'Pipeline[T]':
        """Take first n elements.

        Args:
            n: Number of elements.

        Returns:
            New Pipeline limited to n elements.
        """
        pipeline = Pipeline()
        pipeline._data = self._data[:n]
        pipeline._stages = self._stages
        return pipeline

    def skip(self, n: int) -> 'Pipeline[T]':
        """Skip first n elements.

        Args:
            n: Number of elements to skip.

        Returns:
            New Pipeline with n elements skipped.
        """
        pipeline = Pipeline()
        pipeline._data = self._data[n:]
        pipeline._stages = self._stages
        return pipeline

    def distinct(self) -> 'Pipeline[T]':
        """Remove duplicates.

        Returns:
            New Pipeline with duplicates removed.
        """
        seen = set()
        pipeline = Pipeline()

        def stage(it: Iterator) -> Generator:
            for item in it:
                if item not in seen:
                    seen.add(item)
                    yield item

        pipeline._data = []
        pipeline._stages = self._stages + [stage]
        return pipeline

    def sorted(self, key: Optional[Callable[[T], Any]] = None) -> 'Pipeline[T]':
        """Sort pipeline results.

        Args:
            key: Sort key function.

        Returns:
            New Pipeline with sorted results.
        """
        result = sorted(self, key=key)
        pipeline = Pipeline()
        pipeline._data = result
        pipeline._stages = []
        return pipeline

    def first(self, default: Optional[T] = None) -> Optional[T]:
        """Get first element.

        Args:
            default: Default if empty.

        Returns:
            First element or default.
        """
        for item in self:
            return item
        return default

    def __iter__(self) -> Iterator[T]:
        """Iterate through pipeline."""
        it = iter(self._data)
        for stage in self._stages:
            it = stage(it)
        return it

    def __len__(self) -> int:
        """Get number of results (consumes pipeline)."""
        return len(self.collect())


class Stream(Generic[T]):
    """Lazy stream for large data processing.

    Processes data lazily, suitable for infinite or large streams.
    """

    def __init__(self, source: Callable[[], Iterator[T]]) -> None:
        """Initialize stream.

        Args:
            source: Function that returns iterator.
        """
        self._source = source
        self._operations: List[Callable[[Iterator], Iterator]] = []

    @classmethod
    def of(cls, *items: T) -> 'Stream[T]':
        """Create stream from items.

        Args:
            *items: Items to stream.

        Returns:
            Stream instance.
        """
        return cls(lambda: iter(items))

    @classmethod
    def from_iterable(cls, iterable: List[T]) -> 'Stream[T]':
        """Create stream from iterable.

        Args:
            iterable: Source iterable.

        Returns:
            Stream instance.
        """
        return cls(lambda: iter(iterable))

    def map(self, func: Callable[[T], R]) -> 'Stream[R]':
        """Map transformation.

        Args:
            func: Transformation function.

        Returns:
            New Stream.
        """
        stream = Stream(self._source)
        stream._operations = self._operations + [lambda it: map(func, it)]
        return stream

    def filter(self, predicate: Callable[[T], bool]) -> 'Stream[T]':
        """Filter transformation.

        Args:
            predicate: Filter predicate.

        Returns:
            New Stream.
        """
        stream = Stream(self._source)
        stream._operations = self._operations + [lambda it: filter(predicate, it)]
        return stream

    def limit(self, n: int) -> 'Stream[T]':
        """Limit to n elements.

        Args:
            n: Maximum elements.

        Returns:
            New Stream.
        """
        def source() -> Iterator[T]:
            count = 0
            for item in self._apply_operations():
                if count >= n:
                    break
                yield item
                count += 1

        stream = Stream(source)
        stream._operations = []
        return stream

    def skip(self, n: int) -> 'Stream[T]':
        """Skip n elements.

        Args:
            n: Elements to skip.

        Returns:
            New Stream.
        """
        def source() -> Iterator[T]:
            for i, item in enumerate(self._apply_operations()):
                if i >= n:
                    yield item

        stream = Stream(source)
        stream._operations = []
        return stream

    def _apply_operations(self) -> Iterator[T]:
        """Apply all operations to source."""
        it = self._source()
        for op in self._operations:
            it = op(it)
        return it

    def collect(self, n: Optional[int] = None) -> List[T]:
        """Collect stream results.

        Args:
            n: Optional limit.

        Returns:
            List of results.
        """
        if n:
            return list(self.limit(n))
        return list(self)

    def for_each(self, func: Callable[[T], None]) -> None:
        """Execute function for each element.

        Args:
            func: Function to execute.
        """
        for item in self:
            func(item)


def pipe(*functions: Callable) -> Callable:
    """Create a function pipeline.

    Args:
        *functions: Functions to pipe.

    Returns:
        Composed function.
    """
    def composed(value: Any) -> Any:
        result = value
        for func in functions:
            result = func(result)
        return result
    return composed