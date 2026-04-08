"""Filter chain and pipeline utilities.

Provides composable filter chains for processing data
through multiple transformation stages.
"""

from typing import Any, Callable, Generic, List, Optional, TypeVar


T = TypeVar("T")
U = TypeVar("U")


class Filter(Generic[T, U]):
    """Base filter interface.

    Example:
        filter = LambdaFilter(lambda x: x > 0)
        assert filter.apply(-1) == False
    """

    def apply(self, value: T) -> U:
        """Apply filter to value."""
        raise NotImplementedError


class LambdaFilter(Filter[T, U]):
    """Filter using lambda function."""

    def __init__(self, func: Callable[[T], U]) -> None:
        self._func = func

    def apply(self, value: T) -> U:
        return self._func(value)


class FilterChain(Generic[T, U]):
    """Chain of filters applied in sequence.

    Example:
        chain = FilterChain()
        chain.add(lambda x: x * 2)
        chain.add(lambda x: x + 1)
        result = chain.process(5)  # (5 * 2) + 1 = 11
    """

    def __init__(self) -> None:
        self._filters: List[Callable[[Any], Any]] = []

    def add(self, filter_func: Callable[[Any], Any]) -> "FilterChain":
        """Add filter to chain.

        Args:
            filter_func: Filter function.

        Returns:
            Self for chaining.
        """
        self._filters.append(filter_func)
        return self

    def process(self, value: T) -> U:
        """Process value through all filters.

        Args:
            value: Input value.

        Returns:
            Transformed value.
        """
        result: Any = value
        for f in self._filters:
            result = f(result)
        return result

    def clear(self) -> None:
        """Clear all filters."""
        self._filters.clear()

    @property
    def length(self) -> int:
        """Get number of filters."""
        return len(self._filters)


class Predicate(Generic[T]):
    """Represents a filter predicate.

    Example:
        is_positive = Predicate(lambda x: x > 0)
        is_even = Predicate(lambda x: x % 2 == 0)
        assert is_positive.and_(is_even).matches(4)
    """

    def __init__(self, func: Callable[[T], bool]) -> None:
        self._func = func

    def matches(self, value: T) -> bool:
        """Check if value matches predicate."""
        return self._func(value)

    def and_(self, other: "Predicate[T]") -> "Predicate[T]":
        """Logical AND with another predicate."""
        return Predicate(lambda x: self._func(x) and other._func(x))

    def or_(self, other: "Predicate[T]") -> "Predicate[T]":
        """Logical OR with another predicate."""
        return Predicate(lambda x: self._func(x) or other._func(x))

    def not_(self) -> "Predicate[T]":
        """Logical NOT of predicate."""
        return Predicate(lambda x: not self._func(x))


class FilterPipeline(Generic[T]):
    """Pipeline for filtering and transforming data.

    Example:
        pipeline = FilterPipeline()
        pipeline.source(data_list)
        pipeline.filter(lambda x: x > 0)
        pipeline.map(lambda x: x * 2)
        pipeline.reduce(lambda acc, x: acc + x, 0)
        result = pipeline.execute()
    """

    def __init__(self) -> None:
        self._source: Optional[List[T]] = None
        self._filters: List[Callable[[Any], Any]] = []
        self._post_filter: Optional[Callable[[Any], bool]] = None
        self._mapper: Optional[Callable[[Any], Any]] = None
        self._reducer: Optional[Callable[[Any, Any], Any]] = None
        self._reducer_initial: Any = None

    def source(self, data: List[T]) -> "FilterPipeline":
        """Set data source.

        Args:
            data: Source data list.

        Returns:
            Self for chaining.
        """
        self._source = list(data)
        return self

    def filter(self, predicate: Callable[[T], bool]) -> "FilterPipeline":
        """Add filter predicate.

        Args:
            predicate: Filter function.

        Returns:
            Self for chaining.
        """
        self._filters.append(lambda data: [x for x in data if predicate(x)])
        return self

    def map(self, mapper: Callable[[T], U]) -> "FilterPipeline":
        """Add transformation.

        Args:
            mapper: Transform function.

        Returns:
            Self for chaining.
        """
        self._mapper = mapper
        return self

    def reduce(self, reducer: Callable[[U, U], U], initial: U) -> "FilterPipeline":
        """Add reduction.

        Args:
            reducer: Reduction function.
            initial: Initial accumulator value.

        Returns:
            Self for chaining.
        """
        self._reducer = reducer
        self._reducer_initial = initial
        return self

    def execute(self) -> Any:
        """Execute the pipeline.

        Returns:
            Pipeline result.
        """
        if self._source is None:
            return []

        data = self._source
        for f in self._filters:
            data = f(data)

        if self._mapper:
            data = [self._mapper(x) for x in data]

        if self._reducer:
            result = self._reducer_initial
            for item in data:
                result = self._reducer(result, item)
            return result

        return data


class TapFilter(Generic[T]):
    """Filter that taps into pipeline for side effects.

    Example:
        chain = FilterChain()
        chain.add(lambda x: x * 2)
        chain.add(TapFilter(lambda x: print(f"Debug: {x}")))
        chain.add(lambda x: x + 1)
    """

    def __init__(self, tap_func: Callable[[T], None]) -> None:
        self._tap_func = tap_func

    def apply(self, value: T) -> T:
        """Apply tap and pass through."""
        self._tap_func(value)
        return value
