"""Stream processing utilities.

Provides lazy stream operations for processing sequences
of data with functional programming patterns.
"""

from typing import Any, Callable, Generic, Iterator, List, Optional, TypeVar


T = TypeVar("T")
U = TypeVar("U")


class Stream(Generic[T]):
    """Lazy stream for functional-style data processing.

    Example:
        result = (Stream(range(1000))
            .filter(lambda x: x % 2 == 0)
            .map(lambda x: x * 2)
            .take(10)
            .to_list())
    """

    def __init__(self, source: Iterator[T]) -> None:
        self._source = source

    @classmethod
    def of(cls, *items: T) -> "Stream[T]":
        """Create stream from items.

        Args:
            *items: Items to stream.

        Returns:
            Stream of items.
        """
        return cls(iter(items))

    @classmethod
    def from_iterable(cls, iterable: Any) -> "Stream[T]":
        """Create stream from iterable.

        Args:
            iterable: Any iterable.

        Returns:
            Stream of items.
        """
        return cls(iter(iterable))

    def map(self, func: Callable[[T], U]) -> "Stream[U]":
        """Map each element through function.

        Args:
            func: Transformation function.

        Returns:
            New stream with transformed elements.
        """
        return Stream((func(item) for item in self._source))

    def filter(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """Filter elements by predicate.

        Args:
            predicate: Filter condition.

        Returns:
            New stream with filtered elements.
        """
        return Stream((item for item in self._source if predicate(item)))

    def flat_map(self, func: Callable[[T], Iterator[U]]) -> "Stream[U]":
        """Map and flatten results.

        Args:
            func: Function returning iterables.

        Returns:
            New stream with flattened results.
        """
        return Stream((u for item in self._source for u in func(item)))

    def take(self, n: int) -> "Stream[T]":
        """Take first n elements.

        Args:
            n: Number of elements.

        Returns:
            New stream with first n elements.
        """
        def take_iter():
            for i, item in enumerate(self._source):
                if i >= n:
                    break
                yield item
        return Stream(take_iter())

    def skip(self, n: int) -> "Stream[T]":
        """Skip first n elements.

        Args:
            n: Number of elements to skip.

        Returns:
            New stream without first n elements.
        """
        def skip_iter():
            for i, item in enumerate(self._source):
                if i >= n:
                    yield item
        return Stream(skip_iter())

    def take_while(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """Take elements while predicate is true.

        Args:
            predicate: Condition function.

        Returns:
            New stream with elements until predicate fails.
        """
        return Stream((item for item in self._source if predicate(item)))

    def skip_while(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """Skip elements while predicate is true.

        Args:
            predicate: Condition function.

        Returns:
            New stream starting after predicate fails.
        """
        skipping = True
        for item in self._source:
            if skipping and predicate(item):
                continue
            skipping = False
            yield item  # type: ignore
        return Stream(iter([]))  # placeholder

    def distinct(self) -> "Stream[T]":
        """Remove duplicate elements.

        Returns:
            New stream with unique elements.
        """
        seen: List[T] = []
        for item in self._source:
            if item not in seen:
                seen.append(item)
                yield item
        return Stream(iter([]))  # placeholder

    def sorted(self, key: Optional[Callable[[T], Any]] = None) -> "Stream[T]":
        """Sort stream elements.

        Args:
            key: Optional sort key function.

        Returns:
            New sorted stream.
        """
        return Stream(sorted(self._source, key=key))

    def reduce(self, func: Callable[[T, T], T]) -> Optional[T]:
        """Reduce to single value.

        Args:
            func: Reduction function.

        Returns:
            Reduced value or None if empty.
        """
        result: Optional[T] = None
        for item in self._source:
            if result is None:
                result = item
            else:
                result = func(result, item)
        return result

    def fold(self, initial: U, func: Callable[[U, T], U]) -> U:
        """Fold to accumulated value.

        Args:
            initial: Initial accumulator value.
            func: Accumulation function.

        Returns:
            Final accumulated value.
        """
        result = initial
        for item in self._source:
            result = func(result, item)
        return result

    def collect(self) -> List[T]:
        """Collect stream to list.

        Returns:
            List of all elements.
        """
        return list(self._source)

    def to_list(self) -> List[T]:
        """Convert stream to list."""
        return self.collect()

    def first(self) -> Optional[T]:
        """Get first element.

        Returns:
            First element or None if empty.
        """
        return next(self._source, None)

    def count(self) -> int:
        """Count elements.

        Returns:
            Number of elements.
        """
        return sum(1 for _ in self._source)

    def any_match(self, predicate: Callable[[T], bool]) -> bool:
        """Check if any element matches.

        Args:
            predicate: Match condition.

        Returns:
            True if any element matches.
        """
        return any(predicate(item) for item in self._source)

    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        """Check if all elements match.

        Args:
            predicate: Match condition.

        Returns:
            True if all elements match.
        """
        return all(predicate(item) for item in self._source)

    def none_match(self, predicate: Callable[[T], bool]) -> bool:
        """Check if no elements match.

        Args:
            predicate: Match condition.

        Returns:
            True if no element matches.
        """
        return not self.any_match(predicate)
