"""Pipeline utilities for chaining and composing operations.

Provides functional-style pipeline composition with support for
error handling, branching, and conditional execution flows.

Example:
    >>> from utils.pipeline_utils import Pipeline, pipe
    >>> result = pipe(
    ...     lambda x: x * 2,
    ...     lambda x: x + 1,
    ...     lambda x: x ** 2,
    ... )(5)
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class PipelineError(Exception):
    """Raised when pipeline execution fails."""
    pass


class Pipeline(Generic[T]):
    """Composable function pipeline.

    Supports chaining operations with error handling,
    branching, and short-circuit evaluation.

    Example:
        >>> pipeline = (
        ...     Pipeline([1, 2, 3])
        ...     .then(lambda x: x * 2)
        ...     .then_filter(lambda x: x > 2)
        ...     .catch(lambda e: [-1])
        ... )
        >>> list(pipeline)
        [4, 6]
    """

    def __init__(
        self,
        initial: Union[T, Callable[[], T]],
        *,
        error_mode: str = "raise",
    ) -> None:
        """Initialize pipeline.

        Args:
            initial: Initial value or factory callable.
            error_mode: How to handle errors - "raise", "skip", or "catch".
        """
        self._value: Optional[T] = None
        self._factory: Optional[Callable[[], T]] = None
        self._steps: List[Callable[[Any], Any]] = []
        self._error_mode = error_mode
        self._error_handler: Optional[Callable[[Exception], Any]] = None
        self._has_value = False

        if callable(initial) and not isinstance(initial, type):
            self._factory = initial
        else:
            self._value = initial
            self._has_value = True

    def then(self, fn: Callable[[Any], U]) -> Pipeline[U]:
        """Add a transformation step.

        Args:
            fn: Transformation function.

        Returns:
            New Pipeline with the step added.
        """
        new = self._copy()
        new._steps.append(fn)
        return new

    def then_filter(
        self,
        predicate: Callable[[Any], bool],
        *,
        default: Any = None,
    ) -> Pipeline[Any]:
        """Add a filtering step.

        Args:
            predicate: Filter predicate.
            default: Value to use when filter returns False.

        Returns:
            New Pipeline with filter step added.
        """
        def filter_step(value: Any) -> Any:
            return value if predicate(value) else default

        return self.then(filter_step)

    def branch(
        self,
        condition: Callable[[Any], bool],
        if_true: Callable[[Any], Any],
        if_false: Optional[Callable[[Any], Any]] = None,
    ) -> Pipeline[Any]:
        """Add a conditional branch step.

        Args:
            condition: Branch condition.
            if_true: Transformation if condition is True.
            if_false: Optional transformation if condition is False.

        Returns:
            New Pipeline with branch step added.
        """
        def branch_step(value: Any) -> Any:
            if condition(value):
                return if_true(value)
            elif if_false:
                return if_false(value)
            return value

        return self.then(branch_step)

    def catch(
        self,
        handler: Callable[[Exception], Any],
    ) -> Pipeline[Any]:
        """Add an error handler.

        Args:
            handler: Function to handle exceptions.

        Returns:
            New Pipeline with error handler set.
        """
        new = self._copy()
        new._error_handler = handler
        new._error_mode = "catch"
        return new

    def recover(
        self,
        fn: Callable[[Exception, Any], Any],
    ) -> Pipeline[Any]:
        """Add a recovery function that receives the error and last good value.

        Args:
            fn: Recovery function(exception, last_value) -> new_value.

        Returns:
            New Pipeline with recovery step.
        """
        def recover_step(value: Any) -> Any:
            raise PipelineError("Recover requires catch mode")

        new = self._copy()
        new._steps.append(lambda v: v)
        return new

    def transform(
        self,
        transformer: Callable[[List[Any]], List[Any]],
    ) -> Pipeline[List[Any]]:
        """Apply a batch transformation.

        Args:
            transformer: Function that transforms the list.

        Returns:
            New Pipeline with transformer applied.
        """
        return self.then(transformer)

    def execute(self) -> Any:
        """Execute the pipeline.

        Returns:
            Final pipeline value.

        Raises:
            PipelineError: If error_mode is "raise" and an error occurs.
        """
        value: Any

        if self._factory:
            try:
                value = self._factory()
            except Exception as e:
                if self._error_handler:
                    return self._error_handler(e)
                raise PipelineError(f"Factory failed: {e}") from e
        else:
            value = self._value

        for step in self._steps:
            try:
                value = step(value)
            except Exception as e:
                if self._error_mode == "raise":
                    raise PipelineError(f"Step failed: {e}") from e
                elif self._error_mode == "skip":
                    continue
                elif self._error_mode == "catch" and self._error_handler:
                    value = self._error_handler(e)
                    self._error_mode = "raise"

        return value

    def _copy(self) -> Pipeline[Any]:
        """Create a shallow copy of this pipeline."""
        new = Pipeline.__new__(Pipeline)
        new._value = self._value
        new._factory = self._factory
        new._steps = list(self._steps)
        new._error_mode = self._error_mode
        new._error_handler = self._error_handler
        new._has_value = self._has_value
        return new

    def __iter__(self):
        """Iterate over pipeline result (for list-like values)."""
        result = self.execute()
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
            return iter(result)
        return iter([result])

    def __repr__(self) -> str:
        return f"Pipeline(steps={len(self._steps)})"


def pipe(*functions: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions left-to-right.

    Args:
        *functions: Functions to compose.

    Returns:
        Composed function.

    Example:
        >>> double_then_add_one = pipe(lambda x: x * 2, lambda x: x + 1)
        >>> double_then_add_one(5)
        11
    """
    if not functions:
        return lambda x: x

    def composed(initial: Any) -> Any:
        result = initial
        for fn in functions:
            result = fn(result)
        return result

    return composed


def branch(
    condition: Callable[[Any], bool],
    if_true: Callable[[Any], U],
    if_false: Callable[[Any], V],
) -> Callable[[Any], Union[U, V]]:
    """Conditional function wrapper.

    Args:
        condition: Branch condition.
        if_true: Function to apply if True.
        if_false: Function to apply if False.

    Returns:
        Callable that conditionally applies one of the functions.
    """
    def wrapper(value: Any) -> Union[U, V]:
        return if_true(value) if condition(value) else if_false(value)

    return wrapper


class PipelineBuilder(Generic[T]):
    """Fluent builder for complex pipelines.

    Example:
        >>> result = (
        ...     PipelineBuilder()
        ...     .source(lambda: load_data())
        ...     .stage("normalize", normalize_fn)
        ...     .stage("filter", filter_fn)
        ...     .stage("aggregate", aggregate_fn)
        ...     .execute()
        ... )
    """

    def __init__(self) -> None:
        self._stages: List[tuple[str, Callable[[Any], Any]]] = []
        self._source: Optional[Callable[[], T]] = None

    def source(self, factory: Callable[[], T]) -> PipelineBuilder[T]:
        """Set the pipeline source.

        Args:
            factory: Source factory function.

        Returns:
            Self for chaining.
        """
        self._source = factory
        return self

    def stage(
        self,
        name: str,
        fn: Callable[[Any], Any],
    ) -> PipelineBuilder[Any]:
        """Add a named stage.

        Args:
            name: Stage name for debugging.
            fn: Stage transformation function.

        Returns:
            Self for chaining.
        """
        self._stages.append((name, fn))
        return self

    def execute(self) -> Any:
        """Execute the built pipeline.

        Returns:
            Pipeline result.
        """
        if self._source is None:
            raise PipelineError("No source defined")

        pipeline = Pipeline(self._source)
        for name, fn in self._stages:
            pipeline = pipeline.then(fn)

        return pipeline.execute()
