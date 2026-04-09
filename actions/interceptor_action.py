"""
Interceptor Action Module.

Provides request/response interception for modifying behavior
without changing the core action logic.
"""

import time
import asyncio
import threading
from typing import Callable, Any, Optional, List, Dict, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod


T = TypeVar("T")
R = TypeVar("R")


class InterceptorPhase(Enum):
    """Interceptor execution phases."""
    BEFORE = "before"
    AROUND = "around"
    AFTER = "after"
    ON_ERROR = "on_error"


@dataclass
class InterceptContext:
    """Context for intercepted operations."""
    operation_name: str
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: Optional[Exception] = None


class Interceptor(ABC, Generic[T, R]):
    """Abstract base class for interceptors."""

    @abstractmethod
    def before(self, context: InterceptContext) -> None:
        """Called before the operation."""
        pass

    @abstractmethod
    def after(
        self,
        context: InterceptContext,
        result: R,
    ) -> R:
        """Called after the operation with result."""
        return result

    def on_error(
        self,
        context: InterceptContext,
        error: Exception,
    ) -> Exception:
        """Called when operation raises an exception."""
        return error


@dataclass
class InterceptorChain:
    """Chain of interceptors to execute in order."""
    interceptors: List[Interceptor] = field(default_factory=list)
    index: int = 0


class InterceptorRegistry:
    """Registry for managing interceptors by operation name."""

    def __init__(self):
        self._interceptors: Dict[str, List[Interceptor]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        operation: str,
        interceptor: Interceptor,
        position: Optional[int] = None,
    ) -> None:
        """Register an interceptor for an operation."""
        with self._lock:
            if operation not in self._interceptors:
                self._interceptors[operation] = []
            if position is not None:
                self._interceptors[operation].insert(position, interceptor)
            else:
                self._interceptors[operation].append(interceptor)

    def get_interceptors(self, operation: str) -> List[Interceptor]:
        """Get all interceptors for an operation."""
        with self._lock:
            return list(self._interceptors.get(operation, []))

    def unregister(self, operation: str, interceptor: Interceptor) -> bool:
        """Unregister an interceptor."""
        with self._lock:
            if operation in self._interceptors:
                try:
                    self._interceptors[operation].remove(interceptor)
                    return True
                except ValueError:
                    pass
            return False


class InterceptorAction(Generic[T, R]):
    """
    Action wrapper that supports interception.

    Example:
        def log_interceptor(ctx):
            print(f"Operation: {ctx.operation_name}")

        action = InterceptorAction("fetch_data")
        action.add_interceptor(log_interceptor)
        result = action.execute(fetch_from_api)
    """

    def __init__(
        self,
        name: str,
        registry: Optional[InterceptorRegistry] = None,
    ):
        self.name = name
        self.registry = registry or InterceptorAction._default_registry()
        self._local_interceptors: List[Callable] = []

    def add_interceptor(
        self,
        interceptor: Callable,
        position: Optional[int] = None,
    ) -> None:
        """Add a local interceptor function."""
        if position is not None:
            self._local_interceptors.insert(position, interceptor)
        else:
            self._local_interceptors.append(interceptor)

    def _build_context(
        self,
        args: tuple,
        kwargs: Dict[str, Any],
    ) -> InterceptContext:
        """Build interception context."""
        return InterceptContext(
            operation_name=self.name,
            args=args,
            kwargs=kwargs,
        )

    def _call_before_interceptors(
        self,
        context: InterceptContext,
    ) -> None:
        """Call all before interceptors."""
        for interceptor in self._local_interceptors:
            if callable(interceptor):
                try:
                    interceptor.before(context)
                except Exception:
                    pass

        for reg_interceptor in self.registry.get_interceptors(self.name):
            try:
                reg_interceptor.before(context)
            except Exception:
                pass

    def _call_after_interceptors(
        self,
        context: InterceptContext,
        result: R,
    ) -> R:
        """Call all after interceptors."""
        for interceptor in reversed(self._local_interceptors):
            if callable(interceptor) and hasattr(interceptor, "after"):
                try:
                    result = interceptor.after(context, result)
                except Exception:
                    pass

        for reg_interceptor in reversed(
            self.registry.get_interceptors(self.name)
        ):
            try:
                result = reg_interceptor.after(context, result)
            except Exception:
                pass

        return result

    def _call_error_interceptors(
        self,
        context: InterceptContext,
        error: Exception,
    ) -> Exception:
        """Call all error interceptors."""
        for interceptor in reversed(self._local_interceptors):
            if callable(interceptor) and hasattr(interceptor, "on_error"):
                try:
                    error = interceptor.on_error(context, error)
                except Exception:
                    pass

        for reg_interceptor in reversed(
            self.registry.get_interceptors(self.name)
        ):
            try:
                error = reg_interceptor.on_error(context, error)
            except Exception:
                pass

        return error

    def execute(self, func: Callable[[T], R], arg: T) -> R:
        """Execute function with interception (sync)."""
        context = self._build_context((arg,), {})
        self._call_before_interceptors(context)

        try:
            result = func(arg)
            context.result = result
            return self._call_after_interceptors(context, result)
        except Exception as e:
            context.error = e
            error = self._call_error_interceptors(context, e)
            if error is not None:
                raise error
            return None

    async def execute_async(
        self,
        func: Callable[[T], R],
        arg: T,
    ) -> R:
        """Execute function with interception (async)."""
        context = self._build_context((arg,), {})
        self._call_before_interceptors(context)

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(arg)
            else:
                result = func(arg)
            context.result = result
            return self._call_after_interceptors(context, result)
        except Exception as e:
            context.error = e
            error = self._call_error_interceptors(context, e)
            if error is not None:
                raise error
            return None

    def wrap(self, func: Callable) -> Callable:
        """Wrap a function with interception."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(lambda: func(*args, **kwargs), None)
        return wrapper

    @staticmethod
    def _default_registry() -> InterceptorRegistry:
        """Get or create default registry."""
        if not hasattr(InterceptorAction, "_registry"):
            InterceptorAction._registry = InterceptorRegistry()
        return InterceptorAction._registry


def wrappable(func: Callable) -> Callable:
    """Decorator to make a function interceptable."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper
