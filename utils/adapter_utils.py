"""
Adapter Pattern Implementation

Provides adapters for integrating incompatible interfaces,
including object adapters, class adapters, and two-way adapters.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TAdaptee = TypeVar("TAdaptee")


class AdapterError(Exception):
    """Base exception for adapter errors."""
    pass


class AdaptationError(AdapterError):
    """Raised when adaptation fails."""
    pass


class Target(ABC):
    """
    The interface that the client expects to work with.
    """

    @abstractmethod
    def request(self, *args: Any, **kwargs: Any) -> Any:
        """The request method that the client calls."""
        pass


class Adaptee(ABC):
    """
    The existing interface that needs to be adapted.
    """

    @abstractmethod
    def specific_request(self, *args: Any, **kwargs: Any) -> Any:
        """The specific method that needs adapting."""
        pass


@dataclass
class AdaptationResult:
    """Result of an adaptation operation."""
    success: bool
    value: Any = None
    error: str | None = None
    adapted: bool = False

    @property
    def failed(self) -> bool:
        return not self.success


class ObjectAdapter(Target):
    """
    Object Adapter - Uses composition to adapt the adaptee.
    """

    def __init__(
        self,
        adaptee: Adaptee,
        request_transform: Callable[[Any], Any] | None = None,
        response_transform: Callable[[Any], Any] | None = None,
    ):
        self._adaptee = adaptee
        self._request_transform = request_transform or (lambda x: x)
        self._response_transform = response_transform or (lambda x: x)

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Adapt the request to the adaptee's interface."""
        adapted_args = self._request_transform((args, kwargs))
        result = self._adaptee.specific_request(*adapted_args[0], **adapted_args[1])
        return self._response_transform(result)

    @property
    def adaptee(self) -> Adaptee:
        """Access to the underlying adaptee."""
        return self._adaptee


class ClassAdapter(Target):
    """
    Class Adapter - Uses inheritance to adapt the adaptee.
    Note: Python's multiple inheritance can be used for this pattern.
    """

    def __init__(
        self,
        adaptee_class: type[Adaptee],
        *args: Any,
        **kwargs: Any,
    ):
        self._adaptee_instance: Adaptee = adaptee_class(*args, **kwargs)

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate to the adaptee's specific_request."""
        return self._adaptee_instance.specific_request(*args, **kwargs)

    @property
    def adaptee(self) -> Adaptee:
        """Access to the underlying adaptee instance."""
        return self._adaptee_instance


@dataclass
class TwoWayAdapterState:
    """State shared between two-way adapters."""
    left_value: Any = None
    right_value: Any = None
    sync_enabled: bool = True


class TwoWayAdapter(Generic[TAdaptee, T]):
    """
    Two-way adapter that can convert between two interfaces bidirectionally.
    """

    def __init__(
        self,
        left_object: TAdaptee,
        right_object: T,
        left_to_right: Callable[[Any], Any] | None = None,
        right_to_left: Callable[[Any], Any] | None = None,
    ):
        self._left_object = left_object
        self._right_object = right_object
        self._left_to_right = left_to_right or (lambda x: x)
        self._right_to_left = right_to_left or (lambda x: x)
        self._state = TwoWayAdapterState()

    def from_left(self, value: Any) -> Any:
        """Convert from left interface to right interface."""
        self._state.left_value = value
        if self._state.sync_enabled:
            self._state.right_value = self._left_to_right(value)
        return self._state.right_value

    def from_right(self, value: Any) -> Any:
        """Convert from right interface to left interface."""
        self._state.right_value = value
        if self._state.sync_enabled:
            self._state.left_value = self._right_to_left(value)
        return self._state.left_value

    def get_left(self) -> TAdaptee:
        """Get the left object."""
        return self._left_object

    def get_right(self) -> T:
        """Get the right object."""
        return self._right_object

    @property
    def state(self) -> TwoWayAdapterState:
        """Get the shared state."""
        return self._state


class ChainedAdapter(Target):
    """
    Adapter that chains multiple adapters together.
    Useful when adapting through multiple intermediate interfaces.
    """

    def __init__(self, adapters: list[ObjectAdapter] | None = None):
        self._adapters: list[ObjectAdapter] = adapters or []

    def add_adapter(self, adapter: ObjectAdapter) -> ChainedAdapter:
        """Add an adapter to the chain."""
        self._adapters.append(adapter)
        return self

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Pass the request through all adapters in the chain."""
        if not self._adapters:
            raise AdaptationError("No adapters in chain")

        result: Any = None
        for i, adapter in enumerate(self._adapters):
            try:
                if i == 0:
                    result = adapter.request(*args, **kwargs)
                else:
                    result = adapter.request(result)
            except Exception as e:
                raise AdaptationError(f"Failed at adapter {i}: {e}") from e

        return result

    def get_adapters(self) -> list[ObjectAdapter]:
        """Get the list of adapters."""
        return list(self._adapters)


@dataclass
class AdapterMetrics:
    """Metrics for adapter operations."""
    total_adaptations: int = 0
    successful_adaptations: int = 0
    failed_adaptations: int = 0
    total_latency_ms: float = 0.0
    by_direction: dict[str, int] = field(default_factory=dict)


class MeasuringAdapter(Target):
    """
    Adapter that measures its own performance.
    """

    def __init__(self, adaptee: Adaptee):
        self._adaptee = adaptee
        self._metrics = AdapterMetrics()

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Execute request and collect metrics."""
        import time
        start = time.time()
        self._metrics.total_adaptations += 1

        try:
            result = self._adaptee.specific_request(*args, **kwargs)
            self._metrics.successful_adaptations += 1
            return result
        except Exception as e:
            self._metrics.failed_adaptations += 1
            raise AdaptationError(f"Adaptation failed: {e}") from e
        finally:
            self._metrics.total_latency_ms += (time.time() - start) * 1000

    @property
    def metrics(self) -> AdapterMetrics:
        return self._metrics


class SmartAdapter(Target):
    """
    Adapter that can automatically discover and use adaptation methods.
    """

    def __init__(self, adaptee: Adaptee):
        self._adaptee = adaptee
        self._method_map: dict[str, str] = {}
        self._custom_adapters: dict[str, Callable] = {}
        self._discover_methods()

    def _discover_methods(self) -> None:
        """Discover available methods on the adaptee."""
        for attr_name in dir(self._adaptee):
            if not attr_name.startswith("_"):
                self._method_map[attr_name] = attr_name

    def register_custom_adapter(
        self,
        target_method: str,
        adapter_func: Callable[..., Any],
    ) -> SmartAdapter:
        """Register a custom adapter function for a specific method."""
        self._custom_adapters[target_method] = adapter_func
        return self

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Automatically adapt the request to the appropriate method."""
        # In a real implementation, this would use method_map and custom_adapters
        # to intelligently route the request
        return self._adaptee.specific_request(*args, **kwargs)


class AdapterFactory:
    """
    Factory for creating adapters with consistent configuration.
    """

    def __init__(self):
        self._registry: dict[str, tuple[type, dict[str, Any]]] = {}

    def register(
        self,
        name: str,
        adapter_class: type,
        default_kwargs: dict[str, Any] | None = None,
    ) -> AdapterFactory:
        """Register an adapter class."""
        self._registry[name] = (adapter_class, default_kwargs or {})
        return self

    def create(self, name: str, *args: Any, **kwargs: Any) -> Target:
        """Create an adapter by name."""
        if name not in self._registry:
            raise AdapterError(f"Adapter '{name}' not registered. Available: {list(self._registry.keys())}")

        adapter_class, defaults = self._registry[name]
        merged_kwargs = {**defaults, **kwargs}
        return adapter_class(*args, **merged_kwargs)  # type: ignore

    def list_adapters(self) -> list[str]:
        """List registered adapter names."""
        return list(self._registry.keys())


def adapt(
    adaptee: Adaptee,
    target_class: type[T] | None = None,
    request_transform: Callable[[tuple, dict], tuple[list, dict]] | None = None,
    response_transform: Callable[[Any], Any] | None = None,
) -> Target:
    """
    Convenience function to create an adapter.

    Args:
        adaptee: The object to adapt.
        target_class: Optional target class to instantiate.
        request_transform: Optional function to transform request args.
        response_transform: Optional function to transform response.

    Returns:
        An adapter instance.
    """
    if target_class is not None:
        return target_class(adaptee)  # type: ignore

    return ObjectAdapter(
        adaptee,
        request_transform=request_transform,
        response_transform=response_transform,
    )


def auto_adapt(source: Any, target_interface: type) -> Any:
    """
    Automatically adapt an object to a target interface.

    This attempts to match methods from the source to the target interface.
    """
    adapter = SmartAdapter(source)
    return adapter
