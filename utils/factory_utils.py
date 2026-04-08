"""Factory pattern utilities.

Provides generic factory implementations for creating objects
with registration-based construction.
"""

from typing import Any, Callable, Dict, Generic, Type, TypeVar


T = TypeVar("T")


class Factory(Generic[T]):
    """Generic factory with registration-based product creation.

    Example:
        factory = Factory[Widget]()
        factory.register("basic", BasicWidget)
        factory.register("advanced", AdvancedWidget)
        widget = factory.create("basic", arg1=1, arg2=2)
    """

    def __init__(self) -> None:
        self._builders: Dict[str, Type[T]] = {}
        self._factories: Dict[str, Callable[..., T]] = {}

    def register(self, name: str, cls: Type[T]) -> None:
        """Register a class by name.

        Args:
            name: Product name.
            cls: Class to instantiate.
        """
        self._builders[name] = cls

    def register_factory(self, name: str, factory: Callable[..., T]) -> None:
        """Register a factory function by name.

        Args:
            name: Product name.
            factory: Factory function to call.
        """
        self._factories[name] = factory

    def create(self, name: str, *args: Any, **kwargs: Any) -> T:
        """Create a product by name.

        Args:
            name: Product name.
            *args: Positional arguments for constructor.
            **kwargs: Keyword arguments for constructor.

        Returns:
            New product instance.

        Raises:
            ValueError: If product name is not registered.
        """
        if name in self._builders:
            return self._builders[name](*args, **kwargs)
        if name in self._factories:
            return self._factories[name](*args, **kwargs)
        raise ValueError(f"Unknown product: {name!r}")

    def has(self, name: str) -> bool:
        """Check if a product is registered.

        Args:
            name: Product name.

        Returns:
            True if registered.
        """
        return name in self._builders or name in self._factories

    def names(self) -> list:
        """List all registered product names.

        Returns:
            List of product names.
        """
        return list(set(list(self._builders.keys()) + list(self._factories.keys())))

    def unregister(self, name: str) -> bool:
        """Unregister a product.

        Args:
            name: Product name.

        Returns:
            True if product was removed.
        """
        removed = name in self._builders
        if name in self._builders:
            del self._builders[name]
        if name in self._factories:
            del self._factories[name]
        return removed
