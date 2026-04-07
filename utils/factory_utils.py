"""Factory utilities for RabAI AutoClick.

Provides:
- Factory registry
- Factory builder
- Abstract factory pattern
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class FactoryRegistry(Generic[T]):
    """Registry for factories.

    Example:
        registry = FactoryRegistry[Connection]()

        @registry.register("mysql")
        def mysql_factory() -> Connection:
            return MySQLConnection()

        @registry.register("postgres")
        def postgres_factory() -> Connection:
            return PostgresConnection()

        conn = registry.create("mysql")
    """

    def __init__(self) -> None:
        self._factories: Dict[str, Callable[[], T]] = {}
        self._default: Optional[str] = None

    def register(
        self,
        name: str,
    ) -> Callable[[Callable[[], T]], Callable[[], T]]:
        """Decorator to register a factory.

        Args:
            name: Factory name.

        Returns:
            Decorator function.
        """
        def decorator(factory: Callable[[], T]) -> Callable[[], T]:
            self._factories[name] = factory
            return factory
        return decorator

    def add(self, name: str, factory: Callable[[], T]) -> None:
        """Add a factory.

        Args:
            name: Factory name.
            factory: Factory function.
        """
        self._factories[name] = factory

    def set_default(self, name: str) -> None:
        """Set default factory name."""
        self._default = name

    def create(self, name: Optional[str] = None, **kwargs: Any) -> T:
        """Create instance using named factory.

        Args:
            name: Factory name. Uses default if None.
            **kwargs: Arguments to pass to factory.

        Returns:
            Created instance.
        """
        if name is None:
            name = self._default
        if name is None:
            raise ValueError("No factory name specified and no default set")

        if name not in self._factories:
            raise KeyError(f"Factory not found: {name}")

        return self._factories[name]()

    def has(self, name: str) -> bool:
        """Check if factory exists."""
        return name in self._factories

    def names(self) -> list[str]:
        """Get list of registered factory names."""
        return list(self._factories.keys())


class FactoryBuilder(Generic[T]):
    """Builder for creating objects with fluent API.

    Example:
        builder = FactoryBuilder[Config]()

        config = (
            builder
            .option("host", "localhost")
            .option("port", 5432)
            .option("debug", True)
            .build()
        )
    """

    def __init__(self, factory: Callable[[Dict[str, Any]], T]) -> None:
        self._factory = factory
        self._options: Dict[str, Any] = {}

    def option(self, key: str, value: Any) -> FactoryBuilder[T]:
        """Set an option.

        Args:
            key: Option name.
            value: Option value.

        Returns:
            Self for chaining.
        """
        self._options[key] = value
        return self

    def options(self, **kwargs: Any) -> FactoryBuilder[T]:
        """Set multiple options.

        Args:
            **kwargs: Options to set.

        Returns:
            Self for chaining.
        """
        self._options.update(kwargs)
        return self

    def build(self) -> T:
        """Build the object.

        Returns:
            Created object.
        """
        return self._factory(self._options)


def simple_factory(
    base_class: type[T],
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to create a simple factory.

    Args:
        base_class: Base class to register with.

    Returns:
        Decorator function.
    """
    registry: Dict[str, type[T]] = {}

    def decorator(cls: type[T]) -> type[T]:
        name = cls.__name__
        registry[name] = cls

        def factory(*args, **kwargs) -> T:
            return cls(*args, **kwargs)

        factory.class_name = name  # type: ignore
        factory.registry = registry  # type: ignore
        return cls

    return decorator
