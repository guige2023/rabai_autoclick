"""Dependency injection utilities for RabAI AutoClick.

Provides:
- Simple dependency injection container
- Service registration and resolution
"""

from typing import Any, Callable, Dict, Optional, Type, TypeVar


T = TypeVar("T")


class Container:
    """Simple dependency injection container.

    Usage:
        container = Container()

        # Register a service
        container.register(Database, lambda c: Database())

        # Resolve
        db = container.resolve(Database)
    """

    def __init__(self) -> None:
        self._services: Dict[Type, Callable] = {}
        self._singletons: Dict[Type, Any] = {}
        self._parent: Optional['Container'] = None

    def register(
        self,
        cls: Type[T],
        factory: Optional[Callable[['Container'], T]] = None,
        singleton: bool = False,
    ) -> None:
        """Register a service.

        Args:
            cls: Service class/interface.
            factory: Factory function to create service.
            singleton: If True, create once and reuse.
        """
        if factory is None:
            factory = lambda c: cls()

        self._services[cls] = (factory, singleton)

    def register_instance(self, cls: Type[T], instance: T) -> None:
        """Register an existing instance.

        Args:
            cls: Service class/interface.
            instance: Service instance.
        """
        self._singletons[cls] = instance

    def resolve(self, cls: Type[T]) -> T:
        """Resolve a service.

        Args:
            cls: Service class/interface.

        Returns:
            Service instance.
        """
        # Check if singleton exists
        if cls in self._singletons:
            return self._singletons[cls]

        # Check if registered
        if cls in self._services:
            factory, is_singleton = self._services[cls]
            instance = factory(self)

            if is_singleton:
                self._singletons[cls] = instance

            return instance

        # Check parent container
        if self._parent:
            return self._parent.resolve(cls)

        # Try to instantiate directly
        try:
            instance = cls()
            return instance
        except Exception:
            raise KeyError(f"No service registered for: {cls.__name__}")

    def has(self, cls: Type) -> bool:
        """Check if service is registered.

        Args:
            cls: Service class/interface.

        Returns:
            True if registered.
        """
        if cls in self._services or cls in self._singletons:
            return True
        if self._parent:
            return self._parent.has(cls)
        return False

    def create_child(self) -> 'Container':
        """Create child container.

        Child containers inherit parent's services.

        Returns:
            New child container.
        """
        child = Container()
        child._parent = self
        return child


class ServiceLocator:
    """Simple service locator pattern.

    Global service locator for application-wide access.
    """

    _container = Container()

    @classmethod
    def get_container(cls) -> Container:
        """Get global container."""
        return cls._container

    @classmethod
    def register(cls, cls_type: Type[T], factory: Optional[Callable] = None) -> None:
        """Register service globally."""
        cls._container.register(cls_type, factory)

    @classmethod
    def resolve(cls, cls_type: Type[T]) -> T:
        """Resolve service globally."""
        return cls._container.resolve(cls_type)

    @classmethod
    def reset(cls) -> None:
        """Reset global container."""
        cls._container = Container()


def inject(container: Optional[Container] = None) -> Callable[[Type[T]], Type[T]]:
    """Decorator to inject dependencies.

    Args:
        container: Container to use (defaults to global).

    Returns:
        Decorator function.

    Usage:
        @inject()
        class MyService:
            def __init__(self, db: Database):
                self.db = db
    """
    def decorator(cls: Type[T]) -> Type[T]:
        original_init = cls.__init__

        def new_init(self, *args, **kwargs):
            if container is None:
                cont = ServiceLocator.get_container()
            else:
                cont = container

            # Get original init parameters
            import inspect
            sig = inspect.signature(original_init)
            params = list(sig.parameters.values())

            # Skip 'self' parameter
            for param in params[1:]:
                if param.name in kwargs:
                    continue

                try:
                    service = cont.resolve(param.annotation)
                    kwargs[param.name] = service
                except KeyError:
                    pass

            original_init(self, *args, **kwargs)

        cls.__init__ = new_init
        return cls

    return decorator