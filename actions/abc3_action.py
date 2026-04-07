"""ABC utilities v3 - plugin and component architecture.

ABC utilities for plugin systems,
 component registration, and lifecycle management.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "Component",
    "ComponentRegistry",
    "Plugin",
    "PluginManager",
    "Service",
    "ServiceLocator",
    "Factory",
    "FactoryRegistry",
]


T = TypeVar("T")


class Component(ABC):
    """Base component class."""

    @abstractmethod
    def initialize(self) -> None:
        """Initialize component."""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown component."""
        pass


class ComponentRegistry(Generic[T]):
    """Registry for components."""

    def __init__(self) -> None:
        self._components: dict[str, T] = {}

    def register(self, name: str, component: T) -> None:
        """Register a component."""
        self._components[name] = component

    def get(self, name: str) -> T | None:
        """Get component by name."""
        return self._components.get(name)

    def list_names(self) -> list[str]:
        """List registered names."""
        return list(self._components.keys())


class Plugin(ABC):
    """Base plugin class."""

    @abstractmethod
    def enable(self) -> None:
        """Enable plugin."""
        pass

    @abstractmethod
    def disable(self) -> None:
        """Disable plugin."""
        pass

    @abstractmethod
    def get_info(self) -> dict:
        """Get plugin info."""
        return {}


class PluginManager:
    """Manage plugins with lifecycle."""

    def __init__(self) -> None:
        self._plugins: dict[str, Plugin] = {}

    def register(self, name: str, plugin: Plugin) -> None:
        """Register plugin."""
        self._plugins[name] = plugin

    def enable_all(self) -> None:
        """Enable all plugins."""
        for p in self._plugins.values():
            p.enable()

    def disable_all(self) -> None:
        """Disable all plugins."""
        for p in self._plugins.values():
            p.disable()


class Service(ABC, Generic[T]):
    """Base service class."""

    @abstractmethod
    def start(self) -> None:
        """Start service."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop service."""
        pass


class ServiceLocator(Generic[T]):
    """Service locator pattern."""

    def __init__(self) -> None:
        self._services: dict[type, T] = {}

    def register(self, service_type: type[T], instance: T) -> None:
        """Register service."""
        self._services[service_type] = instance

    def get(self, service_type: type[T]) -> T | None:
        """Get service by type."""
        return self._services.get(service_type)


class Factory(ABC, Generic[T]):
    """Base factory class."""

    @abstractmethod
    def create(self, *args: Any, **kwargs: Any) -> T:
        """Create instance."""
        pass


class FactoryRegistry(Generic[T]):
    """Registry for factories."""

    def __init__(self) -> None:
        self._factories: dict[str, Factory[T]] = {}

    def register(self, name: str, factory: Factory[T]) -> None:
        """Register factory."""
        self._factories[name] = factory

    def create(self, name: str, *args: Any, **kwargs: Any) -> T | None:
        """Create using factory."""
        factory = self._factories.get(name)
        if factory is None:
            return None
        return factory.create(*args, **kwargs)
