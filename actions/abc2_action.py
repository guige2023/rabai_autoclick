"""ABC utilities v2 - abstract base class patterns.

Extended ABC utilities including registration,
 factory patterns, and plugin systems.
"""

from __future__ import annotations

from abc import ABC, abstractmethod, get_cache_token
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "AbstractBase",
    "register_impl",
    "create_factory",
    "PluginRegistry",
    "Mixin",
    "abstractmethod_async",
    "abstract_property",
    "final",
    "ABCRegistry",
    "PluginManager",
    "HookMixin",
    "ObserverMixin",
]


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class AbstractBase(ABC):
    """Base ABC with common patterns."""

    @abstractmethod
    def execute(self) -> Any:
        """Execute the implementation."""
        pass

    def validate(self) -> bool:
        """Validate implementation."""
        return True


def register_impl(abc_class: type, name: str | None = None):
    """Decorator to register implementation.

    Args:
        abc_class: ABC to register with.
        name: Optional registration name.

    Returns:
        Decorator function.
    """
    def decorator(cls: type) -> type:
        reg_name = name or cls.__name__
        if not hasattr(abc_class, "_registry"):
            setattr(abc_class, "_registry", {})
        getattr(abc_class, "_registry")[reg_name] = cls
        return cls
    return decorator


def create_factory(base_class: type[T]) -> Callable[[str], type[T]]:
    """Create factory function for ABC.

    Args:
        base_class: ABC with registered implementations.

    Returns:
        Factory function.
    """
    def factory(name: str) -> type[T]:
        registry = getattr(base_class, "_registry", {})
        if name not in registry:
            available = list(registry.keys())
            raise ValueError(f"Unknown implementation: {name}. Available: {available}")
        return registry[name]
    return factory


class PluginRegistry(Generic[T]):
    """Registry for plugins with activation control."""

    def __init__(self) -> None:
        self._plugins: dict[str, type[T]] = {}
        self._active: set[str] = set()
        self._instances: dict[str, T] = {}

    def register(self, name: str, plugin_cls: type[T]) -> None:
        """Register a plugin class.

        Args:
            name: Plugin name.
            plugin_cls: Plugin class.

        Raises:
            ValueError: If name already registered.
        """
        if name in self._plugins:
            raise ValueError(f"Plugin {name} already registered")
        self._plugins[name] = plugin_cls

    def unregister(self, name: str) -> bool:
        """Unregister a plugin.

        Args:
            name: Plugin name.

        Returns:
            True if was registered.
        """
        if name in self._active:
            self.deactivate(name)
        if name in self._plugins:
            del self._plugins[name]
            return True
        return False

    def activate(self, name: str) -> T:
        """Activate plugin and return instance.

        Args:
            name: Plugin name.

        Returns:
            Plugin instance.
        """
        if name not in self._plugins:
            raise KeyError(f"Plugin {name} not registered")
        if name not in self._active:
            self._instances[name] = self._plugins[name]()
            self._active.add(name)
        return self._instances[name]

    def deactivate(self, name: str) -> None:
        """Deactivate plugin.

        Args:
            name: Plugin name.
        """
        if name in self._active:
            instance = self._instances.get(name)
            if instance and hasattr(instance, "shutdown"):
                instance.shutdown()
            self._active.discard(name)
            if name in self._instances:
                del self._instances[name]

    def get(self, name: str) -> T | None:
        """Get active plugin instance.

        Args:
            name: Plugin name.

        Returns:
            Instance or None.
        """
        return self._instances.get(name)

    def list_plugins(self) -> list[str]:
        """List registered plugin names."""
        return list(self._plugins.keys())

    def list_active(self) -> list[str]:
        """List active plugin names."""
        return list(self._active)


class Mixin:
    """Mixin base class."""

    def __init__(self) -> None:
        self._mixin_data: dict[str, Any] = {}


def abstractmethod_async(func: Callable) -> Callable:
    """Mark async method as abstract.

    Args:
        func: Async function.

    Returns:
        Wrapped function.
    """
    func.__isabstractmethod__ = True
    return func


def abstract_property(func: Callable) -> property:
    """Create abstract read-only property.

    Args:
        func: Getter function.

    Returns:
        Abstract property.
    """
    assert getattr(func, "__isabstractmethod__", False)
    return property(func)


def final(func: Callable) -> Callable:
    """Mark method as final (cannot be overridden).

    Args:
        func: Method to mark.

    Returns:
        Unchanged function.
    """
    func.__final__ = True
    return func


class ABCRegistry(ABC, Generic[T]):
    """ABC with built-in registry."""

    _registry: dict[str, type[T]] = {}

    @classmethod
    def register(cls, name: str | None = None) -> Callable[[type[T]], type[T]]:
        """Register implementation decorator.

        Args:
            name: Optional name.

        Returns:
            Decorator.
        """
        def decorator(impl_cls: type[T]) -> type[T]:
            reg_name = name or impl_cls.__name__
            if not hasattr(cls, "_registry"):
                cls._registry = {}
            cls._registry[reg_name] = impl_cls
            return impl_cls
        return decorator

    @classmethod
    def create(cls, name: str) -> T:
        """Create instance by name.

        Args:
            name: Registered name.

        Returns:
            Instance.

        Raises:
            ValueError: If name not registered.
        """
        if not hasattr(cls, "_registry") or name not in cls._registry:
            raise ValueError(f"Unknown: {name}. Available: {list(getattr(cls, '_registry', {}).keys())}")
        return cls._registry[name]()

    @classmethod
    def available(cls) -> list[str]:
        """List available implementations."""
        return list(getattr(cls, "_registry", {}).keys())


class PluginManager(ABCRegistry[T]):
    """Plugin manager with lifecycle hooks."""

    def __init__(self) -> None:
        self._instances: dict[str, T] = {}
        self._order: list[str] = []

    def register(self, name: str | None = None) -> Callable[[type[T]], type[T]]:
        """Register a plugin.

        Args:
            name: Optional name.

        Returns:
            Decorator.
        """
        def decorator(cls: type[T]) -> type[T]:
            reg_name = name or cls.__name__
            if not hasattr(self, "_registry"):
                self._registry = {}
            self._registry[reg_name] = cls
            return cls
        return decorator

    def initialize_all(self) -> None:
        """Initialize all registered plugins in order."""
        if not hasattr(self, "_registry"):
            return
        for name in self._order:
            if name in self._registry:
                self._instances[name] = self._registry[name]()
                if hasattr(self._instances[name], "initialize"):
                    self._instances[name].initialize()

    def shutdown_all(self) -> None:
        """Shutdown all active plugins."""
        for name in reversed(self._order):
            if name in self._instances:
                instance = self._instances[name]
                if hasattr(instance, "shutdown"):
                    instance.shutdown()
                del self._instances[name]

    def get(self, name: str) -> T | None:
        """Get plugin instance."""
        return self._instances.get(name)


class HookMixin:
    """Mixin providing hook/callback system."""

    def __init__(self) -> None:
        self._hooks: dict[str, list[Callable]] = {}

    def register_hook(self, name: str, callback: Callable) -> None:
        """Register a hook callback.

        Args:
            name: Hook name.
            callback: Function to call.
        """
        if name not in self._hooks:
            self._hooks[name] = []
        self._hooks[name].append(callback)

    def unregister_hook(self, name: str, callback: Callable) -> bool:
        """Unregister a hook.

        Args:
            name: Hook name.
            callback: Callback to remove.

        Returns:
            True if was registered.
        """
        if name in self._hooks and callback in self._hooks[name]:
            self._hooks[name].remove(callback)
            return True
        return False

    def trigger_hook(self, name: str, *args, **kwargs) -> list[Any]:
        """Trigger all callbacks for hook.

        Args:
            name: Hook name.
            *args: Positional args.
            **kwargs: Keyword args.

        Returns:
            List of callback results.
        """
        results = []
        for callback in self._hooks.get(name, []):
            try:
                results.append(callback(*args, **kwargs))
            except Exception as e:
                results.append(e)
        return results


class ObserverMixin:
    """Mixin for observer pattern."""

    def __init__(self) -> None:
        self._observers: list[Any] = []

    def add_observer(self, observer: Any) -> None:
        """Add observer.

        Args:
            observer: Object with update method.
        """
        if observer not in self._observers:
            self._observers.append(observer)

    def remove_observer(self, observer: Any) -> bool:
        """Remove observer.

        Args:
            observer: Observer to remove.

        Returns:
            True if was present.
        """
        if observer in self._observers:
            self._observers.remove(observer)
            return True
        return False

    def notify_observers(self, event: str, *args, **kwargs) -> None:
        """Notify all observers of event.

        Args:
            event: Event name.
            *args: Positional args.
            **kwargs: Keyword args.
        """
        for observer in list(self._observers):
            if hasattr(observer, "update"):
                observer.update(event, *args, **kwargs)
            elif hasattr(observer, "__call__"):
                observer(event, *args, **kwargs)
