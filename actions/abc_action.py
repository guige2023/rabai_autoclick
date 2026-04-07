"""abc action extensions for rabai_autoclick.

Provides abstract base class utilities, mixins, and metaclasses
for creating well-structured class hierarchies.
"""

from __future__ import annotations

import inspect
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Callable, TypeVar, Generic

__all__ = [
    "abstractmethod",
    "ABCMeta",
    "ABC",
    "AbstractBase",
    "Interface",
    "Mixin",
    "Final",
    "final",
    "require_methods",
    "check_implementation",
    "validate_abstract",
    "ProxyBase",
    "WrapperBase",
    "NullObject",
    "NotImplementedMixin",
    "ReprMixin",
    "EqMixin",
    "HashMixin",
    "CopyMixin",
    "RichMixin",
    "AutoRegister",
    "PluginBase",
    "HookMixin",
]


T = TypeVar("T")
C = TypeVar("C", bound=type)


class AbstractBase(ABC):
    """Base class for abstract hierarchies with common utilities."""

    @classmethod
    def subclasses(cls, recursive: bool = True) -> list[type]:
        """Get all subclasses of this class.

        Args:
            recursive: Include nested subclasses.

        Returns:
            List of subclass types.
        """
        result = list(cls.__subclasses__())
        if recursive:
            for sub in result[:]:
                result.extend(sub.subclasses(recursive=True))
        return result

    @classmethod
    def is_abstract(cls) -> bool:
        """Check if class has abstract methods.

        Returns:
            True if class has unimplemented abstract methods.
        """
        return bool(cls.__abstractmethods__)

    @classmethod
    def validate_instantiation(cls) -> None:
        """Validate that class can be instantiated.

        Raises:
            TypeError: If class is abstract.
        """
        if cls.is_abstract():
            unimpl = list(cls.__abstractmethods__)
            raise TypeError(
                f"Cannot instantiate abstract class {cls.__name__} "
                f"with abstract methods: {', '.join(unimpl)}"
            )


def final(cls_or_method: C | Callable) -> C | Callable:
    """Decorator to mark a class or method as final (cannot be overridden).

    Args:
        cls_or_method: Class or method to mark as final.

    Returns:
        Unmodified class or method (marker applied).
    """
    if inspect.isclass(cls_or_method):
        cls_or_method.__final__ = True  # type: ignore
        return cls_or_method
    else:
        func = cls_or_method
        func.__final__ = True  # type: ignore
        return func


def require_methods(*method_names: str) -> Callable[[type], type]:
    """Class decorator to require specific methods be implemented.

    Args:
        *method_names: Names of required methods.

    Returns:
        Class decorator.

    Example:
        @require_methods("process", "validate")
        class Processor:
            pass
    """
    def decorator(cls: type) -> type:
        for name in method_names:
            if not hasattr(cls, name):
                raise TypeError(
                    f"Class {cls.__name__} must implement method '{name}' "
                    f"required by {getattr(cls, '__qualname__', cls)}"
                )
            if getattr(getattr(cls, name), "__isabstractmethod__", False):
                continue
            attr = getattr(cls, name, None)
            if callable(attr) and getattr(attr, "__isabstractmethod__", False):
                continue
        cls.__required_methods__ = method_names  # type: ignore
        return cls
    return decorator


def check_implementation(cls: type, interface: type) -> list[str]:
    """Check if a class implements all methods of an interface.

    Args:
        cls: Class to check.
        interface: Interface class with abstract methods.

    Returns:
        List of missing method names (empty if all implemented).
    """
    if not hasattr(interface, "__abstractmethods__"):
        return []

    missing = []
    for method in interface.__abstractmethods__:
        attr = getattr(cls, method, None)
        if attr is None or getattr(attr, "__isabstractmethod__", False):
            missing.append(method)
    return missing


def validate_abstract(obj: Any) -> None:
    """Validate that an object is not abstract.

    Args:
        obj: Instance or class to validate.

    Raises:
        TypeError: If object is abstract.
    """
    if inspect.isclass(obj):
        if obj.is_abstract():  # type: ignore
            raise TypeError(f"Cannot use abstract class {obj.__name__}")
    elif hasattr(obj, "is_abstract"):
        if obj.is_abstract():  # type: ignore
            raise TypeError(f"Cannot use abstract instance of {type(obj).__name__}")


class Interface(ABC):
    """Base class for interface definitions.

    All methods are abstract by default.
    Subclasses should not provide implementations.

    Example:
        class Drawable(Interface):
            def draw(self, canvas): ...

        class Shape(Drawable):
            def draw(self, canvas):
                ...
    """

    _instance_check_exempt_ = False


class Mixin:
    """Base class for mixins that require multiple inheritance.

    Mixins add functionality to a class through multiple inheritance.
    They should be designed to work with other mixins.

    Example:
        class SerializableMixin:
            def to_json(self):
                return json.dumps(self.__dict__)

        class User(SerializableMixin, BaseModel):
            pass
    """

    _mixin_priority_: int = 100

    @classmethod
    def get_mixin_priority(cls) -> int:
        """Get priority for mixin ordering (lower = earlier).

        Returns:
            Priority value.
        """
        return getattr(cls, "_mixin_priority_", 100)


class ProxyBase(Generic[T]):
    """Base class for proxy objects that delegate to a wrapped object.

    Subclasses override _get_target to provide the wrapped object.

    Example:
        class LazyProxy(ProxyBase[SomeClass]):
            def _get_target(self):
                return self._load_expensive_object()
    """

    _target: T | None = None

    def _get_target(self) -> T:
        """Get the wrapped target object.

        Returns:
            Wrapped target instance.
        """
        if self._target is None:
            raise RuntimeError(f"{self.__class__.__name__}: target not set")
        return self._target

    def _set_target(self, target: T) -> None:
        """Set the wrapped target object.

        Args:
            target: Object to wrap.
        """
        self._target = target

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get_target(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_target":
            super().__setattr__(name, value)
        else:
            setattr(self._get_target(), name, value)


class WrapperBase(Generic[T]):
    """Base class for wrapper objects that add functionality.

    Unlike ProxyBase, WrapperBase wraps an object passed in constructor.

    Example:
        class CachedResult(WrapperBase[Result]):
            def __init__(self, result: Result):
                super().__init__(result)
                self._cache = None

            @property
            def value(self):
                if self._cache is None:
                    self._cache = expensive_computation()
                return self._cache
    """

    def __init__(self, wrapped: T) -> None:
        self._wrapped = wrapped

    @property
    def wrapped(self) -> T:
        """Get the wrapped object.

        Returns:
            Wrapped instance.
        """
        return self._wrapped

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._wrapped!r})"


class NullObject:
    """Base class for null/no-op implementations.

    NullObjects respond to any method call with self or a sensible default,
    allowing polymorphic code to work without null checks.

    Example:
        class NullLogger(NullObject):
            def log(self, msg):
                pass

        logger = NullLogger()
        logger.log("debug")  # does nothing
    """

    _null_response_: Any = None

    def __getattr__(self, name: str) -> Any:
        return self._null_method

    def _null_method(self, *args: Any, **kwargs: Any) -> Any:
        """Null method that does nothing and returns default."""
        return self._null_response_

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}()>"


class NotImplementedMixin:
    """Mixin that raises NotImplementedError for unexpected calls.

    Useful for marking methods that must be called on a subclass
    but shouldn't use the ABC machinery.
    """

    def __getattr__(self, name: str) -> Any:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{name} is not implemented"
        )


class ReprMixin:
    """Mixin that provides a useful __repr__ based on init parameters."""

    def __repr__(self) -> str:
        try:
            sig = inspect.signature(self.__class__.__init__)
            params = []
            for name, param in sig.parameters.items():
                if name == "self":
                    continue
                value = getattr(self, name, getattr(param, "default", None))
                params.append(f"{name}={value!r}")
            return f"{self.__class__.__name__}({', '.join(params)})"
        except (ValueError, TypeError):
            return f"<{self.__class__.__name__}>"


class EqMixin:
    """Mixin for classes with value-based equality."""

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._eq_attrs() == other._eq_attrs()

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def _eq_attrs(self) -> tuple:
        """Return tuple of attributes to compare for equality.

        Returns:
            Tuple of attribute values.
        """
        return tuple(getattr(self, name, None) for name in self._eq_keys())  # type: ignore

    @classmethod
    def _eq_keys(cls) -> tuple[str, ...]:
        """Return attribute names to use for equality.

        Returns:
            Tuple of attribute names.
        """
        return ()


class HashMixin:
    """Mixin that provides __hash__ based on _eq_attrs."""

    def __hash__(self) -> int:
        return hash(self._eq_attrs())


class CopyMixin:
    """Mixin that provides copy functionality."""

    def copy(self) -> Any:
        """Create a shallow copy.

        Returns:
            Copy of this instance.
        """
        cls = self.__class__
        result = object.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def deepcopy(self) -> Any:
        """Create a deep copy.

        Returns:
            Deep copy of this instance.
        """
        import copy
        return copy.deepcopy(self)


class RichMixin:
    """Mixin providing rich comparison operators.

    Implement _compare_attrs to return (attr, multiplier) tuples
    where multiplier is 1 for normal, -1 for reversed ordering.
    """

    def _compare_attrs(self) -> list[tuple[str, int]]:
        """Return list of (attr_name, direction) for comparisons.

        Returns:
            List of (attribute, direction) pairs.
        """
        return []

    def __lt__(self, other: Any) -> bool:
        return self._compare(other) < 0

    def __le__(self, other: Any) -> bool:
        return self._compare(other) <= 0

    def __gt__(self, other: Any) -> bool:
        return self._compare(other) > 0

    def __ge__(self, other: Any) -> bool:
        return self._compare(other) >= 0

    def _compare(self, other: Any) -> int:
        if not isinstance(other, self.__class__):
            return NotImplemented
        self_attrs = self._compare_attrs()
        other_attrs = other._compare_attrs()
        for (name, mult) in self_attrs:
            a = getattr(self, name, None)
            b = getattr(other, name, None)
            if a < b:
                return -1 * mult
            if a > b:
                return 1 * mult
        return 0


class AutoRegister(ABC):
    """Mixin that auto-registers subclasses in a registry.

    Subclasses are automatically added to the registry on creation.

    Example:
        class Plugin(AutoRegister):
            pass

        @Plugin.register
        class MyPlugin(Plugin):
            pass
    """

    _registry_: dict[str, type] = {}
    _registry_key_: str = "name"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        key = getattr(cls, cls._registry_key_, None)
        if key is not None:
            cls._registry_[key] = cls

    @classmethod
    def get(cls, key: str) -> type | None:
        """Get a registered class by key.

        Args:
            key: Registry key.

        Returns:
            Registered class or None.
        """
        return cls._registry_.get(key)

    @classmethod
    def registered(cls) -> list[type]:
        """Get all registered classes.

        Returns:
            List of registered class types.
        """
        return list(cls._registry_.values())


class PluginBase(AutoRegister):
    """Base class for plugin systems.

    Plugins register themselves using the @register class method decorator.

    Example:
        class MyPlugin(PluginBase):
            _registry_key_ = "plugin_id"

            def execute(self):
                pass
    """

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the plugin functionality."""
        ...

    @classmethod
    def register(cls, name: str | None = None) -> type:
        """Class decorator to register a plugin.

        Args:
            name: Optional registry key override.

        Returns:
            Class decorator.
        """
        key = name or cls._registry_key_
        if key != cls._registry_key_:
            cls._registry_[name] = cls  # type: ignore
        return cls


class HookMixin:
    """Mixin that supports hooks/callbacks at specific points.

    Subclasses call self._run_hooks(hook_name, *args) to trigger hooks.

    Example:
        class EventEmitter(HookMixin):
            def __init__(self):
                self._hooks = {}
                self.register_hook("complete", self.on_complete)

            def complete(self):
                return self._run_hooks("complete")
    """

    def __init__(self) -> None:
        self._hooks: dict[str, list[Callable]] = {}

    def register_hook(self, name: str, callback: Callable) -> None:
        """Register a hook callback.

        Args:
            name: Hook name.
            callback: Function to call when hook fires.
        """
        if name not in self._hooks:
            self._hooks[name] = []
        self._hooks[name].append(callback)

    def unregister_hook(self, name: str, callback: Callable) -> None:
        """Unregister a hook callback.

        Args:
            name: Hook name.
            callback: Callback to remove.
        """
        if name in self._hooks:
            self._hooks[name].remove(callback)

    def _run_hooks(self, name: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Run all registered hooks for a name.

        Args:
            name: Hook name.
            *args: Positional args to pass to hooks.
            **kwargs: Keyword args to pass to hooks.

        Returns:
            List of hook return values.
        """
        results = []
        for callback in self._hooks.get(name, []):
            try:
                results.append(callback(*args, **kwargs))
            except Exception:
                pass
        return results
