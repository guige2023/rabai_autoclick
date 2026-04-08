"""
Class Utilities

Provides utilities for working with classes,
including mixins, decorators, and introspection.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, TypeVar, get_type_hints
from functools import wraps
import inspect

T = TypeVar("T")


def mixin(base_class: type[T]) -> Callable[[type[T]], type[T]]:
    """
    Decorator to apply a class as a mixin.
    
    Args:
        base_class: Base class to use as mixin.
        
    Returns:
        Class decorator function.
    """
    def decorator(mixin_class: type[T]) -> type[T]:
        if not issubclass(mixin_class, base_class):
            raise TypeError(
                f"{mixin_class.__name__} must inherit from {base_class.__name__}"
            )
        return mixin_class
    return decorator


def auto_repr(cls: type[T]) -> type[T]:
    """
    Class decorator to automatically generate __repr__.
    
    Args:
        cls: Class to decorate.
        
    Returns:
        Decorated class with __repr__.
    """
    def repr_func(self) -> str:
        attrs = ", ".join(
            f"{k}={v!r}"
            for k, v in vars(self).items()
            if not k.startswith("_")
        )
        return f"{self.__class__.__name__}({attrs})"
    cls.__repr__ = repr_func
    return cls


def auto_eq(cls: type[T]) -> type[T]:
    """
    Class decorator to automatically generate __eq__.
    
    Args:
        cls: Class to decorate.
        
    Returns:
        Decorated class with __eq__.
    """
    def eq_func(self: Any, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return vars(self) == vars(other)
    cls.__eq__ = eq_func
    return cls


def frozen(cls: type[T]) -> type[T]:
    """
    Class decorator to make instances immutable.
    
    Args:
        cls: Class to freeze.
        
    Returns:
        Frozen class.
    """
    def setattr_handler(name: str, value: Any) -> None:
        raise AttributeError(f"Cannot modify frozen class: {name}")
    cls.__setattr__ = setattr_handler  # type: ignore
    return cls


def memoized_property(func: Callable[[Any], T]) -> property:
    """
    Decorator to memoize a property.
    
    Args:
        func: Property getter function.
        
    Returns:
        Memoized property descriptor.
    """
    cache_name = f"_memo_{func.__name__}"

    @property
    def wrapper(self: Any) -> T:
        if not hasattr(self, cache_name):
            setattr(self, cache_name, func(self))
        return getattr(self, cache_name)

    return wrapper


def get_class_members(cls: type) -> dict[str, Any]:
    """
    Get all members of a class including inherited.
    
    Args:
        cls: Class to inspect.
        
    Returns:
        Dictionary of member name to member.
    """
    members = {}
    for klass in reversed(inspect.getmro(cls)):
        for name, member in vars(klass).items():
            if name not in members:
                members[name] = member
    return members
