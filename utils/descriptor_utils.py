"""
Descriptor Utilities

Provides descriptor protocol utilities for
attribute access control in automation.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from weakref import WeakKeyDictionary


class ValidatedDescriptor:
    """
    A descriptor that validates values before setting.
    
    Uses the descriptor protocol to intercept attribute
    access and modification.
    """

    def __init__(
        self,
        validator: Callable[[Any], bool] | None = None,
        default: Any = None,
    ) -> None:
        self._validator = validator
        self._default = default
        self._values: WeakKeyDictionary[Any, Any] = WeakKeyDictionary()

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        return self._values.get(obj, self._default)

    def __set__(self, obj: Any, value: Any) -> None:
        if self._validator and not self._validator(value):
            raise ValueError(f"Invalid value: {value!r}")
        self._values[obj] = value

    def __delete__(self, obj: Any) -> None:
        if obj in self._values:
            del self._values[obj]


class CachedDescriptor:
    """
    A descriptor that caches the computed value.
    
    The value is computed once and then cached
    on the instance.
    """

    def __init__(self, func: Callable[[Any], Any]) -> None:
        self._func = func
        self._cache: WeakKeyDictionary[Any, Any] = WeakKeyDictionary()

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        if obj not in self._cache:
            self._cache[obj] = self._func(obj)
        return self._cache[obj]

    def __set__(self, obj: Any, value: Any) -> None:
        raise AttributeError("Cached values cannot be set directly")

    def __delete__(self, obj: Any) -> None:
        if obj in self._cache:
            del self._cache[obj]

    def invalidate(self, obj: Any) -> None:
        """Invalidate the cache for an object."""
        if obj in self._cache:
            del self._cache[obj]


class ReadOnlyDescriptor:
    """
    A descriptor that allows reading but not writing.
    
    Once set during initialization, values cannot
    be modified.
    """

    def __init__(self, default: Any = None) -> None:
        self._default = default
        self._values: WeakKeyDictionary[Any, Any] = WeakKeyDictionary()

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        return self._values.get(obj, self._default)

    def __set__(self, obj: Any, value: Any) -> None:
        if obj in self._values:
            raise AttributeError("Read-only attribute")
        self._values[obj] = value

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name


def descriptor(
    validator: Callable[[Any], bool] | None = None,
    default: Any = None,
) -> ValidatedDescriptor:
    """
    Create a validated descriptor.
    
    Args:
        validator: Optional validation function.
        default: Default value.
        
    Returns:
        ValidatedDescriptor instance.
    """
    return ValidatedDescriptor(validator=validator, default=default)
