"""Copy utilities v3 - specialized copying patterns.

Specialized copy utilities for data classes,
 nested structures, and immutable types.
"""

from __future__ import annotations

import copy
from dataclasses import is_dataclass, fields
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "copy_dataclass",
    "copy_namedtuple",
    "copy_dataclass_deep",
    "copy_with_transform",
    "copy_frozen",
    "copy_immutable",
    "Immutable",
    "Frozen",
]


T = TypeVar("T")


def copy_dataclass(obj: Any) -> Any:
    """Create shallow copy of dataclass.

    Args:
        obj: Dataclass instance.

    Returns:
        New dataclass instance.
    """
    if not is_dataclass(obj):
        raise TypeError(f"{obj} is not a dataclass")
    return obj.__class__(*(getattr(obj, f.name) for f in fields(obj)))


def copy_namedtuple(obj: Any) -> Any:
    """Create copy of namedtuple.

    Args:
        obj: Namedtuple instance.

    Returns:
        New namedtuple instance.
    """
    return obj.__class__(*obj)


def copy_dataclass_deep(obj: Any) -> Any:
    """Create deep copy of dataclass.

    Args:
        obj: Dataclass instance.

    Returns:
        New deep copied instance.
    """
    if not is_dataclass(obj):
        raise TypeError(f"{obj} is not a dataclass")
    return copy.deepcopy(obj)


def copy_with_transform(obj: Any, **transforms: Callable[[Any], Any]) -> Any:
    """Copy with field transformations.

    Args:
        obj: Object to copy.
        **transforms: Field name to transform function.

    Returns:
        New object with transformed fields.
    """
    if is_dataclass(obj):
        kwargs = {f.name: transforms.get(f.name, lambda x: x)(getattr(obj, f.name)) for f in fields(obj)}
        return obj.__class__(**kwargs)
    return copy.copy(obj)


def copy_frozen(obj: T) -> T:
    """Return frozen (immutable) copy.

    Args:
        obj: Object to freeze.

    Returns:
        Same object (already immutable).
    """
    return obj


def copy_immutable(obj: T) -> T:
    """Return immutable copy.

    Args:
        obj: Object to copy.

    Returns:
        Same object if immutable.
    """
    return obj


class Immutable(Generic[T]):
    """Wrapper for immutable objects."""

    def __init__(self, value: T) -> None:
        self._value = value

    @property
    def value(self) -> T:
        """Get immutable value."""
        return self._value


class Frozen:
    """Base class for frozen (immutable) objects."""

    _frozen: bool = True

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError(f"Cannot set attribute {name} on frozen object")
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError(f"Cannot delete attribute {name}")
        object.__delattr__(self, name)
