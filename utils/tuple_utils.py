"""Tuple utilities for RabAI AutoClick.

Provides:
- Tuple operations
- Named tuple utilities
- Record class
- Data class to tuple conversion
"""

from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass, fields, astuple, asdict
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
)


T = TypeVar("T")


def make_namedtuple(
    typename: str,
    field_names: List[str],
) -> Type[Tuple]:
    """Create a namedtuple type dynamically.

    Args:
        typename: Name of the resulting class.
        field_names: List of field names.

    Returns:
        Namedtuple class.
    """
    return namedtuple(typename, field_names)


def tuple_to_dict(t: Tuple, names: List[str]) -> Dict[str, Any]:
    """Convert tuple to dictionary using field names.

    Args:
        t: Input tuple.
        names: Field names.

    Returns:
        Dictionary mapping field names to values.
    """
    return dict(zip(names, t))


def dict_to_tuple(d: Dict[str, Any], names: List[str]) -> Tuple[Any, ...]:
    """Convert dictionary to tuple using field names.

    Args:
        d: Input dictionary.
        names: Field names in order.

    Returns:
        Tuple of values.
    """
    return tuple(d.get(name) for name in names)


def dataclass_to_namedtuple(
    cls: Type,
    typename: Optional[str] = None,
) -> Type[Tuple]:
    """Convert a dataclass to a namedtuple.

    Args:
        cls: Dataclass type.
        typename: Optional name for the namedtuple.

    Returns:
        Namedtuple class.
    """
    field_names = [f.name for f in fields(cls)]
    name = typename or cls.__name__
    return make_namedtuple(name, field_names)


def tuple_zip(*tuples: Tuple) -> Tuple[Tuple, ...]:
    """Zip multiple tuples together.

    Args:
        *tuples: Tuples to zip.

    Returns:
        Tuple of tuples.
    """
    return tuple(zip(*tuples))


def tuple_map(func: Callable[[Any], Any], t: Tuple) -> Tuple:
    """Apply function to each element of tuple.

    Args:
        func: Function to apply.
        t: Input tuple.

    Returns:
        New tuple with transformed values.
    """
    return tuple(func(x) for x in t)


def tuple_filter(predicate: Callable[[Any], bool], t: Tuple) -> Tuple:
    """Filter tuple by predicate.

    Args:
        predicate: Filter function.
        t: Input tuple.

    Returns:
        New tuple with filtered values.
    """
    return tuple(x for x in t if predicate(x))


class Record:
    """Lightweight record with dict-like access.

    Example:
        r = Record(name="John", age=30, city="NYC")
        r.name        # "John"
        r["age"]      # 30
        r.keys()      # ["name", "age", "city"]
        r.as_dict()   # {"name": "John", "age": 30, "city": "NYC"}
    """

    def __init__(self, **kwargs: Any) -> None:
        self._data: Dict[str, Any] = kwargs
        for k, v in kwargs.items():
            object.__setattr__(self, k, v)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value
        object.__setattr__(self, key, value)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"Record({self._data})"

    def keys(self) -> List[str]:
        return list(self._data.keys())

    def values(self) -> List[Any]:
        return list(self._data.values())

    def items(self) -> List[Tuple[str, Any]]:
        return list(self._data.items())

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._data)

    def as_tuple(self) -> Tuple[Any, ...]:
        return tuple(self._data.values())

    def update(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            self[k] = v


def merge_tuples(*tuples: Tuple) -> Tuple:
    """Merge multiple tuples.

    Args:
        *tuples: Tuples to merge.

    Returns:
        Merged tuple.
    """
    result = ()
    for t in tuples:
        result += t
    return result


def chunk_tuple(t: Tuple, size: int) -> List[Tuple]:
    """Split tuple into chunks.

    Args:
        t: Input tuple.
        size: Chunk size.

    Returns:
        List of tuples.
    """
    return [t[i:i + size] for i in range(0, len(t), size)]


def flatten_tuple(nested: Tuple[Tuple, ...]) -> Tuple:
    """Flatten nested tuple.

    Args:
        nested: Nested tuple.

    Returns:
        Flattened tuple.
    """
    result = ()
    for item in nested:
        if isinstance(item, tuple):
            result += flatten_tuple(item)
        else:
            result += (item,)
    return result


def transpose_tuple(tuples: List[Tuple]) -> List[Tuple]:
    """Transpose list of tuples.

    Args:
        tuples: List of tuples of equal length.

    Returns:
        List of tuples with rows and columns swapped.
    """
    if not tuples:
        return []
    return list(zip(*tuples))
