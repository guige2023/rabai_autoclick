"""Struct utilities for RabAI AutoClick.

Provides:
- Structured data helpers
- Named tuple utilities
- Record/row utilities
- Schema validation helpers
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
)


T = TypeVar("T")


def make_namedtuple(
    name: str,
    fields: List[str],
    defaults: Optional[Dict[str, Any]] = None,
) -> Type[NamedTuple]:
    """Create a namedtuple class dynamically.

    Args:
        name: Name of the namedtuple.
        fields: List of field names.
        defaults: Optional dict of field defaults.

    Returns:
        Namedtuple class.
    """
    defaults = defaults or {}
    field_defaults: List[Any] = []
    for f in fields:
        field_defaults.append(defaults.get(f))

    return NamedTuple(  # type: ignore
        name,
        [(f, Any) for f in fields],
    )


def namedtuple_to_dict(nt: NamedTuple) -> Dict[str, Any]:
    """Convert a namedtuple to a dictionary.

    Args:
        nt: Namedtuple instance.

    Returns:
        Dict representation.
    """
    return dict(nt._asdict())


def namedtuple_from_dict(
    nt_class: Type[NamedTuple],
    data: Dict[str, Any],
) -> NamedTuple:
    """Create a namedtuple from a dictionary.

    Args:
        nt_class: Namedtuple class.
        data: Dict with field values.

    Returns:
        Namedtuple instance.
    """
    return nt_class(**{
        k: v for k, v in data.items()
        if k in nt_class._fields
    })


def dict_to_row(
    data: Dict[str, Any],
    keys: List[str],
) -> Tuple[Any, ...]:
    """Convert a dict to a tuple ordered by keys.

    Args:
        data: Source dict.
        keys: Ordered list of keys.

    Returns:
        Tuple of values in key order.
    """
    return tuple(data.get(k) for k in keys)


def row_to_dict(
    row: Tuple[Any, ...],
    keys: List[str],
) -> Dict[str, Any]:
    """Convert a tuple to a dict using keys.

    Args:
        row: Source tuple.
        keys: Ordered list of keys.

    Returns:
        Dict mapping keys to tuple values.
    """
    return dict(zip(keys, row))


def merge_rows(
    *rows: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge multiple dicts (rows) into one.

    Args:
        *rows: Dicts to merge.

    Returns:
        Merged dict.
    """
    result: Dict[str, Any] = {}
    for row in rows:
        result.update(row)
    return result


def select_fields(
    data: Dict[str, Any],
    fields: List[str],
) -> Dict[str, Any]:
    """Select specific fields from a dict.

    Args:
        data: Source dict.
        fields: Fields to select.

    Returns:
        Dict with only selected fields.
    """
    return {k: data[k] for k in fields if k in data}


def exclude_fields(
    data: Dict[str, Any],
    fields: List[str],
) -> Dict[str, Any]:
    """Exclude specific fields from a dict.

    Args:
        data: Source dict.
        fields: Fields to exclude.

    Returns:
        Dict without excluded fields.
    """
    return {k: v for k, v in data.items() if k not in fields}


def validate_fields(
    data: Dict[str, Any],
    required: List[str],
) -> List[str]:
    """Validate that required fields are present.

    Args:
        data: Dict to validate.
        required: List of required field names.

    Returns:
        List of missing field names (empty if all present).
    """
    return [f for f in required if f not in data]


__all__ = [
    "make_namedtuple",
    "namedtuple_to_dict",
    "namedtuple_from_dict",
    "dict_to_row",
    "row_to_dict",
    "merge_rows",
    "select_fields",
    "exclude_fields",
    "validate_fields",
]
