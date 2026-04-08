"""Structure and data class utilities.

Provides utilities for working with data classes, records,
and structured data in automation workflows.
"""

import copy
from dataclasses import dataclass, fields, is_dataclass, asdict, astuple
from typing import Any, Callable, Dict, List, Optional, TypeVar


T = TypeVar("T")


@dataclass
class Record:
    """Base class for records with utility methods."""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        if is_dataclass(self):
            return asdict(self)
        return vars(self)

    def to_tuple(self) -> tuple:
        """Convert record to tuple."""
        if is_dataclass(self):
            return astuple(self)
        return tuple(vars(self).values())

    @classmethod
    def from_dict(cls: type, d: Dict[str, Any]) -> Any:
        """Create record from dictionary.

        Args:
            d: Dictionary with field values.

        Returns:
            New record instance.
        """
        if is_dataclass(cls):
            return cls(**d)
        obj = cls()
        for k, v in d.items():
            setattr(obj, k, v)
        return obj

    def copy(self) -> Any:
        """Create a shallow copy."""
        return copy.copy(self)

    def deep_copy(self) -> Any:
        """Create a deep copy."""
        return copy.deepcopy(self)


def get_fields(cls: type) -> List[str]:
    """Get field names from a dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        List of field names.
    """
    if is_dataclass(cls):
        return [f.name for f in fields(cls)]
    return list(vars(cls))


def get_field_types(cls: type) -> Dict[str, type]:
    """Get field types from a dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        Dict mapping field names to types.
    """
    if is_dataclass(cls):
        return {f.name: f.type for f in fields(cls)}
    return {}


def apply_defaults(obj: Any) -> None:
    """Fill in default values for missing fields.

    Args:
        obj: Object with dataclass fields.
    """
    if is_dataclass(type(obj)):
        for field_def in fields(obj):
            if not hasattr(obj, field_def.name):
                if field_def.default is not None:
                    setattr(obj, field_def.name, field_def.default)
                elif field_def.default_factory is not None:
                    setattr(obj, field_def.name, field_def.default_factory())


def pick_fields(obj: Any, field_names: List[str]) -> Dict[str, Any]:
    """Pick specific fields from an object.

    Args:
        obj: Object to pick from.
        field_names: Fields to pick.

    Returns:
        Dict with selected fields.
    """
    result = {}
    for name in field_names:
        if hasattr(obj, name):
            result[name] = getattr(obj, name)
    return result


def omit_fields(obj: Any, field_names: List[str]) -> Dict[str, Any]:
    """Omit specific fields from an object.

    Args:
        obj: Object to omit from.
        field_names: Fields to omit.

    Returns:
        Dict without specified fields.
    """
    result = {}
    for name in get_fields(type(obj)):
        if name not in field_names:
            result[name] = getattr(obj, name)
    return result


def merge_records(
    base: T,
    *overlays: Dict[str, Any],
    copy_result: bool = True,
) -> T:
    """Merge overlays into base record.

    Args:
        base: Base record.
        *overlays: Dictionaries to merge.
        copy_result: Create a copy of base before merging.

    Returns:
        Merged record.
    """
    result = base.copy() if copy_result else base
    for overlay in overlays:
        for key, value in overlay.items():
            if hasattr(result, key):
                setattr(result, key, value)
    return result


def filter_record(
    obj: Any,
    predicate: Callable[[str, Any], bool],
) -> Dict[str, Any]:
    """Filter record fields by predicate.

    Args:
        obj: Object to filter.
        predicate: Function(field_name, value) -> bool.

    Returns:
        Dict with filtered fields.
    """
    result = {}
    for name in get_fields(type(obj)):
        value = getattr(obj, name)
        if predicate(name, value):
            result[name] = value
    return result


def transform_record(
    obj: Any,
    transformer: Callable[[str, Any], Any],
) -> Dict[str, Any]:
    """Transform record fields.

    Args:
        obj: Object to transform.
        transformer: Function(field_name, value) -> new_value.

    Returns:
        Dict with transformed fields.
    """
    return {name: transformer(name, getattr(obj, name)) for name in get_fields(type(obj))}
