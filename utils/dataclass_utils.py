"""Dataclass utilities for RabAI AutoClick.

Provides:
- Field metadata extraction and validation
- Dataclass to dict/JSON conversion
- Default factory helpers
- Field comparators and equality
- Copy and replace utilities
"""

from __future__ import annotations

import dataclasses
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)


T = TypeVar("T")


def fields_with_meta(
    dataclass: Type[object],
) -> List[Tuple[str, Any, type]]:
    """Extract fields that have metadata attached.

    Args:
        dataclass: A dataclass type or instance.

    Returns:
        List of (field_name, metadata, field_type) tuples.
    """
    cls = dataclass if isinstance(dataclass, type) else type(dataclass)
    result = []
    for field in dataclasses.fields(cls):
        if field.metadata:
            result.append((field.name, field.metadata, field.type))
    return result


def required_fields(
    dataclass: Type[object],
) -> List[str]:
    """Get field names that have no default value.

    Args:
        dataclass: A dataclass type or instance.

    Returns:
        List of field names without defaults.
    """
    cls = dataclass if isinstance(dataclass, type) else type(dataclass)
    return [
        f.name
        for f in dataclasses.fields(cls)
        if f.default is dataclasses.MISSING
        and f.default_factory is dataclasses.MISSING
    ]


def optional_fields(
    dataclass: Type[object],
) -> List[str]:
    """Get field names that have default values.

    Args:
        dataclass: A dataclass type or instance.

    Returns:
        List of field names with defaults.
    """
    cls = dataclass if isinstance(dataclass, type) else type(dataclass)
    return [
        f.name
        for f in dataclasses.fields(cls)
        if f.default is not dataclasses.MISSING
        or f.default_factory is not dataclasses.MISSING
    ]


def dataclass_to_dict(
    obj: Any,
    *,
    exclude: Optional[FrozenSet[str]] = None,
    rename: bool = False,
) -> Dict[str, Any]:
    """Convert a dataclass instance to a dictionary.

    Args:
        obj: Dataclass instance.
        exclude: Field names to exclude.
        rename: Whether to use field metadata 'alias' as key.

    Returns:
        Dictionary representation of the dataclass.
    """
    exclude = exclude or frozenset()
    result = {}
    for field in dataclasses.fields(obj):
        if field.name in exclude:
            continue
        value = getattr(obj, field.name)
        key = field.metadata.get("alias", field.name) if rename else field.name
        result[key] = _serialize_value(value)
    return result


def _serialize_value(value: Any) -> Any:
    """Recursively serialize a value for dict conversion."""
    if dataclasses.is_dataclass(value):
        return dataclass_to_dict(value)
    elif isinstance(value, list):
        return [_serialize_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    elif isinstance(value, tuple):
        return tuple(_serialize_value(v) for v in value)
    elif isinstance(value, set):
        return {_serialize_value(v) for v in value}
    return value


def dataclass_from_dict(
    dataclass: Type[T],
    data: Dict[str, Any],
    *,
    strict: bool = False,
) -> T:
    """Create a dataclass instance from a dictionary.

    Args:
        dataclass: Target dataclass type.
        data: Dictionary with field values.
        strict: Raise on unknown fields if True.

    Returns:
        Instance of the dataclass.
    """
    if strict:
        valid = {f.name for f in dataclasses.fields(dataclass)}
        unknown = set(data.keys()) - valid
        if unknown:
            raise ValueError(f"Unknown fields: {unknown}")

    kwargs = {}
    for field in dataclasses.fields(dataclass):
        if field.name in data:
            kwargs[field.name] = _deserialize_value(
                data[field.name], field.type
            )
    return dataclass(**kwargs)


def _deserialize_value(value: Any, target_type: type) -> Any:
    """Recursively deserialize a value to the target type."""
    if value is None:
        return None
    if dataclasses.is_dataclass(target_type) and isinstance(value, dict):
        return dataclass_from_dict(target_type, value)
    if hasattr(target_type, "__origin__"):
        origin = getattr(target_type, "__origin__")
        if origin is list and isinstance(value, list):
            item_type = getattr(target_type, "__args__", (Any,))[0]
            return [_deserialize_value(v, item_type) for v in value]
        if origin is dict and isinstance(value, dict):
            key_type, val_type = getattr(target_type, "__args__", (str, Any))
            return {
                _deserialize_value(k, key_type): _deserialize_value(v, val_type)
                for k, v in value.items()
            }
    return value


def replace_with(
    obj: T,
    **changes: Any,
) -> T:
    """Create a copy of a dataclass with updated fields.

    Args:
        obj: Dataclass instance.
        **changes: Fields to update.

    Returns:
        New dataclass instance with changes applied.
    """
    return dataclasses.replace(obj, **changes)


def compare_dataclasses(
    a: Any,
    b: Any,
    fields: Optional[List[str]] = None,
) -> Dict[str, Tuple[Any, Any]]:
    """Compare two dataclass instances field by field.

    Args:
        a: First dataclass instance.
        b: Second dataclass instance.
        fields: Specific fields to compare. None means all.

    Returns:
        Dict of {field_name: (value_a, value_b)} for differing fields.
    """
    if not dataclasses.is_dataclass(a) or not dataclasses.is_dataclass(b):
        raise TypeError("Both arguments must be dataclasses")

    diff: Dict[str, Tuple[Any, Any]] = {}
    cls_fields = dataclasses.fields(a)

    if fields is not None:
        field_names = fields
        cls_fields = [f for f in cls_fields if f.name in fields]
    else:
        field_names = [f.name for f in cls_fields]

    for name in field_names:
        val_a = getattr(a, name)
        val_b = getattr(b, name)
        if val_a != val_b:
            diff[name] = (val_a, val_b)

    return diff


def validate_dataclass(obj: Any) -> List[str]:
    """Run field validators on a dataclass instance.

    Args:
        obj: Dataclass instance.

    Returns:
        List of error messages (empty if all pass).
    """
    errors: List[str] = []
    for field in dataclasses.fields(obj):
        value = getattr(obj, field.name)
        for validator in field.metadata.get("validators", []):
            try:
                validator(value)
            except Exception as e:
                errors.append(f"{field.name}: {e}")
    return errors


def field_hash(obj: Any, fields: Optional[List[str]] = None) -> int:
    """Compute a hash from specific fields of a dataclass.

    Args:
        obj: Dataclass instance.
        fields: Fields to include. None means all fields.

    Returns:
        Hash integer.
    """
    if fields is None:
        values = tuple(getattr(obj, f.name) for f in dataclasses.fields(obj))
    else:
        values = tuple(getattr(obj, name) for name in fields)
    return hash(values)


def defaults_dict(
    dataclass: Type[object],
) -> Dict[str, Any]:
    """Get default values for all fields as a dict.

    Args:
        dataclass: A dataclass type.

    Returns:
        Dict mapping field names to their default values.
    """
    result = {}
    for field in dataclasses.fields(dataclass):
        if field.default is not dataclasses.MISSING:
            result[field.name] = field.default
        elif field.default_factory is not dataclasses.MISSING:
            result[field.name] = field.default_factory()
    return result
