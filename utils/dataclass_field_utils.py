"""
Dataclass field manipulation utilities.

Provides field introspection, defaults, and conversion helpers.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Type, TypeVar, get_type_hints


T = TypeVar("T")


def get_fields(cls: Type) -> list[dataclasses.Field]:
    """Get all fields of a dataclass."""
    return dataclasses.fields(cls)


def get_field_names(cls: Type) -> list[str]:
    """Get field names of a dataclass."""
    return [f.name for f in dataclasses.fields(cls)]


def get_field_types(cls: Type) -> dict[str, type]:
    """Get field types of a dataclass."""
    return {f.name: f.type for f in dataclasses.fields(cls)}


def has_default(field: dataclasses.Field) -> bool:
    """Check if field has a default value."""
    return field.default is not dataclasses.MISSING


def has_default_factory(field: dataclasses.Field) -> bool:
    """Check if field has a default_factory."""
    return field.default_factory is not dataclasses.MISSING


def get_required_fields(cls: Type) -> list[str]:
    """Get fields without defaults (required fields)."""
    return [
        f.name for f in dataclasses.fields(cls)
        if not has_default(f) and not has_default_factory(f)
    ]


def get_optional_fields(cls: Type) -> list[str]:
    """Get fields with defaults (optional fields)."""
    return [
        f.name for f in dataclasses.fields(cls)
        if has_default(f) or has_default_factory(f)
    ]


def create_partial(
    cls: Type[T],
    obj: T,
) -> dict[str, Any]:
    """
    Extract only the fields that are set on an instance.

    Args:
        cls: Dataclass type
        obj: Instance of the dataclass

    Returns:
        Dict of set field values
    """
    result = {}
    for f in dataclasses.fields(cls):
        value = getattr(obj, f.name, dataclasses.MISSING)
        if value is not dataclasses.MISSING:
            if has_default_factory(f) and value == f.default_factory():
                continue
            if has_default(f) and value == f.default:
                continue
            result[f.name] = value
    return result


def merge_dataclasses(
    base: T,
    override: dict[str, Any],
    cls: Type[T] | None = None,
) -> T:
    """
    Merge override dict into dataclass instance.

    Args:
        base: Base dataclass instance
        override: Values to override
        cls: Dataclass type (inferred if None)

    Returns:
        New instance with merged values
    """
    if cls is None:
        cls = type(base)
    kwargs = {f.name: getattr(base, f.name) for f in dataclasses.fields(cls)}
    kwargs.update(override)
    return cls(**kwargs)


def dataclass_to_dict(obj: Any) -> dict[str, Any]:
    """Convert dataclass instance to plain dict."""
    if dataclasses.is_dataclass(obj):
        result = {}
        for f in dataclasses.fields(obj):
            value = getattr(obj, f.name)
            if dataclasses.is_dataclass(value) and not isinstance(value, type):
                result[f.name] = dataclass_to_dict(value)
            else:
                result[f.name] = value
        return result
    return obj


def dict_to_dataclass(
    cls: Type[T],
    data: dict[str, Any],
) -> T:
    """Create dataclass instance from dict, ignoring extra keys."""
    valid_fields = {f.name for f in dataclasses.fields(cls)}
    filtered = {k: v for k, v in data.items() if k in valid_fields}
    return cls(**filtered)


def validate_required_fields(
    cls: Type,
    data: dict[str, Any],
) -> list[str]:
    """Validate that all required fields are present."""
    missing = []
    for name in get_required_fields(cls):
        if name not in data:
            missing.append(name)
    return missing


@dataclass
class FieldSpec:
    """Specification for a dataclass field."""
    name: str
    field_type: type
    default: Any = dataclasses.MISSING
    default_factory: Callable[[], Any] = dataclasses.MISSING
    init: bool = True
    repr: bool = True
    compare: bool = True


def build_dataclass(
    spec: list[FieldSpec],
    class_name: str = "AutoDataclass",
) -> Type:
    """
    Dynamically build a dataclass from field specs.

    Args:
        spec: List of field specifications
        class_name: Name for the generated class

    Returns:
        New dataclass type
    """
    field_defs = []
    for s in spec:
        kwargs: dict[str, Any] = {}
        if s.default is not dataclasses.MISSING:
            kwargs["default"] = s.default
        elif s.default_factory is not dataclasses.MISSING:
            kwargs["default_factory"] = s.default_factory
        field_defs.append((s.name, s.field_type, dataclasses.field(**kwargs)))

    namespace = {"__annotations__": {f[0]: f[1] for f in field_defs}}
    for name, _, field_obj in field_defs:
        Object = dataclasses._field_init  # type: ignore
    return dataclasses.make_dataclass(class_name, field_defs)
