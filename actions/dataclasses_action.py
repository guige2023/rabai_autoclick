"""dataclasses action extensions for rabai_autoclick.

Provides utilities for working with Python dataclasses including
generation, validation, conversion, and field manipulation.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field, fields, is_dataclass
from typing import (
    Any,
    Callable,
    TypeVar,
    Generic,
    get_type_hints,
    get_origin,
    get_args,
)

__all__ = [
    "dataclass",
    "field",
    "fields",
    "is_dataclass",
    "make_dataclass",
    "asdict",
    "astuple",
    "asdict_filtered",
    "astuple_filtered",
    "replace",
    "compare_dataclasses",
    "dataclass_to_dict",
    "dict_to_dataclass",
    "dataclass_to_tuple",
    "tuple_to_dataclass",
    "validate_dataclass",
    "dataclass_defaults",
    "dataclass_required",
    "dataclass_optional",
    "dataclass_hash",
    "dataclass_eq",
    "dataclass_fields",
    "dataclass_field_names",
    "dataclass_field_types",
    "dataclass_has_field",
    "dataclass_get_field",
    "dataclass_set_field",
    "dataclass_copy",
    "dataclass_update",
    "dataclass_merge",
    "dataclass_from_dict",
    "dataclass_to_json",
    "dataclass_from_json",
    "dataclass_schema",
    "dataclass_builder",
    "DataClassBuilder",
    "DataClassValidator",
    "DataClassMerger",
    "DataClassComparator",
]


def make_dataclass(
    cls_name: str,
    field_names: list[str],
    *,
    field_types: dict[str, type] | None = None,
    defaults: dict[str, Any] | None = None,
    frozen: bool = False,
    slots: bool = False,
) -> type:
    """Create a dataclass dynamically.

    Args:
        cls_name: Name for the new class.
        field_names: List of field names.
        field_types: Dict of field name to type.
        defaults: Dict of field name to default value.
        frozen: If True, class is immutable.
        slots: If True, use __slots__.

    Returns:
        New dataclass type.
    """
    field_types = field_types or {}
    defaults = defaults or {}

    field_defs = []
    for name in field_names:
        ft = field_types.get(name, Any)
        default = defaults.get(name, ...)
        if default is not ...:
            field_defs.append((name, ft, field(default=default)))
        else:
            field_defs.append((name, ft))

    namespace = {"__slots__": []} if slots else {}

    return dataclasses.make_dataclass(
        cls_name,
        field_defs,
        frozen=frozen,
        slots=slots,
    )


def asdict(obj: Any) -> dict[str, Any]:
    """Convert dataclass instance to dict.

    Args:
        obj: Dataclass instance.

    Returns:
        Dict representation.
    """
    return dataclasses.asdict(obj)


def astuple(obj: Any) -> tuple:
    """Convert dataclass instance to tuple.

    Args:
        obj: Dataclass instance.

    Returns:
        Tuple representation.
    """
    return dataclasses.astuple(obj)


def asdict_filtered(
    obj: Any,
    filter_func: Callable[[str, Any], bool] | None = None,
) -> dict[str, Any]:
    """Convert dataclass to dict with filtering.

    Args:
        obj: Dataclass instance.
        filter_func: Function(key, value) returning True to include.

    Returns:
        Filtered dict.
    """
    result = dataclasses.asdict(obj)
    if filter_func:
        result = {k: v for k, v in result.items() if filter_func(k, v)}
    return result


def astuple_filtered(
    obj: Any,
    filter_func: Callable[[str, Any], bool] | None = None,
) -> tuple:
    """Convert dataclass to tuple with filtering.

    Args:
        obj: Dataclass instance.
        filter_func: Function(key, value) returning True to include.

    Returns:
        Filtered tuple.
    """
    result = dataclasses.asdict(obj)
    if filter_func:
        items = [(k, v) for k, v in result.items() if filter_func(k, v)]
        return tuple(v for _, v in items)
    return tuple(result.values())


def compare_dataclasses(a: Any, b: Any) -> dict[str, tuple[Any, Any]]:
    """Compare two dataclass instances.

    Args:
        a: First dataclass.
        b: Second dataclass.

    Returns:
        Dict of field names to (value_a, value_b) for differences.
    """
    if not is_dataclass(a) or not is_dataclass(b):
        return {}
    if type(a) is not type(b):
        return {}

    differences = {}
    for f in fields(a):
        val_a = getattr(a, f.name)
        val_b = getattr(b, f.name)
        if val_a != val_b:
            differences[f.name] = (val_a, val_b)

    return differences


def dataclass_to_dict(obj: Any) -> dict[str, Any]:
    """Convert dataclass to plain dict.

    Args:
        obj: Dataclass instance.

    Returns:
        Dict with field values.
    """
    if not is_dataclass(obj):
        return {"value": obj}

    result = {}
    for f in fields(obj):
        value = getattr(obj, f.name)
        if is_dataclass(value) and not isinstance(value, type):
            result[f.name] = dataclass_to_dict(value)
        elif isinstance(value, (list, tuple)):
            result[f.name] = type(value)(
                dataclass_to_dict(v) if is_dataclass(v) else v
                for v in value
            )
        elif isinstance(value, dict):
            result[f.name] = {
                k: dataclass_to_dict(v) if is_dataclass(v) else v
                for k, v in value.items()
            }
        else:
            result[f.name] = value
    return result


def dict_to_dataclass(
    cls: type,
    data: dict[str, Any],
    strict: bool = False,
) -> Any:
    """Convert dict to dataclass instance.

    Args:
        cls: Dataclass type.
        data: Dict with field values.
        strict: If True, raise on unknown fields.

    Returns:
        Dataclass instance.

    Raises:
        TypeError: If conversion fails.
    """
    if not is_dataclass(cls) or isinstance(cls, type) is False:
        raise TypeError(f"{cls} is not a dataclass type")

    field_names = {f.name for f in fields(cls)}
    if strict:
        unknown = set(data.keys()) - field_names
        if unknown:
            raise TypeError(f"Unknown fields: {unknown}")

    kwargs = {}
    for name in field_names:
        if name in data:
            kwargs[name] = data[name]

    return cls(**kwargs)


def dataclass_to_tuple(obj: Any) -> tuple:
    """Convert dataclass to tuple of field values.

    Args:
        obj: Dataclass instance.

    Returns:
        Tuple of values.
    """
    return tuple(getattr(obj, f.name) for f in fields(obj))


def tuple_to_dataclass(cls: type, data: tuple) -> Any:
    """Convert tuple to dataclass instance.

    Args:
        cls: Dataclass type.
        data: Tuple of values.

    Returns:
        Dataclass instance.
    """
    if len(data) != len(fields(cls)):
        raise ValueError(f"Tuple length {len(data)} != field count {len(fields(cls))}")
    return cls(*data)


def validate_dataclass(
    obj: Any,
    raise_on_error: bool = True,
) -> tuple[bool, list[str]]:
    """Validate a dataclass instance.

    Args:
        obj: Dataclass instance to validate.
        raise_on_error: If True, raise on invalid.

    Returns:
        Tuple of (is_valid, error_messages).
    """
    if not is_dataclass(obj):
        msg = f"{type(obj)} is not a dataclass"
        if raise_on_error:
            raise TypeError(msg)
        return False, [msg]

    errors = []
    for f in fields(obj):
        value = getattr(obj, f.name)
        if value is None and not f.type.startswith("Optional"):
            errors.append(f"Field '{f.name}' is None but type is {f.type}")

    if errors and raise_on_error:
        raise ValueError("; ".join(errors))

    return len(errors) == 0, errors


def dataclass_defaults(cls: type) -> dict[str, Any]:
    """Get default values from dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        Dict of field names to defaults.
    """
    if not is_dataclass(cls):
        return {}

    result = {}
    for f in fields(cls):
        if f.default is not dataclasses.MISSING:
            result[f.name] = f.default
        elif f.default_factory is not dataclasses.MISSING:
            result[f.name] = dataclasses.MISSING
    return result


def dataclass_required(cls: type) -> list[str]:
    """Get required fields (no default) from dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        List of required field names.
    """
    if not is_dataclass(cls):
        return []

    return [
        f.name for f in fields(cls)
        if f.default is dataclasses.MISSING
        and f.default_factory is dataclasses.MISSING
    ]


def dataclass_optional(cls: type) -> list[str]:
    """Get optional fields (with default) from dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        List of optional field names.
    """
    if not is_dataclass(cls):
        return []

    return [
        f.name for f in fields(cls)
        if f.default is not dataclasses.MISSING
        or f.default_factory is not dataclasses.MISSING
    ]


def dataclass_hash(obj: Any) -> int:
    """Compute hash of dataclass instance.

    Args:
        obj: Dataclass instance.

    Returns:
        Hash value.
    """
    return hash(dataclass_to_tuple(obj))


def dataclass_eq(cls: type) -> Callable[[Any, Any], bool]:
    """Create equality function for dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        Equality function.
    """
    def eq(a: Any, b: Any) -> bool:
        if not is_dataclass(a) or not is_dataclass(b):
            return False
        if type(a) is not type(b):
            return False
        return all(
            getattr(a, f.name) == getattr(b, f.name)
            for f in fields(cls)
        )

    return eq


def dataclass_field_names(cls: type) -> list[str]:
    """Get field names from dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        List of field names.
    """
    return [f.name for f in fields(cls)]


def dataclass_field_types(cls: type) -> dict[str, type]:
    """Get field types from dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        Dict of field name to type.
    """
    return {f.name: f.type for f in fields(cls)}


def dataclass_has_field(cls: type, name: str) -> bool:
    """Check if dataclass has a field.

    Args:
        cls: Dataclass type.
        name: Field name.

    Returns:
        True if field exists.
    """
    return any(f.name == name for f in fields(cls))


def dataclass_get_field(obj: Any, name: str) -> Any:
    """Get field value from dataclass instance.

    Args:
        obj: Dataclass instance.
        name: Field name.

    Returns:
        Field value.

    Raises:
        AttributeError: If field doesn't exist.
    """
    if not dataclass_has_field(type(obj), name):
        raise AttributeError(f"{type(obj).__name__} has no field '{name}'")
    return getattr(obj, name)


def dataclass_set_field(obj: Any, name: str, value: Any) -> None:
    """Set field value on dataclass instance.

    Args:
        obj: Dataclass instance.
        name: Field name.
        value: Value to set.

    Raises:
        AttributeError: If field doesn't exist.
        dataclasses.FrozenInstanceError: If dataclass is frozen.
    """
    if not dataclass_has_field(type(obj), name):
        raise AttributeError(f"{type(obj).__name__} has no field '{name}'")
    object.__setattr__(obj, name, value)


def dataclass_copy(
    obj: Any,
    **changes: Any,
) -> Any:
    """Create a copy of dataclass with optional changes.

    Args:
        obj: Dataclass instance.
        **changes: Fields to change.

    Returns:
        New dataclass instance.
    """
    return dataclasses.replace(obj, **changes)


def dataclass_update(
    obj: Any,
    updates: dict[str, Any],
) -> Any:
    """Update dataclass with dict of changes.

    Args:
        obj: Dataclass instance.
        updates: Dict of field updates.

    Returns:
        New dataclass instance.
    """
    return dataclasses.replace(obj, **updates)


def dataclass_merge(
    *objs: Any,
) -> Any:
    """Merge multiple dataclasses (later ones override).

    Args:
        *objs: Dataclass instances to merge.

    Returns:
        Merged dataclass instance.
    """
    if not objs:
        return None
    if len(objs) == 1:
        return objs[0]

    base_type = type(objs[0])
    merged: dict[str, Any] = {}

    for obj in objs:
        if type(obj) is not base_type:
            raise TypeError(f"Cannot merge {type(obj)} with {base_type}")
        for f in fields(obj):
            value = getattr(obj, f.name)
            if value is not None:
                merged[f.name] = value

    return base_type(**merged)


def dataclass_from_dict(cls: type, data: dict[str, Any]) -> Any:
    """Create dataclass from dict (alias for dict_to_dataclass).

    Args:
        cls: Dataclass type.
        data: Dict with values.

    Returns:
        Dataclass instance.
    """
    return dict_to_dataclass(cls, data)


def dataclass_to_json(obj: Any, **kwargs: Any) -> str:
    """Convert dataclass to JSON string.

    Args:
        obj: Dataclass instance.
        **kwargs: JSON encoder kwargs.

    Returns:
        JSON string.
    """
    import json
    return json.dumps(dataclass_to_dict(obj), **kwargs)


def dataclass_from_json(cls: type, json_str: str) -> Any:
    """Create dataclass from JSON string.

    Args:
        cls: Dataclass type.
        json_str: JSON string.

    Returns:
        Dataclass instance.
    """
    import json
    data = json.loads(json_str)
    return dict_to_dataclass(cls, data)


def dataclass_schema(cls: type) -> dict[str, Any]:
    """Get schema information for dataclass.

    Args:
        cls: Dataclass type.

    Returns:
        Dict with field names, types, and defaults.
    """
    result = {
        "name": cls.__name__,
        "fields": [],
    }

    for f in fields(cls):
        field_info: dict[str, Any] = {
            "name": f.name,
            "type": str(f.type),
        }
        if f.default is not dataclasses.MISSING:
            field_info["default"] = f.default
        elif f.default_factory is not dataclasses.MISSING:
            field_info["default_factory"] = f.default_factory
        result["fields"].append(field_info)

    return result


class DataClassBuilder(Generic[T]):
    """Builder for constructing dataclass instances."""

    def __init__(self, cls: type[T]) -> None:
        self._cls = cls
        self._values: dict[str, Any] = {}

    def set(self, name: str, value: Any) -> DataClassBuilder[T]:
        """Set a field value.

        Args:
            name: Field name.
            value: Value to set.

        Returns:
            Self for chaining.
        """
        self._values[name] = value
        return self

    def set_if(self, condition: bool, name: str, value: Any) -> DataClassBuilder[T]:
        """Set a field value conditionally.

        Args:
            condition: If True, set the value.
            name: Field name.
            value: Value to set.

        Returns:
            Self for chaining.
        """
        if condition:
            self._values[name] = value
        return self

    def build(self) -> T:
        """Build the dataclass instance.

        Returns:
            Dataclass instance.
        """
        return self._cls(**self._values)


class DataClassValidator:
    """Validator for dataclass instances."""

    def __init__(self, cls: type) -> None:
        self._cls = cls
        self._rules: dict[str, list[Callable]] = {}

    def add_rule(
        self,
        field_name: str,
        validator: Callable[[Any], bool],
        message: str = "Validation failed",
    ) -> None:
        """Add a validation rule.

        Args:
            field_name: Field to validate.
            validator: Function returning True if valid.
            message: Error message if invalid.
        """
        if field_name not in self._rules:
            self._rules[field_name] = []
        self._rules[field_name].append((validator, message))

    def validate(self, obj: Any) -> tuple[bool, list[str]]:
        """Validate a dataclass instance.

        Args:
            obj: Instance to validate.

        Returns:
            Tuple of (is_valid, error_messages).
        """
        errors = []

        for field_name, rules in self._rules.items():
            value = getattr(obj, field_name, None)
            for validator, message in rules:
                try:
                    if not validator(value):
                        errors.append(f"{field_name}: {message}")
                except Exception as e:
                    errors.append(f"{field_name}: {e}")

        return len(errors) == 0, errors


class DataClassMerger:
    """Merger for dataclass instances."""

    @staticmethod
    def merge(
        *objs: Any,
        strategy: str = "last",
    ) -> Any:
        """Merge multiple dataclass instances.

        Args:
            *objs: Instances to merge.
            strategy: Merge strategy ("last", "first", "combine").

        Returns:
            Merged instance.
        """
        if strategy == "last":
            return dataclass_merge(*objs)
        elif strategy == "first":
            return dataclass_merge(*reversed(objs))
        else:
            return dataclass_merge(*objs)


class DataClassComparator:
    """Comparator for dataclass instances."""

    @staticmethod
    def compare(
        a: Any,
        b: Any,
        fields_only: bool = True,
    ) -> dict[str, Any]:
        """Compare two dataclass instances.

        Args:
            a: First instance.
            b: Second instance.
            fields_only: Only compare field values.

        Returns:
            Dict with comparison results.
        """
        return {
            "equal": compare_dataclasses(a, b) == {},
            "differences": compare_dataclasses(a, b),
        }
