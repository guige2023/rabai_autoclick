"""Dataclass utilities for RabAI AutoClick.

Provides:
- Dataclass helpers
- Field validators
- Serialization helpers
"""

from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar


T = TypeVar("T")


def is_dataclass_instance(obj: Any) -> bool:
    """Check if object is a dataclass instance.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a dataclass instance.
    """
    return is_dataclass(obj) and not isinstance(obj, type)


def to_dict(obj: Any) -> Dict[str, Any]:
    """Convert dataclass to dictionary.

    Args:
        obj: Dataclass instance.

    Returns:
        Dictionary representation.
    """
    if not is_dataclass_instance(obj):
        raise TypeError(f"Expected dataclass instance, got {type(obj)}")

    result = {}
    for fld in fields(obj):
        value = getattr(obj, fld.name)
        result[fld.name] = _serialize_value(value)
    return result


def _serialize_value(value: Any) -> Any:
    """Serialize a value to JSON-compatible format."""
    if is_dataclass_instance(value):
        return to_dict(value)
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return str(value)
    return value


def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
    """Create dataclass instance from dictionary.

    Args:
        cls: Dataclass type.
        data: Dictionary data.

    Returns:
        Dataclass instance.
    """
    if not is_dataclass(cls):
        raise TypeError(f"Expected dataclass type, got {type(cls)}")

    valid_fields = {f.name for f in fields(cls)}
    filtered_data = {k: v for k, v in data.items() if k in valid_fields}
    return cls(**filtered_data)


def merge_dataclasses(
    base: T,
    override: Dict[str, Any],
    inplace: bool = False,
) -> T:
    """Merge dataclass with dictionary overrides.

    Args:
        base: Base dataclass instance.
        override: Dictionary with override values.
        inplace: If True, modify base in place.

    Returns:
        Merged dataclass instance.
    """
    if not is_dataclass_instance(base):
        raise TypeError(f"Expected dataclass instance, got {type(base)}")

    if not inplace:
        base = from_dict(type(base), to_dict(base))

    for key, value in override.items():
        if hasattr(base, key):
            setattr(base, key, value)

    return base


def dataclass_with_defaults(cls: Type[T]) -> Type[T]:
    """Decorator to add default values to dataclass fields.

    Args:
        cls: Dataclass to decorate.

    Returns:
        Modified dataclass type.
    """
    if not is_dataclass(cls):
        raise TypeError(f"Expected dataclass type, got {type(cls)}")

    for fld in fields(cls):
        if fld.default is field.DEFAULT:
            if fld.default_factory is field.DEFAULT:
                fld.default = None

    return cls


class FieldValidator:
    """Validator for dataclass fields.

    Usage:
        @dataclass
        class Person:
            name: str = field(metadata={"validator": [str_validator]})
    """

    @staticmethod
    def validate_int(value: Any, min_val: Optional[int] = None, max_val: Optional[int] = None) -> bool:
        """Validate integer is within range."""
        if not isinstance(value, int):
            return False
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True

    @staticmethod
    def validate_str(value: Any, min_len: int = 0, max_len: int = 0, pattern: Optional[str] = None) -> bool:
        """Validate string meets requirements."""
        if not isinstance(value, str):
            return False
        if min_len > 0 and len(value) < min_len:
            return False
        if max_len > 0 and len(value) > max_len:
            return False
        if pattern and not __import__('re').search(pattern, value):
            return False
        return True

    @staticmethod
    def validate_list(value: Any, item_type: Optional[Type] = None, min_len: int = 0) -> bool:
        """Validate list meets requirements."""
        if not isinstance(value, (list, tuple)):
            return False
        if len(value) < min_len:
            return False
        if item_type and not all(isinstance(v, item_type) for v in value):
            return False
        return True


def validate_fields(obj: Any, validators: Dict[str, Callable[[Any], bool]]) -> List[str]:
    """Validate dataclass fields using validators.

    Args:
        obj: Dataclass instance.
        validators: Dict mapping field names to validator functions.

    Returns:
        List of validation error messages (empty if all valid).
    """
    if not is_dataclass_instance(obj):
        raise TypeError(f"Expected dataclass instance, got {type(obj)}")

    errors = []
    for field_name, validator in validators.items():
        if not hasattr(obj, field_name):
            errors.append(f"Unknown field: {field_name}")
            continue

        value = getattr(obj, field_name)
        try:
            if not validator(value):
                errors.append(f"Invalid value for field '{field_name}': {value}")
        except Exception as e:
            errors.append(f"Validation error for field '{field_name}': {e}")

    return errors


@dataclass
class DataclassEncoder:
    """Encoder for dataclass instances to various formats.

    Supports encoding to dict, JSON string, and URL query params.
    """

    @staticmethod
    def to_dict(obj: Any) -> Dict[str, Any]:
        """Encode dataclass to dictionary."""
        return to_dict(obj)

    @staticmethod
    def to_json(obj: Any, indent: Optional[int] = None) -> str:
        """Encode dataclass to JSON string."""
        import json
        return json.dumps(to_dict(obj), indent=indent, ensure_ascii=False)

    @staticmethod
    def to_query_params(obj: Any) -> str:
        """Encode dataclass to URL query parameters."""
        from urllib.parse import urlencode
        return urlencode(to_dict(obj))


def compare_dataclasses(a: Any, b: Any, ignore_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """Compare two dataclass instances.

    Args:
        a: First dataclass instance.
        b: Second dataclass instance.
        ignore_fields: Fields to ignore in comparison.

    Returns:
        Dict with 'equal' bool and 'differences' list.
    """
    if not is_dataclass_instance(a) or not is_dataclass_instance(b):
        raise TypeError("Both arguments must be dataclass instances")

    if type(a) != type(b):
        return {"equal": False, "differences": ["Different types"]}

    ignore_fields = ignore_fields or []
    differences = []

    for fld in fields(a):
        if fld.name in ignore_fields:
            continue

        val_a = getattr(a, fld.name)
        val_b = getattr(b, fld.name)

        if val_a != val_b:
            differences.append({
                "field": fld.name,
                "value_a": val_a,
                "value_b": val_b,
            })

    return {
        "equal": len(differences) == 0,
        "differences": differences,
    }