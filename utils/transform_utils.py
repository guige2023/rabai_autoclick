"""Data transformation utilities for RabAI AutoClick.

Provides:
- Data transformations (map, filter, reduce)
- Record transformations
- Nested data access
- Type coercion
- Data normalization
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)


T = TypeVar("T")
U = TypeVar("U")


def transform_keys(
    data: Dict[T, U],
    func: Callable[[T], Any],
) -> Dict[Any, U]:
    """Transform dictionary keys.

    Args:
        data: Input dictionary.
        func: Function to apply to each key.

    Returns:
        New dictionary with transformed keys.
    """
    return {func(k): v for k, v in data.items()}


def transform_values(
    data: Dict[T, U],
    func: Callable[[U], Any],
) -> Dict[T, Any]:
    """Transform dictionary values.

    Args:
        data: Input dictionary.
        func: Function to apply to each value.

    Returns:
        New dictionary with transformed values.
    """
    return {k: func(v) for k, v in data.items()}


def transform_items(
    data: Dict[T, U],
    func: Callable[[T, U], tuple[Any, Any]],
) -> Dict[Any, Any]:
    """Transform dictionary key-value pairs.

    Args:
        data: Input dictionary.
        func: Function (key, value) -> (new_key, new_value).

    Returns:
        New dictionary with transformed pairs.
    """
    return {func(k, v)[0]: func(k, v)[1] for k, v in data.items()}


def pick(
    data: Dict[str, Any],
    keys: List[str],
    default: Any = None,
) -> Dict[str, Any]:
    """Pick specified keys from dictionary.

    Args:
        data: Input dictionary.
        keys: Keys to pick.
        default: Default value for missing keys.

    Returns:
        New dictionary with only specified keys.
    """
    return {k: data.get(k, default) for k in keys}


def omit(
    data: Dict[str, Any],
    keys: List[str],
) -> Dict[str, Any]:
    """Omit specified keys from dictionary.

    Args:
        data: Input dictionary.
        keys: Keys to omit.

    Returns:
        New dictionary without specified keys.
    """
    return {k: v for k, v in data.items() if k not in keys}


def deep_get(
    data: Any,
    path: str,
    default: Any = None,
    delimiter: str = ".",
) -> Any:
    """Get nested value using dot notation path.

    Args:
        data: Nested data structure.
        path: Dot-separated path (e.g., "user.profile.name").
        default: Default value if path not found.
        delimiter: Path delimiter (default ".").

    Returns:
        Value at path or default.
    """
    keys = path.split(delimiter)
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(key)]
            except (ValueError, IndexError):
                return default
        else:
            return default
        if current is None:
            return default
    return current


def deep_set(
    data: Dict[str, Any],
    path: str,
    value: Any,
    delimiter: str = ".",
    create_missing: bool = True,
) -> None:
    """Set nested value using dot notation path.

    Args:
        data: Target dictionary.
        path: Dot-separated path.
        value: Value to set.
        delimiter: Path delimiter.
        create_missing: If True, create missing intermediate dicts.
    """
    keys = path.split(delimiter)
    current = data
    for key in keys[:-1]:
        if key not in current:
            if create_missing:
                current[key] = {}
            else:
                return
        current = current[key]
    current[keys[-1]] = value


def flatten(
    data: Any,
    parent_key: str = "",
    sep: str = ".",
) -> Dict[str, Any]:
    """Flatten nested dictionary.

    Args:
        data: Nested dictionary.
        parent_key: Prefix for keys.
        sep: Separator for flattened keys.

    Returns:
        Flattened dictionary.
    """
    items: Dict[str, Any] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.update(flatten(value, new_key, sep))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.update(flatten(item, f"{new_key}[{i}]", sep))
                    else:
                        items[f"{new_key}[{i}]"] = item
            else:
                items[new_key] = value
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{parent_key}[{i}]" if parent_key else f"[{i}]"
            if isinstance(item, dict):
                items.update(flatten(item, new_key, sep))
            else:
                items[new_key] = item
    else:
        items[parent_key] = data
    return items


def unflatten(data: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
    """Unflatten dictionary with dot notation keys.

    Args:
        data: Flattened dictionary.
        sep: Separator used in keys.

    Returns:
        Nested dictionary.
    """
    result: Dict[str, Any] = {}
    for flat_key, value in data.items():
        keys = flat_key.split(sep)
        current = result
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value
    return result


def merge(
    *dicts: Dict[str, Any],
    strategy: str = "override",
) -> Dict[str, Any]:
    """Merge multiple dictionaries.

    Args:
        *dicts: Dictionaries to merge.
        strategy: Merge strategy - "override", "keep", "combine".

    Returns:
        Merged dictionary.
    """
    result: Dict[str, Any] = {}
    for d in dicts:
        for key, value in d.items():
            if key in result:
                if strategy == "override":
                    result[key] = value
                elif strategy == "keep":
                    pass
                elif strategy == "combine":
                    if isinstance(result[key], list) and isinstance(value, list):
                        result[key] = result[key] + value
                    elif isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = merge(result[key], value)
                    else:
                        result[key] = value
            else:
                result[key] = value
    return result


def group_by(
    data: List[Dict[str, Any]],
    key: str,
) -> Dict[Any, List[Dict[str, Any]]]:
    """Group list of dictionaries by key.

    Args:
        data: List of dictionaries.
        key: Key to group by.

    Returns:
        Dictionary mapping key value to list of items.
    """
    result: Dict[Any, List[Dict[str, Any]]] = {}
    for item in data:
        if key in item:
            group_key = item[key]
            if group_key not in result:
                result[group_key] = []
            result[group_key].append(item)
    return result


def sort_by(
    data: List[Dict[str, Any]],
    key: str,
    reverse: bool = False,
) -> List[Dict[str, Any]]:
    """Sort list of dictionaries by key.

    Args:
        data: List of dictionaries.
        key: Key to sort by.
        reverse: If True, sort descending.

    Returns:
        Sorted list.
    """
    return sorted(data, key=lambda x: x.get(key), reverse=reverse)


def deduplicate(
    data: List[Dict[str, Any]],
    keys: List[str],
) -> List[Dict[str, Any]]:
    """Remove duplicates based on specified keys.

    Args:
        data: List of dictionaries.
        keys: Keys to check for duplicates.

    Returns:
        Deduplicated list.
    """
    seen: set = set()
    result: List[Dict[str, Any]] = []
    for item in data:
        signature = tuple(item.get(k) for k in keys)
        if signature not in seen:
            seen.add(signature)
            result.append(item)
    return result


def coerce_types(
    data: Dict[str, Any],
    schema: Dict[str, type],
) -> Dict[str, Any]:
    """Coerce dictionary values to specified types.

    Args:
        data: Input dictionary.
        schema: Mapping of key to target type.

    Returns:
        Dictionary with coerced values.
    """
    result = {}
    for key, value in data.items():
        if key in schema:
            try:
                result[key] = schema[key](value)
            except (ValueError, TypeError):
                result[key] = value
        else:
            result[key] = value
    return result


def normalize_string(s: str, lowercase: bool = True, strip: bool = True) -> str:
    """Normalize a string.

    Args:
        s: Input string.
        lowercase: Convert to lowercase.
        strip: Strip whitespace.

    Returns:
        Normalized string.
    """
    if strip:
        s = s.strip()
    if lowercase:
        s = s.lower()
    return s


def normalize_dict(
    data: Dict[str, Any],
    normalize_strings: bool = True,
    remove_empty: bool = False,
) -> Dict[str, Any]:
    """Normalize dictionary values.

    Args:
        data: Input dictionary.
        normalize_strings: Normalize string values.
        remove_empty: Remove None and empty values.

    Returns:
        Normalized dictionary.
    """
    result: Dict[str, Any] = {}
    for key, value in data.items():
        if remove_empty and (value is None or value == "" or value == []):
            continue
        if normalize_strings and isinstance(value, str):
            result[key] = normalize_string(value)
        else:
            result[key] = value
    return result
