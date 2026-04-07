"""Dictionary utilities for RabAI AutoClick.

Provides:
- Dictionary manipulation helpers
- Nested dictionary access
- Dictionary merging and diffing
"""

from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from copy import deepcopy


def get_value(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Get value from dict with default.

    Args:
        data: Dictionary to query.
        key: Key to look up.
        default: Default value if key not found.

    Returns:
        Value or default.
    """
    return data.get(key, default)


def set_value(data: Dict[str, Any], key: str, value: Any) -> None:
    """Set value in dict.

    Args:
        data: Dictionary to modify.
        key: Key to set.
        value: Value to set.
    """
    data[key] = value


def delete_key(data: Dict[str, Any], key: str) -> None:
    """Delete key from dict.

    Args:
        data: Dictionary to modify.
        key: Key to delete.
    """
    if key in data:
        del data[key]


def has_key(data: Dict[str, Any], key: str) -> bool:
    """Check if key exists in dict.

    Args:
        data: Dictionary to check.
        key: Key to look for.

    Returns:
        True if key exists.
    """
    return key in data


def get_nested(data: Dict[str, Any], path: str, default: Any = None, sep: str = ".") -> Any:
    """Get nested value from dict using dot notation.

    Args:
        data: Dictionary to query.
        path: Dot-separated path to value.
        default: Default value if path not found.
        sep: Separator for path components.

    Returns:
        Value at path or default.
    """
    keys = path.split(sep)
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def set_nested(data: Dict[str, Any], path: str, value: Any, sep: str = ".") -> None:
    """Set nested value in dict using dot notation.

    Args:
        data: Dictionary to modify.
        path: Dot-separated path to value.
        value: Value to set.
        sep: Separator for path components.
    """
    keys = path.split(sep)
    current = data
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def delete_nested(data: Dict[str, Any], path: str, sep: str = ".") -> bool:
    """Delete nested value from dict using dot notation.

    Args:
        data: Dictionary to modify.
        path: Dot-separated path to value.
        sep: Separator for path components.

    Returns:
        True if deleted, False if path not found.
    """
    keys = path.split(sep)
    current = data
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            return False
        current = current[key]
    if keys[-1] in current:
        del current[keys[-1]]
        return True
    return False


def merge_dicts(base: Dict[str, Any], *updates: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dicts into one.

    Args:
        base: Base dictionary.
        *updates: Dictionaries to merge in.

    Returns:
        Merged dictionary.
    """
    result = deepcopy(base)
    for update in updates:
        result = _deep_merge(result, update)
    return result


def _deep_merge(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries.

    Args:
        base: Base dictionary.
        update: Dictionary to merge.

    Returns:
        Merged dictionary.
    """
    result = deepcopy(base)
    for key, value in update.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def diff_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Tuple[Any, Any]]:
    """Find differences between two dicts.

    Args:
        dict1: First dictionary.
        dict2: Second dictionary.

    Returns:
        Dict of key -> (value1, value2) for differing keys.
    """
    all_keys = set(dict1.keys()) | set(dict2.keys())
    diff = {}
    for key in all_keys:
        v1 = dict1.get(key)
        v2 = dict2.get(key)
        if v1 != v2:
            diff[key] = (v1, v2)
    return diff


def filter_dict(data: Dict[str, Any], predicate: Callable[[str, Any], bool]) -> Dict[str, Any]:
    """Filter dict by predicate.

    Args:
        data: Dictionary to filter.
        predicate: Function that returns True to keep item.

    Returns:
        Filtered dictionary.
    """
    return {k: v for k, v in data.items() if predicate(k, v)}


def map_dict(data: Dict[str, Any], transformer: Callable[[str, Any], Tuple[str, Any]]) -> Dict[str, Any]:
    """Map dict values using transformer function.

    Args:
        data: Dictionary to transform.
        transformer: Function that returns (key, value) tuple.

    Returns:
        Transformed dictionary.
    """
    return dict(transformer(k, v) for k, v in data.items())


def invert_dict(data: Dict[str, Any]) -> Dict[Any, str]:
    """Invert dict (swap keys and values).

    Args:
        data: Dictionary to invert.

    Returns:
        Inverted dictionary.
    """
    return {v: k for k, v in data.items()}


def flatten_dict(data: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
    """Flatten nested dict to single level.

    Args:
        data: Dictionary to flatten.
        sep: Separator for nested keys.

    Returns:
        Flattened dictionary.
    """
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            nested = flatten_dict(value, sep)
            for nkey, nvalue in nested.items():
                result[f"{key}{sep}{nkey}"] = nvalue
        else:
            result[key] = value
    return result


def unflatten_dict(data: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
    """Unflatten dict with dot notation keys.

    Args:
        data: Dictionary to unflatten.
        sep: Separator for nested keys.

    Returns:
        Nested dictionary.
    """
    result = {}
    for key, value in data.items():
        set_nested(result, key, value, sep)
    return result


def pick_keys(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """Pick specific keys from dict.

    Args:
        data: Dictionary to pick from.
        keys: Keys to pick.

    Returns:
        Dictionary with picked keys only.
    """
    return {k: data[k] for k in keys if k in data}


def omit_keys(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """Omit specific keys from dict.

    Args:
        data: Dictionary to omit from.
        keys: Keys to omit.

    Returns:
        Dictionary without omitted keys.
    """
    return {k: v for k, v in data.items() if k not in keys}


def deep_get(data: Dict[str, Any], path: str, default: Any = None, sep: str = ".") -> Any:
    """Get nested value from dict.

    Alias for get_nested for compatibility.
    """
    return get_nested(data, path, default, sep)


def deep_set(data: Dict[str, Any], path: str, value: Any, sep: str = ".") -> None:
    """Set nested value in dict.

    Alias for set_nested for compatibility.
    """
    set_nested(data, path, value, sep)


def deep_delete(data: Dict[str, Any], path: str, sep: str = ".") -> bool:
    """Delete nested value from dict.

    Alias for delete_nested for compatibility.
    """
    return delete_nested(data, path, sep)


def dict_from_tuples(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    """Create dict from list of key-value tuples.

    Args:
        pairs: List of (key, value) tuples.

    Returns:
        Dictionary.
    """
    return dict(pairs)


def dict_to_tuples(data: Dict[str, Any]) -> List[Tuple[str, Any]]:
    """Convert dict to list of key-value tuples.

    Args:
        data: Dictionary.

    Returns:
        List of (key, value) tuples.
    """
    return list(data.items())


def update_if_exists(data: Dict[str, Any], key: str, value: Any) -> bool:
    """Update value only if key exists.

    Args:
        data: Dictionary to update.
        key: Key to update.
        value: Value to set.

    Returns:
        True if updated, False if key didn't exist.
    """
    if key in data:
        data[key] = value
        return True
    return False


def update_if_not_exists(data: Dict[str, Any], key: str, value: Any) -> bool:
    """Update value only if key doesn't exist.

    Args:
        data: Dictionary to update.
        key: Key to update.
        value: Value to set.

    Returns:
        True if updated, False if key already existed.
    """
    if key not in data:
        data[key] = value
        return True
    return False


def get_or_create(data: Dict[str, Any], key: str, factory: Callable[[], Any]) -> Any:
    """Get value or create if doesn't exist.

    Args:
        data: Dictionary to query/modify.
        key: Key to look up.
        factory: Function to create default value.

    Returns:
        Existing value or newly created.
    """
    if key not in data:
        data[key] = factory()
    return data[key]


def count_values(data: Dict[str, Any]) -> int:
    """Count total values in dict.

    Args:
        data: Dictionary.

    Returns:
        Number of values.
    """
    return len(data)


def count_keys(data: Dict[str, Any]) -> int:
    """Count total keys in dict.

    Args:
        data: Dictionary.

    Returns:
        Number of keys.
    """
    return len(data)


def is_empty(data: Dict[str, Any]) -> bool:
    """Check if dict is empty.

    Args:
        data: Dictionary.

    Returns:
        True if empty.
    """
    return len(data) == 0


def clear_dict(data: Dict[str, Any]) -> None:
    """Clear all items from dict.

    Args:
        data: Dictionary to clear.
    """
    data.clear()


def swap_keys_values(data: Dict[str, Any]) -> Dict[Any, str]:
    """Swap all keys and values in dict.

    Args:
        data: Dictionary to swap.

    Returns:
        Swapped dictionary.
    """
    return invert_dict(data)


def rename_key(data: Dict[str, Any], old_key: str, new_key: str) -> bool:
    """Rename a key in dict.

    Args:
        data: Dictionary to modify.
        old_key: Current key name.
        new_key: New key name.

    Returns:
        True if renamed, False if old_key didn't exist.
    """
    if old_key not in data:
        return False
    data[new_key] = data.pop(old_key)
    return True


def extract_subdict(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """Extract sub-dict with only specified keys.

    Args:
        data: Source dictionary.
        keys: Keys to extract.

    Returns:
        New dictionary with only specified keys.
    """
    return pick_keys(data, keys)


def group_by(data: List[Dict[str, Any]], key: str) -> Dict[Any, List[Dict[str, Any]]]:
    """Group list of dicts by key value.

    Args:
        data: List of dictionaries.
        key: Key to group by.

    Returns:
        Dictionary mapping key value to list of dicts.
    """
    result: Dict[Any, List[Dict[str, Any]]] = {}
    for item in data:
        if key in item:
            value = item[key]
            if value not in result:
                result[value] = []
            result[value].append(item)
    return result
