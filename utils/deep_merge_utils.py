"""Deep dictionary merge utilities.

Provides deep merging strategies for nested dictionaries,
useful for configuration management and state updates.
"""

from typing import Any, Dict, List, Optional, Union


def deep_merge(
    base: Dict[str, Any],
    overlay: Dict[str, Any],
    *,
    arrays: str = "replace",
) -> Dict[str, Any]:
    """Deep merge overlay into base dictionary.

    Args:
        base: Base dictionary to merge into.
        overlay: Overlay dictionary to merge.
        arrays: How to handle arrays - "replace", "append", or "intersect".

    Returns:
        New merged dictionary.
    """
    result = _copy_dict(base)
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value, arrays=arrays)
        elif arrays == "append" and key in result and isinstance(result[key], list) and isinstance(value, list):
            result[key] = result[key] + value
        elif arrays == "intersect" and key in result and isinstance(result[key], list) and isinstance(value, list):
            result[key] = [v for v in value if v in result[key]]
        else:
            result[key] = _copy_dict(value) if isinstance(value, dict) else value
    return result


def _copy_dict(d: Any) -> Any:
    """Recursively copy a dictionary."""
    if isinstance(d, dict):
        return {k: _copy_dict(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_copy_dict(v) for v in d]
    return d


def merge_all(*dicts: Dict[str, Any], arrays: str = "replace") -> Dict[str, Any]:
    """Merge multiple dictionaries left to right.

    Args:
        *dicts: Dictionaries to merge in order.
        arrays: Array merge strategy.

    Returns:
        Merged dictionary.
    """
    if not dicts:
        return {}
    result = dicts[0]
    for d in dicts[1:]:
        result = deep_merge(result, d, arrays=arrays)
    return result


def pick_keys(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """Pick specific keys from dictionary.

    Args:
        d: Source dictionary.
        keys: Keys to pick.

    Returns:
        Dictionary with only the specified keys.
    """
    return {k: v for k, v in d.items() if k in keys}


def omit_keys(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """Omit specific keys from dictionary.

    Args:
        d: Source dictionary.
        keys: Keys to omit.

    Returns:
        Dictionary without the specified keys.
    """
    return {k: v for k, v in d.items() if k not in keys}


def get_path(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Get nested value by dot-notation path.

    Args:
        d: Source dictionary.
        path: Dot-notation path (e.g., "a.b.c").
        default: Default value if path not found.

    Returns:
        Value at path or default.
    """
    keys = path.split(".")
    current: Any = d
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def set_path(d: Dict[str, Any], path: str, value: Any) -> Dict[str, Any]:
    """Set nested value by dot-notation path.

    Args:
        d: Source dictionary.
        path: Dot-notation path (e.g., "a.b.c").
        value: Value to set.

    Returns:
        Updated dictionary.
    """
    keys = path.split(".")
    result = _copy_dict(d)
    current = result
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value
    return result
