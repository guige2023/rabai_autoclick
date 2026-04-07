"""Data transformation utilities for RabAI AutoClick.

Provides:
- Data transformation helpers
- Format converters
- Data sanitization
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Union


T = TypeVar("T")
U = TypeVar("U")


def transform(data: List[T], func: Callable[[T], U]) -> List[U]:
    """Transform list items using function.

    Args:
        data: List of items.
        func: Transformation function.

    Returns:
        Transformed list.
    """
    return [func(item) for item in data]


def transform_dict(
    data: Dict[str, T],
    func: Callable[[T], U],
) -> Dict[str, U]:
    """Transform dictionary values using function.

    Args:
        data: Dictionary to transform.
        func: Transformation function.

    Returns:
        Transformed dictionary.
    """
    return {key: func(value) for key, value in data.items()}


def filter_map(
    data: List[T],
    filter_func: Callable[[T], bool],
    map_func: Callable[[T], U],
) -> List[U]:
    """Filter and map list items.

    Args:
        data: List of items.
        filter_func: Function to filter items.
        map_func: Function to map filtered items.

    Returns:
        Filtered and mapped list.
    """
    return [map_func(item) for item in data if filter_func(item)]


def flatten(nested: List[List[T]]) -> List[T]:
    """Flatten nested lists.

    Args:
        nested: List of lists.

    Returns:
        Flattened list.
    """
    result = []
    for sublist in nested:
        result.extend(sublist)
    return result


def group_by(
    data: List[T],
    key_func: Callable[[T], U],
) -> Dict[U, List[T]]:
    """Group items by key function.

    Args:
        data: List of items.
        key_func: Function to extract group key.

    Returns:
        Dictionary mapping keys to item lists.
    """
    result: Dict[U, List[T]] = {}
    for item in data:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def chunk(items: List[T], size: int) -> List[List[T]]:
    """Split items into chunks of specified size.

    Args:
        items: Items to chunk.
        size: Chunk size.

    Returns:
        List of chunks.
    """
    return [items[i:i + size] for i in range(0, len(items), size)]


def pluck(data: List[Dict[str, Any]], key: str, default: Any = None) -> List[Any]:
    """Extract values for key from list of dicts.

    Args:
        data: List of dictionaries.
        key: Key to extract.
        default: Default value if key not found.

    Returns:
        List of extracted values.
    """
    return [item.get(key, default) for item in data]


def merge(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dictionaries.

    Args:
        *dicts: Dictionaries to merge.

    Returns:
        Merged dictionary.
    """
    result: Dict[str, Any] = {}
    for d in dicts:
        result.update(d)
    return result


def pick(data: Dict[str, T], keys: List[str]) -> Dict[str, T]:
    """Pick specific keys from dictionary.

    Args:
        data: Source dictionary.
        keys: Keys to pick.

    Returns:
        Dictionary with picked keys.
    """
    return {key: data[key] for key in keys if key in data}


def omit(data: Dict[str, T], keys: List[str]) -> Dict[str, T]:
    """Omit specific keys from dictionary.

    Args:
        data: Source dictionary.
        keys: Keys to omit.

    Returns:
        Dictionary without omitted keys.
    """
    return {key: value for key, value in data.items() if key not in keys}


def map_values(
    data: Dict[str, T],
    func: Callable[[str, T], U],
) -> Dict[str, U]:
    """Map dictionary values with key and value.

    Args:
        data: Source dictionary.
        func: Function taking (key, value) and returning new value.

    Returns:
        Dictionary with mapped values.
    """
    return {key: func(key, value) for key, value in data.items()}


def invert(data: Dict[T, U]) -> Dict[U, T]:
    """Invert dictionary keys and values.

    Args:
        data: Source dictionary.

    Returns:
        Inverted dictionary.
    """
    return {value: key for key, value in data.items()}


def deep_get(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Get value from nested dict using dot notation.

    Args:
        data: Source dictionary.
        path: Dot-separated path (e.g., "a.b.c").
        default: Default value if path not found.

    Returns:
        Value at path or default.
    """
    keys = path.split(".")
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def deep_set(data: Dict[str, Any], path: str, value: Any) -> None:
    """Set value in nested dict using dot notation.

    Args:
        data: Source dictionary.
        path: Dot-separated path (e.g., "a.b.c").
        value: Value to set.
    """
    keys = path.split(".")
    current = data
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def sanitize_string(text: str, replacements: Dict[str, str] = None) -> str:
    """Sanitize string by replacing characters.

    Args:
        text: Text to sanitize.
        replacements: Dict of characters to replace.

    Returns:
        Sanitized string.
    """
    if replacements is None:
        replacements = {
            "\r\n": "\n",
            "\r": "\n",
            "\t": "    ",
        }
    result = text
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result


def truncate(text: str, length: int, suffix: str = "...") -> str:
    """Truncate text to specified length.

    Args:
        text: Text to truncate.
        length: Maximum length.
        suffix: Suffix for truncated text.

    Returns:
        Truncated text.
    """
    if len(text) <= length:
        return text
    return text[:length - len(suffix)] + suffix


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace in text.

    Args:
        text: Text to normalize.

    Returns:
        Text with normalized whitespace.
    """
    return " ".join(text.split())


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: Text to convert.

    Returns:
        Converted text.
    """
    import re
    text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", text).lower()


def snake_to_camel(text: str) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: Text to convert.

    Returns:
        Converted text.
    """
    components = text.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def parse_int(value: Any, default: int = 0) -> int:
    """Parse integer from value.

    Args:
        value: Value to parse.
        default: Default value if parsing fails.

    Returns:
        Parsed integer or default.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_float(value: Any, default: float = 0.0) -> float:
    """Parse float from value.

    Args:
        value: Value to parse.
        default: Default value if parsing fails.

    Returns:
        Parsed float or default.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def parse_bool(value: Any) -> bool:
    """Parse boolean from value.

    Args:
        value: Value to parse.

    Returns:
        Parsed boolean.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "on")
    return bool(value)


def coerce_type(value: Any, target_type: type) -> Any:
    """Coerce value to target type.

    Args:
        value: Value to coerce.
        target_type: Target type.

    Returns:
        Value coerced to target type.
    """
    if target_type == bool:
        return parse_bool(value)
    elif target_type == int:
        return parse_int(value)
    elif target_type == float:
        return parse_float(value)
    elif target_type == str:
        return str(value)
    else:
        return value
