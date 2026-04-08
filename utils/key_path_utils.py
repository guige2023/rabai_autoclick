"""Key-path and nested access utilities.

Provides utilities for accessing and manipulating
nested data structures using key paths.
"""

from typing import Any, Callable, Dict, List, Optional, Union


PathElement = Union[str, int]
Path = List[PathElement]


def get_path(data: Any, path: Path, default: Any = None) -> Any:
    """Get value at path in nested structure.

    Example:
        data = {"a": {"b": [1, 2, 3]}}
        get_path(data, ["a", "b", 1])  # 2
        get_path(data, ["a", "c"], "default")  # "default"
    """
    current = data
    for key in path:
        if current is None:
            return default
        try:
            current = current[key]
        except (KeyError, IndexError, TypeError):
            return default
    return current


def set_path(data: Any, path: Path, value: Any) -> Any:
    """Set value at path in nested structure.

    Example:
        data = {"a": {"b": 1}}
        set_path(data, ["a", "b"], 2)  # {"a": {"b": 2}}
    """
    if not path:
        return value

    result = _deep_copy(data)
    current = result
    for key in path[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]

    current[path[-1]] = value
    return result


def has_path(data: Any, path: Path) -> bool:
    """Check if path exists in nested structure.

    Example:
        data = {"a": {"b": 1}}
        has_path(data, ["a", "b"])  # True
        has_path(data, ["a", "c"])  # False
    """
    return get_path(data, path, _NOT_FOUND) is not _NOT_FOUND


def delete_path(data: Any, path: Path) -> Any:
    """Delete value at path from nested structure.

    Example:
        data = {"a": {"b": 1, "c": 2}}
        delete_path(data, ["a", "b"])  # {"a": {"c": 2}}
    """
    if not path:
        return data

    result = _deep_copy(data)
    current = result
    for key in path[:-1]:
        if key not in current:
            return result
        current = current[key]

    if path[-1] in current:
        del current[path[-1]]
    return result


def merge_paths(base: Any, *overlays: Dict[Path, Any]) -> Any:
    """Merge multiple path-value pairs into base.

    Example:
        base = {"a": 1}
        merge_paths(base, {["b"]: 2}, {["c"]: 3})
        # {"a": 1, "b": 2, "c": 3}
    """
    result = _deep_copy(base)
    for overlay in overlays:
        for path, value in overlay.items():
            result = set_path(result, path, value)
    return result


def flatten_paths(data: Any, prefix: Path = None) -> Dict[Path, Any]:
    """Flatten nested structure to path-value dict.

    Example:
        flatten_paths({"a": {"b": 1}})
        # {("a", "b"): 1}
    """
    prefix = prefix or []
    result: Dict[Path, Any] = {}

    if not isinstance(data, dict):
        result[tuple(prefix)] = data
        return result

    for key, value in data.items():
        new_prefix = prefix + [key]
        if isinstance(value, dict):
            result.update(flatten_paths(value, new_prefix))
        else:
            result[tuple(new_prefix)] = value

    return result


def unflatten_paths(flat: Dict[Path, Any]) -> Any:
    """Unflatten path-value dict to nested structure.

    Example:
        unflatten_paths({("a", "b"): 1})
        # {"a": {"b": 1}}
    """
    result: Any = {}
    for path, value in flat.items():
        current = result
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value
    return result


def walk_paths(
    data: Any,
    visitor: Callable[[Path, Any], None],
    path: Path = None,
) -> None:
    """Walk nested structure and visit each path-value.

    Example:
        def visitor(path, value):
            print(f"{'.'.join(str(k) for k in path)} = {value}")
        walk_paths({"a": {"b": 1}}, visitor)
        # a.b = 1
    """
    path = path or []
    if not isinstance(data, dict):
        visitor(path, data)
        return

    for key, value in data.items():
        new_path = path + [key]
        walk_paths(value, visitor, new_path)


def transform_paths(
    data: Any,
    transformer: Callable[[Path, Any], Any],
) -> Any:
    """Transform all values in nested structure.

    Example:
        double = lambda p, v: v * 2 if isinstance(v, int) else v
        transform_paths({"a": 1, "b": {"c": 2}}, double)
        # {"a": 2, "b": {"c": 4}}
    """
    if not isinstance(data, dict):
        return transformer([], data)

    result = {}
    for key, value in data.items():
        new_value = transform_paths(value, lambda p, v: transformer([key] + p, v))
        result[key] = new_value
    return result


def path_to_string(path: Path, separator: str = ".") -> str:
    """Convert path to string representation.

    Example:
        path_to_string(["a", "b", 1])  # "a.b[1]"
        path_to_string(["a", "b", 1], separator="/")  # "a/b[1]"
    """
    parts = []
    for elem in path:
        if isinstance(elem, int):
            parts.append(f"[{elem}]")
        else:
            if parts and not parts[-1].startswith("["):
                parts.append(separator)
            parts[-1] = parts[-1] + str(elem)
    return "".join(parts).lstrip(".")


def string_to_path(s: str) -> Path:
    """Parse path string to path list.

    Example:
        string_to_path("a.b[1]")  # ["a", "b", 1]
    """
    path: Path = []
    current = ""
    i = 0
    while i < len(s):
        c = s[i]
        if c == ".":
            if current:
                path.append(current)
                current = ""
        elif c == "[":
            if current:
                path.append(current)
                current = ""
            j = s.index("]", i)
            path.append(int(s[i+1:j]))
            i = j
        else:
            current += c
        i += 1
    if current:
        path.append(current)
    return path


def _deep_copy(data: Any) -> Any:
    """Simple deep copy."""
    import copy
    return copy.deepcopy(data)


_NOT_FOUND = object()
