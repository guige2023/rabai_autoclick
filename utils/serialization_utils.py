"""Serialization utilities for RabAI AutoClick.

Provides:
- Pickle helpers with security options
- Object serialization/deserialization
- URL encoding helpers
- Base64 utilities
"""

import base64
import json
import pickle
from typing import Any, Callable, Optional, Union
from urllib.parse import quote, unquote, urlencode, parse_qs


def pickle_dumps(
    obj: Any,
    protocol: int = pickle.HIGHEST_PROTOCOL,
) -> bytes:
    """Serialize an object to bytes using pickle.

    Args:
        obj: Object to serialize.
        protocol: Pickle protocol version.

    Returns:
        Pickled bytes.
    """
    return pickle.dumps(obj, protocol=protocol)


def pickle_loads(data: bytes) -> Any:
    """Deserialize pickled bytes to an object.

    Args:
        data: Pickled bytes.

    Returns:
        Deserialized object.
    """
    return pickle.loads(data)


def pickle_dumps_b64(
    obj: Any,
    protocol: int = pickle.HIGHEST_PROTOCOL,
) -> str:
    """Serialize an object to base64-encoded string.

    Args:
        obj: Object to serialize.
        protocol: Pickle protocol version.

    Returns:
        Base64-encoded string.
    """
    return base64.b64encode(pickle.dumps(obj, protocol=protocol)).decode()


def pickle_loads_b64(data: str) -> Any:
    """Deserialize a base64-encoded pickled string.

    Args:
        data: Base64-encoded pickled string.

    Returns:
        Deserialized object.
    """
    return pickle.loads(base64.b64decode(data))


def json_dumps(
    obj: Any,
    *,
    indent: Optional[int] = None,
    sort_keys: bool = False,
    ensure_ascii: bool = True,
    default: Optional[Callable[[Any], Any]] = None,
) -> str:
    """Serialize an object to JSON string.

    Args:
        obj: Object to serialize.
        indent: Indentation level (None for compact).
        sort_keys: If True, sort dictionary keys.
        ensure_ascii: If True, escape non-ASCII characters.
        default: Function to convert non-serializable objects.

    Returns:
        JSON string.
    """
    return json.dumps(
        obj,
        indent=indent,
        sort_keys=sort_keys,
        ensure_ascii=ensure_ascii,
        default=default,
    )


def json_loads(data: str) -> Any:
    """Deserialize a JSON string.

    Args:
        data: JSON string.

    Returns:
        Deserialized object.
    """
    return json.loads(data)


def json_dumps_bytes(
    obj: Any,
    *,
    indent: Optional[int] = None,
    sort_keys: bool = False,
    ensure_ascii: bool = True,
) -> bytes:
    """Serialize an object to JSON bytes.

    Args:
        obj: Object to serialize.
        indent: Indentation level.
        sort_keys: If True, sort dictionary keys.
        ensure_ascii: If True, escape non-ASCII.

    Returns:
        JSON bytes.
    """
    return json.dumps(
        obj,
        indent=indent,
        sort_keys=sort_keys,
        ensure_ascii=ensure_ascii,
    ).encode("utf-8")


def json_loads_bytes(data: bytes) -> Any:
    """Deserialize JSON bytes.

    Args:
        data: JSON bytes.

    Returns:
        Deserialized object.
    """
    return json.loads(data.decode("utf-8"))


def url_encode(params: dict) -> str:
    """URL-encode a dictionary of parameters.

    Args:
        params: Dictionary of parameters.

    Returns:
        URL-encoded string.
    """
    return urlencode(params, safe="")


def url_encode_value(value: Any) -> str:
    """URL-encode a single value.

    Args:
        value: Value to encode.

    Returns:
        URL-encoded string.
    """
    return quote(str(value), safe="")


def url_decode(query: str) -> dict:
    """Decode a URL query string.

    Args:
        query: URL query string (without leading ?).

    Returns:
        Dictionary of parameters.
    """
    parsed = parse_qs(query, keep_blank_values=True)
    return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}


def url_decode_single(query: str) -> dict:
    """Decode a URL query string (always single values).

    Args:
        query: URL query string.

    Returns:
        Dictionary with single values.
    """
    return parse_qs(query, keep_blank_values=True)


def base64_encode(data: Union[bytes, str]) -> str:
    """Encode data to base64 string.

    Args:
        data: Data to encode (bytes or str).

    Returns:
        Base64-encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b64encode(data).decode("ascii")


def base64_decode(data: str) -> bytes:
    """Decode a base64 string.

    Args:
        data: Base64-encoded string.

    Returns:
        Decoded bytes.
    """
    # Handle URL-safe base64
    data = data.replace("-", "+").replace("_", "/")
    # Add padding if needed
    padding = 4 - len(data) % 4
    if padding != 4:
        data += "=" * padding
    return base64.b64decode(data)


def base64_encode_urlsafe(data: Union[bytes, str]) -> str:
    """Encode data to URL-safe base64 string.

    Args:
        data: Data to encode.

    Returns:
        URL-safe base64-encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def base64_decode_urlsafe(data: str) -> bytes:
    """Decode a URL-safe base64 string.

    Args:
        data: URL-safe base64-encoded string.

    Returns:
        Decoded bytes.
    """
    padding = 4 - len(data) % 4
    if padding != 4:
        data += "=" * padding
    return base64.urlsafe_b64decode(data)


class Serializable:
    """Mixin class for objects that can serialize themselves."""

    def to_dict(self) -> dict:
        """Convert object to dictionary."""
        return self.__dict__.copy()

    def to_json(self, **kwargs: Any) -> str:
        """Serialize object to JSON string."""
        return json_dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, data: dict) -> "Serializable":
        """Create object from dictionary."""
        obj = cls.__new__(cls)
        obj.__dict__.update(data)
        return obj

    @classmethod
    def from_json(cls, data: str) -> "Serializable":
        """Create object from JSON string."""
        return cls.from_dict(json_loads(data))


def safe_json_get(
    data: Union[dict, list, str],
    key: str,
    default: Any = None,
) -> Any:
    """Safely get a value from a JSON-serializable structure.

    Supports dot notation for nested keys.

    Args:
        data: Dict or list to search.
        key: Key in dot notation (e.g., "user.profile.name").
        default: Default value if key not found.

    Returns:
        Value at key or default.
    """
    if isinstance(data, str):
        data = json_loads(data)

    keys = key.split(".")
    current = data

    for k in keys:
        if isinstance(current, dict):
            current = current.get(k)
        elif isinstance(current, list) and k.isdigit():
            idx = int(k)
            current = current[idx] if idx < len(current) else None
        else:
            return default
        if current is None:
            return default

    return current


def merge_serialized(
    base: dict,
    updates: dict,
    *,
    deep: bool = True,
) -> dict:
    """Merge two serialized objects.

    Args:
        base: Base dictionary.
        updates: Updates to apply.
        deep: If True, perform deep merge for nested dicts.

    Returns:
        Merged dictionary.
    """
    import copy

    result = base.copy() if not deep else copy.deepcopy(base)

    for key, value in updates.items():
        if deep and key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_serialized(result[key], value, deep=True)
        else:
            result[key] = value

    return result
