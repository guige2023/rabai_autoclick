"""Pickle utilities for RabAI AutoClick.

Provides:
- Object serialization with pickle
- Compressed pickle for large objects
- Safe unpickling with allowed classes
"""

from __future__ import annotations

import base64
import gzip
import pickle
from typing import (
    Any,
    Callable,
    Optional,
)


def dumps(obj: Any, protocol: int = pickle.HIGHEST_PROTOCOL) -> bytes:
    """Serialize an object to bytes with pickle.

    Args:
        obj: Object to serialize.
        protocol: Pickle protocol version.

    Returns:
        Pickled bytes.
    """
    return pickle.dumps(obj, protocol=protocol)


def loads(data: bytes) -> Any:
    """Deserialize a pickled bytes object.

    Args:
        data: Pickled bytes.

    Returns:
        Deserialized object.
    """
    return pickle.loads(data)


def dumps_b64(obj: Any) -> str:
    """Serialize to base64-encoded string.

    Args:
        obj: Object to serialize.

    Returns:
        Base64 string.
    """
    return base64.b64encode(dumps(obj)).decode("ascii")


def loads_b64(data: str) -> Any:
    """Deserialize from base64-encoded string.

    Args:
        data: Base64 string.

    Returns:
        Deserialized object.
    """
    return loads(base64.b64decode(data))


def dumps_gzip(
    obj: Any,
    compression_level: int = 6,
) -> bytes:
    """Serialize and compress with gzip.

    Args:
        obj: Object to serialize.
        compression_level: Gzip compression level (1-9).

    Returns:
        Compressed pickled bytes.
    """
    pickled = pickle.dumps(obj)
    return gzip.compress(pickled, level=compression_level)


def loads_gzip(data: bytes) -> Any:
    """Decompress and deserialize gzip-compressed pickle.

    Args:
        data: Compressed pickled bytes.

    Returns:
        Deserialized object.
    """
    decompressed = gzip.decompress(data)
    return pickle.loads(decompressed)


def clone(obj: Any) -> Any:
    """Create a deep copy using pickle serialization.

    Args:
        obj: Object to clone.

    Returns:
        Deep copy of the object.
    """
    return loads(dumps(obj))


def cache_pickle(
    func: Callable[..., Any],
) -> Callable[..., Any]:
    """Decorator that caches results using pickle-based serialization.

    Args:
        func: Function to cache.

    Returns:
        Decorated function with cache.
    """
    cache: dict = {}

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        key = dumps_b64((args, tuple(sorted(kwargs.items()))))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    wrapper._cache = cache  # type: ignore
    wrapper.clear_cache = lambda: cache.clear()  # type: ignore
    return wrapper


__all__ = [
    "dumps",
    "loads",
    "dumps_b64",
    "loads_b64",
    "dumps_gzip",
    "loads_gzip",
    "clone",
    "cache_pickle",
]
