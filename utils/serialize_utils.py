"""Serialization utilities for object conversion.

Provides serialization to/from JSON, pickle, and other
formats for data persistence and IPC.
"""

import base64
import json
import pickle
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Type
from enum import Enum


class JSONEncoder(json.JSONEncoder):
    """Extended JSON encoder with additional type support."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Enum):
            return {"__enum__": obj.__class__.__name__, "value": obj.value}
        if isinstance(obj, (date, datetime)):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, Decimal):
            return {"__decimal__": str(obj)}
        if isinstance(obj, bytes):
            return {"__bytes__": base64.b64encode(obj).decode()}
        if isinstance(obj, set):
            return {"__set__": list(obj)}
        try:
            return super().default(obj)
        except TypeError:
            return {"__pickle__": base64.b64encode(pickle.dumps(obj)).decode()}


def json_dumps(obj: Any, **kwargs: Any) -> str:
    """Serialize object to JSON string.

    Args:
        obj: Object to serialize.
        **kwargs: Additional JSON encoder kwargs.

    Returns:
        JSON string.
    """
    return json.dumps(obj, cls=JSONEncoder, **kwargs)


def json_loads(s: str) -> Any:
    """Deserialize JSON string to object.

    Args:
        s: JSON string.

    Returns:
        Deserialized object.
    """
    return json.loads(s, object_hook=_json_object_hook)


def _json_object_hook(d: Dict[str, Any]) -> Any:
    if "__enum__" in d:
        return d["__enum__"]
    if "__datetime__" in d:
        return datetime.fromisoformat(d["__datetime__"])
    if "__decimal__" in d:
        return Decimal(d["__decimal__"])
    if "__bytes__" in d:
        return base64.b64decode(d["__bytes__"])
    if "__set__" in d:
        return set(d["__set__"])
    if "__pickle__" in d:
        return pickle.loads(base64.b64decode(d["__pickle__"]))
    return d


def to_json(obj: Any, indent: Optional[int] = 2) -> str:
    """Convert object to formatted JSON.

    Args:
        obj: Object to convert.
        indent: Indentation spaces.

    Returns:
        Formatted JSON string.
    """
    return json.dumps(obj, cls=JSONEncoder, indent=indent)


def from_json(s: str) -> Any:
    """Parse JSON string to object.

    Args:
        s: JSON string.

    Returns:
        Parsed object.
    """
    return json.loads(s, object_hook=_json_object_hook)


def to_pickle(obj: Any) -> bytes:
    """Serialize object to pickle bytes.

    Args:
        obj: Object to serialize.

    Returns:
        Pickle bytes.
    """
    return pickle.dumps(obj)


def from_pickle(data: bytes) -> Any:
    """Deserialize pickle bytes.

    Args:
        data: Pickle bytes.

    Returns:
        Deserialized object.
    """
    return pickle.loads(data)


def to_pickle_b64(obj: Any) -> str:
    """Serialize object to base64-encoded pickle.

    Args:
        obj: Object to serialize.

    Returns:
        Base64 string.
    """
    return base64.b64encode(pickle.dumps(obj)).decode()


def from_pickle_b64(s: str) -> Any:
    """Deserialize base64-encoded pickle.

    Args:
        s: Base64 string.

    Returns:
        Deserialized object.
    """
    return pickle.loads(base64.b64decode(s))


class ObjectRegistry(Generic[T]):
    """Registry for serializable objects with type tags."""

    def __init__(self) -> None:
        self._encoders: Dict[str, Callable[[Any], Dict[str, Any]]] = {}
        self._decoders: Dict[str, Callable[[Dict[str, Any]], Any]] = {}

    def register(
        self,
        cls: Type[T],
        encoder: Callable[[T], Dict[str, Any]],
        decoder: Callable[[Dict[str, Any]], T],
    ) -> None:
        """Register encoder/decoder for a class.

        Args:
            cls: Class type.
            encoder: Function to serialize to dict.
            decoder: Function to deserialize from dict.
        """
        name = cls.__name__
        self._encoders[name] = encoder
        self._decoders[name] = decoder

    def serialize(self, obj: Any) -> Dict[str, Any]:
        """Serialize object with type tag.

        Args:
            obj: Object to serialize.

        Returns:
            Dict with __type__ tag.
        """
        name = obj.__class__.__name__
        if name not in self._encoders:
            raise ValueError(f"No encoder for {name}")
        return {"__type__": name, "data": self._encoders[name](obj)}

    def deserialize(self, data: Dict[str, Any]) -> Any:
        """Deserialize object with type tag.

        Args:
            data: Dict with __type__ tag.

        Returns:
            Deserialized object.
        """
        name = data.get("__type__")
        if name not in self._decoders:
            raise ValueError(f"No decoder for {name}")
        return self._decoders[name](data["data"])


from typing import Generic, TypeVar
T = TypeVar("T")
