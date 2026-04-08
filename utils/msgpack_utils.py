"""
MessagePack serialization utilities.

Provides MessagePack encoding and decoding with support for
custom types, streaming, and schema evolution.

Example:
    >>> from utils.msgpack_utils import pack, unpack
    >>> data = pack({"key": [1, 2, 3]})
    >>> original = unpack(data)
"""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

try:
    import msgpack
except ImportError:
    msgpack = None  # type: ignore


class MsgPackHandler:
    """
    MessagePack encoder and decoder with extensibility.

    Supports custom type encoding/decoding, streaming operations,
    and compatibility options.

    Attributes:
        strict: Enable strict mode (no str decoding).
        raw: Keep bytes as raw type.
    """

    def __init__(
        self,
        strict: bool = False,
        raw: bool = False,
        datetime_format: str = "iso",
    ) -> None:
        """
        Initialize the MessagePack handler.

        Args:
            strict: Enable strict parsing.
            raw: Keep bytes as raw type.
            datetime_format: Format for datetime encoding ('iso' or 'timestamp').
        """
        self.strict = strict
        self.raw = raw
        self.datetime_format = datetime_format
        self._ext_types: Dict[int, type] = {}
        self._ext_encoders: Dict[type, int] = {}

    def pack(self, data: Any) -> bytes:
        """
        Encode data to MessagePack bytes.

        Args:
            data: Python object to encode.

        Returns:
            MessagePack-encoded bytes.

        Raises:
            ImportError: If msgpack is not installed.
        """
        if msgpack is None:
            raise ImportError("msgpack is required. Install with: pip install msgpack")

        def default_handler(obj: Any) -> Any:
            if isinstance(obj, datetime.datetime):
                if self.datetime_format == "iso":
                    return {"__datetime__": obj.isoformat()}
                return {"__timestamp__": obj.timestamp()}
            if isinstance(obj, datetime.date):
                return {"__date__": obj.isoformat()}
            if isinstance(obj, uuid.UUID):
                return {"__uuid__": str(obj)}
            if isinstance(obj, Decimal):
                return {"__decimal__": str(obj)}
            if isinstance(obj, bytes):
                return {"__bytes__": obj.hex()}
            if isinstance(obj, set):
                return {"__set__": list(obj)}
            if isinstance(obj, frozenset):
                return {"__frozenset__": list(obj)}
            raise TypeError(f"Object of type {type(obj)} is not serializable")

        kwargs: Dict[str, Any] = {
            "default": default_handler,
            "use_bin_type": True,
        }

        return msgpack.packb(data, **kwargs)

    def unpack(self, data: bytes) -> Any:
        """
        Decode MessagePack bytes to Python object.

        Args:
            data: MessagePack-encoded bytes.

        Returns:
            Decoded Python object.

        Raises:
            ImportError: If msgpack is not installed.
        """
        if msgpack is None:
            raise ImportError("msgpack is required. Install with: pip install msgpack")

        def object_hook(obj: Dict[str, Any]) -> Any:
            if "__datetime__" in obj:
                return datetime.datetime.fromisoformat(obj["__datetime__"])
            if "__timestamp__" in obj:
                return datetime.datetime.fromtimestamp(obj["__timestamp__"])
            if "__date__" in obj:
                return datetime.date.fromisoformat(obj["__date__"])
            if "__uuid__" in obj:
                return uuid.UUID(obj["__uuid__"])
            if "__decimal__" in obj:
                return Decimal(obj["__decimal__"])
            if "__bytes__" in obj:
                return bytes.fromhex(obj["__bytes__"])
            if "__set__" in obj:
                return set(obj["__set__"])
            if "__frozenset__" in obj:
                return frozenset(obj["__frozenset__"])
            return obj

        kwargs: Dict[str, Any] = {
            "raw": self.raw,
            "object_hook": object_hook,
        }

        if self.strict:
            kwargs["strict_map_key"] = False
            kwargs["strict_str"] = True

        return msgpack.unpackb(data, **kwargs)

    def pack_file(self, path: str, data: Any) -> None:
        """
        Pack data to a MessagePack file.

        Args:
            path: Destination file path.
            data: Python object to encode.
        """
        with open(path, "wb") as f:
            f.write(self.pack(data))

    def unpack_file(self, path: str) -> Any:
        """
        Unpack data from a MessagePack file.

        Args:
            path: Source file path.

        Returns:
            Decoded Python object.
        """
        with open(path, "rb") as f:
            return self.unpack(f.read())

    def pack_stream(
        self,
        items: List[Any]
    ) -> List[bytes]:
        """
        Pack multiple items as separate MessagePack objects.

        Args:
            items: List of objects to pack.

        Returns:
            List of packed bytes.
        """
        return [self.pack(item) for item in items]

    def unpack_stream(
        self,
        data: bytes,
        raw: bool = False
    ) -> List[Any]:
        """
        Unpack multiple MessagePack objects from a stream.

        Args:
            data: Concatenated MessagePack bytes.
            raw: Return raw MessagePack unpacker.

        Returns:
            List of decoded objects.
        """
        if msgpack is None:
            raise ImportError("msgpack is required")

        results: List[Any] = []
        unpacker = msgpack.Unpacker(
            raw=raw,
            object_hook=None,
        )
        unpacker.feed(data)

        for item in unpacker:
            results.append(item)

        return results


def pack(data: Any) -> bytes:
    """
    Convenience function to pack data.

    Args:
        data: Python object to encode.

    Returns:
        MessagePack-encoded bytes.
    """
    return MsgPackHandler().pack(data)


def unpack(data: bytes) -> Any:
    """
    Convenience function to unpack data.

    Args:
        data: MessagePack-encoded bytes.

    Returns:
        Decoded Python object.
    """
    return MsgPackHandler().unpack(data)


def pack_file(path: str, data: Any) -> None:
    """
    Convenience function to pack data to file.

    Args:
        path: Destination file path.
        data: Python object to encode.
    """
    MsgPackHandler().pack_file(path, data)


def unpack_file(path: str) -> Any:
    """
    Convenience function to unpack data from file.

    Args:
        path: Source file path.

    Returns:
        Decoded Python object.
    """
    return MsgPackHandler().unpack_file(path)
