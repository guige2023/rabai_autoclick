"""
CBOR (Concise Binary Object Representation) serialization utilities.

Provides CBOR encoding and decoding with support for
semantic tagging, shared references, and indefinite length items.

Example:
    >>> from utils.cbor_utils import CborHandler, dumps, loads
    >>> handler = CborHandler()
    >>> encoded = handler.encode({"key": "value"})
    >>> decoded = handler.decode(encoded)
"""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

try:
    import cbor2
except ImportError:
    cbor2 = None  # type: ignore


class CborHandler:
    """
    CBOR encoder and decoder with semantic tag support.

    Supports date/time, UUID, decimal, and URI semantic tags
    along with shared references and indefinite length data.

    Attributes:
        date_as_timestamp: Encode dates as Unix timestamps.
        datetime_as_timestamp: Encode datetimes as Unix timestamps.
        timezone: Timezone for datetime encoding.
    """

    def __init__(
        self,
        date_as_timestamp: bool = True,
        datetime_as_timestamp: bool = False,
        timezone: Optional[datetime.timezone] = None,
    ) -> None:
        """
        Initialize the CBOR handler.

        Args:
            date_as_timestamp: Encode dates as Unix timestamps.
            datetime_as_timestamp: Encode datetimes as Unix timestamps.
            timezone: Timezone for datetime encoding.
        """
        self.date_as_timestamp = date_as_timestamp
        self.datetime_as_timestamp = datetime_as_timestamp
        self.timezone = timezone or datetime.timezone.utc

    def encode(self, data: Any) -> bytes:
        """
        Encode data to CBOR bytes.

        Args:
            data: Python object to encode.

        Returns:
            CBOR-encoded bytes.

        Raises:
            ImportError: If cbor2 is not installed.
        """
        if cbor2 is None:
            raise ImportError("cbor2 is required. Install with: pip install cbor2")

        def default_handler(obj: Any) -> Any:
            if isinstance(obj, datetime.datetime):
                if self.datetime_as_timestamp:
                    return cbor2.CBORTag(1, obj.timestamp())
                return cbor2.CBORTag(100, obj.isoformat())
            if isinstance(obj, datetime.date):
                if self.date_as_timestamp:
                    return cbor2.CBORTag(100, obj.isoformat())
                return cbor2.CBORTag(100, obj.isoformat())
            if isinstance(obj, datetime.time):
                return cbor2.CBORTag(1004, obj.isoformat())
            if isinstance(obj, uuid.UUID):
                return cbor2.CBORTag(37, str(obj))
            if isinstance(obj, Decimal):
                return cbor2.CBORTag(6, {"decimal": str(obj)})
            if isinstance(obj, (set, frozenset)):
                return cbor2.CBORTag(258, list(obj))
            raise TypeError(f"Object of type {type(obj)} is not serializable")

        return cbor2.dumps(
            data,
            default=default_handler,
            timezone=self.timezone,
        )

    def decode(self, data: bytes) -> Any:
        """
        Decode CBOR bytes to Python object.

        Args:
            data: CBOR-encoded bytes.

        Returns:
            Decoded Python object.

        Raises:
            ImportError: If cbor2 is not installed.
        """
        if cbor2 is None:
            raise ImportError("cbor2 is required. Install with: pip install cbor2")

        def object_hook(obj: cbor2.CBORTag) -> Any:
            if obj.tag == 1 and isinstance(obj.data, (int, float)):
                return datetime.datetime.fromtimestamp(obj.data, tz=self.timezone)
            if obj.tag == 37 and isinstance(obj.data, (str, bytes)):
                return uuid.UUID(str(obj.data))
            if obj.tag == 100 and isinstance(obj.data, str):
                try:
                    return datetime.datetime.fromisoformat(obj.data)
                except ValueError:
                    return obj.data
            if obj.tag == 258 and isinstance(obj.data, list):
                return set(obj.data)
            return obj

        return cbor2.loads(data, object_hook=object_hook)

    def encode_indefinite(
        self,
        data: List[Any]
    ) -> bytes:
        """
        Encode data as indefinite length array.

        Args:
            data: List of objects to encode.

        Returns:
            CBOR-encoded bytes with indefinite length marker.
        """
        if cbor2 is None:
            raise ImportError("cbor2 is required")

        chunks: List[bytes] = []
        for item in data:
            chunks.append(cbor2.dumps(item))
        chunks.append(cbor2.dumps(None, major_type=7))

        return b"".join(chunks)

    def decode_stream(self, data: bytes) -> List[Any]:
        """
        Decode multiple CBOR items from a stream.

        Args:
            data: Concatenated CBOR bytes.

        Returns:
            List of decoded objects.
        """
        if cbor2 is None:
            raise ImportError("cbor2 is required")

        results: List[Any] = []
        decoder = cbor2.CBORDecoder(open_bytes_stream(data))
        try:
            while True:
                results.append(decoder.decode())
        except EOFError:
            pass
        return results

    def encode_shared_reference(
        self,
        data: Any,
        refs: Dict[int, Any]
    ) -> bytes:
        """
        Encode data with shared references.

        Args:
            data: Object to encode.
            refs: Dictionary of reference ID to object.

        Returns:
            CBOR-encoded bytes with shared references.
        """
        if cbor2 is None:
            raise ImportError("cbor2 is required")

        return cbor2.dumps(data, shareable_index=refs)

    def encode_to_hex(self, data: Any) -> str:
        """
        Encode data and return as hex string.

        Args:
            data: Python object to encode.

        Returns:
            Hex string of encoded bytes.
        """
        return self.encode(data).hex()

    def decode_from_hex(self, hex_data: str) -> Any:
        """
        Decode from a hex string.

        Args:
            hex_data: Hex string of CBOR data.

        Returns:
            Decoded Python object.
        """
        return self.decode(bytes.fromhex(hex_data))


class open_bytes_stream:
    """Helper class to create a file-like object from bytes."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            result = self.data[self.pos :]
            self.pos = len(self.data)
        else:
            result = self.data[self.pos : self.pos + size]
            self.pos += size
        return result


def dumps(data: Any) -> bytes:
    """
    Convenience function to encode data to CBOR.

    Args:
        data: Python object to encode.

    Returns:
        CBOR-encoded bytes.
    """
    return CborHandler().encode(data)


def loads(data: bytes) -> Any:
    """
    Convenience function to decode CBOR data.

    Args:
        data: CBOR-encoded bytes.

    Returns:
        Decoded Python object.
    """
    return CborHandler().decode(data)


def dump_file(path: str, data: Any) -> None:
    """
    Convenience function to write CBOR data to file.

    Args:
        path: Destination file path.
        data: Python object to encode.
    """
    with open(path, "wb") as f:
        f.write(CborHandler().encode(data))


def load_file(path: str) -> Any:
    """
    Convenience function to load CBOR data from file.

    Args:
        path: Source file path.

    Returns:
        Decoded Python object.
    """
    with open(path, "rb") as f:
        return CborHandler().decode(f.read())
