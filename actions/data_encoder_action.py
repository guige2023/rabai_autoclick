"""
Data Encoder Action Module.

Provides encoding and decoding utilities for various formats
including JSON, MessagePack, Protocol Buffers, and custom encodings.

Author: rabai_autoclick team
"""

import json
import base64
import logging
from typing import (
    Optional, Dict, Any, List, Union, Callable, Type, TypeVar
)
from dataclasses import dataclass, is_dataclass, asdict
from enum import Enum
from datetime import datetime, date
from decimal import Decimal

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EncodingFormat(Enum):
    """Supported encoding formats."""
    JSON = "json"
    MSGPACK = "msgpack"
    CBOR = "cbor"
    UBJSON = "ubjson"
    CUSTOM = "custom"


@dataclass
class EncoderConfig:
    """Configuration for encoding operations."""
    format: EncodingFormat = EncodingFormat.JSON
    indent: Optional[int] = None
    ensure_ascii: bool = False
    use_base64: bool = False
    datetime_format: str = "iso"
    decimal_precision: Optional[int] = None
    custom_encoders: Dict[type, Callable] = field(default_factory=dict)


class DataEncoderAction:
    """
    Data Encoding and Decoding Engine.

    Supports multiple serialization formats with extensibility
    for custom types and encoding strategies.

    Example:
        >>> encoder = DataEncoderAction()
        >>> encoded = encoder.encode({"key": "value"}, format=EncodingFormat.JSON)
        >>> decoded = encoder.decode(encoded)
    """

    def __init__(self, config: Optional[EncoderConfig] = None):
        self.config = config or EncoderConfig()
        self._register_default_encoders()

    def _register_default_encoders(self) -> None:
        """Register default type encoders."""
        self.config.custom_encoders[datetime] = self._encode_datetime
        self.config.custom_encoders[date] = self._encode_date
        self.config.custom_encoders[Decimal] = self._encode_decimal
        self.config.custom_encoders[bytes] = self._encode_bytes

    def _encode_datetime(self, value: datetime) -> str:
        """Encode datetime to string."""
        return value.strftime(self.config.datetime_format)

    def _encode_date(self, value: date) -> str:
        """Encode date to string."""
        return value.isoformat()

    def _encode_decimal(self, value: Decimal) -> Union[float, str]:
        """Encode Decimal to float or string."""
        if self.config.decimal_precision is not None:
            return round(float(value), self.config.decimal_precision)
        return str(value)

    def _encode_bytes(self, value: bytes) -> str:
        """Encode bytes to base64 string."""
        return base64.b64encode(value).decode("ascii")

    def _decode_datetime(self, value: str) -> datetime:
        """Decode string to datetime."""
        if self.config.datetime_format == "iso":
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.strptime(value, self.config.datetime_format)

    def _decode_bytes(self, value: str) -> bytes:
        """Decode base64 string to bytes."""
        return base64.b64decode(value)

    def _prepare_data(self, data: Any) -> Any:
        """Prepare data for encoding."""
        if is_dataclass(data) and not isinstance(data, type):
            data = asdict(data)
        elif hasattr(data, "__dict__"):
            data = data.__dict__
        return data

    def _serialize_value(self, value: Any) -> Any:
        """Serialize a single value using registered encoders."""
        if value is None:
            return None

        value_type = type(value)

        if value_type in self.config.custom_encoders:
            encoder = self.config.custom_encoders[value_type]
            return encoder(value)

        if isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, (list, tuple)):
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, (int, float, bool, str)):
            return value
        else:
            return str(value)

    def encode(
        self,
        data: Any,
        format: Optional[EncodingFormat] = None,
    ) -> Union[str, bytes]:
        """
        Encode data to specified format.

        Args:
            data: Data to encode
            format: Encoding format (uses config default if None)

        Returns:
            Encoded data as string or bytes
        """
        fmt = format or self.config.format
        prepared = self._prepare_data(data)
        serialized = self._serialize_value(prepared)

        if fmt == EncodingFormat.JSON:
            return self._encode_json(serialized)
        elif fmt == EncodingFormat.MSGPACK:
            return self._encode_msgpack(serialized)
        elif fmt == EncodingFormat.CBOR:
            return self._encode_cbor(serialized)
        elif fmt == EncodingFormat.UBJSON:
            return self._encode_ubjson(serialized)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def _encode_json(self, data: Any) -> str:
        """Encode data to JSON string."""
        kwargs = {
            "ensure_ascii": self.config.ensure_ascii,
            "indent": self.config.indent,
            "default": str,
        }
        return json.dumps(data, **kwargs)

    def _encode_msgpack(self, data: Any) -> bytes:
        """Encode data to MessagePack."""
        try:
            import msgpack
            return msgpack.packb(data, use_bin_type=True)
        except ImportError:
            logger.warning("msgpack not available, falling back to JSON")
            return json.dumps(data).encode("utf-8")

    def _encode_cbor(self, data: Any) -> bytes:
        """Encode data to CBOR."""
        try:
            import cbor2
            return cbor2.dumps(data)
        except ImportError:
            logger.warning("cbor2 not available, falling back to JSON")
            return json.dumps(data).encode("utf-8")

    def _encode_ubjson(self, data: Any) -> bytes:
        """Encode data to UBJSON."""
        try:
            import ubjson
            return ubjson.dumpb(data)
        except ImportError:
            logger.warning("ubjson not available, falling back to JSON")
            return json.dumps(data).encode("utf-8")

    def decode(
        self,
        data: Union[str, bytes],
        format: Optional[EncodingFormat] = None,
        target_type: Optional[Type[T]] = None,
    ) -> Any:
        """
        Decode data from specified format.

        Args:
            data: Encoded data
            format: Encoding format (uses config default if None)
            target_type: Optional target type to deserialize to

        Returns:
            Decoded data
        """
        fmt = format or self.config.format

        if fmt == EncodingFormat.JSON:
            decoded = self._decode_json(data)
        elif fmt == EncodingFormat.MSGPACK:
            decoded = self._decode_msgpack(data)
        elif fmt == EncodingFormat.CBOR:
            decoded = self._decode_cbor(data)
        elif fmt == EncodingFormat.UBJSON:
            decoded = self._decode_ubjson(data)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        return self._deserialize_value(decoded)

    def _decode_json(self, data: Union[str, bytes]) -> Any:
        """Decode JSON string to data."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def _decode_msgpack(self, data: bytes) -> Any:
        """Decode MessagePack to data."""
        try:
            import msgpack
            return msgpack.unpackb(data, raw=False)
        except ImportError:
            logger.warning("msgpack not available")
            return json.loads(data.decode("utf-8"))

    def _decode_cbor(self, data: bytes) -> Any:
        """Decode CBOR to data."""
        try:
            import cbor2
            return cbor2.loads(data)
        except ImportError:
            logger.warning("cbor2 not available")
            return json.loads(data.decode("utf-8"))

    def _decode_ubjson(self, data: bytes) -> Any:
        """Decode UBJSON to data."""
        try:
            import ubjson
            return ubjson.loadb(data)
        except ImportError:
            logger.warning("ubjson not available")
            return json.loads(data.decode("utf-8"))

    def _deserialize_value(self, value: Any) -> Any:
        """Deserialize a single value."""
        if value is None:
            return None

        if isinstance(value, dict):
            return {k: self._deserialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._deserialize_value(item) for item in value]
        elif isinstance(value, str):
            return self._maybe_decode_special(value)
        else:
            return value

    def _maybe_decode_special(self, value: str) -> Any:
        """Try to decode special string formats."""
        if self.config.use_base64:
            try:
                if len(value) % 4 == 0:
                    return self._decode_bytes(value)
            except Exception:
                pass

        return value

    def encode_to_base64(
        self,
        data: Any,
        format: Optional[EncodingFormat] = None,
    ) -> str:
        """
        Encode data to base64 string.

        Args:
            data: Data to encode
            format: Encoding format

        Returns:
            Base64 encoded string
        """
        encoded = self.encode(data, format)
        if isinstance(encoded, str):
            encoded = encoded.encode("utf-8")
        return base64.b64encode(encoded).decode("ascii")

    def decode_from_base64(
        self,
        data: str,
        format: Optional[EncodingFormat] = None,
    ) -> Any:
        """
        Decode data from base64 string.

        Args:
            data: Base64 encoded string
            format: Encoding format

        Returns:
            Decoded data
        """
        decoded = base64.b64decode(data)
        return self.decode(decoded, format)

    def register_encoder(
        self,
        type_: type,
        encoder: Callable[[Any], Any],
    ) -> None:
        """
        Register a custom encoder for a type.

        Args:
            type_: Type to encode
            encoder: Encoder function
        """
        self.config.custom_encoders[type_] = encoder

    def register_decoder(
        self,
        type_: type,
        decoder: Callable[[Any], Any],
    ) -> None:
        """
        Register a custom decoder for a type.

        Args:
            type_: Type to decode
            decoder: Decoder function
        """
        self._custom_decoders[type_] = decoder

    def encode_batch(
        self,
        data_list: List[Any],
        format: Optional[EncodingFormat] = None,
    ) -> List[Union[str, bytes]]:
        """
        Encode a batch of data items.

        Args:
            data_list: List of data items
            format: Encoding format

        Returns:
            List of encoded items
        """
        return [self.encode(item, format) for item in data_list]

    def decode_batch(
        self,
        data_list: List[Union[str, bytes]],
        format: Optional[EncodingFormat] = None,
    ) -> List[Any]:
        """
        Decode a batch of encoded items.

        Args:
            data_list: List of encoded items
            format: Encoding format

        Returns:
            List of decoded items
        """
        return [self.decode(item, format) for item in data_list]
