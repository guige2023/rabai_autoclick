"""Data encoding action for encoding and decoding data.

Provides various encoding schemes including base64,
URL encoding, and custom transformations.
"""

import base64
import json
import logging
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EncodingType(Enum):
    BASE64 = "base64"
    URL = "url"
    JSON = "json"
    HEX = "hex"
    UTF8 = "utf8"
    CUSTOM = "custom"


@dataclass
class EncodingConfig:
    encoding: EncodingType
    custom_encoder: Optional[Callable[[str], str]] = None
    custom_decoder: Optional[Callable[[str], str]] = None


class DataEncodingAction:
    """Encode and decode data using various schemes.

    Args:
        default_encoding: Default encoding type.
    """

    def __init__(
        self,
        default_encoding: EncodingType = EncodingType.BASE64,
    ) -> None:
        self._default_encoding = default_encoding
        self._encoding_configs: dict[str, EncodingConfig] = {}
        self._stats = {
            "encode_count": 0,
            "decode_count": 0,
            "encode_errors": 0,
            "decode_errors": 0,
        }

    def encode(
        self,
        data: str,
        encoding: Optional[EncodingType] = None,
        custom_encoder: Optional[Callable[[str], str]] = None,
    ) -> str:
        """Encode a string.

        Args:
            data: String to encode.
            encoding: Encoding type.
            custom_encoder: Custom encoder function.

        Returns:
            Encoded string.
        """
        encoding = encoding or self._default_encoding

        try:
            if encoding == EncodingType.BASE64:
                result = base64.b64encode(data.encode()).decode()
            elif encoding == EncodingType.URL:
                result = urllib.parse.quote(data)
            elif encoding == EncodingType.HEX:
                result = data.encode().hex()
            elif encoding == EncodingType.UTF8:
                result = data.encode("utf-8").decode("utf-8")
            elif encoding == EncodingType.CUSTOM and custom_encoder:
                result = custom_encoder(data)
            else:
                result = data

            self._stats["encode_count"] += 1
            return result

        except Exception as e:
            self._stats["encode_errors"] += 1
            logger.error(f"Encoding error: {e}")
            raise

    def decode(
        self,
        data: str,
        encoding: Optional[EncodingType] = None,
        custom_decoder: Optional[Callable[[str], str]] = None,
    ) -> str:
        """Decode a string.

        Args:
            data: String to decode.
            encoding: Encoding type.
            custom_decoder: Custom decoder function.

        Returns:
            Decoded string.
        """
        encoding = encoding or self._default_encoding

        try:
            if encoding == EncodingType.BASE64:
                result = base64.b64decode(data.encode()).decode()
            elif encoding == EncodingType.URL:
                result = urllib.parse.unquote(data)
            elif encoding == EncodingType.HEX:
                result = bytes.fromhex(data).decode()
            elif encoding == EncodingType.UTF8:
                result = data
            elif encoding == EncodingType.CUSTOM and custom_decoder:
                result = custom_decoder(data)
            else:
                result = data

            self._stats["decode_count"] += 1
            return result

        except Exception as e:
            self._stats["decode_errors"] += 1
            logger.error(f"Decoding error: {e}")
            raise

    def encode_dict(
        self,
        data: dict[str, Any],
        encoding: Optional[EncodingType] = None,
    ) -> str:
        """Encode a dictionary as JSON then encode the string.

        Args:
            data: Dictionary to encode.
            encoding: Encoding type.

        Returns:
            Encoded string.
        """
        json_str = json.dumps(data)
        return self.encode(json_str, encoding)

    def decode_dict(
        self,
        data: str,
        encoding: Optional[EncodingType] = None,
    ) -> dict[str, Any]:
        """Decode a string and parse as JSON dictionary.

        Args:
            data: String to decode.
            encoding: Encoding type.

        Returns:
            Decoded dictionary.
        """
        decoded = self.decode(data, encoding)
        return json.loads(decoded)

    def batch_encode(
        self,
        data_list: list[str],
        encoding: Optional[EncodingType] = None,
    ) -> list[str]:
        """Encode multiple strings.

        Args:
            data_list: List of strings to encode.
            encoding: Encoding type.

        Returns:
            List of encoded strings.
        """
        return [self.encode(d, encoding) for d in data_list]

    def batch_decode(
        self,
        data_list: list[str],
        encoding: Optional[EncodingType] = None,
    ) -> list[str]:
        """Decode multiple strings.

        Args:
            data_list: List of strings to decode.
            encoding: Encoding type.

        Returns:
            List of decoded strings.
        """
        return [self.decode(d, encoding) for d in data_list]

    def register_encoding(
        self,
        name: str,
        encoder: Callable[[str], str],
        decoder: Callable[[str], str],
    ) -> bool:
        """Register a custom encoding scheme.

        Args:
            name: Encoding name.
            encoder: Encoder function.
            decoder: Decoder function.

        Returns:
            True if registered.
        """
        self._encoding_configs[name] = EncodingConfig(
            encoding=EncodingType.CUSTOM,
            custom_encoder=encoder,
            custom_decoder=decoder,
        )
        return True

    def get_stats(self) -> dict[str, Any]:
        """Get encoding statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            **self._stats,
            "default_encoding": self._default_encoding.value,
            "registered_encodings": len(self._encoding_configs),
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._stats = {
            "encode_count": 0,
            "decode_count": 0,
            "encode_errors": 0,
            "decode_errors": 0,
        }
