"""Data encoding action for encoding and decoding data.

Handles various encoding schemes including Base64, URL
encoding, HTML entities, and custom transformations.
"""

import base64
import html
import logging
import quopri
import urllib.parse
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EncodingType(Enum):
    """Supported encoding types."""
    BASE64 = "base64"
    URL = "url"
    HTML = "html"
    QUOTED_PRINTABLE = "quoted_printable"
    HEX = "hex"
    Unicode = "unicode"
    XML = "xml"


@dataclass
class EncodingResult:
    """Result of encoding/decoding operation."""
    success: bool
    input_value: str
    output_value: str
    encoding_type: str
    is_encoded: bool
    error: Optional[str] = None


@dataclass
class EncodingStats:
    """Statistics for encoding operations."""
    encode_operations: int = 0
    decode_operations: int = 0
    errors: int = 0


class DataEncodingAction:
    """Encode and decode data in various formats.

    Example:
        >>> encoder = DataEncodingAction()
        >>> result = encoder.encode("hello world", EncodingType.BASE64)
        >>> decoded = encoder.decode(result.output_value, EncodingType.BASE64)
    """

    def __init__(self) -> None:
        self._stats = EncodingStats()

    def encode(
        self,
        value: str,
        encoding_type: EncodingType,
        custom_encoder: Optional[Callable[[str], str]] = None,
    ) -> EncodingResult:
        """Encode a string value.

        Args:
            value: String to encode.
            encoding_type: Type of encoding.
            custom_encoder: Optional custom encoder function.

        Returns:
            Encoding result.
        """
        try:
            self._stats.encode_operations += 1

            if custom_encoder:
                output = custom_encoder(value)
            elif encoding_type == EncodingType.BASE64:
                output = base64.b64encode(value.encode("utf-8")).decode("ascii")
            elif encoding_type == EncodingType.URL:
                output = urllib.parse.quote(value, safe="")
            elif encoding_type == EncodingType.HTML:
                output = html.escape(value)
            elif encoding_type == EncodingType.QUOTED_PRINTABLE:
                output = self._quoted_printable_encode(value)
            elif encoding_type == EncodingType.HEX:
                output = value.encode("utf-8").hex()
            elif encoding_type == EncodingType.Unicode:
                output = self._unicode_encode(value)
            elif encoding_type == EncodingType.XML:
                output = self._xml_encode(value)
            else:
                output = value

            return EncodingResult(
                success=True,
                input_value=value,
                output_value=output,
                encoding_type=encoding_type.value,
                is_encoded=True,
            )

        except Exception as e:
            self._stats.errors += 1
            logger.error(f"Encoding failed: {e}")
            return EncodingResult(
                success=False,
                input_value=value,
                output_value=value,
                encoding_type=encoding_type.value,
                is_encoded=False,
                error=str(e),
            )

    def decode(
        self,
        value: str,
        encoding_type: EncodingType,
        custom_decoder: Optional[Callable[[str], str]] = None,
    ) -> EncodingResult:
        """Decode an encoded string value.

        Args:
            value: String to decode.
            encoding_type: Type of encoding.
            custom_decoder: Optional custom decoder function.

        Returns:
            Encoding result.
        """
        try:
            self._stats.decode_operations += 1

            if custom_decoder:
                output = custom_decoder(value)
            elif encoding_type == EncodingType.BASE64:
                output = base64.b64decode(value.encode("ascii")).decode("utf-8")
            elif encoding_type == EncodingType.URL:
                output = urllib.parse.unquote(value)
            elif encoding_type == EncodingType.HTML:
                output = html.unescape(value)
            elif encoding_type == EncodingType.QUOTED_PRINTABLE:
                output = self._quoted_printable_decode(value)
            elif encoding_type == EncodingType.HEX:
                output = bytes.fromhex(value).decode("utf-8")
            elif encoding_type == EncodingType.Unicode:
                output = self._unicode_decode(value)
            elif encoding_type == EncodingType.XML:
                output = self._xml_decode(value)
            else:
                output = value

            return EncodingResult(
                success=True,
                input_value=value,
                output_value=output,
                encoding_type=encoding_type.value,
                is_encoded=False,
            )

        except Exception as e:
            self._stats.errors += 1
            logger.error(f"Decoding failed: {e}")
            return EncodingResult(
                success=False,
                input_value=value,
                output_value=value,
                encoding_type=encoding_type.value,
                is_encoded=False,
                error=str(e),
            )

    def _quoted_printable_encode(self, value: str) -> str:
        """Encode using quoted-printable.

        Args:
            value: Value to encode.

        Returns:
            Quoted-printable encoded string.
        """
        return quopri.encodestring(value.encode("utf-8")).decode("ascii")

    def _quoted_printable_decode(self, value: str) -> str:
        """Decode quoted-printable encoding.

        Args:
            value: Value to decode.

        Returns:
            Decoded string.
        """
        return quopri.decodestring(value.encode("ascii")).decode("utf-8")

    def _unicode_encode(self, value: str) -> str:
        """Encode to Unicode escape format.

        Args:
            value: Value to encode.

        Returns:
            Unicode escaped string.
        """
        result = []
        for char in value:
            if ord(char) > 127:
                result.append(f"\\u{ord(char):04x}")
            else:
                result.append(char)
        return "".join(result)

    def _unicode_decode(self, value: str) -> str:
        """Decode Unicode escape format.

        Args:
            value: Value to decode.

        Returns:
            Decoded string.
        """
        import re
        pattern = r"\\u([0-9a-fA-F]{4})"
        matches = re.findall(pattern, value)

        result = value
        for hex_code in matches:
            result = result.replace(f"\\u{hex_code}", chr(int(hex_code, 16)))

        return result

    def _xml_encode(self, value: str) -> str:
        """Encode XML special characters.

        Args:
            value: Value to encode.

        Returns:
            XML-encoded string.
        """
        return (
            value.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )

    def _xml_decode(self, value: str) -> str:
        """Decode XML entities.

        Args:
            value: Value to decode.

        Returns:
            Decoded string.
        """
        return (
            value.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&apos;", "'")
        )

    def encode_batch(
        self,
        values: list[str],
        encoding_type: EncodingType,
    ) -> list[EncodingResult]:
        """Encode multiple values.

        Args:
            values: List of values to encode.
            encoding_type: Encoding type.

        Returns:
            List of encoding results.
        """
        return [self.encode(v, encoding_type) for v in values]

    def decode_batch(
        self,
        values: list[str],
        encoding_type: EncodingType,
    ) -> list[EncodingResult]:
        """Decode multiple values.

        Args:
            values: List of values to decode.
            encoding_type: Encoding type.

        Returns:
            List of encoding results.
        """
        return [self.decode(v, encoding_type) for v in values]

    def auto_detect_and_decode(self, value: str) -> EncodingResult:
        """Attempt to auto-detect encoding and decode.

        Args:
            value: Value to decode.

        Returns:
            Encoding result with detected type.
        """
        encoding_types = [
            EncodingType.BASE64,
            EncodingType.URL,
            EncodingType.HTML,
            EncodingType.HEX,
        ]

        for enc_type in encoding_types:
            result = self.decode(value, enc_type)
            if result.success and result.output_value != value:
                result.encoding_type = f"{enc_type.value} (detected)"
                return result

        return EncodingResult(
            success=True,
            input_value=value,
            output_value=value,
            encoding_type="plain",
            is_encoded=False,
        )

    def get_stats(self) -> EncodingStats:
        """Get encoding statistics.

        Returns:
            Current statistics.
        """
        return self._stats
