"""API Formatter Action Module.

Provides request/response formatting, serialization,
content-type handling, and encoding conversion.
"""
from __future__ import annotations

import json
import base64
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class ContentType(Enum):
    """Content type."""
    JSON = "application/json"
    FORM = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data"
    XML = "application/xml"
    TEXT = "text/plain"
    BINARY = "application/octet-stream"


@dataclass
class FormatConfig:
    """Format configuration."""
    content_type: ContentType = ContentType.JSON
    encoding: str = "utf-8"
    indent: Optional[int] = 2
    base64_encode_binary: bool = False
    date_format: str = "%Y-%m-%dT%H:%M:%S"


class APIFormatterAction:
    """Request/response formatter.

    Example:
        formatter = APIFormatterAction()

        formatted = formatter.format_request({
            "data": b"binary_content",
            "date": datetime.now()
        }, FormatConfig(content_type=ContentType.JSON))

        response = formatter.parse_response(
            response_text,
            ContentType.JSON
        )
    """

    def __init__(self) -> None:
        self._encoders: Dict[type, Callable] = {}
        self._decoders: Dict[ContentType, Callable] = {}

    def register_encoder(
        self,
        type: type,
        encoder: Callable[[Any], Any],
    ) -> None:
        """Register custom encoder for type."""
        self._encoders[type] = encoder

    def format_request(
        self,
        data: Any,
        config: FormatConfig,
    ) -> Union[str, bytes, Dict]:
        """Format request data.

        Args:
            data: Request data
            config: Format configuration

        Returns:
            Formatted data
        """
        if config.content_type == ContentType.JSON:
            return self._format_json(data, config)

        elif config.content_type == ContentType.FORM:
            return self._format_form(data, config)

        elif config.content_type == ContentType.MULTIPART:
            return self._format_multipart(data, config)

        elif config.content_type == ContentType.XML:
            return self._format_xml(data, config)

        elif config.content_type == ContentType.TEXT:
            return self._format_text(data, config)

        elif config.content_type == ContentType.BINARY:
            return self._format_binary(data, config)

        return data

    def parse_response(
        self,
        data: Union[str, bytes],
        content_type: ContentType,
    ) -> Any:
        """Parse response data.

        Args:
            data: Response data
            content_type: Content type of response

        Returns:
            Parsed data
        """
        try:
            if content_type == ContentType.JSON:
                return self._parse_json(data)

            elif content_type == ContentType.FORM:
                return self._parse_form(data)

            elif content_type == ContentType.XML:
                return self._parse_xml(data)

            elif content_type == ContentType.TEXT:
                return self._parse_text(data)

            else:
                return data

        except Exception as e:
            logger.error(f"Parse error: {e}")
            return data

    def _format_json(self, data: Any, config: FormatConfig) -> str:
        """Format as JSON."""
        formatted = self._encode_values(data, config)
        if config.indent:
            return json.dumps(formatted, indent=config.indent, ensure_ascii=False)
        return json.dumps(formatted, ensure_ascii=False)

    def _format_form(self, data: Dict, config: FormatConfig) -> str:
        """Format as form data."""
        encoded = self._encode_values(data, config)
        pairs = []
        for key, value in encoded.items():
            pairs.append(f"{key}={value}")
        return "&".join(pairs)

    def _format_multipart(self, data: Dict, config: FormatConfig) -> Dict:
        """Format as multipart form."""
        return self._encode_values(data, config)

    def _format_xml(self, data: Any, config: FormatConfig) -> str:
        """Format as XML."""
        encoded = self._encode_values(data, config)
        return self._dict_to_xml(encoded)

    def _format_text(self, data: Any, config: FormatConfig) -> str:
        """Format as text."""
        return str(data)

    def _format_binary(self, data: Any, config: FormatConfig) -> bytes:
        """Format binary data."""
        if isinstance(data, bytes):
            return data
        return str(data).encode(config.encoding)

    def _parse_json(self, data: Union[str, bytes]) -> Any:
        """Parse JSON."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def _parse_form(self, data: Union[str, bytes]) -> Dict:
        """Parse form data."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        result = {}
        for pair in data.split("&"):
            if "=" in pair:
                key, value = pair.split("=", 1)
                result[key] = value
        return result

    def _parse_xml(self, data: Union[str, bytes]) -> Any:
        """Parse XML (simplified)."""
        return {"raw": data.decode("utf-8") if isinstance(data, bytes) else data}

    def _parse_text(self, data: Union[str, bytes]) -> str:
        """Parse text."""
        if isinstance(data, bytes):
            return data.decode("utf-8")
        return data

    def _encode_values(self, data: Any, config: FormatConfig) -> Any:
        """Encode values for formatting."""
        from datetime import datetime

        if isinstance(data, dict):
            return {k: self._encode_values(v, config) for k, v in data.items()}

        if isinstance(data, list):
            return [self._encode_values(item, config) for item in data]

        if isinstance(data, datetime):
            return data.strftime(config.date_format)

        if isinstance(data, bytes):
            if config.base64_encode_binary:
                return base64.b64encode(data).decode(config.encoding)
            return data

        for target_type, encoder in self._encoders.items():
            if isinstance(data, target_type):
                return encoder(data)

        return data

    def _dict_to_xml(self, data: Dict, root: str = "root") -> str:
        """Convert dict to XML string."""
        lines = [f"<{root}>"]
        for key, value in data.items():
            if isinstance(value, dict):
                lines.append(self._dict_to_xml(value, key))
            elif isinstance(value, list):
                for item in value:
                    lines.append(f"<{key}>{item}</{key}>")
            else:
                lines.append(f"<{key}>{value}</{key}>")
        lines.append(f"</{root}>")
        return "\n".join(lines)
