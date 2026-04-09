"""API payload builder and serializer module.

This module handles construction, serialization, and validation of API request payloads
with support for multiple content types and encoding strategies.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import json
import base64
import zlib
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
import hashlib
import hmac


class ContentType(Enum):
    """Supported content types for API payloads."""
    JSON = "application/json"
    XML = "application/xml"
    FORM = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data"
    BINARY = "application/octet-stream"
    GRAPHQL = "application/graphql"


class EncodingType(Enum):
    """Payload encoding types."""
    UTF8 = "utf-8"
    GZIP = "gzip"
    DEFLATE = "deflate"
    BROTLI = "br"
    BASE64 = "base64"


@dataclass
class PayloadConfig:
    """Configuration for payload construction."""
    content_type: ContentType = ContentType.JSON
    encoding: Optional[EncodingType] = None
    include_metadata: bool = True
    include_timestamp: bool = True
    include_checksum: bool = False
    checksum_algorithm: str = "sha256"
    compress_threshold: int = 1024
    sort_keys: bool = False


@dataclass
class PayloadMetadata:
    """Metadata attached to payloads."""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    request_id: Optional[str] = None
    source: str = "rabai_autoclick"
    version: str = "1.0"
    environment: Optional[str] = None


class PayloadBuilder:
    """Builder for constructing API request payloads.
    
    Example:
        >>> builder = PayloadBuilder()
        >>> payload = builder.set_type(ContentType.JSON)\\
        ...     .add_field("user_id", "123")\\
        ...     .add_nested("profile", {"name": "Alice"})\\
        ...     .build()
        >>> print(payload)
    """
    
    def __init__(self, config: Optional[PayloadConfig] = None):
        self._config = config or PayloadConfig()
        self._fields: Dict[str, Any] = {}
        self._metadata = PayloadMetadata()
        
    def set_type(self, content_type: ContentType) -> "PayloadBuilder":
        """Set the content type for the payload."""
        self._config.content_type = content_type
        return self
    
    def set_encoding(self, encoding: Optional[EncodingType]) -> "PayloadBuilder":
        """Set the encoding type."""
        self._config.encoding = encoding
        return self
    
    def add_field(self, key: str, value: Any) -> "PayloadBuilder":
        """Add a field to the payload."""
        self._fields[key] = value
        return self
    
    def add_fields(self, fields: Dict[str, Any]) -> "PayloadBuilder":
        """Add multiple fields at once."""
        self._fields.update(fields)
        return self
    
    def add_nested(self, key: str, data: Dict[str, Any]) -> "PayloadBuilder":
        """Add a nested object to the payload."""
        self._fields[key] = data
        return self
    
    def add_list(self, key: str, items: List[Any]) -> "PayloadBuilder":
        """Add a list to the payload."""
        self._fields[key] = items
        return self
    
    def set_request_id(self, request_id: str) -> "PayloadBuilder":
        """Set a custom request ID."""
        self._metadata.request_id = request_id
        return self
    
    def set_source(self, source: str) -> "PayloadBuilder":
        """Set the source identifier."""
        self._metadata.source = source
        return self
    
    def set_environment(self, env: str) -> "PayloadBuilder":
        """Set the environment."""
        self._metadata.environment = env
        return self
    
    def _compute_checksum(self, data: bytes) -> str:
        """Compute checksum of payload data."""
        algo = self._config.checksum_algorithm.lower()
        if algo == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif algo == "md5":
            return hashlib.md5(data).hexdigest()
        elif algo == "sha1":
            return hashlib.sha1(data).hexdigest()
        else:
            raise ValueError(f"Unsupported checksum algorithm: {algo}")
    
    def _serialize_json(self, data: Dict[str, Any]) -> bytes:
        """Serialize data to JSON bytes."""
        return json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=self._config.sort_keys,
            default=str
        ).encode("utf-8")
    
    def _encode_payload(self, data: bytes) -> bytes:
        """Apply encoding to payload data."""
        if self._config.encoding == EncodingType.GZIP:
            return zlib.compress(data)
        elif self._config.encoding == EncodingType.DEFLATE:
            return zlib.compress(data, level=9)
        elif self._config.encoding == EncodingType.BASE64:
            return base64.b64encode(data)
        elif self._config.encoding == EncodingType.BROTLI:
            return zlib.compress(data)
        return data
    
    def build(self) -> Dict[str, Any]:
        """Build the final payload dictionary."""
        if self._config.include_metadata:
            self._fields["_meta"] = asdict(self._metadata)
        
        if self._config.include_timestamp:
            self._fields["_timestamp"] = datetime.now(timezone.utc).isoformat()
        
        return self._fields.copy()
    
    def build_bytes(self) -> bytes:
        """Build payload as bytes, including encoding."""
        payload = self.build()
        
        if self._config.content_type == ContentType.JSON:
            data = self._serialize_json(payload)
        elif self._config.content_type == ContentType.XML:
            raise NotImplementedError("XML serialization not yet implemented")
        elif self._config.content_type == ContentType.GRAPHQL:
            data = json.dumps(payload).encode("utf-8")
        else:
            data = json.dumps(payload, default=str).encode("utf-8")
        
        if self._config.encoding:
            data = self._encode_payload(data)
        
        return data
    
    def build_with_headers(self) -> Dict[str, Any]:
        """Build payload along with recommended headers."""
        payload = self.build()
        headers = {
            "Content-Type": self._config.content_type.value,
        }
        
        if self._config.encoding:
            headers["Content-Encoding"] = self._config.encoding.value
        
        if self._config.include_checksum:
            data = self._serialize_json(payload)
            headers["X-Checksum"] = self._compute_checksum(data)
        
        if self._metadata.request_id:
            headers["X-Request-ID"] = self._metadata.request_id
        
        return {
            "payload": payload,
            "headers": headers,
        }


class PayloadValidator:
    """Validator for API payloads.
    
    Example:
        >>> validator = PayloadValidator()
        >>> result = validator.validate(
        ...     {"user_id": "123", "email": "a@b.com"},
        ...     {"user_id": str, "email": str}
        ... )
        >>> print(result.is_valid)
    """
    
    def __init__(self):
        self._validators: Dict[str, Callable[[Any], bool]] = {}
        self._required_fields: List[str] = []
        self._optional_fields: List[str] = []
        
    def add_validator(self, field: str, validator: Callable[[Any], bool]) -> "PayloadValidator":
        """Add a custom validator for a field."""
        self._validators[field] = validator
        return self
    
    def set_required(self, fields: List[str]) -> "PayloadValidator":
        """Set required fields."""
        self._required_fields = fields
        return self
    
    def set_optional(self, fields: List[str]) -> "PayloadValidator":
        """Set optional fields."""
        self._optional_fields = fields
        return self
    
    def validate(self, payload: Dict[str, Any], schema: Optional[Dict[str, type]] = None) -> ValidationResult:
        """Validate a payload against schema and rules."""
        errors: List[str] = []
        warnings: List[str] = []
        
        for field in self._required_fields:
            if field not in payload:
                errors.append(f"Missing required field: {field}")
        
        if schema:
            for field, expected_type in schema.items():
                if field in payload and payload[field] is not None:
                    if not isinstance(payload[field], expected_type):
                        errors.append(
                            f"Field '{field}' expected {expected_type.__name__}, "
                            f"got {type(payload[field]).__name__}"
                        )
        
        for field, validator in self._validators.items():
            if field in payload:
                try:
                    if not validator(payload[field]):
                        errors.append(f"Field '{field}' failed validation")
                except Exception as e:
                    errors.append(f"Field '{field}' validator error: {e}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            validated_at=datetime.now(timezone.utc).isoformat()
        )


@dataclass
class ValidationResult:
    """Result of payload validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    validated_at: str


class HMACSigner:
    """Signs payloads with HMAC for authentication.
    
    Example:
        >>> signer = HMACSigner(secret_key="my-secret")
        >>> signature = signer.sign({"data": "value"})
        >>> print(signature)
    """
    
    def __init__(self, secret_key: str, algorithm: str = "sha256"):
        self._secret = secret_key.encode("utf-8")
        self._algorithm = algorithm
    
    def sign(self, payload: Dict[str, Any]) -> str:
        """Generate HMAC signature for payload."""
        data = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        
        if self._algorithm == "sha256":
            return hmac.new(self._secret, data, hashlib.sha256).hexdigest()
        elif self._algorithm == "sha512":
            return hmac.new(self._secret, data, hashlib.sha512).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {self._algorithm}")
    
    def verify(self, payload: Dict[str, Any], signature: str) -> bool:
        """Verify HMAC signature."""
        expected = self.sign(payload)
        return hmac.compare_digest(expected, signature)


__all__ = [
    "ContentType",
    "EncodingType",
    "PayloadConfig",
    "PayloadMetadata",
    "PayloadBuilder",
    "PayloadValidator",
    "ValidationResult",
    "HMACSigner",
]
