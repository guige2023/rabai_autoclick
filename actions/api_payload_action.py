"""
API Payload Builder and Normalizer Action.

Provides a unified interface for building, normalizing, and transforming
API request payloads with support for multiple formats and schemas.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import base64
import hashlib
import json
import zlib
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from dataclasses import dataclass, field
from enum import Enum, auto
import logging

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

T = TypeVar("T")
logger = logging.getLogger(__name__)


class PayloadFormat(Enum):
    """Supported payload formats."""
    JSON = auto()
    XML = auto()
    FORM_URLENCODED = auto()
    MULTIPART = auto()
    BINARY = auto()
    PROTOBUF = auto()
    MSGPACK = auto()


class CompressionType(Enum):
    """Payload compression types."""
    NONE = auto()
    GZIP = auto()
    DEFLATE = auto()
    BROTLI = auto()
    ZSTD = auto()


class ValidationLevel(Enum):
    """Payload validation strictness levels."""
    LENIENT = auto()
    STANDARD = auto()
    STRICT = auto()
    SCHEMA = auto()


@dataclass(frozen=True)
class PayloadMetadata:
    """Immutable metadata attached to a payload."""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    format: PayloadFormat = PayloadFormat.JSON
    compression: CompressionType = CompressionType.NONE
    version: str = "1.0"
    source: str = "unknown"
    correlation_id: Optional[str] = None
    content_encoding: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """Generate a unique fingerprint for this metadata."""
        raw = f"{self.timestamp.isoformat()}:{self.format.name}:{self.version}:{self.source}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


@dataclass
class PayloadValidationError:
    """Detailed validation error information."""
    field_path: str
    error_type: str
    message: str
    value: Any
    constraint: Any

    def __str__(self) -> str:
        return f"[{self.field_path}] {self.error_type}: {self.message} (got: {self.value!r}, expected: {self.constraint!r})"


@dataclass
class ValidationResult:
    """Result of payload validation."""
    valid: bool
    errors: List[PayloadValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Optional[PayloadMetadata] = None

    def __bool__(self) -> bool:
        return self.valid

    def summary(self) -> str:
        if self.valid:
            return f"Valid payload ({len(self.warnings)} warnings)"
        return f"Invalid payload: {len(self.errors)} error(s), {len(self.warnings)} warning(s)"


@dataclass
class PayloadContext:
    """Context for payload transformation operations."""
    metadata: PayloadMetadata
    schema: Optional[Dict[str, Any]] = None
    transformers: List[Callable[[Dict[str, Any]], Dict[str, Any]]] = field(default_factory=list)
    validators: List[Callable[[Dict[str, Any]], ValidationResult]] = field(default_factory=list)


class APIPayloadBuilder:
    """
    Builder for constructing API payloads with normalization and validation.

    Supports chaining transformations, multiple output formats,
    compression, and schema validation.

    Example:
        builder = APIPayloadBuilder()
        payload = (builder
            .set_source("my-service")
            .add_field("user_id", 12345)
            .add_nested("profile", {"name": "Alice", "email": "alice@example.com"})
            .normalize()
            .compress(CompressionType.GZIP)
            .build())
    """

    def __init__(
        self,
        initial_data: Optional[Dict[str, Any]] = None,
        format: PayloadFormat = PayloadFormat.JSON,
    ) -> None:
        self._data: Dict[str, Any] = initial_data.copy() if initial_data else {}
        self._format = format
        self._metadata = PayloadMetadata(format=format)
        self._transformers: List[Callable[[Dict[str, Any]], Dict[str, Any]]] = []
        self._validation_level = ValidationLevel.STANDARD
        self._strict_fields: Dict[str, Tuple[Any, str]] = {}  # field -> (validator, error_msg)
        self._computed_fields: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._exclude_none = False
        self._exclude_empty = False

    def set_source(self, source: str) -> Self:
        """Set the payload source identifier."""
        self._metadata = PayloadMetadata(
            timestamp=self._metadata.timestamp,
            format=self._metadata.format,
            compression=self._metadata.compression,
            version=self._metadata.version,
            source=source,
            correlation_id=self._metadata.correlation_id,
            content_encoding=self._metadata.content_encoding,
            extra=self._metadata.extra,
        )
        return self

    def set_correlation_id(self, correlation_id: str) -> Self:
        """Set correlation ID for request tracing."""
        object.__setattr__(self._metadata, "correlation_id", correlation_id)
        return self

    def set_version(self, version: str) -> Self:
        """Set payload schema version."""
        object.__setattr__(self._metadata, "version", version)
        return self

    def add_field(self, key: str, value: Any) -> Self:
        """Add a field to the payload."""
        self._data[key] = value
        return self

    def add_nested(self, key: str, nested: Dict[str, Any]) -> Self:
        """Add a nested dictionary field."""
        self._data[key] = nested
        return self

    def add_list(self, key: str, items: List[Any]) -> Self:
        """Add a list field."""
        self._data[key] = items
        return self

    def add_computed(self, key: str, func: Callable[[Dict[str, Any]], Any]) -> Self:
        """Add a computed field that is derived during build()."""
        self._computed_fields[key] = func
        return self

    def set_strict(
        self,
        field: str,
        validator: Callable[[Any], bool],
        error_msg: str,
    ) -> Self:
        """Add a strict validation rule for a field."""
        self._strict_fields[field] = (validator, error_msg)
        return self

    def normalize(self) -> Self:
        """Add default normalization transformer."""
        def normalizer(data: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            for k, v in data.items():
                if self._exclude_none and v is None:
                    continue
                if self._exclude_empty and not v:
                    continue
                # Snake case keys
                normalized_key = self._to_snake_case(k)
                result[normalized_key] = self._normalize_value(v)
            return result
        self._transformers.append(normalizer)
        return self

    def exclude_none(self, exclude: bool = True) -> Self:
        """Exclude None values from the payload."""
        self._exclude_none = exclude
        return self

    def exclude_empty(self, exclude: bool = True) -> Self:
        """Exclude empty strings, lists, and dicts from the payload."""
        self._exclude_empty = exclude
        return self

    def compress(self, compression: CompressionType) -> Self:
        """Set compression type for the built payload."""
        object.__setattr__(self._metadata, "compression", compression)
        return self

    def set_format(self, format: PayloadFormat) -> Self:
        """Set the output format."""
        self._format = format
        object.__setattr__(self._metadata, "format", format)
        return self

    def add_transformer(
        self,
        transformer: Callable[[Dict[str, Any]], Dict[str, Any]],
    ) -> Self:
        """Add a custom transformer function."""
        self._transformers.append(transformer)
        return self

    def set_validation_level(self, level: ValidationLevel) -> Self:
        """Set validation strictness level."""
        self._validation_level = level
        return self

    def build(self) -> Tuple[Union[Dict[str, Any], str, bytes], PayloadMetadata]:
        """
        Build and return the final payload with metadata.

        Returns:
            A tuple of (payload, metadata). Payload type depends on format:
            - JSON: dict
            - XML/FormUrlEncoded: str
            - Binary/Protobuf/MsgPack: bytes
        """
        data = self._data.copy()

        # Apply computed fields
        for key, func in self._computed_fields.items():
            try:
                data[key] = func(data)
            except Exception as exc:
                logger.warning("Computed field %s raised %s: %s", key, type(exc).__name__, exc)

        # Apply transformers in order
        for transformer in self._transformers:
            try:
                data = transformer(data)
            except Exception as exc:
                logger.error("Transformer %s failed: %s", transformer.__name__, exc)
                raise PayloadBuildError(f"Transformer {transformer.__name__} failed: {exc}") from exc

        # Apply compression
        payload = self._serialize(data)
        if self._metadata.compression != CompressionType.NONE:
            payload = self._compress(payload, self._metadata.compression)

        # Attach metadata fingerprint
        object.__setattr__(self._metadata, "content_encoding", self._metadata.compression.name.lower())

        return payload, self._metadata

    def validate(self, data: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """
        Validate the current or provided payload data.

        Returns ValidationResult with detailed error information.
        """
        data = data or self._data.copy()
        errors: List[PayloadValidationError] = []
        warnings: List[str] = []

        # Apply transformers for validation context
        for transformer in self._transformers:
            try:
                data = transformer(data)
            except Exception as exc:
                errors.append(PayloadValidationError(
                    field_path="",
                    error_type="TransformError",
                    message=str(exc),
                    value=None,
                    constraint=transformer.__name__,
                ))

        # Run strict field validators
        for field, (validator, error_msg) in self._strict_fields.items():
            value = self._get_nested(data, field)
            try:
                if not validator(value):
                    errors.append(PayloadValidationError(
                        field_path=field,
                        error_type="StrictValidationFailed",
                        message=error_msg,
                        value=value,
                        constraint="strict_validator",
                    ))
            except Exception as exc:
                errors.append(PayloadValidationError(
                    field_path=field,
                    error_type="ValidatorException",
                    message=str(exc),
                    value=value,
                    constraint=validator.__name__,
                ))

        # Standard validation
        if self._validation_level in (ValidationLevel.STANDARD, ValidationLevel.STRICT, ValidationLevel.SCHEMA):
            if not data:
                warnings.append("Payload is empty")

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            metadata=self._metadata,
        )

    def _serialize(self, data: Dict[str, Any]) -> Union[Dict[str, Any], str, bytes]:
        """Serialize data according to the current format."""
        if self._format == PayloadFormat.JSON:
            return data
        elif self._format == PayloadFormat.XML:
            return self._to_xml(data)
        elif self._format == PayloadFormat.FORM_URLENCODED:
            return self._to_form_urlencoded(data)
        elif self._format == PayloadFormat.BINARY:
            return json.dumps(data).encode("utf-8")
        elif self._format == PayloadFormat.MSGPACK:
            try:
                import msgpack
                return msgpack.packb(data, use_bin_type=True)
            except ImportError:
                logger.warning("msgpack not available, falling back to JSON bytes")
                return json.dumps(data).encode("utf-8")
        return data

    def _compress(self, payload: Union[Dict[str, Any], str, bytes], compression: CompressionType) -> bytes:
        """Compress the payload."""
        if isinstance(payload, dict):
            payload = json.dumps(payload).encode("utf-8")
        elif isinstance(payload, str):
            payload = payload.encode("utf-8")

        if compression == CompressionType.GZIP:
            return base64.b64encode(zlib.compress(payload, level=6))
        elif compression == CompressionType.DEFLATE:
            return base64.b64encode(zlib.compress(payload))
        elif compression == CompressionType.BROTLI:
            try:
                import brotli
                return base64.b64encode(brotli.compress(payload))
            except ImportError:
                logger.warning("brotli not available, skipping compression")
                return payload
        elif compression == CompressionType.ZSTD:
            try:
                import zstandard
                return base64.b64encode(zstandard.compress(payload))
            except ImportError:
                logger.warning("zstandard not available, skipping compression")
                return payload
        return payload

    def _to_snake_case(self, key: str) -> str:
        """Convert camelCase or PascalCase to snake_case."""
        result = []
        for i, ch in enumerate(key):
            if ch.isupper() and i > 0:
                result.append("_")
            result.append(ch.lower())
        return "".join(result)

    def _normalize_value(self, value: Any) -> Any:
        """Normalize a single value."""
        if isinstance(value, dict):
            return {self._to_snake_case(k): self._normalize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._normalize_value(item) for item in value]
        elif isinstance(value, datetime):
            return value.isoformat()
        return value

    def _to_xml(self, data: Dict[str, Any], root: str = "payload") -> str:
        """Convert dictionary to simple XML string."""
        def dict_to_xml(d: Dict[str, Any], tag: str) -> str:
            items = []
            items.append(f"<{tag}>")
            for k, v in d.items():
                if isinstance(v, dict):
                    items.append(dict_to_xml(v, k))
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            items.append(dict_to_xml(item, k))
                        else:
                            items.append(f"<{k}>{self._escape_xml(str(item))}</{k}>")
                else:
                    items.append(f"<{k}>{self._escape_xml(str(v))}</{k}>")
            items.append(f"</{tag}>")
            return "\n".join(items)
        return dict_to_xml(data, root)

    def _escape_xml(self, s: str) -> str:
        """Escape special XML characters."""
        return (s
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;"))

    def _to_form_urlencoded(self, data: Dict[str, Any]) -> str:
        """Convert dictionary to URL-encoded form data."""
        def flatten(d: Dict[str, Any], prefix: str = "") -> List[Tuple[str, str]]:
            items = []
            for k, v in d.items():
                key = f"{prefix}[{k}]" if prefix else k
                if isinstance(v, dict):
                    items.extend(flatten(v, key))
                elif isinstance(v, list):
                    for item in v:
                        items.append((f"{key}[]", str(item)))
                else:
                    items.append((key, str(v)))
            return items
        from urllib.parse import urlencode
        return urlencode(flatten(data))

    def _get_nested(self, data: Dict[str, Any], path: str) -> Any:
        """Get a nested value using dot notation."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


class PayloadBuildError(Exception):
    """Raised when payload building or transformation fails."""
    pass


class PayloadValidator:
    """
    Standalone payload validator with schema support.

    Example:
        validator = PayloadValidator(schema={"user_id": int, "email": str})
        result = validator.validate({"user_id": 1, "email": "test@example.com"})
        print(result.valid)
    """

    def __init__(self, schema: Optional[Dict[str, type]] = None) -> None:
        self._schema = schema or {}

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against the schema."""
        errors: List[PayloadValidationError] = []
        for field, expected_type in self._schema.items():
            value = data.get(field)
            if value is None:
                errors.append(PayloadValidationError(
                    field_path=field,
                    error_type="MissingField",
                    message=f"Required field '{field}' is missing",
                    value=None,
                    constraint=expected_type.__name__,
                ))
            elif not isinstance(value, expected_type):
                errors.append(PayloadValidationError(
                    field_path=field,
                    error_type="TypeMismatch",
                    message=f"Field '{field}' must be {expected_type.__name__}",
                    value=type(value).__name__,
                    constraint=expected_type.__name__,
                ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)


# Convenience factory functions
def build_json_payload(
    source: str,
    fields: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> Tuple[Dict[str, Any], PayloadMetadata]:
    """Build a JSON payload from field dictionary."""
    builder = (APIPayloadBuilder()
        .set_source(source)
        .set_format(PayloadFormat.JSON)
        .exclude_none(True)
        .normalize())
    if correlation_id:
        builder.set_correlation_id(correlation_id)
    for k, v in fields.items():
        builder.add_field(k, v)
    return builder.build()


def build_multipart_payload(
    source: str,
    fields: Dict[str, Any],
    files: Dict[str, bytes],
) -> Tuple[Dict[str, Any], PayloadMetadata]:
    """Build a multipart payload with file data."""
    data = fields.copy()
    data["_files"] = {k: base64.b64encode(v).decode() for k, v in files.items()}
    builder = (APIPayloadBuilder(initial_data=data)
        .set_source(source)
        .set_format(PayloadFormat.MULTIPART))
    return builder.build()
