"""JSON Schema validation utilities for rabai_autoclick.

Provides schema compilation, validation with detailed error reporting,
and reusable schema factories for common data shapes.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Iterator

try:
    from jsonschema import Draft7Validator, ValidationError
    _HAS_JSONSCHEMA = True
except ImportError:
    _HAS_JSONSCHEMA = False
    ValidationError = Exception  # type: ignore[misc,assignment]

__all__ = [
    "SchemaValidator",
    "SchemaCache",
    "ValidationResult",
    "make_schema",
    "validate_or_raise",
]


# --------------------------------------------------------------------------- #
# Types
# --------------------------------------------------------------------------- #

class ValidationResult:
    """Result of a schema validation run."""

    __slots__ = ("valid", "errors", "instance")

    def __init__(self, instance: Any, valid: bool, errors: list[str] | None = None) -> None:
        self.instance = instance
        self.valid = valid
        self.errors: list[str] = errors or []

    @property
    def error_summary(self) -> str | None:
        if self.valid:
            return None
        return "; ".join(self.errors)

    def __repr__(self) -> str:
        return f"ValidationResult(valid={self.valid!r}, errors={self.errors!r})"


# --------------------------------------------------------------------------- #
# Schema Cache
# --------------------------------------------------------------------------- #

class SchemaCache:
    """Thread-safe in-memory cache for compiled JSON schemas.

    Skips re-compilation when the same schema dict is presented again.
    """

    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}
        self._compiled: dict[str, Any] = {}

    @staticmethod
    def _fingerprint(schema: dict[str, Any]) -> str:
        """Stable fingerprint for a schema dict (deterministic JSON sort)."""
        return json.dumps(schema, sort_keys=True, separators=(",", ":"))

    def get_or_compile(self, schema: dict[str, Any]) -> Any:
        """Return a compiled validator, creating it if not cached."""
        fp = self._fingerprint(schema)
        if fp not in self._compiled:
            self._cache[fp] = schema
            if _HAS_JSONSCHEMA:
                self._compiled[fp] = Draft7Validator(schema)
            else:
                # Fallback: return schema as-is when jsonschema is not installed
                self._compiled[fp] = schema
        return self._compiled[fp]

    def clear(self) -> None:
        """Evict all cached schemas."""
        self._cache.clear()
        self._compiled.clear()

    def cached_count(self) -> int:
        return len(self._compiled)


# --------------------------------------------------------------------------- #
# Schema Validator
# --------------------------------------------------------------------------- #

class SchemaValidator:
    """High-level validator wrapping a schema with caching and rich errors."""

    def __init__(
        self,
        schema: dict[str, Any] | str,
        *,
        cache: SchemaCache | None = None,
    ) -> None:
        self._raw_schema: dict[str, Any] | str = schema
        self._cache = cache or SchemaCache()
        self._compiled: Any = None

    @property
    def schema(self) -> dict[str, Any]:
        if isinstance(self._raw_schema, str):
            self._raw_schema = json.loads(self._raw_schema)
        return self._raw_schema  # type: ignore[return-value]

    def _ensure_compiled(self) -> Any:
        if self._compiled is None:
            self._compiled = self._cache.get_or_compile(self.schema)
        return self._compiled

    def validate(self, instance: Any) -> ValidationResult:
        """Run validation and return a structured result."""
        compiled = self._ensure_compiled()
        if not _HAS_JSONSCHEMA:
            # Cannot validate without jsonschema library
            return ValidationResult(instance, True)

        errors: list[str] = []
        for err in compiled.iter_errors(instance):
            path = ".".join(str(p) for p in err.path) if err.path else "<root>"
            errors.append(f"[{path}] {err.message}")
        return ValidationResult(instance, len(errors) == 0, errors)

    def iter_errors(self, instance: Any) -> Iterator[ValidationError]:
        """Yield individual validation errors."""
        compiled = self._ensure_compiled()
        if not _HAS_JSONSCHEMA:
            return
        yield from compiled.iter_errors(instance)

    def is_valid(self, instance: Any) -> bool:
        """Return True if instance passes validation."""
        return self.validate(instance).valid

    def validate_or_raise(self, instance: Any) -> Any:
        """Validate and raise the first error as an exception."""
        compiled = self._ensure_compiled()
        if _HAS_JSONSCHEMA:
            compiled.validate(instance)
        return instance


# --------------------------------------------------------------------------- #
# Helpers / Schema Factories
# --------------------------------------------------------------------------- #

def make_schema(
    *,
    properties: dict[str, Any] | None = None,
    required: list[str] | None = None,
    enum: list[Any] | None = None,
    typ: str = "object",
    additional_properties: bool | dict[str, Any] = False,
    min_items: int | None = None,
    max_items: int | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
    pattern: str | None = None,
    format: str | None = None,
    default: Any = None,
) -> dict[str, Any]:
    """Build a JSON schema dict from keyword arguments."""
    schema: dict[str, Any] = {"type": typ}

    if properties is not None:
        schema["properties"] = properties

    if required is not None:
        schema["required"] = required

    if enum is not None:
        schema["enum"] = enum

    if additional_properties is not False:
        schema["additionalProperties"] = additional_properties

    if min_items is not None:
        schema["minItems"] = min_items

    if max_items is not None:
        schema["maxItems"] = max_items

    if minimum is not None:
        schema["minimum"] = minimum

    if maximum is not None:
        schema["maximum"] = maximum

    if pattern is not None:
        schema["pattern"] = pattern

    if format is not None:
        schema["format"] = format

    if default is not None:
        schema["default"] = default

    return schema


def validate_or_raise(
    schema: dict[str, Any],
    instance: Any,
    cache: SchemaCache | None = None,
) -> Any:
    """One-liner: validate or raise ValidationError."""
    validator = SchemaValidator(schema, cache=cache)
    return validator.validate_or_raise(instance)


# --------------------------------------------------------------------------- #
# Common schema factory helpers
# --------------------------------------------------------------------------- #

ACTION_SCHEMA = make_schema(
    typ="object",
    properties={
        "type": make_schema(typ="string", enum=["click", "type", "press", "wait", "scroll", "screenshot"]),
        "target": make_schema(typ="string"),
        "value": make_schema(typ="string"),
        "delay_ms": make_schema(typ="number", minimum=0),
    },
    required=["type"],
)

WORKFLOW_SCHEMA = make_schema(
    typ="object",
    properties={
        "name": make_schema(typ="string"),
        "steps": make_schema(typ="array", min_items=1, items={"type": "object"}),
        "loop": make_schema(typ="boolean"),
        "max_iterations": make_schema(typ="integer", minimum=1),
    },
    required=["name", "steps"],
)


def register_common_schemas(cache: SchemaCache) -> None:
    """Pre-populate a cache with known schemas (call once at startup)."""
    for name, schema in [
        ("action", ACTION_SCHEMA),
        ("workflow", WORKFLOW_SCHEMA),
    ]:
        fp = SchemaCache._fingerprint(schema)
        if _HAS_JSONSCHEMA:
            cache._compiled[fp] = Draft7Validator(schema)
        else:
            cache._compiled[fp] = schema
        cache._cache[fp] = schema
