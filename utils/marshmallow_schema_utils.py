"""
Marshmallow schema utilities for data validation and serialization.

Provides schema definition helpers, nested validation, field converters,
and integration with web frameworks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional, Type

logger = logging.getLogger(__name__)


@dataclass
class FieldDefinition:
    """Definition for a schema field."""
    name: str
    field_type: type
    required: bool = False
    default: Any = None
    validate: Optional[Callable] = None
    error_messages: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaDefinition:
    """Definition for a complete schema."""
    name: str
    fields: list[FieldDefinition] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


class MarshmallowFieldBuilder:
    """Builds Marshmallow fields programmatically."""

    TYPE_MAPPING = {
        str: "String",
        int: "Integer",
        float: "Float",
        bool: "Boolean",
        datetime: "DateTime",
        list: "List",
        dict: "Dict",
    }

    @classmethod
    def build_field(cls, field_def: FieldDefinition) -> Any:
        """Build a single Marshmallow field from a definition."""
        try:
            import marshmallow as ma
        except ImportError:
            logger.warning("marshmallow not installed")
            return None

        field_class = getattr(ma.fields, cls.TYPE_MAPPING.get(field_def.field_type, "String"), ma.fields.String)

        kwargs: dict[str, Any] = {
            "required": field_def.required,
            "load_default": field_def.default,
        }
        if field_def.error_messages:
            kwargs["error_messages"] = field_def.error_messages
        if field_def.metadata:
            kwargs["metadata"] = field_def.metadata
        if field_def.validate:
            kwargs["validate"] = field_def.validate

        return field_class(**kwargs)


class MarshmallowSchemaBuilder:
    """Builds complete Marshmallow schemas dynamically."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._fields: dict[str, Any] = {}
        self._validators: list[Callable] = []
        self._preprocessors: list[Callable] = []
        self._postprocessors: list[Callable] = []

    def add_field(self, field_def: FieldDefinition) -> "MarshmallowSchemaBuilder":
        """Add a field to the schema."""
        field_obj = MarshmallowFieldBuilder.build_field(field_def)
        if field_obj:
            self._fields[field_def.name] = field_obj
        return self

    def add_validator(self, func: Callable) -> "MarshmallowSchemaBuilder":
        """Add a schema-level validator."""
        self._validators.append(func)
        return self

    def pre_load(self, func: Callable) -> "MarshmallowSchemaBuilder":
        """Add a pre-load processor."""
        self._preprocessors.append(func)
        return self

    def post_load(self, func: Callable) -> "MarshmallowSchemaBuilder":
        """Add a post-load processor."""
        self._postprocessors.append(func)
        return self

    def build(self) -> type:
        """Build the Marshmallow schema class."""
        try:
            import marshmallow as ma

            class DynamicSchema(ma.Schema):
                pass

            schema = DynamicSchema()
            for name, field_obj in self._fields.items():
                setattr(schema, name, field_obj)

            for validator in self._validators:
                setattr(schema, "validate", validator)

            return schema
        except ImportError:
            logger.warning("marshmallow not installed")
            return type(self.name, (), {})


class SchemaValidator:
    """Validates data against schema definitions."""

    def __init__(self) -> None:
        self._schemas: dict[str, type] = {}

    def register_schema(self, name: str, schema: type) -> None:
        """Register a schema."""
        self._schemas[name] = schema

    def validate(self, schema_name: str, data: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """Validate data against a registered schema."""
        schema = self._schemas.get(schema_name)
        if not schema:
            return False, {"error": f"Schema {schema_name} not found"}

        try:
            schema_instance = schema()
            result = schema_instance.load(data)
            return True, result
        except Exception as e:
            errors = getattr(e, "messages", {}) or {"error": str(e)}
            return False, errors


class AutoSchemaGenerator:
    """Generates OpenAPI schemas from Marshmallow schemas."""

    @staticmethod
    def generate_openapi(schema_class: type) -> dict[str, Any]:
        """Generate OpenAPI schema from a Marshmallow schema."""
        properties = {}
        required = []

        for name in dir(schema_class):
            if name.startswith("_"):
                continue
            field_obj = getattr(schema_class, name, None)
            if field_obj and hasattr(field_obj, "field_name"):
                openapi_type = AutoSchemaGenerator._marshmallow_to_openapi(field_obj)
                properties[name] = {"type": openapi_type}
                if getattr(field_obj, "required", False):
                    required.append(name)

        return {
            "type": "object",
            "properties": properties,
            "required": required,
        }

    @staticmethod
    def _marshmallow_to_openapi(field_obj: Any) -> str:
        """Convert Marshmallow field to OpenAPI type."""
        field_class_name = field_obj.__class__.__name__
        mapping = {
            "String": "string",
            "Integer": "integer",
            "Float": "number",
            "Boolean": "boolean",
            "DateTime": "string",
            "List": "array",
            "Dict": "object",
        }
        return mapping.get(field_class_name, "string")
