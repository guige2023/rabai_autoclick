"""
Data Schema Action - Schema management and validation.

This module provides schema management capabilities including
schema creation, validation, and inference.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class FieldSchema:
    """Schema for a single field."""
    name: str
    field_type: str
    required: bool = False
    default: Any = None
    min_value: float | None = None
    max_value: float | None = None
    pattern: str | None = None
    enum_values: list[Any] | None = None


@dataclass
class Schema:
    """A data schema."""
    name: str
    fields: list[FieldSchema]
    version: str = "1.0"


@dataclass
class SchemaValidationResult:
    """Result of schema validation."""
    valid: bool
    errors: list[str] = field(default_factory=list)


class SchemaValidator:
    """Validates data against schemas."""
    
    def __init__(self) -> None:
        self._schemas: dict[str, Schema] = {}
    
    def register_schema(self, schema: Schema) -> None:
        """Register a schema."""
        self._schemas[schema.name] = schema
    
    def validate(self, data: dict[str, Any], schema_name: str) -> SchemaValidationResult:
        """Validate data against a schema."""
        if schema_name not in self._schemas:
            return SchemaValidationResult(valid=False, errors=[f"Schema {schema_name} not found"])
        
        schema = self._schemas[schema_name]
        errors = []
        
        for field_schema in schema.fields:
            value = data.get(field_schema.name)
            
            if field_schema.required and value is None:
                errors.append(f"Required field missing: {field_schema.name}")
                continue
            
            if value is None:
                continue
            
            if field_schema.field_type == "integer":
                if not isinstance(value, int) or isinstance(value, bool):
                    errors.append(f"Field {field_schema.name} must be integer")
            elif field_schema.field_type == "number":
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    errors.append(f"Field {field_schema.name} must be number")
            elif field_schema.field_type == "string":
                if not isinstance(value, str):
                    errors.append(f"Field {field_schema.name} must be string")
            
            if field_schema.min_value is not None and isinstance(value, (int, float)):
                if value < field_schema.min_value:
                    errors.append(f"Field {field_schema.name} below minimum {field_schema.min_value}")
            
            if field_schema.max_value is not None and isinstance(value, (int, float)):
                if value > field_schema.max_value:
                    errors.append(f"Field {field_schema.name} above maximum {field_schema.max_value}")
            
            if field_schema.enum_values and value not in field_schema.enum_values:
                errors.append(f"Field {field_schema.name} not in allowed values")
        
        return SchemaValidationResult(valid=len(errors) == 0, errors=errors)


class DataSchemaAction:
    """Data schema action for automation workflows."""
    
    def __init__(self) -> None:
        self.validator = SchemaValidator()
    
    def create_schema(self, name: str, fields: list[dict[str, Any]], version: str = "1.0") -> Schema:
        """Create a new schema."""
        field_schemas = [FieldSchema(name=f["name"], field_type=f["type"], **f) for f in fields]
        schema = Schema(name=name, fields=field_schemas, version=version)
        self.validator.register_schema(schema)
        return schema
    
    def validate(self, data: dict[str, Any], schema_name: str) -> SchemaValidationResult:
        """Validate data against schema."""
        return self.validator.validate(data, schema_name)


__all__ = ["FieldSchema", "Schema", "SchemaValidationResult", "SchemaValidator", "DataSchemaAction"]
