# Copyright (c) 2024. coded by claude
"""Data Schema Action Module.

Provides schema validation and enforcement for API data structures
with support for JSON Schema-like validation rules.
"""
from typing import Optional, Dict, Any, List, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SchemaType(Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    NULL = "null"


@dataclass
class SchemaField:
    name: str
    field_type: SchemaType
    required: bool = False
    default: Any = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    pattern: Optional[str] = None
    enum_values: Optional[List[Any]] = None
    items: Optional["SchemaField"] = None
    properties: Optional[Dict[str, "SchemaField"]] = None


@dataclass
class SchemaValidationError:
    field: str
    message: str
    value: Any


@dataclass
class SchemaValidationResult:
    valid: bool
    errors: List[SchemaValidationError] = field(default_factory=list)


class DataSchema:
    def __init__(self, name: str, fields: Optional[List[SchemaField]] = None):
        self.name = name
        self.fields: Dict[str, SchemaField] = {}
        if fields:
            for field in fields:
                self.fields[field.name] = field

    def add_field(self, field: SchemaField) -> None:
        self.fields[field.name] = field

    def validate(self, data: Dict[str, Any]) -> SchemaValidationResult:
        errors: List[SchemaValidationError] = []
        for field_name, field_def in self.fields.items():
            value = data.get(field_name)
            if value is None:
                if field_def.required:
                    errors.append(SchemaValidationError(
                        field=field_name,
                        message=f"Field '{field_name}' is required",
                        value=None,
                    ))
                continue
            field_errors = self._validate_field(field_name, value, field_def)
            errors.extend(field_errors)
        return SchemaValidationResult(valid=len(errors) == 0, errors=errors)

    def _validate_field(self, field_name: str, value: Any, field_def: SchemaField) -> List[SchemaValidationError]:
        errors: List[SchemaValidationError] = []
        if field_def.field_type == SchemaType.STRING:
            if not isinstance(value, str):
                errors.append(SchemaValidationError(field=field_name, message="Must be a string", value=value))
            else:
                if field_def.min_length and len(value) < field_def.min_length:
                    errors.append(SchemaValidationError(field=field_name, message=f"Min length is {field_def.min_length}", value=value))
                if field_def.max_length and len(value) > field_def.max_length:
                    errors.append(SchemaValidationError(field=field_name, message=f"Max length is {field_def.max_length}", value=value))
                if field_def.pattern and not self._match_pattern(field_def.pattern, value):
                    errors.append(SchemaValidationError(field=field_name, message=f"Does not match pattern {field_def.pattern}", value=value))
        elif field_def.field_type in (SchemaType.NUMBER, SchemaType.INTEGER):
            if not isinstance(value, (int, float)):
                errors.append(SchemaValidationError(field=field_name, message="Must be a number", value=value))
            else:
                if field_def.minimum is not None and value < field_def.minimum:
                    errors.append(SchemaValidationError(field=field_name, message=f"Min value is {field_def.minimum}", value=value))
                if field_def.maximum is not None and value > field_def.maximum:
                    errors.append(SchemaValidationError(field=field_name, message=f"Max value is {field_def.maximum}", value=value))
        elif field_def.field_type == SchemaType.BOOLEAN:
            if not isinstance(value, bool):
                errors.append(SchemaValidationError(field=field_name, message="Must be a boolean", value=value))
        elif field_def.field_type == SchemaType.ARRAY:
            if not isinstance(value, list):
                errors.append(SchemaValidationError(field=field_name, message="Must be an array", value=value))
            elif field_def.items:
                for i, item in enumerate(value):
                    item_errors = self._validate_field(f"{field_name}[{i}]", item, field_def.items)
                    errors.extend(item_errors)
        elif field_def.field_type == SchemaType.OBJECT:
            if not isinstance(value, dict):
                errors.append(SchemaValidationError(field=field_name, message="Must be an object", value=value))
            elif field_def.properties:
                schema = DataSchema("temp", list(field_def.properties.values()))
                obj_errors = schema.validate(value)
                for err in obj_errors.errors:
                    errors.append(SchemaValidationError(field=f"{field_name}.{err.field}", message=err.message, value=err.value))
        if field_def.enum_values and value not in field_def.enum_values:
            errors.append(SchemaValidationError(field=field_name, message=f"Must be one of {field_def.enum_values}", value=value))
        return errors

    def _match_pattern(self, pattern: str, value: str) -> bool:
        import re
        try:
            return bool(re.match(pattern, value))
        except Exception:
            return pattern in value
