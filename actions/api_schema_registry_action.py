"""
API Schema Registry Action Module.

Schema registry for API request/response validation
with versioning, compatibility checking, and schema evolution.
"""

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SchemaType(Enum):
    """Schema types."""
    REQUEST = "request"
    RESPONSE = "response"
    WEBHOOK = "webhook"


class CompatibilityMode(Enum):
    """Schema compatibility modes."""
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"


@dataclass
class SchemaField:
    """Schema field definition."""
    name: str
    type: str
    required: bool = False
    default: Any = None
    enum_values: Optional[list] = None
    pattern: Optional[str] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None


@dataclass
class Schema:
    """
    API schema definition.

    Attributes:
        schema_id: Unique schema identifier.
        name: Schema name.
        version: Schema version.
        schema_type: Type of schema.
        fields: List of field definitions.
        compatibility: Compatibility mode.
        created_at: Creation timestamp.
    """
    schema_id: str
    name: str
    version: str
    schema_type: SchemaType
    fields: list[SchemaField] = field(default_factory=list)
    compatibility: CompatibilityMode = CompatibilityMode.BACKWARD
    created_at: float = field(default_factory=time.time, init=False)


@dataclass
class ValidationError:
    """Schema validation error."""
    field: str
    expected: str
    actual: Any
    message: str


class APISchemaRegistryAction:
    """
    Schema registry for API validation and versioning.

    Example:
        registry = APISchemaRegistryAction()
        registry.register_schema("UserRequest", SchemaType.REQUEST, user_fields)
        validated = registry.validate("UserRequest", request_data)
    """

    def __init__(self):
        """Initialize schema registry."""
        self._schemas: dict[str, list[Schema]] = {}
        self._latest_versions: dict[str, Schema] = {}

    def register_schema(
        self,
        name: str,
        schema_type: SchemaType,
        fields: list[SchemaField],
        version: str = "1.0.0",
        compatibility: CompatibilityMode = CompatibilityMode.BACKWARD
    ) -> Schema:
        """
        Register a new schema.

        Args:
            name: Schema name.
            schema_type: Type of schema.
            fields: List of field definitions.
            version: Schema version.
            compatibility: Compatibility mode.

        Returns:
            Created Schema.
        """
        import uuid

        schema_id = str(uuid.uuid4())[:8]

        schema = Schema(
            schema_id=schema_id,
            name=name,
            version=version,
            schema_type=schema_type,
            fields=fields,
            compatibility=compatibility
        )

        if name not in self._schemas:
            self._schemas[name] = []

        existing = [s for s in self._schemas[name] if s.version == version]
        if existing:
            raise ValueError(f"Schema {name} v{version} already exists")

        self._schemas[name].append(schema)
        self._latest_versions[name] = schema

        logger.debug(f"Registered schema: {name} v{version}")
        return schema

    def get_schema(
        self,
        name: str,
        version: Optional[str] = None
    ) -> Optional[Schema]:
        """
        Get schema by name and optional version.

        Args:
            name: Schema name.
            version: Specific version (returns latest if None).

        Returns:
            Schema or None if not found.
        """
        if name not in self._schemas:
            return None

        schemas = self._schemas[name]

        if version:
            for schema in schemas:
                if schema.version == version:
                    return schema
            return None

        return max(schemas, key=lambda s: s.created_at) if schemas else None

    def validate(
        self,
        schema_name: str,
        data: dict,
        version: Optional[str] = None
    ) -> tuple[bool, list[ValidationError]]:
        """
        Validate data against schema.

        Args:
            schema_name: Schema name to validate against.
            data: Data to validate.
            version: Specific version (validates against latest if None).

        Returns:
            Tuple of (is_valid, list_of_errors).
        """
        schema = self.get_schema(schema_name, version)

        if not schema:
            return False, [ValidationError(
                field="",
                expected="",
                actual="",
                message=f"Schema not found: {schema_name}"
            )]

        errors = []
        field_map = {f.name: f for f in schema.fields}

        for field_def in schema.fields:
            value = data.get(field_def.name)

            if value is None:
                if field_def.required:
                    errors.append(ValidationError(
                        field=field_def.name,
                        expected="required",
                        actual=None,
                        message=f"Required field missing: {field_def.name}"
                    ))
                continue

            if not self._validate_type(field_def, value):
                errors.append(ValidationError(
                    field=field_def.name,
                    expected=field_def.type,
                    actual=type(value).__name__,
                    message=f"Type mismatch for {field_def.name}"
                ))

            if field_def.enum_values and value not in field_def.enum_values:
                errors.append(ValidationError(
                    field=field_def.name,
                    expected=str(field_def.enum_values),
                    actual=value,
                    message=f"Value not in enum: {field_def.name}"
                ))

            if field_def.pattern and isinstance(value, str):
                import re
                if not re.match(field_def.pattern, value):
                    errors.append(ValidationError(
                        field=field_def.name,
                        expected=field_def.pattern,
                        actual=value,
                        message=f"Pattern mismatch: {field_def.name}"
                    ))

            if field_def.minimum is not None and isinstance(value, (int, float)):
                if value < field_def.minimum:
                    errors.append(ValidationError(
                        field=field_def.name,
                        expected=f">= {field_def.minimum}",
                        actual=value,
                        message=f"Below minimum: {field_def.name}"
                    ))

            if field_def.maximum is not None and isinstance(value, (int, float)):
                if value > field_def.maximum:
                    errors.append(ValidationError(
                        field=field_def.name,
                        expected=f"<= {field_def.maximum}",
                        actual=value,
                        message=f"Above maximum: {field_def.name}"
                    ))

        extra_fields = set(data.keys()) - {f.name for f in schema.fields}
        if extra_fields:
            logger.warning(f"Extra fields in data: {extra_fields}")

        return len(errors) == 0, errors

    def _validate_type(self, field_def: SchemaField, value: Any) -> bool:
        """Validate value type against field definition."""
        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict
        }

        expected_type = type_map.get(field_def.type)
        if expected_type:
            return isinstance(value, expected_type)
        return True

    def check_compatibility(
        self,
        schema_name: str,
        new_version: str,
        old_version: str
    ) -> bool:
        """
        Check compatibility between schema versions.

        Args:
            schema_name: Schema name.
            new_version: New schema version.
            old_version: Old schema version.

        Returns:
            True if compatible.
        """
        new_schema = self.get_schema(schema_name, new_version)
        old_schema = self.get_schema(schema_name, old_version)

        if not new_schema or not old_schema:
            return False

        mode = new_schema.compatibility

        if mode == CompatibilityMode.NONE:
            return True

        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}

        if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL):
            for name, field_def in old_fields.items():
                if name not in new_fields:
                    if mode == CompatibilityMode.BACKWARD:
                        return False

        if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FULL):
            for name, field_def in new_fields.items():
                if name not in old_fields:
                    if not field_def.required:
                        continue
                    if mode == CompatibilityMode.FORWARD:
                        return False

        return True

    def evolve_schema(
        self,
        schema_name: str,
        new_fields: list[SchemaField],
        new_version: str
    ) -> Optional[Schema]:
        """
        Create new version of schema with evolution.

        Args:
            schema_name: Schema to evolve.
            new_fields: New field definitions.
            new_version: New version string.

        Returns:
            New Schema or None if incompatible.
        """
        old_schema = self.get_schema(schema_name)

        if not old_schema:
            return self.register_schema(
                schema_name,
                old_schema.schema_type,
                new_fields,
                version=new_version
            )

        if not self.check_compatibility(schema_name, new_version, old_schema.version):
            logger.error(f"Schema evolution incompatible: {schema_name}")
            return None

        return self.register_schema(
            schema_name,
            old_schema.schema_type,
            new_fields,
            version=new_version,
            compatibility=old_schema.compatibility
        )

    def get_schema_hash(self, schema_name: str) -> Optional[str]:
        """Get hash of latest schema version."""
        schema = self.get_schema(schema_name)
        if not schema:
            return None

        schema_data = {
            "name": schema.name,
            "version": schema.version,
            "fields": [(f.name, f.type) for f in schema.fields]
        }

        return hashlib.sha256(
            json.dumps(schema_data, sort_keys=True).encode()
        ).hexdigest()[:16]

    def list_schemas(self) -> list[dict]:
        """List all registered schemas."""
        result = []

        for name, schemas in self._schemas.items():
            latest = max(schemas, key=lambda s: s.created_at)
            result.append({
                "name": name,
                "latest_version": latest.version,
                "versions": len(schemas),
                "type": latest.schema_type.value,
                "fields_count": len(latest.fields)
            })

        return result
