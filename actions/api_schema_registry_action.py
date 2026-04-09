"""API Schema Registry Action Module.

Provides schema registry for API contracts with support for schema
evolution, versioning, compatibility checking, and automatic
documentation generation.
"""

from __future__ import annotations

import json
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class SchemaType(Enum):
    """Types of schemas."""
    JSON_SCHEMA = "json_schema"
    OPENAPI = "openapi"
    GRAPHQL = "graphql"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class CompatibilityType(Enum):
    """Schema compatibility types."""
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"


@dataclass
class SchemaVersion:
    """A specific version of a schema."""
    version: str
    schema_content: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)
    created_by: Optional[str] = None
    description: Optional[str] = None
    deprecated: bool = False
    breaking_changes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaDefinition:
    """A registered schema with all its versions."""
    schema_id: str
    schema_type: SchemaType
    latest_version: str
    versions: Dict[str, SchemaVersion] = field(default_factory=dict)
    compatibility: CompatibilityType = CompatibilityType.FULL
    created_at: datetime = field(default_factory=datetime.now)
    owner: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaRegistryConfig:
    """Configuration for schema registry."""
    default_compatibility: CompatibilityType = CompatibilityType.FULL
    allow_auto_registration: bool = True
    require_versioning: bool = True
    validate_on_register: bool = True
    enforce_strict_validation: bool = True
    max_versions_per_schema: int = 100


class SchemaCompatibilityChecker:
    """Check schema compatibility between versions."""

    @staticmethod
    def check_compatibility(
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        compatibility_type: CompatibilityType
    ) -> Tuple[bool, List[str]]:
        """Check if new schema is compatible with old schema."""
        errors = []

        if compatibility_type == CompatibilityType.NONE:
            return True, []

        required_fields_old = SchemaCompatibilityChecker._extract_required(old_schema)
        required_fields_new = SchemaCompatibilityChecker._extract_required(new_schema)

        if compatibility_type in (CompatibilityType.BACKWARD, CompatibilityType.FULL):
            added_required = required_fields_new - required_fields_old
            if added_required:
                errors.append(f"Added required fields: {added_required}")

        if compatibility_type in (CompatibilityType.FORWARD, CompatibilityType.FULL):
            removed_required = required_fields_old - required_fields_new
            if removed_required:
                errors.append(f"Removed required fields: {removed_required}")

        type_changes = SchemaCompatibilityChecker._check_type_changes(
            old_schema, new_schema
        )
        errors.extend(type_changes)

        return len(errors) == 0, errors

    @staticmethod
    def _extract_required(schema: Dict[str, Any]) -> Set[str]:
        """Extract required fields from JSON schema."""
        required = set()
        if "required" in schema:
            required.update(schema["required"])
        if "properties" in schema:
            for prop_name, prop_schema in schema["properties"].items():
                if "required" in prop_schema:
                    for nested in SchemaCompatibilityChecker._extract_nested_required(prop_schema):
                        required.add(f"{prop_name}.{nested}")
        return required

    @staticmethod
    def _extract_nested_required(schema: Dict[str, Any]) -> Set[str]:
        """Extract required fields from nested objects."""
        required = set()
        if "required" in schema:
            required.update(schema["required"])
        if "properties" in schema:
            for prop_schema in schema["properties"].values():
                required.update(SchemaCompatibilityChecker._extract_nested_required(prop_schema))
        return required

    @staticmethod
    def _check_type_changes(
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any]
    ) -> List[str]:
        """Check for breaking type changes."""
        errors = []

        old_types = SchemaCompatibilityChecker._get_types(old_schema)
        new_types = SchemaCompatibilityChecker._get_types(new_schema)

        if old_types and new_types:
            breaking_changes = [
                f"Type change from {ot} to {nt}"
                for ot in old_types
                for nt in new_types
                if not SchemaCompatibilityChecker._types_compatible(ot, nt)
            ]
            errors.extend(breaking_changes)

        return errors

    @staticmethod
    def _get_types(schema: Dict[str, Any]) -> Set[str]:
        """Get type information from schema."""
        types = set()
        if "type" in schema:
            types.add(schema["type"])
        if "anyOf" in schema:
            for s in schema["anyOf"]:
                if "type" in s:
                    types.add(s["type"])
        if "oneOf" in schema:
            for s in schema["oneOf"]:
                if "type" in s:
                    types.add(s["type"])
        return types

    @staticmethod
    def _types_compatible(type1: str, type2: str) -> bool:
        """Check if two types are compatible."""
        compatible_pairs = {
            ("integer", "number"),
            ("number", "integer"),
            ("string", "string"),
            ("boolean", "boolean"),
            ("array", "array"),
            ("object", "object")
        }
        return (type1, type2) in compatible_pairs or type1 == type2


class SchemaRegistryValidator:
    """Validate schemas against standard rules."""

    @staticmethod
    def validate(schema: Dict[str, Any], schema_type: SchemaType) -> Tuple[bool, List[str]]:
        """Validate schema structure."""
        errors = []

        if schema_type == SchemaType.JSON_SCHEMA:
            if "type" not in schema and "$ref" not in schema:
                errors.append("JSON Schema must have 'type' or '$ref'")

        elif schema_type == SchemaType.OPENAPI:
            if "openapi" not in schema and "swagger" not in schema:
                errors.append("OpenAPI schema must have 'openapi' or 'swagger' version")
            if "paths" not in schema:
                errors.append("OpenAPI schema must have 'paths' defined")

        return len(errors) == 0, errors


class ApiSchemaRegistryAction(BaseAction):
    """Action for API schema registry management."""

    def __init__(self):
        super().__init__(name="api_schema_registry")
        self._config = SchemaRegistryConfig()
        self._schemas: Dict[str, SchemaDefinition] = {}
        self._lock = threading.Lock()
        self._compatibility_checker = SchemaCompatibilityChecker()

    def configure(self, config: SchemaRegistryConfig):
        """Configure schema registry settings."""
        self._config = config

    def register_schema(
        self,
        schema_id: str,
        schema_type: SchemaType,
        version: str,
        schema_content: Dict[str, Any],
        compatibility: Optional[CompatibilityType] = None,
        created_by: Optional[str] = None,
        description: Optional[str] = None
    ) -> ActionResult:
        """Register a new schema or a new version of an existing schema."""
        try:
            with self._lock:
                if self._config.validate_on_register:
                    valid, errors = SchemaRegistryValidator.validate(schema_content, schema_type)
                    if not valid:
                        return ActionResult(success=False, error=f"Schema validation failed: {errors}")

                schema_version = SchemaVersion(
                    version=version,
                    schema_content=schema_content,
                    created_by=created_by,
                    description=description
                )

                if schema_id in self._schemas:
                    schema_def = self._schemas[schema_id]

                    if version in schema_def.versions:
                        return ActionResult(
                            success=False,
                            error=f"Version {version} already exists for schema {schema_id}"
                        )

                    if self._config.require_versioning:
                        latest = schema_def.versions.get(schema_def.latest_version)
                        if latest:
                            compat = compatibility or schema_def.compatibility
                            is_compatible, compat_errors = self._compatibility_checker.check_compatibility(
                                latest.schema_content,
                                schema_content,
                                compat
                            )
                            if not is_compatible:
                                return ActionResult(
                                    success=False,
                                    error=f"Compatibility check failed: {compat_errors}"
                                )

                    schema_def.versions[version] = schema_version
                    schema_def.latest_version = version

                else:
                    schema_def = SchemaDefinition(
                        schema_id=schema_id,
                        schema_type=schema_type,
                        latest_version=version,
                        versions={version: schema_version},
                        compatibility=compatibility or self._config.default_compatibility,
                        owner=created_by
                    )
                    self._schemas[schema_id] = schema_def

                return ActionResult(success=True, data={
                    "schema_id": schema_id,
                    "version": version,
                    "is_new": schema_id not in self._schemas
                })
        except Exception as e:
            logger.exception("Schema registration failed")
            return ActionResult(success=False, error=str(e))

    def get_schema(
        self,
        schema_id: str,
        version: Optional[str] = None
    ) -> ActionResult:
        """Get a schema by ID and optional version."""
        try:
            with self._lock:
                if schema_id not in self._schemas:
                    return ActionResult(success=False, error=f"Schema {schema_id} not found")

                schema_def = self._schemas[schema_id]

                if version:
                    if version not in schema_def.versions:
                        return ActionResult(success=False, error=f"Version {version} not found")
                    schema_version = schema_def.versions[version]
                else:
                    schema_version = schema_def.versions[schema_def.latest_version]
                    version = schema_def.latest_version

                return ActionResult(success=True, data={
                    "schema_id": schema_id,
                    "version": version,
                    "schema_type": schema_def.schema_type.value,
                    "schema_content": schema_version.schema_content,
                    "is_latest": version == schema_def.latest_version,
                    "deprecated": schema_version.deprecated
                })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def list_versions(self, schema_id: str) -> ActionResult:
        """List all versions of a schema."""
        try:
            with self._lock:
                if schema_id not in self._schemas:
                    return ActionResult(success=False, error=f"Schema {schema_id} not found")

                schema_def = self._schemas[schema_id]
                versions = []

                for ver, schema_ver in schema_def.versions.items():
                    versions.append({
                        "version": ver,
                        "created_at": schema_ver.created_at.isoformat(),
                        "created_by": schema_ver.created_by,
                        "description": schema_ver.description,
                        "deprecated": schema_ver.deprecated
                    })

                versions.sort(key=lambda v: v["version"], reverse=True)

                return ActionResult(success=True, data={
                    "schema_id": schema_id,
                    "versions": versions,
                    "latest_version": schema_def.latest_version
                })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def deprecate_version(
        self,
        schema_id: str,
        version: str
    ) -> ActionResult:
        """Mark a schema version as deprecated."""
        try:
            with self._lock:
                if schema_id not in self._schemas:
                    return ActionResult(success=False, error=f"Schema {schema_id} not found")

                schema_def = self._schemas[schema_id]
                if version not in schema_def.versions:
                    return ActionResult(success=False, error=f"Version {version} not found")

                schema_def.versions[version].deprecated = True
                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def check_compatibility(
        self,
        schema_id: str,
        new_version: str,
        new_schema: Dict[str, Any]
    ) -> ActionResult:
        """Check if a new schema version is compatible."""
        try:
            with self._lock:
                if schema_id not in self._schemas:
                    return ActionResult(success=False, error=f"Schema {schema_id} not found")

                schema_def = self._schemas[schema_id]
                latest = schema_def.versions[schema_def.latest_version]

                is_compatible, errors = self._compatibility_checker.check_compatibility(
                    latest.schema_content,
                    new_schema,
                    schema_def.compatibility
                )

                return ActionResult(
                    success=is_compatible,
                    data={
                        "schema_id": schema_id,
                        "new_version": new_version,
                        "compatible": is_compatible,
                        "errors": errors
                    }
                )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute schema registry action."""
        try:
            action = params.get("action")

            if action == "register":
                return self.register_schema(
                    params["schema_id"],
                    SchemaType(params["schema_type"]),
                    params["version"],
                    params["schema_content"],
                    CompatibilityType(params.get("compatibility", "full")) if params.get("compatibility") else None,
                    params.get("created_by"),
                    params.get("description")
                )
            elif action == "get":
                return self.get_schema(params["schema_id"], params.get("version"))
            elif action == "list_versions":
                return self.list_versions(params["schema_id"])
            elif action == "deprecate":
                return self.deprecate_version(params["schema_id"], params["version"])
            elif action == "check_compatibility":
                return self.check_compatibility(
                    params["schema_id"],
                    params["new_version"],
                    params["new_schema"]
                )
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
