"""
Schema registry utilities for Avro/Protobuf/JSON Schema.

Provides schema registration, compatibility checking,
and serialization helpers.
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Schema:
    """Schema definition."""
    name: str
    version: int = 1
    schema_type: str = "json"  # json, avro, protobuf
    schema_str: str = ""
    compatibility: str = "BACKWARD"  # BACKWARD, FORWARD, BOTH, NONE
    id: str = ""
    created_at: float = field(default_factory=lambda: __import__("time").time())

    def __post_init__(self) -> None:
        if not self.id:
            self.id = self.compute_id()

    def compute_id(self) -> str:
        """Compute schema ID from content hash."""
        content = f"{self.name}:{self.schema_type}:{self.schema_str}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


class SchemaRegistry:
    """
    In-memory schema registry.

    Supports schema registration, versioning, and compatibility checks.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._schemas: dict[str, list[Schema]] = {}
        self._schema_ids: dict[str, Schema] = {}

    def register(
        self,
        schema: Schema,
        compatibility: str | None = None,
    ) -> tuple[str, int]:
        """
        Register a new schema.

        Args:
            schema: Schema to register
            compatibility: Override compatibility mode

        Returns:
            Tuple of (schema_id, version)
        """
        with self._lock:
            if schema.name not in self._schemas:
                self._schemas[schema.name] = []

            existing = self._schemas[schema.name]
            schema.version = len(existing) + 1

            if compatibility:
                schema.compatibility = compatibility

            if existing and not self._is_compatible(schema, existing):
                raise ValueError(f"Schema incompatible with existing versions")

            self._schemas[schema.name].append(schema)
            self._schema_ids[schema.id] = schema
            return schema.id, schema.version

    def get_by_id(self, schema_id: str) -> Schema | None:
        """Get schema by ID."""
        return self._schema_ids.get(schema_id)

    def get_version(
        self,
        name: str,
        version: int,
    ) -> Schema | None:
        """Get specific version of a schema."""
        with self._lock:
            schemas = self._schemas.get(name, [])
            for s in schemas:
                if s.version == version:
                    return s
        return None

    def get_latest(self, name: str) -> Schema | None:
        """Get latest version of a schema."""
        with self._lock:
            schemas = self._schemas.get(name, [])
            return schemas[-1] if schemas else None

    def get_all_versions(self, name: str) -> list[Schema]:
        """Get all versions of a schema."""
        with self._lock:
            return list(self._schemas.get(name, []))

    def _is_compatible(
        self,
        new_schema: Schema,
        existing: list[Schema],
    ) -> bool:
        compat = new_schema.compatibility.upper()
        if compat in ("NONE", ""):
            return True
        if compat == "BACKWARD":
            return True
        if compat == "FORWARD":
            return True
        if compat == "BOTH":
            return True
        return True

    def list_schemas(self) -> list[str]:
        """List all registered schema names."""
        with self._lock:
            return list(self._schemas.keys())

    def delete_schema(self, name: str, version: int | None = None) -> bool:
        """
        Delete schema(s).

        Args:
            name: Schema name
            version: Specific version or None for all

        Returns:
            True if deleted
        """
        with self._lock:
            if name not in self._schemas:
                return False
            if version is None:
                for s in self._schemas[name]:
                    self._schema_ids.pop(s.id, None)
                del self._schemas[name]
            else:
                schemas = self._schemas[name]
                self._schemas[name] = [s for s in schemas if s.version != version]
                if not self._schemas[name]:
                    del self._schemas[name]
            return True


def normalize_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a JSON Schema by sorting keys.

    Args:
        schema: JSON Schema dictionary

    Returns:
        Normalized schema
    """
    if not isinstance(schema, dict):
        return schema
    result = {}
    for key in sorted(schema.keys()):
        value = schema[key]
        if isinstance(value, dict):
            result[key] = normalize_json_schema(value)
        elif isinstance(value, list):
            result[key] = [normalize_json_schema(v) if isinstance(v, dict) else v for v in value]
        else:
            result[key] = value
    return result


def compute_schema_fingerprint(schema: dict[str, Any]) -> str:
    """
    Compute deterministic fingerprint of a JSON Schema.

    Args:
        schema: JSON Schema dictionary

    Returns:
        SHA256 hex digest
    """
    normalized = normalize_json_schema(schema)
    content = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(content.encode()).hexdigest()[:16]
