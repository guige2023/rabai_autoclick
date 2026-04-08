"""Data schema action module for RabAI AutoClick.

Provides data schema operations:
- SchemaDefineAction: Define a data schema
- SchemaValidateAction: Validate data against schema
- SchemaInferAction: Infer schema from data
- SchemaMergeAction: Merge two schemas
- SchemaEvolveAction: Evolve schema with backward compatibility
"""

import hashlib
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchemaDefineAction(BaseAction):
    """Define a data schema."""
    action_type = "schema_define"
    display_name = "定义Schema"
    description = "定义数据Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            fields = params.get("fields", [])
            version = params.get("version", "1.0.0")

            if not name:
                return ActionResult(success=False, message="name is required")
            if not fields:
                return ActionResult(success=False, message="fields are required")

            schema_id = hashlib.md5(f"{name}:{version}".encode()).hexdigest()[:12]

            if not hasattr(context, "data_schemas"):
                context.data_schemas = {}
            context.data_schemas[schema_id] = {
                "schema_id": schema_id,
                "name": name,
                "version": version,
                "fields": fields,
                "defined_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"schema_id": schema_id, "name": name, "version": version, "field_count": len(fields)},
                message=f"Schema {schema_id} defined: {name} v{version} with {len(fields)} fields",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema define failed: {e}")


class SchemaValidateAction(BaseAction):
    """Validate data against schema."""
    action_type = "schema_validate"
    display_name = "Schema验证"
    description = "验证数据是否符合Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema_id = params.get("schema_id", "")
            data = params.get("data", {})

            if not schema_id:
                return ActionResult(success=False, message="schema_id is required")

            schemas = getattr(context, "data_schemas", {})
            if schema_id not in schemas:
                return ActionResult(success=False, message=f"Schema {schema_id} not found")

            schema = schemas[schema_id]
            errors = []
            for field in schema.get("fields", []):
                field_name = field.get("name")
                required = field.get("required", False)
                field_type = field.get("type", "string")

                if required and field_name not in data:
                    errors.append(f"Missing required field: {field_name}")
                elif field_name in data and not isinstance(data[field_name], (str, int, float, bool, list, dict)):
                    errors.append(f"Field {field_name} type mismatch: expected {field_type}")

            return ActionResult(
                success=len(errors) == 0,
                data={"schema_id": schema_id, "valid": len(errors) == 0, "errors": errors},
                message=f"Schema validation: {'PASSED' if not errors else f'{len(errors)} errors'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema validate failed: {e}")


class SchemaInferAction(BaseAction):
    """Infer schema from data."""
    action_type = "schema_infer"
    display_name = "推断Schema"
    description = "从数据推断Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            if not data:
                return ActionResult(success=False, message="data is required")

            inferred_fields = []
            for key, value in data.items():
                inferred_type = type(value).__name__
                inferred_fields.append({
                    "name": key,
                    "type": inferred_type,
                    "required": False,
                    "inferred": True,
                })

            schema_id = hashlib.md5(str(data).encode()).hexdigest()[:12]

            return ActionResult(
                success=True,
                data={"schema_id": schema_id, "inferred_fields": inferred_fields, "field_count": len(inferred_fields)},
                message=f"Inferred schema with {len(inferred_fields)} fields",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema infer failed: {e}")


class SchemaMergeAction(BaseAction):
    """Merge two schemas."""
    action_type = "schema_merge"
    display_name = "合并Schema"
    description = "合并两个Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema_id_a = params.get("schema_id_a", "")
            schema_id_b = params.get("schema_id_b", "")

            if not schema_id_a or not schema_id_b:
                return ActionResult(success=False, message="schema_id_a and schema_id_b are required")

            schemas = getattr(context, "data_schemas", {})
            schema_a = schemas.get(schema_id_a, {})
            schema_b = schemas.get(schema_id_b, {})

            fields_a = {f["name"]: f for f in schema_a.get("fields", [])}
            fields_b = {f["name"]: f for f in schema_b.get("fields", [])}

            merged_fields = list(fields_a.values())
            added = []
            for name, field in fields_b.items():
                if name not in fields_a:
                    merged_fields.append(field)
                    added.append(name)

            merged_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"merged_id": merged_id, "field_count": len(merged_fields), "added_from_b": added},
                message=f"Merged schemas: {len(merged_fields)} total, {len(added)} added",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema merge failed: {e}")


class SchemaEvolveAction(BaseAction):
    """Evolve schema with backward compatibility."""
    action_type = "schema_evolve"
    display_name = "演进Schema"
    description = "演进Schema保持向后兼容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema_id = params.get("schema_id", "")
            new_fields = params.get("new_fields", [])
            breaking = params.get("breaking_changes", False)

            if not schema_id:
                return ActionResult(success=False, message="schema_id is required")

            schemas = getattr(context, "data_schemas", {})
            if schema_id not in schemas:
                return ActionResult(success=False, message=f"Schema {schema_id} not found")

            schema = schemas[schema_id]
            current_version = schema.get("version", "1.0.0")
            parts = current_version.split(".")
            if breaking:
                parts[0] = str(int(parts[0]) + 1)
                parts[1] = "0"
            else:
                parts[-1] = str(int(parts[-1]) + 1)
            new_version = ".".join(parts)

            schema["version"] = new_version
            schema["fields"].extend(new_fields)

            return ActionResult(
                success=True,
                data={"schema_id": schema_id, "old_version": current_version, "new_version": new_version, "breaking": breaking},
                message=f"Schema {schema_id} evolved: {current_version} -> {new_version}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema evolve failed: {e}")
