"""Schema registry action module for RabAI AutoClick.

Provides schema registry operations:
- SchemaRegistryClient: Register and manage schemas
- SchemaVersionManager: Manage schema versions
- SchemaCompatibility: Check compatibility between versions
- AvroSchemaHandler: Handle Avro schemas specifically
- ProtoSchemaHandler: Handle Protocol Buffer schemas
"""

from __future__ import annotations

import json
import sys
import os
import hashlib
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchemaRegistryClientAction(BaseAction):
    """Register and manage schemas in the registry."""
    action_type = "schema_registry_client"
    display_name = "Schema注册客户端"
    description = "注册和管理Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            registry_path = params.get("registry_path", "/tmp/schema_registry")
            subject = params.get("subject", "")
            schema_str = params.get("schema_str", "")
            schema_type = params.get("schema_type", "AVRO")
            description = params.get("description", "")
            compatibility = params.get("compatibility", "BACKWARD")

            if not subject:
                return ActionResult(success=False, message="subject is required")

            subject_dir = os.path.join(registry_path, subject)
            os.makedirs(subject_dir, exist_ok=True)

            meta_file = os.path.join(subject_dir, "_meta.json")
            if os.path.exists(meta_file):
                with open(meta_file) as f:
                    meta = json.load(f)
            else:
                meta = {"subject": subject, "compatibility": compatibility, "versions": []}

            if operation == "register":
                if not schema_str:
                    return ActionResult(success=False, message="schema_str required")

                schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()[:16]

                existing = [v for v in meta.get("versions", []) if v.get("hash") == schema_hash]
                if existing:
                    return ActionResult(
                        success=True,
                        message=f"Schema already registered: version {existing[0]['version']}",
                        data=existing[0]
                    )

                new_version = len(meta.get("versions", [])) + 1
                version_entry = {
                    "version": new_version,
                    "schema_str": schema_str,
                    "schema_type": schema_type,
                    "hash": schema_hash,
                    "description": description,
                    "registered_at": datetime.now().isoformat(),
                    "compatibility": compatibility,
                }

                meta["versions"].append(version_entry)
                meta["latest_version"] = new_version
                meta["latest_hash"] = schema_hash
                meta["compatibility"] = compatibility

                with open(meta_file, "w") as f:
                    json.dump(meta, f, indent=2)

                return ActionResult(
                    success=True,
                    message=f"Registered schema: {subject} v{new_version}",
                    data={"subject": subject, "version": new_version, "hash": schema_hash}
                )

            elif operation == "get":
                version = params.get("version", meta.get("latest_version", 1))
                version_entry = next((v for v in meta.get("versions", []) if v["version"] == version), None)
                if not version_entry:
                    return ActionResult(success=False, message=f"Version {version} not found")

                return ActionResult(success=True, message=f"Schema: {subject} v{version}", data=version_entry)

            elif operation == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(meta.get('versions', []))} versions",
                    data={"subject": subject, "versions": meta.get("versions", [])}
                )

            elif operation == "delete":
                del_version = params.get("version", None)
                if del_version is None:
                    return ActionResult(success=False, message="version required for delete")

                meta["versions"] = [v for v in meta.get("versions", []) if v["version"] != del_version]
                with open(meta_file, "w") as f:
                    json.dump(meta, f, indent=2)

                return ActionResult(success=True, message=f"Deleted version {del_version}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class SchemaCompatibilityAction(BaseAction):
    """Check schema compatibility between versions."""
    action_type = "schema_compatibility"
    display_name = "Schema兼容性检查"
    description = "检查Schema版本兼容性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            old_schema = params.get("old_schema", {})
            new_schema = params.get("new_schema", {})
            schema_type = params.get("schema_type", "AVRO")
            compatibility_mode = params.get("compatibility_mode", "BACKWARD")

            if operation == "check":
                if not old_schema or not new_schema:
                    return ActionResult(success=False, message="old_schema and new_schema required")

                old_fields = set(f["name"] for f in old_schema.get("fields", []))
                new_fields = set(f["name"] for f in new_schema.get("fields", []))
                removed_fields = old_fields - new_fields
                added_fields = new_fields - old_fields

                if compatibility_mode in ("BACKWARD", "FULL"):
                    if removed_fields:
                        return ActionResult(
                            success=True,
                            message="NOT COMPATIBLE: fields removed",
                            data={"compatible": False, "removed_fields": list(removed_fields), "reason": "backward_compat_violation"}
                        )

                if compatibility_mode in ("FORWARD", "FULL"):
                    if added_fields:
                        return ActionResult(
                            success=True,
                            message="NOT COMPATIBLE: fields added (FORWARD mode)",
                            data={"compatible": False, "added_fields": list(added_fields), "reason": "forward_compat_violation"}
                        )

                for field_name in old_fields & new_fields:
                    old_field = next((f for f in old_schema.get("fields", []) if f["name"] == field_name), None)
                    new_field = next((f for f in new_schema.get("fields", []) if f["name"] == field_name), None)
                    if old_field and new_field:
                        if old_field.get("type") != new_field.get("type"):
                            return ActionResult(
                                success=True,
                                message=f"NOT COMPATIBLE: type changed for '{field_name}'",
                                data={"compatible": False, "field": field_name, "old_type": old_field.get("type"), "new_type": new_field.get("type")}
                            )

                return ActionResult(
                    success=True,
                    message="COMPATIBLE",
                    data={
                        "compatible": True,
                        "compatibility_mode": compatibility_mode,
                        "removed_fields": list(removed_fields),
                        "added_fields": list(added_fields),
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class AvroSchemaHandlerAction(BaseAction):
    """Handle Avro schema-specific operations."""
    action_type = "avro_schema_handler"
    display_name = "Avro Schema处理"
    description = "Avro Schema特定操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "parse")
            schema_str = params.get("schema_str", "")
            schema_dict = params.get("schema_dict", {})

            if operation == "parse":
                if not schema_str and not schema_dict:
                    return ActionResult(success=False, message="schema_str or schema_dict required")

                if schema_str:
                    try:
                        schema_dict = json.loads(schema_str)
                    except:
                        return ActionResult(success=False, message="Invalid JSON schema")

                errors = self._validate_avro(schema_dict)
                if errors:
                    return ActionResult(success=False, message=f"Schema validation errors: {errors}")

                return ActionResult(
                    success=True,
                    message="Avro schema valid",
                    data={"schema": schema_dict, "name": schema_dict.get("name", ""), "namespace": schema_dict.get("namespace", "")}
                )

            elif operation == "extract_fields":
                if not schema_dict and not schema_str:
                    return ActionResult(success=False, message="schema required")

                if schema_str:
                    schema_dict = json.loads(schema_str)

                fields = schema_dict.get("fields", [])
                return ActionResult(
                    success=True,
                    message=f"Extracted {len(fields)} fields",
                    data={"fields": fields, "count": len(fields)}
                )

            elif operation == "extend_schema":
                base_schema = params.get("base_schema", {})
                additional_fields = params.get("additional_fields", [])

                if not base_schema or not additional_fields:
                    return ActionResult(success=False, message="base_schema and additional_fields required")

                new_schema = {**base_schema}
                existing_names = {f["name"] for f in base_schema.get("fields", [])}
                new_fields = base_schema.get("fields", []) + [
                    f for f in additional_fields if f.get("name") not in existing_names
                ]
                new_schema["fields"] = new_fields

                return ActionResult(
                    success=True,
                    message=f"Extended schema with {len(additional_fields)} fields",
                    data={"schema": new_schema, "fields_count": len(new_fields)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _validate_avro(self, schema: Dict[str, Any]) -> List[str]:
        errors = []
        if "type" not in schema:
            errors.append("Missing 'type' field")
        if schema.get("type") == "record" and "name" not in schema:
            errors.append("Record schema missing 'name'")
        if "fields" in schema and not isinstance(schema["fields"], list):
            errors.append("'fields' must be a list")
        return errors


class ProtoSchemaHandlerAction(BaseAction):
    """Handle Protocol Buffer schema operations."""
    action_type = "proto_schema_handler"
    display_name = "Proto Schema处理"
    description = "Protocol Buffer Schema处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "parse")
            proto_str = params.get("proto_str", "")
            proto_file = params.get("proto_file", "")

            if operation == "parse":
                if proto_file and os.path.exists(proto_file):
                    with open(proto_file) as f:
                        proto_str = f.read()
                elif not proto_str:
                    return ActionResult(success=False, message="proto_str or proto_file required")

                messages = self._extract_messages(proto_str)
                services = self._extract_services(proto_str)
                enums = self._extract_enums(proto_str)

                return ActionResult(
                    success=True,
                    message=f"Parsed proto: {len(messages)} messages, {len(services)} services",
                    data={"messages": messages, "services": services, "enums": enums}
                )

            elif operation == "validate":
                if proto_file and os.path.exists(proto_file):
                    with open(proto_file) as f:
                        proto_str = f.read()

                errors = []
                if "syntax" not in proto_str:
                    errors.append("Missing syntax declaration")
                if "message" not in proto_str and "service" not in proto_str:
                    errors.append("No messages or services found")

                return ActionResult(
                    success=len(errors) == 0,
                    message="Valid proto" if not errors else f"Errors: {errors}",
                    data={"errors": errors}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _extract_messages(self, proto_str: str) -> List[Dict[str, Any]]:
        import re
        messages = []
        pattern = r'message\s+(\w+)\s*\{([^}]*)\}'
        for match in re.finditer(pattern, proto_str):
            name = match.group(1)
            body = match.group(2)
            fields = re.findall(r'(\w+)\s+(\w+)\s*=\s*(\d+);', body)
            messages.append({
                "name": name,
                "fields": [{"type": f[0], "name": f[1], "tag": int(f[2])} for f in fields],
            })
        return messages

    def _extract_services(self, proto_str: str) -> List[str]:
        import re
        return re.findall(r'service\s+(\w+)\s*\{', proto_str)

    def _extract_enums(self, proto_str: str) -> List[Dict[str, Any]]:
        import re
        enums = []
        pattern = r'enum\s+(\w+)\s*\{([^}]*)\}'
        for match in re.finditer(pattern, proto_str):
            name = match.group(1)
            values = re.findall(r'(\w+)\s*=\s*(\d+);', match.group(2))
            enums.append({
                "name": name,
                "values": {v[0]: int(v[1]) for v in values},
            })
        return enums
