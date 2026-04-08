"""API schema action module for RabAI AutoClick.

Provides API schema operations:
- SchemaGeneratorAction: Generate API schemas
- SchemaValidatorAction: Validate API schemas
- SchemaMergerAction: Merge API schemas
- SchemaConverterAction: Convert between schema formats
- SchemaVersionerAction: Version API schemas
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchemaGeneratorAction(BaseAction):
    """Generate API schemas."""
    action_type = "schema_generator"
    display_name = "Schema生成"
    description = "生成API Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema_type = params.get("schema_type", "openapi")
            api_name = params.get("api_name", "MyAPI")
            version = params.get("version", "1.0.0")
            base_path = params.get("base_path", "/api/v1")
            endpoints = params.get("endpoints", [])

            if schema_type == "openapi":
                schema = self._generate_openapi_schema(api_name, version, base_path, endpoints)
            elif schema_type == "graphql":
                schema = self._generate_graphql_schema(api_name, endpoints)
            elif schema_type == "json":
                schema = self._generate_json_schema(api_name, endpoints)
            else:
                schema = {"name": api_name, "version": version}

            return ActionResult(
                success=True,
                data={
                    "schema_type": schema_type,
                    "api_name": api_name,
                    "version": version,
                    "schema": schema,
                    "endpoints_count": len(endpoints),
                    "generated_at": datetime.now().isoformat()
                },
                message=f"Schema generated: {schema_type} for {api_name} v{version}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema generator error: {str(e)}")

    def _generate_openapi_schema(self, name: str, version: str, base_path: str, endpoints: List) -> Dict:
        return {
            "openapi": "3.0.0",
            "info": {
                "title": name,
                "version": version
            },
            "paths": {
                f"{base_path}/{ep.get('path', 'default')}": {
                    ep.get("method", "get").lower(): {
                        "responses": {"200": {"description": "Success"}}
                    }
                } for ep in endpoints
            }
        }

    def _generate_graphql_schema(self, name: str, endpoints: List) -> Dict:
        return {
            "type": "graphql",
            "name": name,
            "types": [{"name": "Query"}, {"name": "Mutation"}]
        }

    def _generate_json_schema(self, name: str, endpoints: List) -> Dict:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": name,
            "type": "object",
            "properties": {}
        }


class SchemaValidatorAction(BaseAction):
    """Validate API schemas."""
    action_type = "schema_validator"
    display_name = "Schema验证"
    description = "验证API Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema = params.get("schema", {})
            schema_type = params.get("schema_type", "openapi")
            strict_mode = params.get("strict_mode", False)

            if not schema:
                return ActionResult(success=False, message="schema is required")

            errors = []
            warnings = []

            if schema_type == "openapi":
                errors.extend(self._validate_openapi(schema))
            elif schema_type == "graphql":
                errors.extend(self._validate_graphql(schema))
            elif schema_type == "json":
                errors.extend(self._validate_json_schema(schema))

            if strict_mode and "version" not in schema:
                errors.append("Missing 'version' field in strict mode")

            return ActionResult(
                success=len(errors) == 0,
                data={
                    "schema_type": schema_type,
                    "valid": len(errors) == 0,
                    "errors": errors,
                    "warnings": warnings,
                    "error_count": len(errors),
                    "warning_count": len(warnings)
                },
                message=f"Schema validation: {'PASSED' if len(errors) == 0 else 'FAILED'} ({len(errors)} errors)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema validator error: {str(e)}")

    def _validate_openapi(self, schema: Dict) -> List:
        errors = []
        if "openapi" not in schema and "swagger" not in schema:
            errors.append("Missing 'openapi' or 'swagger' field")
        if "info" not in schema:
            errors.append("Missing 'info' field")
        return errors

    def _validate_graphql(self, schema: Dict) -> List:
        errors = []
        if "types" not in schema:
            errors.append("Missing 'types' field")
        return errors

    def _validate_json_schema(self, schema: Dict) -> List:
        errors = []
        if "$schema" not in schema:
            errors.append("Missing '$schema' field")
        return errors


class SchemaMergerAction(BaseAction):
    """Merge API schemas."""
    action_type = "schema_merger"
    display_name = "Schema合并"
    description = "合并API Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schemas = params.get("schemas", [])
            merge_strategy = params.get("merge_strategy", "override")

            if len(schemas) < 2:
                return ActionResult(success=False, message="At least 2 schemas required for merge")

            merged = {}

            for schema in schemas:
                if merge_strategy == "override":
                    merged.update(schema)
                elif merge_strategy == "deep":
                    merged = self._deep_merge(merged, schema)
                elif merge_strategy == "keep":
                    for key, value in schema.items():
                        if key not in merged:
                            merged[key] = value

            return ActionResult(
                success=True,
                data={
                    "schemas_merged": len(schemas),
                    "merge_strategy": merge_strategy,
                    "merged_schema": merged,
                    "merged_keys": list(merged.keys())
                },
                message=f"Merged {len(schemas)} schemas using {merge_strategy} strategy"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema merger error: {str(e)}")

    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        result = base.copy()
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result


class SchemaConverterAction(BaseAction):
    """Convert between schema formats."""
    action_type = "schema_converter"
    display_name = "Schema转换"
    description = "Schema格式转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema = params.get("schema", {})
            source_format = params.get("source_format", "openapi")
            target_format = params.get("target_format", "json")

            if not schema:
                return ActionResult(success=False, message="schema is required")

            converted = {}

            if source_format == "openapi" and target_format == "json":
                converted = self._openapi_to_json(schema)
            elif source_format == "json" and target_format == "openapi":
                converted = self._json_to_openapi(schema)
            elif source_format == "graphql" and target_format == "openapi":
                converted = self._graphql_to_openapi(schema)
            else:
                converted = schema

            return ActionResult(
                success=True,
                data={
                    "source_format": source_format,
                    "target_format": target_format,
                    "converted_schema": converted,
                    "converted_at": datetime.now().isoformat()
                },
                message=f"Schema converted: {source_format} -> {target_format}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema converter error: {str(e)}")

    def _openapi_to_json(self, schema: Dict) -> Dict:
        return {"$schema": "http://json-schema.org/draft-07/schema#", "paths": schema.get("paths", {})}

    def _json_to_openapi(self, schema: Dict) -> Dict:
        return {"openapi": "3.0.0", "info": {"title": "Converted"}, "paths": schema.get("paths", {})}

    def _graphql_to_openapi(self, schema: Dict) -> Dict:
        return {"openapi": "3.0.0", "info": {"title": "GraphQL Converted"}, "paths": {}}


class SchemaVersionerAction(BaseAction):
    """Version API schemas."""
    action_type = "schema_versioner"
    display_name = "Schema版本控制"
    description = "管理Schema版本"

    def __init__(self):
        super().__init__()
        self._versions = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            schema_name = params.get("schema_name", "default")
            version = params.get("version", "1.0.0")
            schema = params.get("schema", {})

            if operation == "add":
                if schema_name not in self._versions:
                    self._versions[schema_name] = {}
                self._versions[schema_name][version] = {
                    "schema": schema,
                    "added_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "schema_name": schema_name,
                        "version": version,
                        "versions_count": len(self._versions[schema_name])
                    },
                    message=f"Schema version {version} added for '{schema_name}'"
                )

            elif operation == "get":
                if schema_name not in self._versions:
                    return ActionResult(success=False, message=f"Schema '{schema_name}' not found")
                if version not in self._versions[schema_name]:
                    return ActionResult(success=False, message=f"Version {version} not found")

                return ActionResult(
                    success=True,
                    data={
                        "schema_name": schema_name,
                        "version": version,
                        "schema": self._versions[schema_name][version]["schema"]
                    },
                    message=f"Retrieved {schema_name} v{version}"
                )

            elif operation == "list":
                if schema_name not in self._versions:
                    return ActionResult(success=True, data={"versions": [], "count": 0})
                versions = list(self._versions[schema_name].keys())
                return ActionResult(
                    success=True,
                    data={
                        "schema_name": schema_name,
                        "versions": versions,
                        "count": len(versions)
                    },
                    message=f"Versions for '{schema_name}': {versions}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Schema versioner error: {str(e)}")
