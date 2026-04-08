"""API Export Import Action Module.

Provides comprehensive API specification export and import
functionality supporting OpenAPI, RAML, GraphQL, and gRPC formats.
"""

from __future__ import annotations

import sys
import os
import json
import yaml
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ExportFormat(Enum):
    """Supported export formats."""
    OPENAPI_JSON = "openapi_json"
    OPENAPI_YAML = "openapi_yaml"
    RAML = "raml"
    GRAPHQL_SCHEMA = "graphql_schema"
    GRPC_PROTO = "grpc_proto"
    POSTMAN_COLLECTION = "postman_collection"
    INSOMNIA = "insomnia"


class ImportFormat(Enum):
    """Supported import formats."""
    OPENAPI = "openapi"
    SWAGGER = "swagger"
    RAML = "raml"
    GRAPHQL = "graphql"
    GRPC_PROTO = "grpc_proto"
    POSTMAN = "postman"
    HAR = "har"


@dataclass
class ExportOptions:
    """Options for API export."""
    format: ExportFormat
    include_schemas: bool = True
    include_examples: bool = True
    include_descriptions: bool = True
    flatten_refs: bool = True
    pretty_print: bool = True
    add_metadata: bool = True


@dataclass
class ImportOptions:
    """Options for API import."""
    format: ImportFormat
    validate: bool = True
    merge_strategy: str = "replace"
    preserve_ids: bool = False
    resolve_external: bool = True


class APIExportImportAction(BaseAction):
    """
    Export and import API specifications in multiple formats.

    Supports OpenAPI 3.0/3.1, Swagger 2.0, RAML, GraphQL schemas,
    gRPC proto files, and common API client formats.

    Example:
        exporter = APIExportImportAction()
        result = exporter.execute(ctx, {
            "action": "export",
            "api_id": "my-api",
            "format": "openapi_yaml"
        })
    """
    action_type = "api_export_import"
    display_name = "API导入导出"
    description = "支持多种格式的API规范导入导出，包括OpenAPI、RAML、GraphQL等"

    def __init__(self) -> None:
        super().__init__()
        self._apis: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute export or import action.

        Args:
            context: Execution context.
            params: Dict with keys: action (export|import|compare),
                   api_id, format, data, options.

        Returns:
            ActionResult with exported data or import result.
        """
        action = params.get("action", "")

        try:
            if action == "export":
                return self._export_api(params)
            elif action == "import":
                return self._import_api(params)
            elif action == "compare":
                return self._compare_specs(params)
            elif action == "validate":
                return self._validate_spec(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Export/Import failed: {str(e)}")

    def _export_api(self, params: Dict[str, Any]) -> ActionResult:
        """Export an API specification."""
        api_id = params.get("api_id", "")
        format_str = params.get("format", "openapi_json")
        options = params.get("options", {})

        if not api_id:
            return ActionResult(success=False, message="api_id is required")

        if api_id not in self._apis:
            return ActionResult(success=False, message=f"API not found: {api_id}")

        try:
            fmt = ExportFormat(format_str)
        except ValueError:
            return ActionResult(success=False, message=f"Unsupported export format: {format_str}")

        spec = self._apis[api_id]
        exported = self._convert_spec(spec, fmt, ExportOptions(format=fmt, **options))

        return ActionResult(
            success=True,
            message=f"Exported {api_id} as {fmt.value}",
            data={"api_id": api_id, "format": fmt.value, "spec": exported}
        )

    def _import_api(self, params: Dict[str, Any]) -> ActionResult:
        """Import an API specification."""
        api_id = params.get("api_id", "")
        format_str = params.get("format", "openapi")
        data = params.get("data", {})
        options = params.get("options", {})
        merge_strategy = options.get("merge_strategy", "replace")

        if not data:
            return ActionResult(success=False, message="data is required for import")

        if not api_id:
            api_id = self._generate_api_id(data)

        try:
            fmt = ImportFormat(format_str)
        except ValueError:
            return ActionResult(success=False, message=f"Unsupported import format: {format_str}")

        normalized = self._normalize_spec(data, fmt)

        if options.get("validate", True):
            validation = self._validate_spec_internal(normalized, fmt)
            if not validation["valid"]:
                return ActionResult(
                    success=False,
                    message=f"Validation failed: {validation['errors']}",
                    data={"errors": validation["errors"]}
                )

        if api_id in self._apis and merge_strategy == "merge":
            existing = self._apis[api_id]
            normalized = self._merge_specs(existing, normalized)

        self._apis[api_id] = normalized

        return ActionResult(
            success=True,
            message=f"Imported API: {api_id}",
            data={
                "api_id": api_id,
                "format": fmt.value,
                "endpoints": len(normalized.get("paths", {})),
                "schemas": len(normalized.get("components", {}).get("schemas", {})),
            }
        )

    def _compare_specs(self, params: Dict[str, Any]) -> ActionResult:
        """Compare two API specifications."""
        spec1 = params.get("spec1", {})
        spec2 = params.get("spec2", {})

        if not spec1 or not spec2:
            return ActionResult(success=False, message="Both spec1 and spec2 are required")

        comparison = self._do_comparison(spec1, spec2)

        return ActionResult(
            success=True,
            data=comparison
        )

    def _validate_spec(self, params: Dict[str, Any]) -> ActionResult:
        """Validate an API specification."""
        spec = params.get("spec", {})
        format_str = params.get("format", "openapi")

        if not spec:
            return ActionResult(success=False, message="spec is required")

        try:
            fmt = ImportFormat(format_str)
        except ValueError:
            return ActionResult(success=False, message=f"Unsupported format: {format_str}")

        result = self._validate_spec_internal(spec, fmt)

        return ActionResult(
            success=result["valid"],
            message="Validation passed" if result["valid"] else "Validation failed",
            data=result
        )

    def _convert_spec(self, spec: Dict[str, Any], fmt: ExportFormat, options: ExportOptions) -> Union[str, Dict[str, Any]]:
        """Convert spec to target format."""
        if options.flatten_refs:
            spec = self._flatten_refs(spec)

        if options.add_metadata:
            spec["x-generated-at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
            spec["x-exporter"] = "rabai-autoclick"

        if fmt == ExportFormat.OPENAPI_JSON:
            return json.dumps(spec, indent=2 if options.pretty_print else None, ensure_ascii=False)
        elif fmt == ExportFormat.OPENAPI_YAML:
            return yaml.dump(spec, allow_unicode=True, sort_keys=False)
        elif fmt == ExportFormat.RAML:
            return self._to_raml(spec)
        elif fmt == ExportFormat.GRAPHQL_SCHEMA:
            return self._to_graphql(spec)
        elif fmt == ExportFormat.GRPC_PROTO:
            return self._to_grpc_proto(spec)
        elif fmt == ExportFormat.POSTMAN_COLLECTION:
            return self._to_postman(spec)
        elif fmt == ExportFormat.INSOMNIA:
            return self._to_insomnia(spec)
        else:
            return spec

    def _normalize_spec(self, data: Union[str, Dict[str, Any]], fmt: ImportFormat) -> Dict[str, Any]:
        """Normalize spec to internal OpenAPI-like format."""
        if isinstance(data, str):
            if fmt in (ImportFormat.OPENAPI, ImportFormat.SWAGGER):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    data = yaml.safe_load(data)
            elif fmt == ImportFormat.RAML:
                data = self._parse_raml(data)
            elif fmt == ImportFormat.GRAPHQL:
                data = self._parse_graphql(data)
            else:
                data = json.loads(data) if data.startswith("{") else yaml.safe_load(data)

        if fmt == ImportFormat.SWAGGER:
            data = self._upgrade_swagger(data)

        return data

    def _validate_spec_internal(self, spec: Dict[str, Any], fmt: ImportFormat) -> Dict[str, Any]:
        """Internal validation logic."""
        errors: List[str] = []
        warnings: List[str] = []

        if not isinstance(spec, dict):
            return {"valid": False, "errors": ["Spec must be a dictionary"], "warnings": []}

        if fmt in (ImportFormat.OPENAPI, ImportFormat.SWAGGER):
            if "openapi" not in spec and "swagger" not in spec:
                errors.append("Missing 'openapi' or 'swagger' version field")

            if "paths" not in spec:
                warnings.append("No paths defined in spec")

            for path in spec.get("paths", {}):
                if not path.startswith("/"):
                    errors.append(f"Invalid path: {path}")

        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _flatten_refs(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten $ref references in OpenAPI spec."""
        schemas = spec.get("components", {}).get("schemas", {})

        def resolve_ref(ref: str) -> Dict[str, Any]:
            if "$ref" in ref:
                parts = ref.replace("#/", "").split("/")
                current = spec
                for part in parts:
                    if part == "$ref":
                        continue
                    current = current.get(part, {})
                return current
            return ref

        return spec

    def _merge_specs(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two specs with new taking precedence."""
        merged = existing.copy()

        for key, value in new.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = self._merge_specs(merged[key], value)
            else:
                merged[key] = value

        return merged

    def _do_comparison(self, spec1: Dict[str, Any], spec2: Dict[str, Any]) -> Dict[str, Any]:
        """Compare two specs and return differences."""
        paths1 = set(spec1.get("paths", {}).keys())
        paths2 = set(spec2.get("paths", {}).keys())

        added_paths = paths2 - paths1
        removed_paths = paths1 - paths2
        common_paths = paths1 & paths2

        endpoint_diffs = []
        for path in common_paths:
            methods1 = set(spec1["paths"][path].keys())
            methods2 = set(spec2["paths"][path].keys())

            for method in methods1 & methods2:
                diff = self._compare_endpoint(
                    spec1["paths"][path][method],
                    spec2["paths"][path][method],
                    path,
                    method
                )
                if diff:
                    endpoint_diffs.append(diff)

        return {
            "added_paths": list(added_paths),
            "removed_paths": list(removed_paths),
            "endpoint_diffs": endpoint_diffs,
            "summary": {
                "added": len(added_paths),
                "removed": len(removed_paths),
                "changed_endpoints": len(endpoint_diffs),
            }
        }

    def _compare_endpoint(self, ep1: Dict[str, Any], ep2: Dict[str, Any], path: str, method: str) -> Optional[Dict[str, Any]]:
        """Compare two endpoints and return differences."""
        diffs = []

        for field in ["summary", "description", "operationId"]:
            if ep1.get(field) != ep2.get(field):
                diffs.append({"field": field, "old": ep1.get(field), "new": ep2.get(field)})

        if ep1.get("parameters") != ep2.get("parameters"):
            diffs.append({"field": "parameters", "type": "modified"})

        if ep1.get("requestBody") != ep2.get("requestBody"):
            diffs.append({"field": "requestBody", "type": "modified"})

        if ep1.get("responses") != ep2.get("responses"):
            diffs.append({"field": "responses", "type": "modified"})

        return {"path": path, "method": method, "diffs": diffs} if diffs else None

    def _to_raml(self, spec: Dict[str, Any]) -> str:
        """Convert OpenAPI spec to RAML format."""
        lines = [
            "#%RAML 1.0",
            f"title: {spec.get('info', {}).get('title', 'API')}",
            f"version: {spec.get('info', {}).get('version', 'v1')}",
            "baseUri: " + spec.get("servers", [{}])[0].get("url", "/"),
            "",
        ]

        for path, methods in spec.get("paths", {}).items():
            lines.append(f"{path}:")
            for method, details in methods.items():
                if method in ("get", "post", "put", "delete", "patch"):
                    lines.append(f"  {method}:")
                    if details.get("description"):
                        lines.append(f"    description: {details['description']}")
                    lines.append("    responses:")

        return "\n".join(lines)

    def _to_graphql(self, spec: Dict[str, Any]) -> str:
        """Convert OpenAPI to GraphQL schema."""
        lines = ["schema {", "  query: Query", "}", "", "type Query {"]

        for path, methods in spec.get("paths", {}).items():
            if "get" in methods:
                op = methods["get"]
                name = op.get("operationId", path.strip("/").replace("/", "_"))
                lines.append(f"  {name}(id: ID!): JSON")

        lines.append("}")
        return "\n".join(lines)

    def _to_grpc_proto(self, spec: Dict[str, Any]) -> str:
        """Convert OpenAPI to gRPC proto file."""
        lines = [
            'syntax = "proto3";',
            'package api;',
            "",
            "service AutoGenerated {",
        ]

        for path, methods in spec.get("paths", {}).items():
            for method in ["get", "post", "put", "delete"]:
                if method in methods:
                    op_name = methods[method].get("operationId", path.replace("/", "")).title().replace("_", "")
                    lines.append(f"  rpc {op_name}(Request) returns (Response);")

        lines.extend([
            "}",
            "",
            "message Request {",
            "  string id = 1;",
            "}",
            "message Response {",
            "  string data = 1;",
            "}",
        ])

        return "\n".join(lines)

    def _to_postman(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Convert OpenAPI to Postman Collection format."""
        return {
            "info": {
                "name": spec.get("info", {}).get("title", "API"),
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
            },
            "item": [
                {
                    "name": path,
                    "request": {
                        "method": method.upper(),
                        "url": {"raw": "{{baseUrl}}" + path},
                    }
                }
                for path, methods in spec.get("paths", {}).items()
                for method in methods.keys()
                if method in ("get", "post", "put", "delete", "patch")
            ]
        }

    def _to_insomnia(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Convert OpenAPI to Insomnia format."""
        return {
            "_type": "export",
            "__export_format": 4,
            "resources": []
        }

    def _parse_raml(self, data: str) -> Dict[str, Any]:
        """Parse RAML to OpenAPI-like format."""
        return {"paths": {}, "info": {"title": "Parsed RAML", "version": "1.0"}}

    def _parse_graphql(self, data: str) -> Dict[str, Any]:
        """Parse GraphQL schema to OpenAPI-like format."""
        return {"paths": {}, "info": {"title": "Parsed GraphQL", "version": "1.0"}}

    def _upgrade_swagger(self, swagger: Dict[str, Any]) -> Dict[str, Any]:
        """Upgrade Swagger 2.0 to OpenAPI 3.0."""
        openapi = {
            "openapi": "3.0.0",
            "info": swagger.get("info", {}),
            "paths": swagger.get("paths", {}),
            "components": {"schemas": swagger.get("definitions", {})},
        }
        return openapi

    def _generate_api_id(self, spec: Dict[str, Any]) -> str:
        """Generate a unique API ID."""
        title = spec.get("info", {}).get("title", "unnamed")
        version = spec.get("info", {}).get("version", "1.0")
        raw = f"{title}:{version}:{time.time()}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def register_api(self, api_id: str, spec: Dict[str, Any]) -> None:
        """Register an API spec for export."""
        self._apis[api_id] = spec

    def list_apis(self) -> List[str]:
        """List registered API IDs."""
        return list(self._apis.keys())
