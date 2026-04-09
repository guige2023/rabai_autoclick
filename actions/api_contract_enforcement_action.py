"""API contract enforcement action module for RabAI AutoClick.

Provides API contract operations:
- ContractValidatorAction: Validate requests/responses against schemas
- ContractSchemaBuilderAction: Build JSON Schema from samples
- ContractDiffAction: Detect breaking changes between versions
- ContractMockGeneratorAction: Generate mocks from contracts
- ContractComplianceReporterAction: Generate compliance reports
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ContractValidatorAction(BaseAction):
    """Validate requests and responses against JSON Schema."""
    action_type = "contract_validator"
    display_name = "契约验证"
    description = "根据契约规范验证请求和响应"

    def __init__(self) -> None:
        super().__init__()
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._validation_history: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema_name = params.get("schema_name", "")
            data = params.get("data", {})
            validate_type = params.get("validate_type", "request")
            strict = params.get("strict", False)

            if not schema_name:
                return ActionResult(success=False, message="schema_name is required")

            schema = self._schemas.get(schema_name)
            if not schema:
                return ActionResult(success=False, message=f"Schema not found: {schema_name}")

            violations = self._validate(data, schema, strict)
            record = {
                "id": str(uuid.uuid4()),
                "schema_name": schema_name,
                "validate_type": validate_type,
                "passed": len(violations) == 0,
                "violations": violations,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self._validation_history.append(record)
            return ActionResult(
                success=len(violations) == 0,
                message=f"{'PASSED' if not violations else 'FAILED'} validation ({len(violations)} violations)",
                data=record,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract validation failed: {e}")

    def _validate(self, data: Any, schema: Dict[str, Any], strict: bool) -> List[str]:
        violations: List[str] = []
        required_fields = schema.get("required", [])
        properties = schema.get("properties", {})

        for field in required_fields:
            if field not in data:
                violations.append(f"Missing required field: {field}")

        for field, value in data.items():
            if field in properties:
                expected_type = properties[field].get("type")
                if expected_type and not self._check_type(value, expected_type):
                    violations.append(f"Field '{field}' type mismatch: expected {expected_type}")

        if strict:
            for field in data:
                if field not in properties:
                    violations.append(f"Unknown field in strict mode: {field}")

        return violations

    def _check_type(self, value: Any, expected: str) -> bool:
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }
        return isinstance(value, type_map.get(expected, str))

    def register_schema(self, name: str, schema: Dict[str, Any]) -> None:
        """Register a JSON Schema for validation."""
        self._schemas[name] = schema


class ContractSchemaBuilderAction(BaseAction):
    """Build JSON Schema from sample data."""
    action_type = "contract_schema_builder"
    display_name = "契约模式构建"
    description = "从样本数据构建JSON Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            samples = params.get("samples", [])
            required = params.get("required", [])
            metadata = params.get("metadata", {})
            if not samples:
                return ActionResult(success=False, message="samples are required")

            schema = self._infer_schema(samples[0], required, metadata)
            return ActionResult(success=True, message="Schema built from samples", data={"schema": schema})
        except Exception as e:
            return ActionResult(success=False, message=f"Schema building failed: {e}")

    def _infer_schema(self, sample: Any, required: List[str], metadata: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(sample, dict):
            properties: Dict[str, Any] = {}
            for key, value in sample.items():
                properties[key] = self._infer_schema(value, [], {})
            schema: Dict[str, Any] = {
                "type": "object",
                "properties": properties,
            }
            if required:
                schema["required"] = required
            if metadata:
                schema["description"] = metadata.get("description", "")
                schema["title"] = metadata.get("title", "")
            return schema
        elif isinstance(sample, list):
            items = sample[0] if sample else None
            return {"type": "array", "items": self._infer_schema(items, [], {}) if items else {}}
        elif isinstance(sample, bool):
            return {"type": "boolean"}
        elif isinstance(sample, int):
            return {"type": "integer"}
        elif isinstance(sample, float):
            return {"type": "number"}
        else:
            return {"type": "string"}


class ContractDiffAction(BaseAction):
    """Detect breaking changes between contract versions."""
    action_type = "contract_diff"
    display_name = "契约差异检测"
    description = "检测契约版本间的破坏性变更"

    BREAKING_CHANGES = {"REMOVED_FIELD", "TYPE_CHANGED", "REQUIRED_ADDED", "ENUM_CHANGED"}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            old_schema = params.get("old_schema", {})
            new_schema = params.get("new_schema", {})
            if not old_schema or not new_schema:
                return ActionResult(success=False, message="old_schema and new_schema are required")

            breaking, non_breaking = self._diff_schemas(old_schema, new_schema)
            return ActionResult(
                success=True,
                message=f"Found {len(breaking)} breaking, {len(non_breaking)} non-breaking changes",
                data={
                    "breaking_changes": breaking,
                    "non_breaking_changes": non_breaking,
                    "has_breaking": len(breaking) > 0,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract diff failed: {e}")

    def _diff_schemas(self, old: Dict[str, Any], new: Dict[str, Any]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        breaking: List[Dict[str, Any]] = []
        non_breaking: List[Dict[str, Any]] = []

        old_props = old.get("properties", {})
        new_props = new.get("properties", {})
        old_required = set(old.get("required", []))
        new_required = set(new.get("required", []))

        for field in old_props:
            if field not in new_props:
                breaking.append({"type": "REMOVED_FIELD", "field": field})
            else:
                old_type = old_props[field].get("type")
                new_type = new_props[field].get("type")
                if old_type != new_type:
                    breaking.append({"type": "TYPE_CHANGED", "field": field, "old": old_type, "new": new_type})

        for field in new_required:
            if field not in old_required:
                breaking.append({"type": "REQUIRED_ADDED", "field": field})

        for field in new_props:
            if field not in old_props:
                non_breaking.append({"type": "FIELD_ADDED", "field": field})

        return breaking, non_breaking


class ContractMockGeneratorAction(BaseAction):
    """Generate mock data from contract schemas."""
    action_type = "contract_mock_generator"
    display_name = "契约Mock生成"
    description = "从契约Schema生成Mock数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema = params.get("schema", {})
            count = params.get("count", 1)
            if not schema:
                return ActionResult(success=False, message="schema is required")

            mocks = [self._generate_mock(schema) for _ in range(count)]
            return ActionResult(success=True, message=f"Generated {count} mock(s)", data={"mocks": mocks})
        except Exception as e:
            return ActionResult(success=False, message=f"Mock generation failed: {e}")

    def _generate_mock(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        import random
        mock: Dict[str, Any] = {}
        for field, field_schema in schema.get("properties", {}).items():
            field_type = field_schema.get("type", "string")
            if field_type == "string":
                mock[field] = field_schema.get("example", f"mock_{field}_{random.randint(1000,9999)}")
            elif field_type == "integer" or field_type == "number":
                mock[field] = field_schema.get("example", random.randint(1, 100))
            elif field_type == "boolean":
                mock[field] = field_schema.get("example", random.choice([True, False]))
            elif field_type == "array":
                mock[field] = [self._generate_mock(field_schema.get("items", {}))]
            elif field_type == "object":
                mock[field] = self._generate_mock(field_schema)
            else:
                mock[field] = None
        return mock


class ContractComplianceReporterAction(BaseAction):
    """Generate API contract compliance reports."""
    action_type = "contract_compliance_reporter"
    display_name = "契约合规报告"
    description = "生成API契约合规报告"

    def __init__(self) -> None:
        super().__init__()
        self._reports: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoints = params.get("endpoints", [])
            schemas = params.get("schemas", {})
            compliance_level = params.get("level", "basic")
            report = {
                "id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "endpoints_checked": len(endpoints),
                "compliance_level": compliance_level,
                "issues": [],
                "score": 100,
            }
            for endpoint in endpoints:
                path = endpoint.get("path", "")
                method = endpoint.get("method", "GET")
                schema = schemas.get(f"{method}:{path}")
                if not schema:
                    report["issues"].append({"type": "MISSING_SCHEMA", "endpoint": f"{method} {path}"})
                    report["score"] -= 5
            self._reports.append(report)
            return ActionResult(
                success=True,
                message=f"Compliance report generated: score={report['score']}",
                data=report,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compliance report failed: {e}")
