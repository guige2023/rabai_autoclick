# Copyright (c) 2024. coded by claude
"""Data Inspector Action Module.

Inspects API responses to extract structure, validate schema,
and identify data quality issues.
"""
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DataIssue(Enum):
    MISSING_FIELD = "missing_field"
    TYPE_MISMATCH = "type_mismatch"
    NULL_VALUE = "null_value"
    EMPTY_COLLECTION = "empty_collection"
    DUPLICATE_KEY = "duplicate_key"
    UNEXPECTED_FIELD = "unexpected_field"


@dataclass
class InspectionResult:
    has_issues: bool
    issue_count: int
    issues: List[Dict[str, Any]]
    structure: Dict[str, Any]


class DataInspector:
    def __init__(self):
        self._expected_fields: Set[str] = set()
        self._required_fields: Set[str] = set()

    def set_expected_fields(self, fields: Set[str]) -> None:
        self._expected_fields = fields

    def set_required_fields(self, fields: Set[str]) -> None:
        self._required_fields = fields

    def inspect(self, data: Any, path: str = "") -> InspectionResult:
        issues: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            structure = self._inspect_dict(data, path, issues)
        elif isinstance(data, list):
            structure = self._inspect_list(data, path, issues)
        else:
            structure = {"type": type(data).__name__, "value": str(data)[:100]}
        return InspectionResult(
            has_issues=len(issues) > 0,
            issue_count=len(issues),
            issues=issues,
            structure=structure,
        )

    def _inspect_dict(self, data: dict, path: str, issues: List) -> Dict[str, Any]:
        result: Dict[str, Any] = {"type": "object", "fields": {}}
        actual_fields = set(data.keys())
        for field_name in self._required_fields:
            if field_name not in actual_fields:
                issues.append({
                    "type": DataIssue.MISSING_FIELD.value,
                    "path": f"{path}.{field_name}" if path else field_name,
                    "message": f"Required field '{field_name}' is missing",
                })
        for key, value in data.items():
            full_path = f"{path}.{key}" if path else key
            if value is None:
                issues.append({
                    "type": DataIssue.NULL_VALUE.value,
                    "path": full_path,
                    "message": f"Field '{key}' has null value",
                })
            elif isinstance(value, dict):
                result["fields"][key] = self._inspect_dict(value, full_path, issues)
            elif isinstance(value, list):
                result["fields"][key] = self._inspect_list(value, full_path, issues)
            else:
                result["fields"][key] = {"type": type(value).__name__}
        return result

    def _inspect_list(self, data: list, path: str, issues: List) -> Dict[str, Any]:
        if len(data) == 0:
            issues.append({
                "type": DataIssue.EMPTY_COLLECTION.value,
                "path": path,
                "message": f"List at '{path}' is empty",
            })
            return {"type": "array", "length": 0}
        first_item = data[0]
        if isinstance(first_item, dict):
            return {"type": "array", "length": len(data), "item_type": "object"}
        return {"type": "array", "length": len(data), "item_type": type(first_item).__name__}

    def get_summary(self, result: InspectionResult) -> Dict[str, Any]:
        return {
            "has_issues": result.has_issues,
            "issue_count": result.issue_count,
            "structure_summary": self._summarize_structure(result.structure),
        }

    def _summarize_structure(self, structure: Dict[str, Any]) -> str:
        if structure.get("type") == "object":
            field_count = len(structure.get("fields", {}))
            return f"Object with {field_count} fields"
        elif structure.get("type") == "array":
            return f"Array of {structure.get('item_type', 'unknown')} ({structure.get('length', 0)} items)"
        return f"Primitive: {structure.get('type', 'unknown')}"
