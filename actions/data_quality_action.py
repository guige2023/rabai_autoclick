"""Data quality action module for RabAI AutoClick.

Provides data quality checks:
- DataQualityAction: Check data quality
- CompletenessCheckerAction: Check completeness
- ValidityCheckerAction: Check validity
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataQualityAction(BaseAction):
    """Check data quality."""
    action_type = "data_quality"
    display_name = "数据质量"
    description = "检查数据质量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            quality_rules = params.get("quality_rules", [])

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            issues = []
            for i, item in enumerate(data):
                for rule in quality_rules:
                    rule_name = rule.get("name", "unnamed")
                    field = rule.get("field")
                    check_type = rule.get("type", "not_null")

                    if field and isinstance(item, dict):
                        value = item.get(field)
                        if check_type == "not_null" and value is None:
                            issues.append({"row": i, "rule": rule_name, "issue": "null_value"})
                        elif check_type == "not_empty" and (value is None or str(value).strip() == ""):
                            issues.append({"row": i, "rule": rule_name, "issue": "empty_value"})

            quality_score = max(0, 100 - (len(issues) / len(data) * 100)) if data else 100

            return ActionResult(
                success=True,
                data={
                    "total_rows": len(data),
                    "issues_count": len(issues),
                    "quality_score": round(quality_score, 2),
                    "issues": issues[:10]
                },
                message=f"Quality check: score={quality_score:.1f}%, {len(issues)} issues found"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data quality error: {str(e)}")


class CompletenessCheckerAction(BaseAction):
    """Check completeness."""
    action_type = "completeness_checker"
    display_name = "完整性检查"
    description = "检查数据完整性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            required_fields = params.get("required_fields", [])

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            field_completeness = {}
            for field in required_fields:
                non_null_count = sum(1 for item in data if isinstance(item, dict) and item.get(field) is not None)
                field_completeness[field] = {
                    "non_null_count": non_null_count,
                    "null_count": len(data) - non_null_count,
                    "completeness_rate": non_null_count / len(data) if data else 0
                }

            return ActionResult(
                success=True,
                data={
                    "total_rows": len(data),
                    "required_fields": required_fields,
                    "field_completeness": field_completeness
                },
                message=f"Completeness: {len(required_fields)} fields checked"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Completeness checker error: {str(e)}")


class ValidityCheckerAction(BaseAction):
    """Check validity."""
    action_type = "validity_checker"
    display_name = "有效性检查"
    description = "检查数据有效性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            valid_type = params.get("valid_type", "string")

            valid_count = 0
            invalid_count = 0
            invalid_items = []

            for i, item in enumerate(data):
                value = item.get(field, item) if isinstance(item, dict) else item

                is_valid = False
                if valid_type == "string":
                    is_valid = isinstance(value, str)
                elif valid_type == "number":
                    is_valid = isinstance(value, (int, float))
                elif valid_type == "boolean":
                    is_valid = isinstance(value, bool)
                elif valid_type == "email":
                    import re
                    is_valid = isinstance(value, str) and re.match(r"[^@]+@[^@]+\.[^@]+", value)

                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1
                    invalid_items.append({"index": i, "value": value})

            validity_rate = valid_count / len(data) if data else 0

            return ActionResult(
                success=True,
                data={
                    "total": len(data),
                    "valid": valid_count,
                    "invalid": invalid_count,
                    "validity_rate": round(validity_rate, 3),
                    "sample_invalid": invalid_items[:5]
                },
                message=f"Validity: {validity_rate:.1%} valid ({valid_count}/{len(data)})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Validity checker error: {str(e)}")
