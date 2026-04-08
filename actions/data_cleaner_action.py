"""Data cleaner action module for RabAI AutoClick.

Provides data cleaning operations:
- CleanMissingAction: Handle missing values
- CleanOutliersAction: Handle outliers
- CleanNormalizeAction: Normalize text
- CleanDedupeAction: Remove duplicates
- CleanValidateAction: Validate and clean
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CleanMissingAction(BaseAction):
    """Handle missing values."""
    action_type = "clean_missing"
    display_name = "清理缺失值"
    description = "处理缺失值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            strategy = params.get("strategy", "remove")
            fill_value = params.get("fill_value", 0)

            if not data:
                return ActionResult(success=False, message="data is required")

            if strategy == "remove":
                cleaned = [d for d in data if None not in d.values()]
            elif strategy == "fill":
                cleaned = []
                for d in data:
                    new_d = d.copy()
                    for k, v in new_d.items():
                        if v is None:
                            new_d[k] = fill_value
                    cleaned.append(new_d)
            elif strategy == "forward":
                cleaned = []
                last_valid = {}
                for d in data:
                    new_d = {}
                    for k, v in d.items():
                        if v is not None:
                            last_valid[k] = v
                            new_d[k] = v
                        else:
                            new_d[k] = last_valid.get(k, fill_value)
                    cleaned.append(new_d)
            else:
                cleaned = data

            return ActionResult(
                success=True,
                data={"cleaned": cleaned, "original_count": len(data), "cleaned_count": len(cleaned), "strategy": strategy},
                message=f"Cleaned missing: {len(data)} → {len(cleaned)} (strategy={strategy})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clean missing failed: {e}")


class CleanOutliersAction(BaseAction):
    """Handle outliers."""
    action_type = "clean_outliers"
    display_name = "清理异常值"
    description = "处理异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            method = params.get("method", "iqr")
            threshold = params.get("threshold", 1.5)
            action = params.get("action", "remove")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            sorted_vals = sorted(values)
            n = len(sorted_vals)

            if method == "iqr":
                q1 = sorted_vals[n // 4]
                q3 = sorted_vals[3 * n // 4]
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
            else:
                mean = sum(values) / n
                std = (sum((v - mean) ** 2 for v in values) / n) ** 0.5
                lower = mean - threshold * std
                upper = mean + threshold * std

            cleaned = []
            removed = []
            for d in data:
                val = d.get(field, 0)
                if lower <= val <= upper:
                    cleaned.append(d)
                else:
                    removed.append(d)

            return ActionResult(
                success=True,
                data={"cleaned": cleaned, "removed_count": len(removed), "lower": lower, "upper": upper, "method": method},
                message=f"Outlier clean: removed {len(removed)} outliers (method={method})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clean outliers failed: {e}")


class CleanNormalizeAction(BaseAction):
    """Normalize text."""
    action_type = "clean_normalize"
    display_name = "规范化文本"
    description = "规范化文本数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "text")
            operations = params.get("operations", ["lower", "strip"])

            if not data:
                return ActionResult(success=False, message="data is required")

            cleaned = []
            for d in data:
                new_d = d.copy()
                text = str(d.get(field, ""))
                if "lower" in operations:
                    text = text.lower()
                if "strip" in operations:
                    text = text.strip()
                if "remove_extra_spaces" in operations:
                    text = " ".join(text.split())
                new_d[field] = text
                cleaned.append(new_d)

            return ActionResult(
                success=True,
                data={"cleaned": cleaned, "count": len(cleaned), "operations": operations},
                message=f"Normalized {len(cleaned)} text values",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clean normalize failed: {e}")


class CleanDedupeAction(BaseAction):
    """Remove duplicates."""
    action_type = "clean_dedupe"
    display_name = "清理重复"
    description = "移除重复数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_fields = params.get("key_fields", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            seen = set()
            cleaned = []
            for d in data:
                if key_fields:
                    key = tuple(d.get(f) for f in key_fields)
                else:
                    key = tuple(sorted(d.items()))
                if key not in seen:
                    seen.add(key)
                    cleaned.append(d)

            return ActionResult(
                success=True,
                data={"cleaned": cleaned, "original_count": len(data), "cleaned_count": len(cleaned), "removed": len(data) - len(cleaned)},
                message=f"Dedupe clean: {len(data)} → {len(cleaned)} (removed {len(data) - len(cleaned)})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clean dedupe failed: {e}")


class CleanValidateAction(BaseAction):
    """Validate and clean data."""
    action_type = "clean_validate"
    display_name = "验证清理"
    description = "验证并清理数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rules = params.get("rules", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            cleaned = []
            errors = []
            for i, d in enumerate(data):
                valid = True
                for rule in rules:
                    field = rule.get("field", "")
                    rule_type = rule.get("type", "")
                    value = rule.get("value")
                    item_val = d.get(field)

                    if rule_type == "required" and not item_val:
                        valid = False
                        errors.append(f"Row {i}: {field} is required")
                    elif rule_type == "type" and item_val and not isinstance(item_val, eval(value)):
                        valid = False
                        errors.append(f"Row {i}: {field} has wrong type")
                    elif rule_type == "range" and item_val:
                        min_v, max_v = value.get("min"), value.get("max")
                        if min_v is not None and item_val < min_v:
                            valid = False
                        if max_v is not None and item_val > max_v:
                            valid = False

                if valid:
                    cleaned.append(d)

            return ActionResult(
                success=True,
                data={"cleaned": cleaned, "original_count": len(data), "cleaned_count": len(cleaned), "error_count": len(errors), "errors": errors[:10]},
                message=f"Validate clean: {len(data)} → {len(cleaned)} ({len(errors)} errors)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clean validate failed: {e}")
