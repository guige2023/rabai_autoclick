"""Data comparator action module for RabAI AutoClick.

Provides data comparison operations:
- DataComparatorAction: Compare data values
- DeepCompareAction: Deep comparison of complex structures
- SchemaComparatorAction: Compare data schemas
- DiffGeneratorAction: Generate differences between datasets
"""

from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataComparatorAction(BaseAction):
    """Compare data values."""
    action_type = "data_comparator"
    display_name = "数据比较器"
    description = "比较数据值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value_a = params.get("value_a")
            value_b = params.get("value_b")
            comparison_type = params.get("comparison_type", "equals")
            tolerance = params.get("tolerance", 0)

            comparison_functions = {
                "equals": lambda a, b: a == b,
                "not_equals": lambda a, b: a != b,
                "greater_than": lambda a, b: a > b,
                "less_than": lambda a, b: a < b,
                "greater_equal": lambda a, b: a >= b,
                "less_equal": lambda a, b: a <= b,
                "contains": lambda a, b: b in a if a is not None else False,
                "starts_with": lambda a, b: str(a).startswith(str(b)) if a is not None else False,
                "ends_with": lambda a, b: str(a).endswith(str(b)) if a is not None else False,
                "similar": lambda a, b: self._similar(str(a), str(b), tolerance),
            }

            if comparison_type not in comparison_functions:
                return ActionResult(success=False, message=f"Unknown comparison type: {comparison_type}")

            try:
                result = comparison_functions[comparison_type](value_a, value_b)
            except Exception:
                result = False

            return ActionResult(
                success=True,
                data={
                    "result": result,
                    "comparison_type": comparison_type,
                    "value_a": value_a,
                    "value_b": value_b,
                    "tolerance": tolerance
                },
                message=f"Comparison '{comparison_type}': {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data comparator error: {str(e)}")

    def _similar(self, a: str, b: str, tolerance: float) -> bool:
        if not a or not b:
            return False
        max_len = max(len(a), len(b))
        if max_len == 0:
            return True
        distance = self._levenshtein_distance(a, b)
        similarity = 1 - (distance / max_len)
        return similarity >= (1 - tolerance)

    def _levenshtein_distance(self, a: str, b: str) -> int:
        if len(a) < len(b):
            return self._levenshtein_distance(b, a)
        if len(b) == 0:
            return len(a)

        previous_row = range(len(b) + 1)
        for i, c1 in enumerate(a):
            current_row = [i + 1]
            for j, c2 in enumerate(b):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def get_required_params(self) -> List[str]:
        return ["value_a", "value_b"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"comparison_type": "equals", "tolerance": 0}


class DeepCompareAction(BaseAction):
    """Deep comparison of complex structures."""
    action_type = "data_deep_compare"
    display_name = "深度比较"
    description = "深度比较复杂结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data_a = params.get("data_a", {})
            data_b = params.get("data_b", {})
            ignore_fields = params.get("ignore_fields", [])
            compare_order = params.get("compare_order", True)

            if isinstance(data_a, str):
                data_a = {"_value": data_a}
            if isinstance(data_b, str):
                data_b = {"_value": data_b}

            differences = []
            all_keys = set()

            if isinstance(data_a, dict) and isinstance(data_b, dict):
                all_keys = set(data_a.keys()) | set(data_b.keys())
            elif isinstance(data_a, (list, tuple)) and isinstance(data_b, (list, tuple)):
                all_keys = set(range(max(len(data_a), len(data_b))))
            else:
                if data_a != data_b:
                    differences.append({
                        "type": "value_mismatch",
                        "path": "",
                        "value_a": data_a,
                        "value_b": data_b
                    })
                return ActionResult(
                    success=len(differences) == 0,
                    data={
                        "equal": len(differences) == 0,
                        "differences": differences,
                        "count": len(differences)
                    },
                    message=f"Deep compare: {'equal' if len(differences) == 0 else f'{len(differences)} differences'}"
                )

            for key in all_keys:
                if key in ignore_fields:
                    continue

                path = str(key)
                val_a = data_a.get(key) if isinstance(data_a, dict) else (data_a[key] if key < len(data_a) else None)
                val_b = data_b.get(key) if isinstance(data_b, dict) else (data_b[key] if key < len(data_b) else None)

                if isinstance(val_a, dict) and isinstance(val_b, dict):
                    nested_result = self._deep_compare_dicts(val_a, val_b, path, ignore_fields)
                    differences.extend(nested_result)
                elif isinstance(val_a, list) and isinstance(val_b, list):
                    nested_result = self._deep_compare_lists(val_a, val_b, path, ignore_fields, compare_order)
                    differences.extend(nested_result)
                elif val_a != val_b:
                    differences.append({
                        "type": "value_mismatch",
                        "path": path,
                        "value_a": val_a,
                        "value_b": val_b
                    })

            return ActionResult(
                success=len(differences) == 0,
                data={
                    "equal": len(differences) == 0,
                    "differences": differences,
                    "difference_count": len(differences),
                    "data_a_type": type(data_a).__name__,
                    "data_b_type": type(data_b).__name__
                },
                message=f"Deep compare: {'equal' if len(differences) == 0 else f'{len(differences)} differences'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deep compare error: {str(e)}")

    def _deep_compare_dicts(self, dict_a: Dict, dict_b: Dict, path: str, ignore_fields: List) -> List:
        differences = []
        all_keys = set(dict_a.keys()) | set(dict_b.keys())

        for key in all_keys:
            if key in ignore_fields:
                continue
            full_path = f"{path}.{key}" if path else key
            val_a = dict_a.get(key)
            val_b = dict_b.get(key)

            if isinstance(val_a, dict) and isinstance(val_b, dict):
                differences.extend(self._deep_compare_dicts(val_a, val_b, full_path, ignore_fields))
            elif isinstance(val_a, list) and isinstance(val_b, list):
                differences.extend(self._deep_compare_lists(val_a, val_b, full_path, ignore_fields, True))
            elif val_a != val_b:
                differences.append({
                    "type": "value_mismatch",
                    "path": full_path,
                    "value_a": val_a,
                    "value_b": val_b
                })

        return differences

    def _deep_compare_lists(self, list_a: List, list_b: List, path: str, ignore_fields: List, compare_order: bool) -> List:
        differences = []
        if not compare_order:
            list_a = sorted(list_a, key=str)
            list_b = sorted(list_b, key=str)

        max_len = max(len(list_a), len(list_b))
        for i in range(max_len):
            full_path = f"{path}[{i}]"
            val_a = list_a[i] if i < len(list_a) else None
            val_b = list_b[i] if i < len(list_b) else None

            if isinstance(val_a, dict) and isinstance(val_b, dict):
                differences.extend(self._deep_compare_dicts(val_a, val_b, full_path, ignore_fields))
            elif val_a != val_b:
                differences.append({
                    "type": "index_mismatch" if compare_order else "value_mismatch",
                    "path": full_path,
                    "value_a": val_a,
                    "value_b": val_b
                })

        return differences

    def get_required_params(self) -> List[str]:
        return ["data_a", "data_b"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"ignore_fields": [], "compare_order": True}


class SchemaComparatorAction(BaseAction):
    """Compare data schemas."""
    action_type = "data_schema_comparator"
    display_name = "Schema比较器"
    description = "比较数据Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema_a = params.get("schema_a", {})
            schema_b = params.get("schema_b", {})

            differences = []

            fields_a = set(schema_a.get("fields", {}).keys())
            fields_b = set(schema_b.get("fields", {}).keys())

            only_in_a = fields_a - fields_b
            only_in_b = fields_b - fields_a
            common_fields = fields_a & fields_b

            if only_in_a:
                differences.append({"type": "field_only_in_a", "fields": list(only_in_a)})
            if only_in_b:
                differences.append({"type": "field_only_in_b", "fields": list(only_in_b)})

            for field in common_fields:
                type_a = schema_a["fields"][field].get("type")
                type_b = schema_b["fields"][field].get("type")
                if type_a != type_b:
                    differences.append({
                        "type": "type_mismatch",
                        "field": field,
                        "type_a": type_a,
                        "type_b": type_b
                    })

            return ActionResult(
                success=len(differences) == 0,
                data={
                    "schemas_equal": len(differences) == 0,
                    "differences": differences,
                    "common_fields": list(common_fields),
                    "only_in_a": list(only_in_a),
                    "only_in_b": list(only_in_b)
                },
                message=f"Schema compare: {'equal' if len(differences) == 0 else f'{len(differences)} differences'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema comparator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["schema_a", "schema_b"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DiffGeneratorAction(BaseAction):
    """Generate differences between datasets."""
    action_type = "data_diff_generator"
    display_name = "差异生成器"
    description = "生成数据集之间的差异"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data_a = params.get("data_a", [])
            data_b = params.get("data_b", [])
            diff_type = params.get("diff_type", "unified")
            key_field = params.get("key_field")

            if not isinstance(data_a, list):
                data_a = [data_a]
            if not isinstance(data_b, list):
                data_b = [data_b]

            added = []
            removed = []
            modified = []
            unchanged = []

            if key_field:
                index_b = {item.get(key_field): item for item in data_b if isinstance(item, dict)}
                index_a = {item.get(key_field): item for item in data_a if isinstance(item, dict)}

                all_keys = set(index_a.keys()) | set(index_b.keys())

                for key in all_keys:
                    in_a = key in index_a
                    in_b = key in index_b

                    if in_a and not in_b:
                        removed.append({"key": key, "item": index_a[key]})
                    elif not in_a and in_b:
                        added.append({"key": key, "item": index_b[key]})
                    else:
                        if index_a[key] != index_b[key]:
                            modified.append({
                                "key": key,
                                "old": index_a[key],
                                "new": index_b[key]
                            })
                        else:
                            unchanged.append({"key": key, "item": index_a[key]})
            else:
                max_len = max(len(data_a), len(data_b))
                for i in range(max_len):
                    val_a = data_a[i] if i < len(data_a) else None
                    val_b = data_b[i] if i < len(data_b) else None

                    if val_a is None:
                        added.append({"index": i, "item": val_b})
                    elif val_b is None:
                        removed.append({"index": i, "item": val_a})
                    elif val_a != val_b:
                        modified.append({"index": i, "old": val_a, "new": val_b})
                    else:
                        unchanged.append({"index": i, "item": val_a})

            if diff_type == "unified":
                diff_output = self._generate_unified_diff(added, removed, modified)
            else:
                diff_output = {"added": added, "removed": removed, "modified": modified, "unchanged": unchanged}

            return ActionResult(
                success=True,
                data={
                    "diff": diff_output,
                    "added_count": len(added),
                    "removed_count": len(removed),
                    "modified_count": len(modified),
                    "unchanged_count": len(unchanged),
                    "diff_type": diff_type
                },
                message=f"Diff generated: +{len(added)} -{len(removed)} ~{len(modified)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Diff generator error: {str(e)}")

    def _generate_unified_diff(self, added: List, removed: List, modified: List) -> str:
        lines = []
        for item in removed:
            lines.append(f"- {item}")
        for item in added:
            lines.append(f"+ {item}")
        for item in modified:
            lines.append(f"~ {item}")
        return "\n".join(lines)

    def get_required_params(self) -> List[str]:
        return ["data_a", "data_b"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"diff_type": "unified", "key_field": None}
