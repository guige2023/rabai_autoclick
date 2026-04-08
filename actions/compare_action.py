"""Comparison action module for RabAI AutoClick.

Provides data comparison operations:
- CompareEqualAction: Check equality
- CompareDiffAction: Find differences
- CompareGreaterAction: Compare greater than
- CompareSimilarityAction: Calculate similarity
"""

from typing import Any, Dict, List, Optional


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompareEqualAction(BaseAction):
    """Check equality."""
    action_type = "compare_equal"
    display_name = "比较相等"
    description = "检查是否相等"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", None)
            right = params.get("right", None)
            strict = params.get("strict", True)

            if strict:
                equal = left is not None and right is not None and left == right
            else:
                equal = str(left) == str(right) if left is not None and right is not None else left == right

            return ActionResult(
                success=True,
                message=f"{left} == {right}: {equal}",
                data={"equal": equal, "left": left, "right": right}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Compare equal failed: {str(e)}")


class CompareDiffAction(BaseAction):
    """Find differences between two data structures."""
    action_type = "compare_diff"
    display_name = "比较差异"
    description = "找出两个数据结构的差异"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", None)
            right = params.get("right", None)
            ignore_fields = params.get("ignore_fields", [])
            deep = params.get("deep", True)

            diffs = []

            def compare(a, b, path=""):
                if isinstance(a, dict) and isinstance(b, dict):
                    all_keys = set(a.keys()) | set(b.keys())
                    for key in all_keys:
                        if key in ignore_fields:
                            continue
                        new_path = f"{path}.{key}" if path else key
                        if key not in a:
                            diffs.append({"path": new_path, "type": "added_right", "value": b[key]})
                        elif key not in b:
                            diffs.append({"path": new_path, "type": "added_left", "value": a[key]})
                        elif deep:
                            compare(a[key], b[key], new_path)
                        elif a[key] != b[key]:
                            diffs.append({"path": new_path, "type": "modified", "left": a[key], "right": b[key]})
                elif isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
                    max_len = max(len(a), len(b))
                    for i in range(max_len):
                        new_path = f"{path}[{i}]"
                        if i >= len(a):
                            diffs.append({"path": new_path, "type": "added_right", "value": b[i]})
                        elif i >= len(b):
                            diffs.append({"path": new_path, "type": "added_left", "value": a[i]})
                        elif deep:
                            compare(a[i], b[i], new_path)
                        elif a[i] != b[i]:
                            diffs.append({"path": new_path, "type": "modified", "left": a[i], "right": b[i]})
                else:
                    if a != b:
                        diffs.append({"path": path, "type": "value_changed", "left": a, "right": b})

            compare(left, right)

            return ActionResult(
                success=True,
                message=f"Found {len(diffs)} differences",
                data={"diffs": diffs, "diff_count": len(diffs)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Compare diff failed: {str(e)}")


class CompareGreaterAction(BaseAction):
    """Compare greater than."""
    action_type = "compare_greater"
    display_name = "比较大小"
    description = "比较大小关系"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", None)
            right = params.get("right", None)
            operator = params.get("operator", ">")
            allow_none = params.get("allow_none", False)

            if left is None or right is None:
                if allow_none:
                    return ActionResult(
                        success=True,
                        message="Comparison with None (allow_none=True)",
                        data={"result": False, "left": left, "right": right}
                    )
                return ActionResult(success=False, message="Cannot compare None values")

            try:
                left_num = float(left)
                right_num = float(right)
            except (ValueError, TypeError):
                return ActionResult(success=False, message="Values must be numeric for comparison")

            result = False
            if operator == ">":
                result = left_num > right_num
            elif operator == ">=":
                result = left_num >= right_num
            elif operator == "<":
                result = left_num < right_num
            elif operator == "<=":
                result = left_num <= right_num

            return ActionResult(
                success=True,
                message=f"{left} {operator} {right}: {result}",
                data={"result": result, "left": left_num, "right": right_num, "operator": operator}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Compare greater failed: {str(e)}")


class CompareSimilarityAction(BaseAction):
    """Calculate similarity between strings or collections."""
    action_type = "compare_similarity"
    display_name = "相似度计算"
    description = "计算相似度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", "")
            right = params.get("right", "")
            method = params.get("method", "levenshtein")

            if not isinstance(left, str):
                left = str(left)
            if not isinstance(right, str):
                right = str(right)

            left = left.lower()
            right = right.lower()

            if method == "levenshtein":
                similarity = self._levenshtein_distance(left, right)
                max_len = max(len(left), len(right))
                score = 1.0 - (similarity / max_len) if max_len > 0 else 1.0
            elif method == "jaccard":
                set_left = set(left.split())
                set_right = set(right.split())
                intersection = len(set_left & set_right)
                union = len(set_left | set_right)
                score = intersection / union if union > 0 else 0.0
            elif method == "cosine":
                words_left = left.split()
                words_right = right.split()
                common = len(set(words_left) & set(words_right))
                score = common / (len(words_left) + len(words_right) - common) if (len(words_left) + len(words_right) - common) > 0 else 0.0
            elif method == "exact":
                score = 1.0 if left == right else 0.0
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            return ActionResult(
                success=True,
                message=f"Similarity ({method}): {score:.4f}",
                data={"score": score, "method": method, "left": left, "right": right}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Compare similarity failed: {str(e)}")

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)

        prev_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row

        return prev_row[-1]
