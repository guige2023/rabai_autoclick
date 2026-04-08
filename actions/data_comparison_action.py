"""Data comparison action module for RabAI AutoClick.

Provides data comparison operations:
- SetOperationsAction: Set operations (union, intersection, difference)
- DataMatchingAction: Match and align data records
- FuzzyMatchAction: Fuzzy string matching
- SimilarityScoreAction: Compute similarity scores
"""

from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SetOperationsAction(BaseAction):
    """Set operations (union, intersection, difference)."""
    action_type = "set_operations"
    display_name = "集合运算"
    description = "集合操作：并集、交集、差集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            operation = params.get("operation", "union")
            key_field = params.get("key_field", None)

            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]

            if key_field:
                left_set = {str(item.get(key_field, "")) for item in left if isinstance(item, dict)}
                right_set = {str(item.get(key_field, "")) for item in right if isinstance(item, dict)}
            else:
                left_set = {str(item) for item in left}
                right_set = {str(item) for item in right}

            if operation == "union":
                result_keys = left_set | right_set
                result = [{"key": k} for k in result_keys]
            elif operation == "intersection":
                result_keys = left_set & right_set
                result = [{"key": k} for k in result_keys]
            elif operation == "difference":
                result_keys = left_set - right_set
                result = [{"key": k} for k in result_keys]
            elif operation == "symmetric_difference":
                result_keys = left_set ^ right_set
                result = [{"key": k} for k in result_keys]
            elif operation == "is_subset":
                is_subset = left_set <= right_set
                return ActionResult(success=is_subset, message=f"Left is subset of right: {is_subset}", data={"is_subset": is_subset})
            elif operation == "is_superset":
                is_superset = left_set >= right_set
                return ActionResult(success=is_superset, message=f"Left is superset of right: {is_superset}", data={"is_superset": is_superset})
            elif operation == "is_disjoint":
                is_disjoint = left_set.isdisjoint(right_set)
                return ActionResult(success=is_disjoint, message=f"Sets are disjoint: {is_disjoint}", data={"is_disjoint": is_disjoint})
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(
                success=True,
                message=f"Set {operation}: {len(result)} items",
                data={
                    "result": result,
                    "count": len(result),
                    "operation": operation,
                    "left_count": len(left_set),
                    "right_count": len(right_set),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SetOperations error: {e}")


class DataMatchingAction(BaseAction):
    """Match and align data records."""
    action_type = "data_matching"
    display_name: "数据匹配"
    description = "匹配和对齐数据记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            left_key = params.get("left_key", "id")
            right_key = params.get("right_key", "id")
            match_type = params.get("match_type", "inner")

            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]

            right_index = {str(item.get(right_key, "")): item for item in right if isinstance(item, dict)}

            matched = []
            left_unmatched = []
            right_unmatched = list(right)

            for l_item in left:
                if not isinstance(l_item, dict):
                    continue
                lkey = str(l_item.get(left_key, ""))
                if lkey in right_index:
                    matched.append({"left": l_item, "right": right_index[lkey]})
                    right_unmatched = [r for r in right_unmatched if str(r.get(right_key, "")) != lkey]
                else:
                    left_unmatched.append(l_item)

            if match_type == "left":
                return ActionResult(
                    success=True,
                    message=f"Matched {len(matched)} records, {len(left_unmatched)} left unmatched",
                    data={"matched": matched, "left_unmatched": left_unmatched, "matched_count": len(matched)},
                )
            elif match_type == "right":
                return ActionResult(
                    success=True,
                    message=f"Matched {len(matched)} records, {len(right_unmatched)} right unmatched",
                    data={"matched": matched, "right_unmatched": right_unmatched, "matched_count": len(matched)},
                )
            elif match_type == "outer":
                return ActionResult(
                    success=True,
                    message=f"Outer join: {len(matched)} matched, {len(left_unmatched)} left-only, {len(right_unmatched)} right-only",
                    data={
                        "matched": matched,
                        "left_unmatched": left_unmatched,
                        "right_unmatched": right_unmatched,
                        "matched_count": len(matched),
                    },
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Inner match: {len(matched)} matched",
                    data={"matched": matched, "matched_count": len(matched)},
                )
        except Exception as e:
            return ActionResult(success=False, message=f"DataMatching error: {e}")


class FuzzyMatchAction(BaseAction):
    """Fuzzy string matching."""
    action_type = "fuzzy_match"
    display_name = "模糊匹配"
    description = "模糊字符串匹配"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strings = params.get("strings", [])
            query = params.get("query", "")
            threshold = params.get("threshold", 0.6)
            limit = params.get("limit", 10)

            if not strings:
                return ActionResult(success=False, message="strings is required")
            if not query:
                return ActionResult(success=False, message="query is required")

            results = []
            for s in strings:
                ratio = SequenceMatcher(None, query, str(s)).ratio()
                if ratio >= threshold:
                    results.append({"string": s, "score": round(ratio, 4)})

            results.sort(key=lambda x: x["score"], reverse=True)
            results = results[:limit]

            return ActionResult(
                success=True,
                message=f"Fuzzy match: {len(results)} results above threshold {threshold}",
                data={"results": results, "count": len(results), "threshold": threshold},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FuzzyMatch error: {e}")


class SimilarityScoreAction(BaseAction):
    """Compute similarity scores."""
    action_type = "similarity_score"
    display_name = "相似度计算"
    description = "计算数据相似度分数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", "")
            right = params.get("right", "")
            metric = params.get("metric", "levenshtein")

            if not left or not right:
                return ActionResult(success=False, message="left and right are required")

            if metric == "levenshtein":
                score = self._levenshtein_similarity(str(left), str(right))
            elif metric == "jaccard":
                score = self._jaccard_similarity(str(left), str(right))
            elif metric == "cosine":
                score = self._cosine_similarity(str(left), str(right))
            elif metric == "sequence":
                score = SequenceMatcher(None, str(left), str(right)).ratio()
            else:
                score = SequenceMatcher(None, str(left), str(right)).ratio()

            return ActionResult(
                success=True,
                message=f"Similarity ({metric}): {score:.4f}",
                data={"similarity": round(score, 4), "metric": metric, "left": left, "right": right},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SimilarityScore error: {e}")

    def _levenshtein_similarity(self, s1: str, s2: str) -> float:
        if len(s1) < len(s2):
            return self._levenshtein_similarity(s2, s1)
        if len(s2) == 0:
            return 0.0
        prev_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row
        distance = prev_row[-1]
        max_len = max(len(s1), len(s2))
        return 1 - distance / max_len if max_len > 0 else 1.0

    def _jaccard_similarity(self, s1: str, s2: str) -> float:
        set1 = set(s1.lower())
        set2 = set(s2.lower())
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0

    def _cosine_similarity(self, s1: str, s2: str) -> float:
        words1 = set(s1.lower().split())
        words2 = set(s2.lower().split())
        intersection = len(words1 & words2)
        norm1 = len(words1) ** 0.5
        norm2 = len(words2) ** 0.5
        return intersection / (norm1 * norm2) if norm1 > 0 and norm2 > 0 else 0.0
