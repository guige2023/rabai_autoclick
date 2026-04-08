"""Data deduplication action module for RabAI AutoClick.

Provides data deduplication:
- DataDedupAction: Deduplicate data
- ExactDedupAction: Exact match deduplication
- FuzzyDedupAction: Fuzzy match deduplication
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataDedupAction(BaseAction):
    """Deduplicate data."""
    action_type = "data_dedup"
    display_name = "数据去重"
    description = "删除重复数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            dedup_key = params.get("dedup_key", None)
            keep = params.get("keep", "first")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            seen = set()
            deduplicated = []
            removed_count = 0

            for item in data:
                if dedup_key and isinstance(item, dict):
                    key = str(item.get(dedup_key, id(item)))
                else:
                    key = str(item)

                if key not in seen:
                    seen.add(key)
                    deduplicated.append(item)
                else:
                    removed_count += 1

            return ActionResult(
                success=True,
                data={
                    "original_count": len(data),
                    "deduplicated_count": len(deduplicated),
                    "removed_count": removed_count,
                    "deduplicated": deduplicated
                },
                message=f"Deduplication: {len(data)} -> {len(deduplicated)} ({removed_count} removed)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data dedup error: {str(e)}")


class ExactDedupAction(BaseAction):
    """Exact match deduplication."""
    action_type = "exact_dedup"
    display_name = "精确去重"
    description = "精确匹配去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])

            seen = set()
            deduplicated = []
            removed = 0

            for item in data:
                if isinstance(item, dict) and fields:
                    key = tuple(str(item.get(f, "")) for f in fields)
                else:
                    key = str(item)

                if key not in seen:
                    seen.add(key)
                    deduplicated.append(item)
                else:
                    removed += 1

            return ActionResult(
                success=True,
                data={
                    "original": len(data),
                    "deduplicated": len(deduplicated),
                    "removed": removed
                },
                message=f"Exact dedup: removed {removed} duplicates"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Exact dedup error: {str(e)}")


class FuzzyDedupAction(BaseAction):
    """Fuzzy match deduplication."""
    action_type = "fuzzy_dedup"
    display_name = "模糊去重"
    description = "模糊匹配去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            threshold = params.get("threshold", 0.9)
            field = params.get("field", "text")

            deduplicated = []
            removed = 0

            for item in data:
                if isinstance(item, dict) and field in item:
                    text = str(item[field])
                else:
                    text = str(item)

                is_duplicate = False
                for existing in deduplicated:
                    existing_text = existing.get(field, str(existing)) if isinstance(existing, dict) else str(existing)
                    similarity = self._calculate_similarity(text, str(existing_text))
                    if similarity >= threshold:
                        is_duplicate = True
                        break

                if not is_duplicate:
                    deduplicated.append(item)
                else:
                    removed += 1

            return ActionResult(
                success=True,
                data={
                    "original": len(data),
                    "deduplicated": len(deduplicated),
                    "removed": removed,
                    "threshold": threshold
                },
                message=f"Fuzzy dedup: removed {removed} duplicates (threshold={threshold})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fuzzy dedup error: {str(e)}")

    def _calculate_similarity(self, s1: str, s2: str) -> float:
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
        s1_set = set(s1)
        s2_set = set(s2)
        intersection = len(s1_set & s2_set)
        union = len(s1_set | s2_set)
        return intersection / union if union > 0 else 0.0
