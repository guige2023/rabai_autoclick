"""Data deduplication action module for RabAI AutoClick.

Provides deduplication operations:
- ExactDedupeAction: Exact match deduplication
- FuzzyDedupeAction: Fuzzy string matching deduplication
- DedupeByKeyAction: Deduplication by specific keys
- NearDuplicateDetectionAction: Detect near-duplicate records
"""

import hashlib
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ExactDedupeAction(BaseAction):
    """Exact match deduplication."""
    action_type = "exact_dedupe"
    display_name = "精确去重"
    description = "基于完全匹配的数据去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_field = params.get("key_field", None)
            keep = params.get("keep", "first")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not data:
                return ActionResult(success=True, message="Empty dataset", data={"deduped": [], "removed": 0})

            if key_field:
                seen: Set[str] = set()
                deduped = []
                removed = 0
                for item in data:
                    if isinstance(item, dict):
                        key = str(item.get(key_field, ""))
                    else:
                        key = str(item)
                    if key not in seen:
                        seen.add(key)
                        deduped.append(item)
                    else:
                        removed += 1
            else:
                seen: Set[str] = set()
                deduped = []
                removed = 0
                for item in data:
                    key = str(item)
                    if key not in seen:
                        seen.add(key)
                        deduped.append(item)
                    else:
                        removed += 1

            return ActionResult(
                success=True,
                message=f"Deduplication: removed {removed} duplicates from {len(data)}",
                data={"deduped": deduped, "removed": removed, "original_count": len(data), "final_count": len(deduped)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ExactDedupe error: {e}")


class FuzzyDedupeAction(BaseAction):
    """Fuzzy string matching deduplication."""
    action_type = "fuzzy_dedupe"
    display_name = "模糊去重"
    description = "基于模糊匹配的重复数据检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            text_field = params.get("text_field", "text")
            threshold = params.get("threshold", 0.85)
            keep = params.get("keep", "first")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not data:
                return ActionResult(success=True, message="Empty dataset", data={"deduped": [], "groups": []})

            groups: List[List[int]] = []
            used_indices: Set[int] = set()

            for i, item in enumerate(data):
                if i in used_indices:
                    continue
                if not isinstance(item, dict):
                    text_a = str(item)
                else:
                    text_a = str(item.get(text_field, ""))

                group = [i]
                for j in range(i + 1, len(data)):
                    if j in used_indices:
                        continue
                    item_b = data[j]
                    if isinstance(item_b, dict):
                        text_b = str(item_b.get(text_field, ""))
                    else:
                        text_b = str(item_b)
                    ratio = SequenceMatcher(None, text_a, text_b).ratio()
                    if ratio >= threshold:
                        group.append(j)
                        used_indices.add(j)
                groups.append(group)
                used_indices.add(i)

            deduped = []
            for group in groups:
                idx = group[0] if keep == "first" else group[-1]
                deduped.append(data[idx])

            return ActionResult(
                success=True,
                message=f"Fuzzy dedupe: {len(data)} -> {len(deduped)} ({len(groups)} groups)",
                data={
                    "deduped": deduped,
                    "groups": [{"size": len(g), "indices": g} for g in groups],
                    "original_count": len(data),
                    "final_count": len(deduped),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FuzzyDedupe error: {e}")


class DedupeByKeyAction(BaseAction):
    """Deduplication by specific keys."""
    action_type = "dedupe_by_key"
    display_name = "键值去重"
    description = "基于指定键组合去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            keys = params.get("keys", [])
            keep = params.get("keep", "first")
            strategies = params.get("strategies", {})

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not keys:
                return ActionResult(success=False, message="keys is required")

            seen: Dict[Tuple, Dict] = {}
            duplicates: List[Dict] = []

            for item in data:
                if not isinstance(item, dict):
                    continue
                key_tuple = tuple(str(item.get(k, "")) for k in keys)
                if key_tuple in seen:
                    duplicates.append(item)
                    if strategies.get("merge"):
                        existing = seen[key_tuple]
                        for k, v in item.items():
                            if k not in existing or strategies.get("prefer") == "new":
                                existing[k] = v
                        seen[key_tuple] = existing
                else:
                    seen[key_tuple] = {**(strategies.get("merge") or {}), **item}
                    if keep == "last":
                        seen[key_tuple] = item

            deduped = list(seen.values())

            return ActionResult(
                success=True,
                message=f"Key dedupe: {len(data)} -> {len(deduped)}",
                data={"deduped": deduped, "duplicates": duplicates, "removed": len(duplicates)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DedupeByKey error: {e}")


class NearDuplicateDetectionAction(BaseAction):
    """Detect near-duplicate records using MinHash."""
    action_type = "near_duplicate_detection"
    display_name = "近似重复检测"
    description = "检测近似重复的记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            text_fields = params.get("text_fields", ["text"])
            jaccard_threshold = params.get("jaccard_threshold", 0.5)
            shingle_size = params.get("shingle_size", 3)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be list")

            def get_shingles(text: str, size: int) -> Set[str]:
                text = text.lower()
                return set(text[i : i + size] for i in range(max(1, len(text) - size + 1)))

            def jaccard(s1: Set[str], s2: Set[str]) -> float:
                if not s1 or not s2:
                    return 0.0
                return len(s1 & s2) / len(s1 | s2)

            records = []
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    combined = " ".join(str(item.get(f, "")) for f in text_fields)
                else:
                    combined = str(item)
                shingles = get_shingles(combined, shingle_size)
                records.append({"index": i, "item": item, "shingles": shingles})

            duplicates: List[Dict] = []
            for i, rec_a in enumerate(records):
                for j, rec_b in enumerate(records[i + 1 :], start=i + 1):
                    similarity = jaccard(rec_a["shingles"], rec_b["shingles"])
                    if similarity >= jaccard_threshold:
                        duplicates.append({
                            "index_a": rec_a["index"],
                            "index_b": rec_b["index"],
                            "similarity": round(similarity, 4),
                        })

            return ActionResult(
                success=True,
                message=f"Near-duplicate detection: found {len(duplicates)} similar pairs",
                data={"duplicates": duplicates, "pairs_found": len(duplicates)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"NearDuplicateDetection error: {e}")
