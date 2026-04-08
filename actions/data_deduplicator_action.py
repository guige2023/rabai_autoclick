"""Data deduplicator action module for RabAI AutoClick.

Provides deduplication operations:
- DedupeExactAction: Exact match deduplication
- DedupeFuzzyAction: Fuzzy deduplication
- DedupeHashAction: Hash-based deduplication
- DedupeNearDupeAction: Near-duplicate detection
- DedupeSummaryAction: Deduplication summary
"""

from typing import Any, Dict, List, Set

import hashlib
import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DedupeExactAction(BaseAction):
    """Exact match deduplication."""
    action_type = "dedupe_exact"
    display_name = "精确去重"
    description = "精确匹配去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_fields = params.get("key_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            seen: Set = set()
            unique = []
            duplicates = []

            for item in data:
                if key_fields:
                    key = tuple(item.get(f) for f in key_fields)
                else:
                    key = tuple(sorted(item.items()))

                if key in seen:
                    duplicates.append(item)
                else:
                    seen.add(key)
                    unique.append(item)

            return ActionResult(
                success=True,
                data={"unique": unique, "duplicate_count": len(duplicates), "unique_count": len(unique), "original_count": len(data)},
                message=f"Exact dedupe: {len(data)} → {len(unique)} unique (removed {len(duplicates)})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe exact failed: {e}")


class DedupeFuzzyAction(BaseAction):
    """Fuzzy deduplication."""
    action_type = "dedupe_fuzzy"
    display_name = "模糊去重"
    description = "模糊去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "text")
            threshold = params.get("threshold", 0.9)

            if not data:
                return ActionResult(success=False, message="data is required")

            def similarity(s1: str, s2: str) -> float:
                s1 = s1.lower()
                s2 = s2.lower()
                if s1 == s2:
                    return 1.0
                longer = max(len(s1), len(s2))
                if longer == 0:
                    return 1.0
                return sum(c1 == c2 for c1, c2 in zip(s1, s2)) / longer

            unique = []
            duplicates = []

            for item in data:
                text = item.get(field, "")
                is_dup = False
                for u in unique:
                    if similarity(str(text), str(u.get(field, ""))) >= threshold:
                        duplicates.append(item)
                        is_dup = True
                        break
                if not is_dup:
                    unique.append(item)

            return ActionResult(
                success=True,
                data={"unique": unique, "duplicate_count": len(duplicates), "threshold": threshold},
                message=f"Fuzzy dedupe: {len(data)} → {len(unique)} (threshold={threshold})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe fuzzy failed: {e}")


class DedupeHashAction(BaseAction):
    """Hash-based deduplication."""
    action_type = "dedupe_hash"
    display_name = "哈希去重"
    description = "基于哈希的去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            hash_fields = params.get("hash_fields", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            seen_hashes: Set[str] = set()
            unique = []
            duplicates = []

            for item in data:
                if hash_fields:
                    hash_input = "|".join(str(item.get(f, "")) for f in hash_fields)
                else:
                    hash_input = "|".join(f"{k}={v}" for k, v in sorted(item.items()))

                item_hash = hashlib.md5(hash_input.encode()).hexdigest()
                if item_hash in seen_hashes:
                    duplicates.append(item)
                else:
                    seen_hashes.add(item_hash)
                    unique.append(item)

            return ActionResult(
                success=True,
                data={"unique": unique, "duplicate_count": len(duplicates), "unique_count": len(unique)},
                message=f"Hash dedupe: {len(data)} → {len(unique)} (removed {len(duplicates)})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe hash failed: {e}")


class DedupeNearDupeAction(BaseAction):
    """Near-duplicate detection."""
    action_type = "dedupe_near"
    display_name = "近似去重"
    description = "近似重复检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "text")
            min_similarity = params.get("min_similarity", 0.8)

            if not data:
                return ActionResult(success=False, message="data is required")

            def ngrams(s: str, n: int = 3) -> Set:
                s = s.lower()
                return set(s[i : i + n] for i in range(len(s) - n + 1))

            def jaccard(s1: str, s2: str) -> float:
                ng1 = ngrams(s1, 3)
                ng2 = ngrams(s2, 3)
                if not ng1 or not ng2:
                    return 0.0
                return len(ng1 & ng2) / len(ng1 | ng2)

            near_pairs = []
            for i in range(len(data)):
                for j in range(i + 1, len(data)):
                    sim = jaccard(str(data[i].get(field, "")), str(data[j].get(field, "")))
                    if sim >= min_similarity:
                        near_pairs.append({"pair": (i, j), "similarity": sim})

            return ActionResult(
                success=True,
                data={"near_duplicates": near_pairs, "pair_count": len(near_pairs), "min_similarity": min_similarity},
                message=f"Near-dupe detection: found {len(near_pairs)} similar pairs",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe near failed: {e}")


class DedupeSummaryAction(BaseAction):
    """Deduplication summary."""
    action_type = "dedupe_summary"
    display_name = "去重摘要"
    description = "去重统计摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            field_counts: Dict[str, int] = {}
            for item in data:
                key = str(sorted(item.items()))
                field_counts[key] = field_counts.get(key, 0) + 1

            duplicates = {k: v for k, v in field_counts.items() if v > 1}
            total_dupes = sum(v - 1 for v in duplicates.values())
            unique_items = len(data) - total_dupes

            return ActionResult(
                success=True,
                data={"unique_count": unique_items, "duplicate_count": total_dupes, "duplicate_groups": len(duplicates)},
                message=f"Dedupe summary: {unique_items} unique, {total_dupes} duplicates in {len(duplicates)} groups",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe summary failed: {e}")
