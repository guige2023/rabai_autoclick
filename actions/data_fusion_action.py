"""Data fusion action module for RabAI AutoClick.

Provides data fusion and merging operations:
- RecordMergeAction: Merge duplicate records
- MultiSourceFusionAction: Fuse data from multiple sources
- RecordLinkingAction: Link related records across datasets
- ConflictResolutionAction: Resolve data conflicts
"""

from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RecordMergeAction(BaseAction):
    """Merge duplicate records."""
    action_type = "record_merge"
    display_name = "记录合并"
    description = "合并重复记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            merge_key = params.get("merge_key", "id")
            strategy = params.get("strategy", "first")
            conflict_resolution = params.get("conflict_resolution", "prefer_latest")

            if not isinstance(records, list):
                records = [records]

            if not records:
                return ActionResult(success=False, message="records is required")

            groups: Dict[Tuple, List[Dict]] = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                key = record.get(merge_key)
                if key is None:
                    continue
                key_tuple = (merge_key, str(key))
                if key_tuple not in groups:
                    groups[key_tuple] = []
                groups[key_tuple].append(record)

            merged = []
            merge_count = 0

            for key_tuple, group in groups.items():
                if len(group) == 1:
                    merged.append(group[0])
                    continue

                merge_count += 1
                result = self._merge_records(group, strategy, conflict_resolution)
                merged.append(result)

            return ActionResult(
                success=True,
                message=f"Merged {merge_count} groups from {len(records)} records into {len(merged)}",
                data={"merged": merged, "merged_count": len(merged), "groups_merged": merge_count},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RecordMerge error: {e}")

    def _merge_records(self, records: List[Dict], strategy: str, conflict_resolution: str) -> Dict:
        all_keys: Set[str] = set()
        for record in records:
            all_keys.update(record.keys())

        result = {}
        for key in all_keys:
            values = [r.get(key) for r in records if key in r and r.get(key) is not None]

            if not values:
                result[key] = None
            elif len(values) == 1:
                result[key] = values[0]
            else:
                if strategy == "first":
                    result[key] = values[0]
                elif strategy == "last":
                    result[key] = values[-1]
                elif strategy == "longest":
                    result[key] = max(values, key=lambda v: len(str(v)) if v is not None else 0)
                elif strategy == "shortest":
                    result[key] = min(values, key=lambda v: len(str(v)) if v is not None else float("inf"))
                elif strategy == "most_common":
                    counter = Counter(values)
                    result[key] = counter.most_common(1)[0][0]
                elif strategy == "keep_all":
                    result[key] = values

        return result


class MultiSourceFusionAction(BaseAction):
    """Fuse data from multiple sources."""
    action_type = "multi_source_fusion"
    display_name = "多源数据融合"
    description = "融合来自多个数据源的数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            sources = params.get("sources", [])
            link_fields = params.get("link_fields", ["id"])
            fusion_strategy = params.get("fusion_strategy", "merge_all")
            priority_order = params.get("priority_order", [])

            if not sources:
                return ActionResult(success=False, message="sources is required")

            all_records: List[Dict] = []
            source_names = []
            for source in sources:
                source_name = source.get("name", "unknown")
                source_data = source.get("data", [])
                if isinstance(source_data, list):
                    all_records.extend(source_data)
                    source_names.append(source_name)

            if fusion_strategy == "merge_all":
                groups: Dict[Tuple, List[Dict]] = {}
                for record in all_records:
                    if not isinstance(record, dict):
                        continue
                    key = tuple(record.get(f) for f in link_fields)
                    if None not in key:
                        if key not in groups:
                            groups[key] = []
                        groups[key].append(record)

                fused = []
                for key, group in groups.items():
                    fused_record = self._fuse_group(group, priority_order)
                    fused.append(fused_record)

            elif fusion_strategy == "union":
                seen = set()
                fused = []
                for record in all_records:
                    key = tuple(record.get(f) for f in link_fields)
                    key_str = str(key)
                    if key_str not in seen:
                        seen.add(key_str)
                        fused.append(record)

            return ActionResult(
                success=True,
                message=f"Fused {len(fused)} records from {len(sources)} sources",
                data={"fused": fused, "fused_count": len(fused), "source_count": len(sources)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MultiSourceFusion error: {e}")

    def _fuse_group(self, group: List[Dict], priority_order: List[str]) -> Dict:
        all_keys: Set[str] = set()
        for record in group:
            all_keys.update(record.keys())

        result = {}
        for key in all_keys:
            values = [r.get(key) for r in group if key in r and r.get(key) is not None]
            if not values:
                result[key] = None
            elif len(values) == 1:
                result[key] = values[0]
            else:
                result[key] = values[0]

        return result


class RecordLinkingAction(BaseAction):
    """Link related records across datasets."""
    action_type = "record_linking"
    display_name = "记录链接"
    description = "跨数据集链接相关记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left_data = params.get("left_data", [])
            right_data = params.get("right_data", [])
            left_key = params.get("left_key", "id")
            right_key = params.get("right_key", "id")
            link_type = params.get("link_type", "inner")
            similarity_threshold = params.get("similarity_threshold", 0.8)

            if not isinstance(left_data, list):
                left_data = [left_data]
            if not isinstance(right_data, list):
                right_data = [right_data]

            if link_type == "inner":
                right_index = {r.get(right_key): r for r in right_data if r.get(right_key) is not None}
                links = []
                for left_record in left_data:
                    lkey = left_record.get(left_key)
                    if lkey in right_index:
                        links.append({"left": left_record, "right": right_index[lkey]})
                unlinked_left = []
                unlinked_right = list(right_data)

            elif link_type == "left":
                right_index = {r.get(right_key): r for r in right_data if r.get(right_key) is not None}
                links = []
                unlinked_left = []
                for left_record in left_data:
                    lkey = left_record.get(left_key)
                    if lkey in right_index:
                        links.append({"left": left_record, "right": right_index[lkey]})
                    else:
                        unlinked_left.append(left_record)
                unlinked_right = []

            elif link_type == "fuzzy":
                links = []
                unlinked_left = []
                unlinked_right = list(right_data)
                right_used = set()
                for left_record in left_data:
                    best_match = None
                    best_score = 0
                    for i, right_record in enumerate(right_data):
                        if i in right_used:
                            continue
                        score = self._compute_similarity(left_record, right_record, left_key, right_key)
                        if score >= similarity_threshold and score > best_score:
                            best_match = right_record
                            best_score = score
                            best_idx = i
                    if best_match:
                        links.append({"left": left_record, "right": best_match, "score": best_score})
                        right_used.add(best_idx)
                    else:
                        unlinked_left.append(left_record)
                unlinked_right = [r for i, r in enumerate(right_data) if i not in right_used]

            return ActionResult(
                success=True,
                message=f"Linked {len(links)} record pairs",
                data={
                    "links": links,
                    "link_count": len(links),
                    "unlinked_left": unlinked_left,
                    "unlinked_right": unlinked_right,
                    "unlinked_left_count": len(unlinked_left),
                    "unlinked_right_count": len(unlinked_right),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RecordLinking error: {e}")

    def _compute_similarity(self, left: Dict, right: Dict, left_key: str, right_key: str) -> float:
        lv = str(left.get(left_key, ""))
        rv = str(right.get(right_key, ""))
        if not lv or not rv:
            return 0.0

        max_len = max(len(lv), len(rv))
        if max_len == 0:
            return 1.0

        from difflib import SequenceMatcher
        return SequenceMatcher(None, lv, rv).ratio()


class ConflictResolutionAction(BaseAction):
    """Resolve data conflicts."""
    action_type = "conflict_resolution"
    display_name = "冲突解决"
    description = "解决数据冲突"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            conflict_fields = params.get("conflict_fields", [])
            resolution_strategy = params.get("resolution_strategy", "majority")
            timestamp_field = params.get("timestamp_field", "updated_at")

            if not isinstance(records, list):
                records = [records]

            if not records:
                return ActionResult(success=False, message="records is required")

            resolved = {}
            conflict_count = 0

            for record in records:
                if not isinstance(record, dict):
                    continue

                record_id = record.get("id", str(id(record)))
                if record_id not in resolved:
                    resolved[record_id] = {}

                for field, value in record.items():
                    if field not in conflict_fields:
                        resolved[record_id][field] = value
                    else:
                        if field not in resolved[record_id]:
                            resolved[record_id][field] = value
                        else:
                            existing = resolved[record_id][field]
                            if existing != value:
                                conflict_count += 1
                                resolved_value = self._resolve_conflict(existing, value, resolution_strategy, record, timestamp_field)
                                resolved[record_id][field] = resolved_value

            return ActionResult(
                success=True,
                message=f"Resolved conflicts in {len(resolved)} records",
                data={"resolved": list(resolved.values()), "resolved_count": len(resolved), "conflict_count": conflict_count},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ConflictResolution error: {e}")

    def _resolve_conflict(self, val1: Any, val2: Any, strategy: str, record: Dict, timestamp_field: str) -> Any:
        if strategy == "prefer_first":
            return val1
        elif strategy == "prefer_second":
            return val2
        elif strategy == "prefer_latest":
            ts1 = record.get(timestamp_field, "")
            ts2 = record.get(timestamp_field, "")
            return val2 if ts2 > ts1 else val1
        elif strategy == "keep_max":
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                return max(val1, val2)
            return val2 if str(val2) > str(val1) else val1
        elif strategy == "keep_min":
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                return min(val1, val2)
            return val1 if str(val1) < str(val2) else val2
        elif strategy == "concat":
            return f"{val1} {val2}"
        return val2
