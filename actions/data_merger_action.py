"""Data merger action module for RabAI AutoClick.

Provides data merging operations:
- MergeJoinAction: Join two datasets
- MergeUnionAction: Union of datasets
- MergeIntersectAction: Intersection of datasets
- MergeDifferenceAction: Set difference
- MergeLookupAction: Lookup merge
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MergeJoinAction(BaseAction):
    """Join two datasets."""
    action_type = "merge_join"
    display_name = "连接合并"
    description = "连接两个数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            left_key = params.get("left_key", "id")
            right_key = params.get("right_key", "id")
            join_type = params.get("join_type", "inner")

            if not left or not right:
                return ActionResult(success=False, message="left and right datasets are required")

            right_index = {item.get(right_key): item for item in right}
            joined = []

            left_keys_in_right = set(right_index.keys())

            for l_item in left:
                l_key = l_item.get(left_key)
                if l_key in right_index:
                    merged = {**l_item, **right_index[l_key]}
                    joined.append(merged)
                elif join_type == "left":
                    joined.append(l_item)

            if join_type == "outer":
                for r_item in right:
                    r_key = r_item.get(right_key)
                    if r_key not in left_keys_in_right:
                        joined.append(r_item)

            return ActionResult(
                success=True,
                data={"joined": joined, "join_type": join_type, "result_count": len(joined)},
                message=f"{join_type.capitalize()} join: {len(joined)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge join failed: {e}")


class MergeUnionAction(BaseAction):
    """Union of datasets."""
    action_type = "merge_union"
    display_name = "并集合并"
    description = "合并数据集的并集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            dedupe = params.get("dedupe", True)
            key_fields = params.get("key_fields", [])

            if not datasets:
                return ActionResult(success=False, message="datasets list is required")

            union = []
            for ds in datasets:
                union.extend(ds)

            if dedupe and key_fields:
                seen: Set = set()
                unique = []
                for item in union:
                    key = tuple(item.get(f) for f in key_fields)
                    if key not in seen:
                        seen.add(key)
                        unique.append(item)
                union = unique

            return ActionResult(
                success=True,
                data={"union": union, "count": len(union), "source_count": len(datasets)},
                message=f"Union of {len(datasets)} datasets: {len(union)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge union failed: {e}")


class MergeIntersectAction(BaseAction):
    """Intersection of datasets."""
    action_type = "merge_intersect"
    display_name = "交集合并"
    description = "数据集的交集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            key_fields = params.get("key_fields", ["id"])

            if len(datasets) < 2:
                return ActionResult(success=False, message="at least 2 datasets required")

            result = datasets[0]
            for ds in datasets[1:]:
                keys_in_ds = {tuple(item.get(f) for f in key_fields for item in ds}
                result = [item for item in result if tuple(item.get(f) for f in key_fields) in keys_in_ds]

            return ActionResult(
                success=True,
                data={"intersection": result, "count": len(result)},
                message=f"Intersection of {len(datasets)} datasets: {len(result)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge intersect failed: {e}")


class MergeDifferenceAction(BaseAction):
    """Set difference of datasets."""
    action_type = "merge_difference"
    display_name = "差集合并"
    description = "数据集的差集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            key_fields = params.get("key_fields", ["id"])

            if not left or not right:
                return ActionResult(success=False, message="left and right datasets are required")

            right_keys = {tuple(item.get(f) for f in key_fields) for item in right}
            difference = [item for item in left if tuple(item.get(f) for f in key_fields) not in right_keys]

            return ActionResult(
                success=True,
                data={"difference": difference, "count": len(difference), "removed": len(left) - len(difference)},
                message=f"Difference: {len(difference)} rows remain (removed {len(left) - len(difference)})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge difference failed: {e}")


class MergeLookupAction(BaseAction):
    """Lookup merge with reference data."""
    action_type = "merge_lookup"
    display_name = "查找合并"
    description = "引用数据查找合并"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            lookup_table = params.get("lookup_table", [])
            data_key = params.get("data_key", "type_id")
            lookup_key = params.get("lookup_key", "id")
            lookup_fields = params.get("lookup_fields", ["name"])

            if not data or not lookup_table:
                return ActionResult(success=False, message="data and lookup_table are required")

            lookup_index = {item.get(lookup_key): item for item in lookup_table}
            merged = []

            for item in data:
                key = item.get(data_key)
                if key in lookup_index:
                    lookup_item = lookup_index[key]
                    new_item = item.copy()
                    for field in lookup_fields:
                        new_item[field] = lookup_item.get(field)
                    merged.append(new_item)
                else:
                    merged.append(item)

            return ActionResult(
                success=True,
                data={"merged": merged, "count": len(merged), "lookup_hits": sum(1 for m in merged if data_key in [k for k in m if k in lookup_fields])},
                message=f"Lookup merge: {len(merged)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge lookup failed: {e}")
