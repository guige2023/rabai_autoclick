"""Data merge action module for RabAI AutoClick.

Provides data merging operations:
- MergeConcatAction: Concatenate data
- MergeUnionAction: Union of datasets
- MergeCombineAction: Combine by key
- MergeOverlayAction: Overlay data
- MergeAlignAction: Align data by index/column
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MergeConcatAction(BaseAction):
    """Concatenate datasets."""
    action_type = "merge_concat"
    display_name = "合并连接"
    description = "拼接多个数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            axis = params.get("axis", 0)
            ignore_index = params.get("ignore_index", False)

            if not datasets or len(datasets) < 2:
                return ActionResult(success=False, message="at least 2 datasets are required")

            if axis == 0:
                result = []
                for ds in datasets:
                    result.extend(ds if isinstance(ds, list) else [ds])
            else:
                result = datasets

            return ActionResult(
                success=True,
                data={"row_count": len(result) if isinstance(result, list) else 0, "dataset_count": len(datasets), "axis": axis},
                message=f"Concatenated {len(datasets)} datasets: {len(result)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge concat failed: {e}")


class MergeUnionAction(BaseAction):
    """Union of datasets."""
    action_type = "merge_union"
    display_name = "合并联合"
    description = "数据集联合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            dedup = params.get("deduplicate", True)

            if not datasets:
                return ActionResult(success=False, message="datasets list is required")

            seen = set()
            result = []
            for ds in datasets:
                items = ds if isinstance(ds, list) else [ds]
                for item in items:
                    key = str(item)
                    if not dedup or key not in seen:
                        seen.add(key)
                        result.append(item)

            return ActionResult(
                success=True,
                data={"union_count": len(result), "dataset_count": len(datasets), "deduplicated": dedup},
                message=f"Union of {len(datasets)} datasets: {len(result)} unique items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge union failed: {e}")


class MergeCombineAction(BaseAction):
    """Combine datasets by key."""
    action_type = "merge_combine"
    display_name = "按键合并"
    description = "按键合并数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", {})
            right = params.get("right", {})
            key = params.get("key", "id")
            how = params.get("how", "inner")

            if not left or not right:
                return ActionResult(success=False, message="left and right datasets are required")

            left_data = left.get("data", [])
            right_data = right.get("data", [])
            left_key = key
            right_key = key

            if how == "inner":
                result = [l for l in left_data if any(str(r.get(right_key)) == str(l.get(left_key)) for r in right_data)]
            elif how == "left":
                result = left_data[:]
            elif how == "right":
                result = right_data[:]
            else:
                result = left_data + right_data

            return ActionResult(
                success=True,
                data={"combined_count": len(result), "how": how, "left_count": len(left_data), "right_count": len(right_data)},
                message=f"Combined {len(result)} rows ({how} join)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge combine failed: {e}")


class MergeOverlayAction(BaseAction):
    """Overlay data on top of another."""
    action_type = "merge_overlay"
    display_name = "数据覆盖"
    description = "数据覆盖叠加"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base = params.get("base", [])
            overlay = params.get("overlay", [])
            key_field = params.get("key_field", "id")

            if not base or not overlay:
                return ActionResult(success=False, message="base and overlay datasets are required")

            base_dict = {str(item.get(key_field, "")): item for item in base}
            for item in overlay:
                k = str(item.get(key_field, ""))
                if k:
                    base_dict[k] = item

            result = list(base_dict.values())

            return ActionResult(
                success=True,
                data={"base_count": len(base), "overlay_count": len(overlay), "result_count": len(result)},
                message=f"Overlay: {len(base)} + {len(overlay)} -> {len(result)}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge overlay failed: {e}")


class MergeAlignAction(BaseAction):
    """Align data by index or column."""
    action_type = "merge_align"
    display_name = "对齐数据"
    description = "按索引或列对齐数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            reference = params.get("reference", [])
            fill_value = params.get("fill_value", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            data_list = data if isinstance(data, list) else [data]
            ref_set = set(reference) if reference else set()

            if ref_set:
                aligned = [d if str(d.get("id", d)) in ref_set else fill_value for d in data_list]
            else:
                aligned = data_list

            return ActionResult(
                success=True,
                data={"original_count": len(data_list), "aligned_count": len(aligned), "fill_value": fill_value},
                message=f"Aligned: {len(aligned)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge align failed: {e}")
