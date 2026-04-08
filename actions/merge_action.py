"""Data merge action module for RabAI AutoClick.

Provides data merging operations:
- MergeConcatAction: Concatenate multiple datasets
- MergeJoinAction: Join datasets
- MergeUnionAction: Union datasets
- MergeDedupeAction: Deduplicate merged data
"""

from typing import Any, Dict, List, Optional, Set


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MergeConcatAction(BaseAction):
    """Concatenate multiple datasets."""
    action_type = "merge_concat"
    display_name = "数据拼接"
    description = "拼接多个数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            ignore_fields = params.get("ignore_fields", [])

            if not datasets:
                return ActionResult(success=False, message="datasets are required")

            if not all(isinstance(d, list) for d in datasets):
                return ActionResult(success=False, message="All datasets must be lists")

            result = []
            for ds in datasets:
                for record in ds:
                    if isinstance(record, dict):
                        new_record = {k: v for k, v in record.items() if k not in ignore_fields}
                        result.append(new_record)
                    else:
                        result.append(record)

            return ActionResult(
                success=True,
                message=f"Concatenated {len(datasets)} datasets: {len(result)} records",
                data={"result": result, "count": len(result), "datasets_count": len(datasets)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Merge concat failed: {str(e)}")


class MergeJoinAction(BaseAction):
    """Join datasets."""
    action_type = "merge_join"
    display_name = "数据连接"
    description = "连接多个数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            left_key = params.get("left_key", "")
            right_key = params.get("right_key", "")
            join_type = params.get("join_type", "inner")
            suffix = params.get("suffix", "_right")

            if not left or not right:
                return ActionResult(success=False, message="left and right datasets are required")
            if not left_key or not right_key:
                return ActionResult(success=False, message="left_key and right_key are required")

            right_dict = {}
            for r in right:
                if isinstance(r, dict):
                    key = r.get(right_key)
                    if key is not None:
                        right_dict[key] = r

            result = []
            matched_keys = set()

            for l in left:
                if isinstance(l, dict):
                    key = l.get(left_key)
                    if key is not None and key in right_dict:
                        matched_keys.add(key)
                        merged = dict(l)
                        for k, v in right_dict[key].items():
                            if k != right_key:
                                if k in merged:
                                    merged[f"{k}{suffix}"] = v
                                else:
                                    merged[k] = v
                        result.append(merged)
                    elif join_type in ("left", "outer"):
                        result.append(dict(l))
                else:
                    result.append(l)

            if join_type in ("right", "outer"):
                for r in right:
                    if isinstance(r, dict):
                        key = r.get(right_key)
                        if key not in matched_keys:
                            new_record = {f"{k}{suffix}" if k != right_key else left_key: (v if k != right_key else key) for k, v in r.items()}
                            result.append(new_record)

            return ActionResult(
                success=True,
                message=f"Joined: {len(result)} records ({join_type} join)",
                data={"result": result, "count": len(result), "join_type": join_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Merge join failed: {str(e)}")


class MergeUnionAction(BaseAction):
    """Union datasets."""
    action_type = "merge_union"
    display_name = "数据并集"
    description = "合并数据集的并集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            dedupe = params.get("dedupe", True)
            key_fields = params.get("key_fields", [])

            if not datasets:
                return ActionResult(success=False, message="datasets are required")

            result = []
            seen_keys: Set[str] = set()

            for ds in datasets:
                for record in ds:
                    if isinstance(record, dict):
                        if dedupe and key_fields:
                            key = tuple(record.get(f) for f in key_fields)
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)
                        result.append(record)
                    else:
                        if not dedupe or record not in result:
                            result.append(record)

            return ActionResult(
                success=True,
                message=f"Union: {len(result)} records",
                data={"result": result, "count": len(result), "deduped": dedupe}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Merge union failed: {str(e)}")


class MergeDedupeAction(BaseAction):
    """Deduplicate data."""
    action_type = "merge_dedupe"
    display_name = "数据去重"
    description = "去除重复数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_fields = params.get("key_fields", [])
            keep = params.get("keep", "first")

            if not data:
                return ActionResult(success=False, message="data is required")

            seen: Set[str] = set()
            result = []
            duplicates = []

            if key_fields:
                for record in data:
                    if isinstance(record, dict):
                        key = tuple(record.get(f) for f in key_fields)
                        key_str = str(key)
                        if key_str in seen:
                            duplicates.append(record)
                            if keep == "last":
                                result = [r for r in result if str(tuple(r.get(f) for f in key_fields)) != key_str]
                                result.append(record)
                        else:
                            seen.add(key_str)
                            result.append(record)
                    else:
                        result.append(record)
            else:
                for record in data:
                    if isinstance(record, dict):
                        key = str(sorted(record.items()))
                    else:
                        key = str(record)
                    if key not in seen:
                        seen.add(key)
                        result.append(record)
                    else:
                        duplicates.append(record)

            return ActionResult(
                success=True,
                message=f"Deduplicated {len(data)} to {len(result)} records ({len(duplicates)} removed)",
                data={"result": result, "count": len(result), "duplicates_count": len(duplicates), "original_count": len(data)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Merge dedupe failed: {str(e)}")
