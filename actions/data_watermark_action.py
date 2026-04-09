"""Data watermark action module for RabAI AutoClick.

Provides data watermarking and tracking:
- DataWatermarkAction: Embed and detect watermarks
- DataVersioningAction: Track data versions
- DataComparisonAction: Compare datasets
- DataDiffAction: Diff two data versions
"""

import time
import hashlib
import json
import copy
from typing import Any, Dict, List, Optional, Set
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataWatermarkAction(BaseAction):
    """Embed and detect watermarks in data."""
    action_type = "data_watermark"
    display_name = "数据水印"
    description = "数据水印嵌入与检测"

    def __init__(self):
        super().__init__()
        self._watermarks: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "embed")
            data = params.get("data")
            watermark_id = params.get("watermark_id", "")

            if operation == "embed":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                if not watermark_id:
                    watermark_id = f"wm_{int(time.time() * 1000)}"

                owner = params.get("owner", "unknown")
                message = params.get("message", "")

                watermark_data = {
                    "id": watermark_id,
                    "owner": owner,
                    "message": message,
                    "timestamp": time.time(),
                    "algorithm": params.get("algorithm", "sha256")
                }

                if params.get("algorithm", "sha256") == "sha256":
                    wm_hash = hashlib.sha256(json.dumps(watermark_data, sort_keys=True).encode()).hexdigest()[:16]
                elif params.get("algorithm", "sha256") == "blake2b":
                    wm_hash = hashlib.blake2b(json.dumps(watermark_data, sort_keys=True).encode()).hexdigest()[:16]
                else:
                    wm_hash = hashlib.sha256(json.dumps(watermark_data, sort_keys=True).encode()).hexdigest()[:16]

                watermark_data["hash"] = wm_hash

                if isinstance(data, dict):
                    watermarked = copy.deepcopy(data)
                    watermarked["_watermark"] = watermark_data
                elif isinstance(data, list):
                    watermarked = copy.deepcopy(data)
                    watermarked.append({"_watermark": watermark_data})
                else:
                    watermarked = {"data": data, "_watermark": watermark_data}

                self._watermarks[watermark_id] = watermark_data

                return ActionResult(
                    success=True,
                    data={
                        "watermark_id": watermark_id,
                        "hash": wm_hash,
                        "owner": owner,
                        "embedded": True
                    },
                    message=f"Watermark '{watermark_id}' embedded"
                )

            elif operation == "detect":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                detected = None
                if isinstance(data, dict) and "_watermark" in data:
                    detected = data["_watermark"]
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "_watermark" in item:
                            detected = item["_watermark"]
                            break

                if detected:
                    return ActionResult(
                        success=True,
                        data={
                            "watermark_id": detected.get("id"),
                            "owner": detected.get("owner"),
                            "message": detected.get("message"),
                            "timestamp": detected.get("timestamp"),
                            "hash": detected.get("hash"),
                            "detected": True
                        },
                        message=f"Watermark detected: {detected.get('id')}"
                    )

                return ActionResult(success=True, data={"detected": False}, message="No watermark detected")

            elif operation == "verify":
                if not watermark_id:
                    return ActionResult(success=False, message="watermark_id required")

                if watermark_id not in self._watermarks:
                    return ActionResult(success=False, message=f"Watermark '{watermark_id}' not found")

                original = self._watermarks[watermark_id]
                return ActionResult(
                    success=True,
                    data={"verified": True, "watermark": original}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Watermark error: {str(e)}")


class DataVersioningAction(BaseAction):
    """Track data versions and history."""
    action_type = "data_versioning"
    display_name = "数据版本管理"
    description = "数据版本追踪"

    def __init__(self):
        super().__init__()
        self._versions: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "commit")
            dataset_id = params.get("dataset_id", "")

            if operation == "commit":
                if not dataset_id:
                    return ActionResult(success=False, message="dataset_id required")

                data = params.get("data")
                message = params.get("message", "")

                if dataset_id not in self._versions:
                    self._versions[dataset_id] = []

                version_number = len(self._versions[dataset_id]) + 1
                version_id = f"{dataset_id}_v{version_number}"

                version_entry = {
                    "version_id": version_id,
                    "version_number": version_number,
                    "dataset_id": dataset_id,
                    "data_hash": hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest() if data is not None else "",
                    "message": message,
                    "timestamp": time.time(),
                    "metadata": params.get("metadata", {}),
                    "parent_version": self._versions[dataset_id][-1]["version_id"] if self._versions[dataset_id] else None
                }

                self._versions[dataset_id].append(version_entry)

                return ActionResult(
                    success=True,
                    data={
                        "version_id": version_id,
                        "version_number": version_number,
                        "data_hash": version_entry["data_hash"]
                    },
                    message=f"Committed version {version_number} for '{dataset_id}'"
                )

            elif operation == "list":
                if not dataset_id:
                    return ActionResult(success=False, message="dataset_id required")

                if dataset_id not in self._versions:
                    return ActionResult(success=True, data={"versions": [], "count": 0})

                versions = self._versions[dataset_id]
                return ActionResult(
                    success=True,
                    data={"versions": versions, "count": len(versions)}
                )

            elif operation == "diff":
                v1_id = params.get("version1", "")
                v2_id = params.get("version2", "")

                for did, versions in self._versions.items():
                    for v in versions:
                        if v["version_id"] == v1_id:
                            v1 = v
                        if v["version_id"] == v2_id:
                            v2 = v

                if not v1 or not v2:
                    return ActionResult(success=False, message="One or both versions not found")

                return ActionResult(
                    success=True,
                    data={
                        "version1": v1_id,
                        "version2": v2_id,
                        "v1_timestamp": v1.get("timestamp"),
                        "v2_timestamp": v2.get("timestamp"),
                        "same_hash": v1["data_hash"] == v2["data_hash"]
                    },
                    message=f"Diff: {v1_id} vs {v2_id}"
                )

            elif operation == "rollback":
                target_version = params.get("target_version", 1)
                if dataset_id not in self._versions:
                    return ActionResult(success=False, message=f"No versions for '{dataset_id}'")

                if target_version > len(self._versions[dataset_id]):
                    return ActionResult(success=False, message=f"Version {target_version} not found")

                return ActionResult(
                    success=True,
                    data={"rolled_back_to": target_version}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Versioning error: {str(e)}")


class DataComparisonAction(BaseAction):
    """Compare datasets and find differences."""
    action_type = "data_comparison"
    display_name = "数据比较"
    description = "数据集比较"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compare")
            data1 = params.get("data1", [])
            data2 = params.get("data2", [])

            if operation == "compare":
                if not isinstance(data1, list) or not isinstance(data2, list):
                    return ActionResult(success=False, message="Both data1 and data2 must be lists")

                key_field = params.get("key_field", "")

                if key_field:
                    set1 = {json.dumps(row, sort_keys=True, default=str) for row in data1 if isinstance(row, dict) and key_field in row}
                    set2 = {json.dumps(row, sort_keys=True, default=str) for row in data2 if isinstance(row, dict) and key_field in row}
                else:
                    set1 = {json.dumps(row, sort_keys=True, default=str) for row in data1}
                    set2 = {json.dumps(row, sort_keys=True, default=str) for row in data2}

                only_in_1 = set1 - set2
                only_in_2 = set2 - set1
                common = set1 & set2

                return ActionResult(
                    success=True,
                    data={
                        "only_in_data1": len(only_in_1),
                        "only_in_data2": len(only_in_2),
                        "common": len(common),
                        "total_data1": len(data1),
                        "total_data2": len(data2),
                        "similarity": round(len(common) / max(len(set1 | set2), 1), 4)
                    },
                    message=f"Comparison: {len(common)} common, {len(only_in_1)} only in data1, {len(only_in_2)} only in data2"
                )

            elif operation == "row_diff":
                if not isinstance(data1, list) or not isinstance(data2, list):
                    return ActionResult(success=False, message="Both data1 and data2 must be lists")

                key_field = params.get("key_field", "")

                if key_field:
                    dict1 = {row.get(key_field): row for row in data1 if isinstance(row, dict)}
                    dict2 = {row.get(key_field): row for row in data2 if isinstance(row, dict)}

                    diffs = []
                    for key in set(dict1.keys()) | set(dict2.keys()):
                        if key in dict1 and key in dict2:
                            if dict1[key] != dict2[key]:
                                diffs.append({
                                    "key": key,
                                    "type": "modified",
                                    "old": dict1[key],
                                    "new": dict2[key]
                                })
                        elif key in dict1:
                            diffs.append({"key": key, "type": "removed", "old": dict1[key]})
                        else:
                            diffs.append({"key": key, "type": "added", "new": dict2[key]})
                else:
                    diffs = []

                return ActionResult(
                    success=True,
                    data={
                        "differences": diffs,
                        "total_diffs": len(diffs),
                        "added": len([d for d in diffs if d["type"] == "added"]),
                        "removed": len([d for d in diffs if d["type"] == "removed"]),
                        "modified": len([d for d in diffs if d["type"] == "modified"])
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Comparison error: {str(e)}")


class DataDiffAction(BaseAction):
    """Diff two data versions with detailed change tracking."""
    action_type = "data_diff"
    display_name = "数据差异"
    description = "数据差异分析"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "diff")
            data1 = params.get("old_data", {})
            data2 = params.get("new_data", {})

            if operation == "diff":
                if not isinstance(data1, dict) or not isinstance(data2, dict):
                    return ActionResult(success=False, message="Both old_data and new_data must be dicts")

                diff_result = self._compute_diff(data1, data2)

                return ActionResult(
                    success=True,
                    data={
                        "changes": diff_result,
                        "total_changes": len(diff_result),
                        "added": len([c for c in diff_result if c["type"] == "added"]),
                        "removed": len([c for c in diff_result if c["type"] == "removed"]),
                        "modified": len([c for c in diff_result if c["type"] == "modified"])
                    },
                    message=f"Diff: {len(diff_result)} changes"
                )

            elif operation == "unified":
                data1_str = json.dumps(data1, sort_keys=True, default=str, indent=2)
                data2_str = json.dumps(data2, sort_keys=True, default=str, indent=2)

                return ActionResult(
                    success=True,
                    data={
                        "old": data1_str,
                        "new": data2_str,
                        "old_size": len(data1_str),
                        "new_size": len(data2_str)
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Diff error: {str(e)}")

    def _compute_diff(self, old: Any, new: Any, path: str = "") -> List[Dict]:
        changes = []
        if type(old) != type(new):
            changes.append({"path": path, "type": "modified", "old_type": type(old).__name__, "new_type": type(new).__name__})
            return changes

        if isinstance(old, dict):
            all_keys = set(old.keys()) | set(new.keys())
            for key in all_keys:
                child_path = f"{path}.{key}" if path else key
                if key not in old:
                    changes.append({"path": child_path, "type": "added", "value": new[key]})
                elif key not in new:
                    changes.append({"path": child_path, "type": "removed", "value": old[key]})
                else:
                    changes.extend(self._compute_diff(old[key], new[key], child_path))

        elif isinstance(old, list):
            if old != new:
                changes.append({"path": path, "type": "modified", "old": old, "new": new})

        else:
            if old != new:
                changes.append({"path": path, "type": "modified", "old": old, "new": new})

        return changes
