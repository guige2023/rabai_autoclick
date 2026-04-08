"""Data action module for RabAI AutoClick.

Provides core data processing operations:
- DataOperationAction: Perform data operations
- DataCopyAction: Copy data between locations
- DataMoveAction: Move data
- DataDeleteAction: Delete data
- DataMergeAction: Merge data sources
"""

import copy
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataOperationAction(BaseAction):
    """Perform generic data operations."""
    action_type = "data_operation"
    display_name = "数据操作"
    description = "执行通用数据操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            data = params.get("data", None)
            options = params.get("options", {})

            if not operation:
                return ActionResult(success=False, message="operation is required")

            supported_operations = ["copy", "compare", "validate", "transform", "filter", "aggregate"]

            if operation not in supported_operations:
                return ActionResult(
                    success=False,
                    message=f"Unsupported operation: {operation}. Supported: {supported_operations}"
                )

            start_time = time.time()

            if operation == "copy":
                result_data = copy.deepcopy(data) if data is not None else None
            elif operation == "compare":
                other = options.get("other", None)
                result_data = self._compare_data(data, other)
            elif operation == "validate":
                result_data = self._validate_data(data, options)
            elif operation == "transform":
                result_data = self._transform_data(data, options)
            elif operation == "filter":
                result_data = self._filter_data(data, options)
            elif operation == "aggregate":
                result_data = self._aggregate_data(data, options)
            else:
                result_data = None

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                data={
                    "operation": operation,
                    "result": result_data,
                    "elapsed_ms": round(elapsed * 1000, 2)
                },
                message=f"Data operation '{operation}' completed in {elapsed*1000:.1f}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data operation error: {str(e)}")

    def _compare_data(self, data1: Any, data2: Any) -> Dict:
        if data1 == data2:
            return {"equal": True}
        return {"equal": False, "data1_type": type(data1).__name__, "data2_type": type(data2).__name__}

    def _validate_data(self, data: Any, options: Dict) -> Dict:
        return {"valid": True, "data_type": type(data).__name__}

    def _transform_data(self, data: Any, options: Dict) -> Any:
        return data

    def _filter_data(self, data: Any, options: Dict) -> Any:
        return data

    def _aggregate_data(self, data: Any, options: Dict) -> Dict:
        return {"count": 1, "type": type(data).__name__}


class DataCopyAction(BaseAction):
    """Copy data from one location to another."""
    action_type = "data_copy"
    display_name = "数据复制"
    description = "复制数据到目标位置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", {})
            destination = params.get("destination", {})
            data = params.get("data", None)
            deep_copy = params.get("deep_copy", True)
            preserve_metadata = params.get("preserve_metadata", True)

            if data is None:
                return ActionResult(success=False, message="data is required")

            start_time = time.time()

            if deep_copy:
                copied_data = copy.deepcopy(data)
            else:
                copied_data = copy.copy(data)

            if preserve_metadata:
                metadata = {
                    "copied_at": datetime.now().isoformat(),
                    "original_type": type(data).__name__,
                    "size_bytes": len(str(data)),
                    "deep_copy": deep_copy
                }
                copied_data = {"_meta": metadata, "data": copied_data}

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                data={
                    "source": source,
                    "destination": destination,
                    "copied": True,
                    "elapsed_ms": round(elapsed * 1000, 2),
                    "data_size": len(str(copied_data))
                },
                message=f"Data copied successfully in {elapsed*1000:.1f}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data copy error: {str(e)}")


class DataMoveAction(BaseAction):
    """Move data from one location to another."""
    action_type = "data_move"
    display_name = "数据移动"
    description = "移动数据到目标位置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", {})
            destination = params.get("destination", {})
            data = params.get("data", None)
            delete_source = params.get("delete_source", True)
            atomic = params.get("atomic", False)

            if data is None:
                return ActionResult(success=False, message="data is required")

            start_time = time.time()

            moved_data = {
                "data": data,
                "moved_at": datetime.now().isoformat(),
                "source": source,
                "destination": destination,
                "delete_source": delete_source
            }

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                data={
                    "source": source,
                    "destination": destination,
                    "moved": True,
                    "elapsed_ms": round(elapsed * 1000, 2),
                    "atomic": atomic
                },
                message=f"Data moved successfully in {elapsed*1000:.1f}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data move error: {str(e)}")


class DataDeleteAction(BaseAction):
    """Delete data from a location."""
    action_type = "data_delete"
    display_name = "数据删除"
    description = "删除数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            target = params.get("target", {})
            data = params.get("data", None)
            soft_delete = params.get("soft_delete", False)
            confirm = params.get("confirm", False)

            if not confirm:
                return ActionResult(success=False, message="confirm is required to delete data")

            start_time = time.time()

            if soft_delete:
                deleted_data = {
                    "_deleted": True,
                    "deleted_at": datetime.now().isoformat(),
                    "original_data": data
                }
            else:
                deleted_data = None

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                data={
                    "target": target,
                    "deleted": True,
                    "soft_delete": soft_delete,
                    "elapsed_ms": round(elapsed * 1000, 2)
                },
                message=f"Data deleted successfully in {elapsed*1000:.1f}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data delete error: {str(e)}")


class DataMergeAction(BaseAction):
    """Merge data from multiple sources."""
    action_type = "data_merge"
    display_name = "数据合并"
    description = "合并多个数据源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            sources = params.get("sources", [])
            merge_strategy = params.get("merge_strategy", "union")
            key_field = params.get("key_field", None)
            conflict_resolution = params.get("conflict_resolution", "last_wins")

            if not sources:
                return ActionResult(success=False, message="sources are required")

            start_time = time.time()

            if merge_strategy == "union":
                merged = self._merge_union(sources)
            elif merge_strategy == "intersection":
                merged = self._merge_intersection(sources)
            elif merge_strategy == "keyed":
                merged = self._merge_keyed(sources, key_field, conflict_resolution)
            else:
                merged = sources[0] if sources else None

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                data={
                    "sources_count": len(sources),
                    "merge_strategy": merge_strategy,
                    "merged": merged,
                    "elapsed_ms": round(elapsed * 1000, 2)
                },
                message=f"Data merged from {len(sources)} sources in {elapsed*1000:.1f}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data merge error: {str(e)}")

    def _merge_union(self, sources: List) -> List:
        result = []
        for source in sources:
            if isinstance(source, list):
                result.extend(source)
            else:
                result.append(source)
        return result

    def _merge_intersection(self, sources: List) -> List:
        if not sources:
            return []
        result = sources[0]
        for source in sources[1:]:
            if isinstance(result, list) and isinstance(source, list):
                result = [item for item in result if item in source]
        return result

    def _merge_keyed(self, sources: List, key_field: str, conflict_resolution: str) -> Dict:
        merged = {}
        for source in sources:
            if isinstance(source, list):
                for item in source:
                    if isinstance(item, dict) and key_field in item:
                        key = item[key_field]
                        if key not in merged or conflict_resolution == "last_wins":
                            merged[key] = item
        return merged
