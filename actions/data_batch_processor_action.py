"""Data batch processor action module for RabAI AutoClick.

Provides batch data processing operations:
- BatchProcessorAction: Process data in configurable batches
- BatchLoaderAction: Load data in batches from source
- BatchWriterAction: Write data in batches to destination
- BatchValidatorAction: Validate data in batches
- BatchTransformerAction: Transform data in batches
"""

import time
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchProcessorAction(BaseAction):
    """Process data in configurable batches."""
    action_type = "data_batch_processor"
    display_name = "批处理器"
    description = "按可配置批次处理数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            batch_size = params.get("batch_size", 100)
            process_func = params.get("process_func", "identity")
            parallel = params.get("parallel", False)
            max_workers = params.get("max_workers", 4)

            if not isinstance(data, list):
                data = [data]

            total_items = len(data)
            if total_items == 0:
                return ActionResult(success=False, message="No data to process")

            batches = []
            for i in range(0, total_items, batch_size):
                batches.append(data[i:i + batch_size])

            processed_batches = []
            failed_batches = []

            for i, batch in enumerate(batches):
                batch_result = self._process_batch(batch, process_func)
                if batch_result["success"]:
                    processed_batches.append(batch_result["data"])
                else:
                    failed_batches.append({"batch_index": i, "error": batch_result["error"]})

            all_processed = []
            for pb in processed_batches:
                if isinstance(pb, list):
                    all_processed.extend(pb)
                else:
                    all_processed.append(pb)

            return ActionResult(
                success=len(failed_batches) == 0,
                data={
                    "processed": all_processed,
                    "total_items": total_items,
                    "batch_count": len(batches),
                    "batch_size": batch_size,
                    "processed_batches": len(processed_batches),
                    "failed_batches": len(failed_batches),
                    "parallel": parallel
                },
                message=f"Batch processed: {len(all_processed)}/{total_items} items in {len(batches)} batches"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch processor error: {str(e)}")

    def _process_batch(self, batch: List, process_func: str) -> Dict:
        try:
            results = []
            for item in batch:
                if process_func == "identity":
                    result = item
                elif process_func == "uppercase":
                    result = str(item).upper()
                elif process_func == "lowercase":
                    result = str(item).lower()
                elif process_func == "hash":
                    import hashlib
                    result = hashlib.md5(str(item).encode()).hexdigest()
                elif process_func == "length":
                    result = len(str(item))
                else:
                    result = item
                results.append(result)
            return {"success": True, "data": results}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"batch_size": 100, "process_func": "identity", "parallel": False, "max_workers": 4}


class BatchLoaderAction(BaseAction):
    """Load data in batches from source."""
    action_type = "data_batch_loader"
    display_name = "批量加载器"
    description = "从数据源批量加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", {})
            batch_size = params.get("batch_size", 100)
            max_batches = params.get("max_batches")
            offset = params.get("offset", 0)
            filter_criteria = params.get("filter_criteria", {})
            sort_by = params.get("sort_by")
            sort_order = params.get("sort_order", "asc")

            source_type = source.get("type", "mock")
            total_available = source.get("total", 1000)

            loaded_batches = []
            current_offset = offset
            batch_count = 0

            while True:
                if max_batches and batch_count >= max_batches:
                    break

                batch_data = [f"{source_type}_item_{current_offset + i}" for i in range(batch_size)]
                current_offset += batch_size
                batch_count += 1

                if current_offset >= total_available:
                    loaded_batches.append(batch_data)
                    break

                loaded_batches.append(batch_data)

            all_items = []
            for batch in loaded_batches:
                all_items.extend(batch)

            return ActionResult(
                success=True,
                data={
                    "loaded_batches": loaded_batches,
                    "all_items": all_items,
                    "total_loaded": len(all_items),
                    "batch_count": len(loaded_batches),
                    "batch_size": batch_size,
                    "source_type": source_type,
                    "offset": offset
                },
                message=f"Loaded {len(all_items)} items in {len(loaded_batches)} batches"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch loader error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["source"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"batch_size": 100, "max_batches": None, "offset": 0, "filter_criteria": {}, "sort_by": None, "sort_order": "asc"}


class BatchWriterAction(BaseAction):
    """Write data in batches to destination."""
    action_type = "data_batch_writer"
    display_name = "批量写入器"
    description = "批量写入数据到目标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            destination = params.get("destination", {})
            batch_size = params.get("batch_size", 100)
            write_mode = params.get("write_mode", "append")
            on_duplicate = params.get("on_duplicate", "ignore")

            if not isinstance(data, list):
                data = [data]

            dest_type = destination.get("type", "mock")
            total_items = len(data)

            batches = []
            for i in range(0, total_items, batch_size):
                batches.append(data[i:i + batch_size])

            written_batches = []
            failed_batches = []

            for i, batch in enumerate(batches):
                batch_success = destination.get("success", True)
                if batch_success:
                    written_batches.append({
                        "batch_index": i,
                        "items_written": len(batch),
                        "destination": dest_type
                    })
                else:
                    failed_batches.append({"batch_index": i, "items": len(batch)})

            total_written = sum(b["items_written"] for b in written_batches)

            return ActionResult(
                success=len(failed_batches) == 0,
                data={
                    "written_batches": written_batches,
                    "total_written": total_written,
                    "total_items": total_items,
                    "batch_count": len(batches),
                    "batch_size": batch_size,
                    "write_mode": write_mode,
                    "failed_batches": len(failed_batches)
                },
                message=f"Wrote {total_written}/{total_items} items in {len(batches)} batches"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch writer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data", "destination"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"batch_size": 100, "write_mode": "append", "on_duplicate": "ignore"}


class BatchValidatorAction(BaseAction):
    """Validate data in batches."""
    action_type = "data_batch_validator"
    display_name = "批量验证器"
    description = "批量验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            validators = params.get("validators", ["not_null", "type_check"])
            batch_size = params.get("batch_size", 100)
            stop_on_error = params.get("stop_on_error", False)
            error_threshold = params.get("error_threshold", 0.1)

            if not isinstance(data, list):
                data = [data]

            batches = []
            for i in range(0, len(data), batch_size):
                batches.append(data[i:i + batch_size])

            all_errors = []
            valid_items = []
            batch_results = []

            for i, batch in enumerate(batches):
                batch_errors = []
                batch_valid = []

                for j, item in enumerate(batch):
                    item_errors = self._validate_item(item, validators)
                    if item_errors:
                        batch_errors.append({
                            "item_index": j,
                            "global_index": i * batch_size + j,
                            "errors": item_errors
                        })
                    else:
                        batch_valid.append(item)

                all_errors.extend(batch_errors)
                valid_items.extend(batch_valid)

                error_rate = len(batch_errors) / len(batch) if batch else 0
                batch_results.append({
                    "batch_index": i,
                    "valid_count": len(batch_valid),
                    "error_count": len(batch_errors),
                    "error_rate": error_rate,
                    "pass": error_rate <= error_threshold
                })

                if stop_on_error and len(all_errors) > 0:
                    break

            total_errors = sum(b["error_count"] for b in batch_results)
            total_valid = sum(b["valid_count"] for b in batch_results)

            return ActionResult(
                success=total_errors == 0,
                data={
                    "valid_items": valid_items,
                    "errors": all_errors,
                    "total_valid": total_valid,
                    "total_errors": total_errors,
                    "batch_results": batch_results,
                    "error_threshold": error_threshold
                },
                message=f"Validated: {total_valid} valid, {total_errors} errors"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch validator error: {str(e)}")

    def _validate_item(self, item: Any, validators: List[str]) -> List[str]:
        errors = []
        for validator in validators:
            if validator == "not_null" and item is None:
                errors.append("not_null: item is None")
            elif validator == "type_check":
                if not isinstance(item, (dict, list, str, int, float, bool)):
                    errors.append(f"type_check: unexpected type {type(item).__name__}")
        return errors

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"validators": ["not_null", "type_check"], "batch_size": 100, "stop_on_error": False, "error_threshold": 0.1}


class BatchTransformerAction(BaseAction):
    """Transform data in batches."""
    action_type = "data_batch_transformer"
    display_name = "批量转换器"
    description = "批量转换数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            transforms = params.get("transforms", [])
            batch_size = params.get("batch_size", 100)
            preserve_original = params.get("preserve_original", False)

            if not isinstance(data, list):
                data = [data]

            batches = []
            for i in range(0, len(data), batch_size):
                batches.append(data[i:i + batch_size])

            transformed_batches = []
            transform_counts = {t: 0 for t in transforms}

            for batch in batches:
                transformed_batch = []
                for item in batch:
                    current = item
                    if isinstance(item, dict):
                        current = dict(item)
                        for transform in transforms:
                            if transform == "flatten":
                                current = {f"{k}": v for k, v in current.items()}
                            elif transform == "uppercase_keys":
                                current = {k.upper(): v for k, v in current.items()}
                            elif transform == "lowercase_keys":
                                current = {k.lower(): v for k, v in current.items()}
                            elif transform == "strip_values":
                                current = {k: str(v).strip() if isinstance(v, str) else v for k, v in current.items()}
                            transform_counts[transform] += 1
                    transformed_batch.append(current)
                transformed_batches.append(transformed_batch)

            all_transformed = []
            for tb in transformed_batches:
                all_transformed.extend(tb)

            return ActionResult(
                success=True,
                data={
                    "transformed": all_transformed,
                    "original_count": len(data),
                    "transformed_count": len(all_transformed),
                    "batch_count": len(transformed_batches),
                    "transform_counts": transform_counts
                },
                message=f"Transformed {len(data)} items with {len(transforms)} transforms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch transformer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"transforms": [], "batch_size": 100, "preserve_original": False}
