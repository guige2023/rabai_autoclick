"""Batch processing action module for RabAI AutoClick.

Provides batch processing operations:
- BatchProcessAction: Process items in batches
- BatchParallelAction: Process items in parallel
- BatchChunkAction: Split data into chunks
- BatchMergeAction: Merge batch results
- BatchRetryAction: Retry failed operations
- BatchThrottleAction: Throttle batch operations
"""

import concurrent.futures
import time
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchProcessAction(BaseAction):
    """Process items in batches."""
    action_type = "batch_process"
    display_name = "批量处理"
    description = "批量处理数据项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            batch_size = params.get("batch_size", 10)
            operation = params.get("operation", "identity")
            params_config = params.get("params", {})

            if not items:
                return ActionResult(success=False, message="items list is required")

            results = []
            failed = []
            total_batches = (len(items) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                start = batch_idx * batch_size
                end = min(start + batch_size, len(items))
                batch = items[start:end]

                batch_results = []
                for item in batch:
                    try:
                        result = self._apply_operation(operation, item, params_config)
                        batch_results.append(result)
                        results.append({"item": item, "result": result, "success": True})
                    except Exception as e:
                        failed.append({"item": item, "error": str(e)})
                        results.append({"item": item, "result": None, "success": False, "error": str(e)})

                params_config["_batch_index"] = batch_idx

            return ActionResult(
                success=True,
                message=f"Processed {len(items)} items in {total_batches} batches",
                data={
                    "results": results,
                    "total": len(items),
                    "succeeded": len(items) - len(failed),
                    "failed": len(failed),
                    "batch_count": total_batches
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch process error: {str(e)}")

    def _apply_operation(self, operation: str, item: Any, params: Dict) -> Any:
        """Apply operation to item."""
        if operation == "identity":
            return item
        elif operation == "double":
            try:
                return float(item) * 2
            except:
                return item
        elif operation == "uppercase":
            return str(item).upper()
        elif operation == "lowercase":
            return str(item).lower()
        elif operation == "trim":
            return str(item).strip()
        elif operation == "length":
            return len(item) if hasattr(item, "__len__") else 1
        elif operation == "hash":
            import hashlib
            return hashlib.md5(str(item).encode()).hexdigest()
        elif operation == "json":
            import json
            return json.dumps(item) if isinstance(item, dict) else item
        else:
            return item


class BatchParallelAction(BaseAction):
    """Process items in parallel."""
    action_type = "batch_parallel"
    display_name = "并行处理"
    description = "并行处理数据项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            max_workers = params.get("max_workers", 4)
            operation = params.get("operation", "identity")
            params_config = params.get("params", {})

            if not items:
                return ActionResult(success=False, message="items list is required")

            results = []
            failed = []

            def process_item(item):
                try:
                    result = self._apply_operation(operation, item, params_config)
                    return {"item": item, "result": result, "success": True}
                except Exception as e:
                    return {"item": item, "result": None, "success": False, "error": str(e)}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_item, item) for item in items]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                        if not result["success"]:
                            failed.append(result)
                    except Exception as e:
                        failed.append({"error": str(e)})

            return ActionResult(
                success=True,
                message=f"Parallel processed {len(items)} items",
                data={
                    "results": results,
                    "total": len(items),
                    "succeeded": len(items) - len(failed),
                    "failed": len(failed),
                    "workers": max_workers
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Parallel process error: {str(e)}")

    def _apply_operation(self, operation: str, item: Any, params: Dict) -> Any:
        if operation == "identity":
            return item
        elif operation == "double":
            try:
                return float(item) * 2
            except:
                return item
        elif operation == "uppercase":
            return str(item).upper()
        elif operation == "hash":
            import hashlib
            return hashlib.sha256(str(item).encode()).hexdigest()
        else:
            return item


class BatchChunkAction(BaseAction):
    """Split data into chunks."""
    action_type = "batch_chunk"
    display_name = "数据分块"
    description = "将数据拆分为块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            chunk_size = params.get("chunk_size", 10)
            overlap = params.get("overlap", 0)

            if not data:
                return ActionResult(success=False, message="data list is required")

            if isinstance(data, str):
                data = list(data)

            chunks = []
            if overlap > 0 and overlap < chunk_size:
                step = chunk_size - overlap
                for i in range(0, len(data), step):
                    chunk = data[i:i + chunk_size]
                    if chunk:
                        chunks.append(chunk)
                    if i + chunk_size >= len(data):
                        break
            else:
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i + chunk_size]
                    if chunk:
                        chunks.append(chunk)

            return ActionResult(
                success=True,
                message=f"Split into {len(chunks)} chunks",
                data={"chunks": chunks, "chunk_count": len(chunks), "chunk_size": chunk_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Chunk error: {str(e)}")


class BatchMergeAction(BaseAction):
    """Merge batch results."""
    action_type = "batch_merge"
    display_name = "合并结果"
    description = "合并批量处理结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batches = params.get("batches", [])
            merge_strategy = params.get("strategy", "concat")

            if not batches:
                return ActionResult(success=False, message="batches list is required")

            if merge_strategy == "concat":
                merged = []
                for batch in batches:
                    if isinstance(batch, list):
                        merged.extend(batch)
                    else:
                        merged.append(batch)

            elif merge_strategy == "union":
                seen = set()
                merged = []
                for batch in batches:
                    if isinstance(batch, list):
                        for item in batch:
                            key = str(item)
                            if key not in seen:
                                seen.add(key)
                                merged.append(item)
                    else:
                        key = str(batch)
                        if key not in seen:
                            seen.add(key)
                            merged.append(batch)

            elif merge_strategy == "zip":
                merged = []
                max_len = max(len(b) for b in batches if isinstance(b, list))
                for i in range(max_len):
                    row = []
                    for batch in batches:
                        if isinstance(batch, list) and i < len(batch):
                            row.append(batch[i])
                        else:
                            row.append(None)
                    merged.append(row)

            elif merge_strategy == "dict_update":
                merged = {}
                for batch in batches:
                    if isinstance(batch, dict):
                        merged.update(batch)
                merged = [merged]

            else:
                merged = []

            return ActionResult(
                success=True,
                message=f"Merged {len(batches)} batches into {len(merged)} items",
                data={"merged": merged, "merged_count": len(merged), "strategy": merge_strategy}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Merge error: {str(e)}")


class BatchRetryAction(BaseAction):
    """Retry failed operations."""
    action_type = "batch_retry"
    display_name = "重试失败操作"
    description = "重试失败的批量操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            failed_items = params.get("failed_items", [])
            operation = params.get("operation", "identity")
            max_retries = params.get("max_retries", 3)
            retry_delay = params.get("retry_delay", 1.0)
            params_config = params.get("params", {})

            if not failed_items:
                return ActionResult(success=False, message="failed_items list is required")

            results = []
            still_failing = []

            for item in failed_items:
                last_error = None
                for attempt in range(max_retries):
                    try:
                        result = self._apply_operation(operation, item, params_config)
                        results.append({"item": item, "result": result, "success": True, "attempts": attempt + 1})
                        break
                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay * (attempt + 1))
                else:
                    still_failing.append({"item": item, "error": last_error, "attempts": max_retries})
                    results.append({"item": item, "result": None, "success": False, "error": last_error, "attempts": max_retries})

            return ActionResult(
                success=True,
                message=f"Retry: {len(failed_items) - len(still_failing)}/{len(failed_items)} recovered",
                data={
                    "results": results,
                    "recovered": len(failed_items) - len(still_failing),
                    "still_failing": len(still_failing),
                    "max_retries": max_retries
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Retry error: {str(e)}")

    def _apply_operation(self, operation: str, item: Any, params: Dict) -> Any:
        if operation == "identity":
            return item
        elif operation == "double":
            return float(item) * 2
        elif operation == "uppercase":
            return str(item).upper()
        else:
            return item


class BatchThrottleAction(BaseAction):
    """Throttle batch operations."""
    action_type = "batch_throttle"
    display_name = "批量限流"
    description = "对批量操作进行限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            rate_limit = params.get("rate_limit", 10)
            time_window = params.get("time_window", 1.0)

            if not items:
                return ActionResult(success=False, message="items list is required")

            interval = time_window / rate_limit if rate_limit > 0 else 0
            processed = 0
            start_time = time.time()

            results = []
            for item in items:
                result = self._process_item(item)
                results.append(result)
                processed += 1

                if processed < len(items) and interval > 0:
                    elapsed = time.time() - start_time
                    expected_elapsed = processed * interval
                    if elapsed < expected_elapsed:
                        time.sleep(expected_elapsed - elapsed)

            total_time = time.time() - start_time

            return ActionResult(
                success=True,
                message=f"Throttled processed {len(items)} items in {total_time:.2f}s",
                data={
                    "results": results,
                    "count": len(items),
                    "rate_limit": rate_limit,
                    "time_window": time_window,
                    "actual_rate": len(items) / total_time if total_time > 0 else 0,
                    "total_time": total_time
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Throttle error: {str(e)}")

    def _process_item(self, item: Any) -> Any:
        return item
