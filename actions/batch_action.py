"""Batch processor action module for RabAI AutoClick.

Provides batch processing operations:
- BatchProcessAction: Process items in batches
- BatchChunkAction: Split data into chunks
- BatchParallelAction: Process batches in parallel
- BatchWindowAction: Sliding window batch processing
"""

import concurrent.futures
import threading
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

T = TypeVar("T")


class BatchProcessor:
    """Generic batch processor."""
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size

    def process(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        max_workers: int = 4,
        fail_fast: bool = False
    ) -> Dict[str, Any]:
        results = []
        errors = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(processor, item): item for item in items}
            for future in concurrent.futures.as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    results.append({"item": item, "result": result})
                except Exception as ex:
                    errors.append({"item": item, "error": str(ex)})
                    if fail_fast:
                        for f in futures:
                            f.cancel()
                        break
        return {"results": results, "errors": errors, "total": len(items)}


class BatchProcessAction(BaseAction):
    """Process items in batches."""
    action_type = "batch_process"
    display_name = "批量处理"
    description = "批量处理数据项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            batch_size = params.get("batch_size", 10)
            processor_ref = params.get("processor_ref", None)
            parallel = params.get("parallel", False)
            max_workers = params.get("max_workers", 4)
            fail_fast = params.get("fail_fast", False)

            if not items:
                return ActionResult(success=False, message="items are required")

            def default_processor(item):
                return {"processed": item}

            processor = processor_ref or default_processor

            if parallel:
                bp = BatchProcessor(batch_size=batch_size)
                result = bp.process(items, processor, max_workers=max_workers, fail_fast=fail_fast)
                results = result["results"]
                errors = result["errors"]
            else:
                results = []
                errors = []
                for item in items:
                    try:
                        results.append({"item": item, "result": processor(item)})
                    except Exception as ex:
                        errors.append({"item": item, "error": str(ex)})
                        if fail_fast:
                            break

            return ActionResult(
                success=len(errors) == 0,
                message=f"Batch processed: {len(results)} succeeded, {len(errors)} failed",
                data={"results": results, "errors": errors, "total": len(items), "succeeded": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch process failed: {str(e)}")


class BatchChunkAction(BaseAction):
    """Split data into chunks."""
    action_type = "batch_chunk"
    display_name = "数据分块"
    description = "将数据分割成块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            chunk_size = params.get("chunk_size", 10)
            overlap = params.get("overlap", 0)

            if not data:
                return ActionResult(success=False, message="data is required")
            if chunk_size <= 0:
                return ActionResult(success=False, message="chunk_size must be positive")

            if overlap < 0:
                overlap = 0

            chunks = []
            if overlap == 0:
                for i in range(0, len(data), chunk_size):
                    chunks.append(data[i:i + chunk_size])
            else:
                i = 0
                while i < len(data):
                    chunks.append(data[i:i + chunk_size])
                    i += chunk_size - overlap

            return ActionResult(
                success=True,
                message=f"Split {len(data)} items into {len(chunks)} chunks",
                data={
                    "chunks": chunks,
                    "chunk_count": len(chunks),
                    "chunk_size": chunk_size,
                    "overlap": overlap,
                    "total_items": len(data)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch chunk failed: {str(e)}")


class BatchParallelAction(BaseAction):
    """Process batches in parallel."""
    action_type = "batch_parallel"
    display_name = "并行批量处理"
    description = "并行处理多个批次"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batches = params.get("batches", [])
            batch_processor_ref = params.get("batch_processor_ref", None)
            max_workers = params.get("max_workers", 4)
            timeout = params.get("timeout", 60)

            if not batches:
                return ActionResult(success=False, message="batches are required")

            def default_batch_processor(batch):
                return [{"processed": item} for item in batch]

            processor = batch_processor_ref or default_batch_processor
            batch_results = []
            errors = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(processor, batch): idx for idx, batch in enumerate(batches)}
                for future in concurrent.futures.as_completed(futures, timeout=timeout):
                    idx = futures[future]
                    try:
                        result = future.result()
                        batch_results.append({"batch_index": idx, "result": result})
                    except Exception as ex:
                        errors.append({"batch_index": idx, "error": str(ex)})

            return ActionResult(
                success=len(errors) == 0,
                message=f"Parallel batch processing: {len(batch_results)} succeeded, {len(errors)} failed",
                data={
                    "batch_results": batch_results,
                    "errors": errors,
                    "total_batches": len(batches),
                    "succeeded": len(batch_results)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch parallel failed: {str(e)}")


class BatchWindowAction(BaseAction):
    """Sliding window batch processing."""
    action_type = "batch_window"
    display_name = "滑动窗口批处理"
    description = "滑动窗口方式批处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 5)
            step = params.get("step", 1)
            processor_ref = params.get("processor_ref", None)

            if not data:
                return ActionResult(success=False, message="data is required")
            if window_size <= 0:
                return ActionResult(success=False, message="window_size must be positive")

            def default_processor(window):
                return {"window": list(window), "size": len(window)}

            processor = processor_ref or default_processor

            windows = []
            results = []
            for i in range(0, len(data), step):
                window = data[i:i + window_size]
                if len(window) == window_size:
                    windows.append(window)
                    try:
                        results.append({"window_index": i // step, "result": processor(window)})
                    except Exception as ex:
                        results.append({"window_index": i // step, "error": str(ex)})

            return ActionResult(
                success=True,
                message=f"Created {len(windows)} windows from {len(data)} items",
                data={
                    "windows": windows,
                    "results": results,
                    "window_count": len(windows),
                    "window_size": window_size,
                    "step": step
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch window failed: {str(e)}")
