"""Batch V2 action module for RabAI AutoClick.

Provides advanced batch processing capabilities including
chunking, parallel processing, and result aggregation.
"""

import time
import threading
import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BatchResult:
    """Result of a batch operation.
    
    Attributes:
        total: Total items processed.
        successful: Number of successful items.
        failed: Number of failed items.
        results: Individual item results.
        duration: Total processing time.
        errors: Any errors encountered.
    """
    total: int
    successful: int
    failed: int
    results: List[Any]
    duration: float
    errors: List[Dict[str, Any]] = field(default_factory=list)


class BatchProcessor:
    """Processes items in batches with configurable parallelism."""
    
    def __init__(self, max_workers: int = 4, chunk_size: int = 10):
        """Initialize batch processor.
        
        Args:
            max_workers: Maximum parallel workers.
            chunk_size: Items per batch chunk.
        """
        self.max_workers = max_workers
        self.chunk_size = chunk_size
    
    def process(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        on_error: Callable[[Any, Exception], Any] = None,
        stop_on_error: bool = False
    ) -> BatchResult:
        """Process items in batches.
        
        Args:
            items: Items to process.
            processor: Callable to apply to each item.
            on_error: Optional error handler.
            stop_on_error: Stop processing on first error.
        
        Returns:
            BatchResult with processing outcomes.
        """
        start_time = time.time()
        results = []
        errors = []
        successful = 0
        failed = 0
        
        chunks = [items[i:i + self.chunk_size] for i in range(0, len(items), self.chunk_size)]
        
        for chunk_idx, chunk in enumerate(chunks):
            if stop_on_error and errors:
                break
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_item = {executor.submit(self._process_item, item, processor): item for item in chunk}
                
                for future in as_completed(future_to_item):
                    item = future_to_item[future]
                    
                    try:
                        result = future.result()
                        results.append(result)
                        successful += 1
                    except Exception as e:
                        failed += 1
                        error_info = {"item": str(item), "error": str(e), "type": type(e).__name__}
                        errors.append(error_info)
                        
                        if on_error:
                            try:
                                error_result = on_error(item, e)
                                results.append(error_result)
                            except Exception:
                                pass
                        
                        if stop_on_error:
                            break
        
        return BatchResult(
            total=len(items),
            successful=successful,
            failed=failed,
            results=results,
            duration=time.time() - start_time,
            errors=errors
        )
    
    def _process_item(self, item: Any, processor: Callable) -> Any:
        """Process a single item."""
        return processor(item)
    
    def chunk_list(self, items: List[Any]) -> List[List[Any]]:
        """Split a list into chunks.
        
        Args:
            items: List to chunk.
        
        Returns:
            List of chunks.
        """
        return [items[i:i + self.chunk_size] for i in range(0, len(items), self.chunk_size)]


class BatchV2Action(BaseAction):
    """Advanced batch processing with parallelism."""
    action_type = "batch_v2"
    display_name = "批量处理V2"
    description = "高级批量并行处理"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch processing.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, processor_type, 
                   max_workers, chunk_size, stop_on_error.
        
        Returns:
            ActionResult with batch processing results.
        """
        items = params.get('items', [])
        processor_type = params.get('processor_type', 'identity')
        max_workers = params.get('max_workers', 4)
        chunk_size = params.get('chunk_size', 10)
        stop_on_error = params.get('stop_on_error', False)
        
        if not items:
            return ActionResult(success=False, message="items list is required")
        
        processor = BatchProcessor(max_workers=max_workers, chunk_size=chunk_size)
        
        def identity_processor(item):
            return item
        
        def transform_processor(item):
            if isinstance(item, dict):
                return {k: v for k, v in item.items() if v is not None}
            return item
        
        def filter_processor(item):
            if isinstance(item, (list, dict)):
                return bool(item)
            return item is not None
        
        processors = {
            'identity': identity_processor,
            'transform': transform_processor,
            'filter': filter_processor
        }
        
        proc = processors.get(processor_type, identity_processor)
        
        result = processor.process(items, proc, stop_on_error=stop_on_error)
        
        return ActionResult(
            success=True,
            message=f"Batch processed {result.total} items: {result.successful} successful, {result.failed} failed",
            data={
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration_ms": round(result.duration * 1000, 2),
                "errors": result.errors[:10] if result.errors else []
            }
        )


class ChunkAction(BaseAction):
    """Split data into chunks."""
    action_type = "chunk"
    display_name = "数据分块"
    description = "将数据拆分为多个块"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split data into chunks.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, chunk_size.
        
        Returns:
            ActionResult with chunks.
        """
        data = params.get('data', [])
        chunk_size = params.get('chunk_size', 10)
        
        if not isinstance(data, (list, tuple)):
            return ActionResult(success=False, message="data must be a list or tuple")
        
        if chunk_size <= 0:
            return ActionResult(success=False, message="chunk_size must be positive")
        
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        return ActionResult(
            success=True,
            message=f"Split {len(data)} items into {len(chunks)} chunks",
            data={
                "chunks": chunks,
                "chunk_count": len(chunks),
                "chunk_size": chunk_size,
                "total_items": len(data)
            }
        )


class BatchMapAction(BaseAction):
    """Map a function over items in parallel."""
    action_type = "batch_map"
    display_name = "批量映射"
    description = "并行映射函数到数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Map function over items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, func_body, max_workers.
        
        Returns:
            ActionResult with mapped results.
        """
        items = params.get('items', [])
        func_body = params.get('func_body', 'lambda x: x')
        max_workers = params.get('max_workers', 4)
        
        if not items:
            return ActionResult(success=False, message="items list is required")
        
        try:
            func = eval(func_body) if isinstance(func_body, str) else func_body
        except Exception as e:
            return ActionResult(success=False, message=f"Invalid function: {str(e)}")
        
        start_time = time.time()
        results = []
        errors = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(func, item): idx for idx, item in enumerate(items)}
            
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    results.append({"index": idx, "result": result, "success": True})
                except Exception as e:
                    results.append({"index": idx, "error": str(e), "success": False})
                    errors.append({"index": idx, "error": str(e)})
        
        sorted_results = sorted(results, key=lambda x: x["index"])
        
        return ActionResult(
            success=True,
            message=f"Mapped {len(items)} items in {time.time() - start_time:.2f}s",
            data={
                "results": [r.get("result") for r in sorted_results],
                "total": len(items),
                "successful": len([r for r in results if r.get("success")]),
                "failed": len(errors),
                "errors": errors[:10] if errors else []
            }
        )


class BatchFilterAction(BaseAction):
    """Filter items in parallel using a predicate."""
    action_type = "batch_filter"
    display_name = "批量过滤"
    description = "并行过滤数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter items using predicate.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, predicate_body, max_workers.
        
        Returns:
            ActionResult with filtered items.
        """
        items = params.get('items', [])
        predicate_body = params.get('predicate_body', 'lambda x: bool(x)')
        max_workers = params.get('max_workers', 4)
        
        if not items:
            return ActionResult(success=False, message="items list is required")
        
        try:
            predicate = eval(predicate_body) if isinstance(predicate_body, str) else predicate_body
        except Exception as e:
            return ActionResult(success=False, message=f"Invalid predicate: {str(e)}")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(predicate, item): idx for idx, item in enumerate(items)}
            kept = []
            
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    if future.result():
                        kept.append({"index": idx, "item": items[idx]})
                except Exception:
                    pass
        
        kept_sorted = sorted(kept, key=lambda x: x["index"])
        
        return ActionResult(
            success=True,
            message=f"Filtered {len(items)} items to {len(kept)} matches",
            data={
                "items": [x["item"] for x in kept_sorted],
                "original_count": len(items),
                "filtered_count": len(kept),
                "indices": [x["index"] for x in kept_sorted]
            }
        )


class BatchReduceAction(BaseAction):
    """Reduce items to a single value using a combining function."""
    action_type = "batch_reduce"
    display_name = "批量聚合"
    description = "将数据批量聚合为单一值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reduce items to single value.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, reduce_body, initial.
        
        Returns:
            ActionResult with reduced value.
        """
        items = params.get('items', [])
        reduce_body = params.get('reduce_body', 'lambda acc, x: acc + x')
        initial = params.get('initial', None)
        
        if not items:
            return ActionResult(success=False, message="items list is required")
        
        try:
            reducer = eval(reduce_body) if isinstance(reduce_body, str) else reduce_body
        except Exception as e:
            return ActionResult(success=False, message=f"Invalid reducer: {str(e)}")
        
        start_time = time.time()
        accumulator = initial
        
        for item in items:
            try:
                accumulator = reducer(accumulator, item)
            except Exception as e:
                return ActionResult(success=False, message=f"Reduce error: {str(e)}")
        
        return ActionResult(
            success=True,
            message=f"Reduced {len(items)} items in {time.time() - start_time:.2f}s",
            data={
                "result": accumulator,
                "item_count": len(items),
                "duration_ms": round((time.time() - start_time) * 1000, 2)
            }
        )
