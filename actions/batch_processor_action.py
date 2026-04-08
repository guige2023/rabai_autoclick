"""Batch processor action module for RabAI AutoClick.

Provides batch processing capabilities for handling large datasets
with chunking, parallel execution, and progress tracking.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Iterable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

T = TypeVar('T')
R = TypeVar('R')


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    chunk_size: int = 100
    max_workers: int = 4
    stop_on_error: bool = False
    continue_on_error: bool = True
    progress_callback: Optional[Callable[[int, int], None]] = None


@dataclass
class BatchResult:
    """Result of a batch operation."""
    total_items: int
    successful: int
    failed: int
    results: List[Any]
    errors: List[Dict[str, Any]]
    elapsed_time: float


class BatchProcessorAction(BaseAction):
    """Process items in configurable batches with parallel execution.
    
    Supports chunking, parallel processing, error handling, and progress
    tracking for large dataset operations.
    """
    action_type = "batch_processor"
    display_name = "批量处理"
    description = "批量处理大数据集，支持分块和并行执行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch processing.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - items: Iterable of items to process
                - process_func: Callable(item) -> result
                - chunk_size: Items per batch (default 100)
                - max_workers: Parallel workers (default 4)
                - stop_on_error: Stop on first error (default False)
                - continue_on_error: Continue on error (default True)
                - progress_callback: Optional callback(progress, total)
        
        Returns:
            ActionResult with batch processing results and statistics.
        """
        # Extract parameters
        items = params.get('items')
        if items is None:
            return ActionResult(success=False, message="items is required")
        
        process_func = params.get('process_func')
        if not callable(process_func):
            return ActionResult(success=False, message="process_func must be callable")
        
        chunk_size = params.get('chunk_size', 100)
        max_workers = params.get('max_workers', 4)
        stop_on_error = params.get('stop_on_error', False)
        continue_on_error = params.get('continue_on_error', True)
        progress_callback = params.get('progress_callback')
        
        # Validate chunk_size
        if chunk_size <= 0:
            return ActionResult(
                success=False,
                message=f"chunk_size must be positive, got {chunk_size}"
            )
        
        # Convert items to list if needed
        if isinstance(items, (str, bytes)):
            return ActionResult(
                success=False,
                message="items cannot be string or bytes"
            )
        
        try:
            items_list = list(items)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to convert items to list: {e}"
            )
        
        total_items = len(items_list)
        if total_items == 0:
            return ActionResult(
                success=True,
                message="No items to process",
                data={'total': 0, 'successful': 0, 'failed': 0}
            )
        
        # Create chunks
        chunks = self._chunk_list(items_list, chunk_size)
        num_chunks = len(chunks)
        
        # Process
        start_time = time.time()
        all_results = []
        all_errors = []
        successful = 0
        failed = 0
        should_stop = False
        
        for chunk_idx, chunk in enumerate(chunks):
            if should_stop:
                break
            
            chunk_num = chunk_idx + 1
            
            # Process chunk items in parallel
            chunk_results = []
            chunk_errors = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_item = {
                    executor.submit(self._safe_process, process_func, item): item
                    for item in chunk
                }
                
                for future in as_completed(future_to_item):
                    item = future_to_item[future]
                    try:
                        result = future.result()
                        chunk_results.append(result)
                        successful += 1
                    except Exception as e:
                        error_info = {'item': str(item), 'error': str(e)}
                        chunk_errors.append(error_info)
                        failed += 1
                        
                        if stop_on_error:
                            should_stop = True
                            break
            
            all_results.extend(chunk_results)
            all_errors.extend(chunk_errors)
            
            # Progress callback
            processed = min(chunk_num * chunk_size, total_items)
            if progress_callback:
                try:
                    progress_callback(processed, total_items)
                except Exception:
                    pass
        
        elapsed_time = time.time() - start_time
        
        # Build result
        result = BatchResult(
            total_items=total_items,
            successful=successful,
            failed=failed,
            results=all_results,
            errors=all_errors,
            elapsed_time=elapsed_time
        )
        
        success = failed == 0 or continue_on_error
        
        return ActionResult(
            success=success,
            message=f"Processed {successful}/{total_items} items in {elapsed_time:.2f}s",
            data={
                'total': total_items,
                'successful': successful,
                'failed': failed,
                'results': all_results[:100],  # Limit returned results
                'errors': all_errors[:100],
                'elapsed_time': elapsed_time,
                'items_per_second': total_items / elapsed_time if elapsed_time > 0 else 0
            }
        )
    
    def _chunk_list(self, items: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split list into chunks."""
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    
    def _safe_process(
        self, 
        func: Callable[[Any], Any], 
        item: Any
    ) -> Any:
        """Safely process an item."""
        return func(item)


class ChunkedIteratorAction(BaseAction):
    """Process items in chunks without loading all into memory.
    
    Memory-efficient processing for large iterables.
    """
    action_type = "chunked_iterator"
    display_name = "分块迭代"
    description = "内存高效的分块迭代处理"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute chunked iteration.
        
        Args:
            context: Execution context.
            params: Dict with:
                - iterator: Iterable to process
                - process_func: Callable(item) -> result
                - chunk_size: Items per chunk (default 50)
                - max_chunks: Maximum chunks to process
        
        Returns:
            ActionResult with aggregated results.
        """
        iterator = params.get('iterator')
        process_func = params.get('process_func')
        chunk_size = params.get('chunk_size', 50)
        max_chunks = params.get('max_chunks')
        
        if not callable(process_func):
            return ActionResult(success=False, message="process_func required")
        
        if not hasattr(iterator, '__iter__'):
            return ActionResult(success=False, message="iterator must be iterable")
        
        results = []
        chunk_count = 0
        item_count = 0
        
        chunk = []
        for item in iterator:
            chunk.append(item)
            if len(chunk) >= chunk_size:
                chunk_results = [process_func(i) for i in chunk]
                results.extend(chunk_results)
                item_count += len(chunk)
                chunk_count += 1
                chunk = []
                
                if max_chunks and chunk_count >= max_chunks:
                    break
        
        # Process remaining items
        if chunk and not (max_chunks and chunk_count >= max_chunks):
            chunk_results = [process_func(i) for i in chunk]
            results.extend(chunk_results)
            item_count += len(chunk)
            chunk_count += 1
        
        return ActionResult(
            success=True,
            message=f"Processed {item_count} items in {chunk_count} chunks",
            data={'results': results, 'item_count': item_count, 'chunk_count': chunk_count}
        )
