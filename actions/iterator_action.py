"""Iterator action module for RabAI AutoClick.

Provides iterator and generator-based actions for processing
sequences, with support for pagination and streaming.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Iterator, Callable, Generator
from dataclasses import dataclass
from collections.abc import Iterable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IteratorProcessor:
    """Process items from an iterator with batching and transformation."""
    
    def __init__(self, batch_size: int = 10):
        """Initialize iterator processor.
        
        Args:
            batch_size: Items per batch.
        """
        self.batch_size = batch_size
    
    def process(
        self,
        items: Iterable,
        processor: Callable[[Any], Any],
        filter_func: Callable[[Any], bool] = None,
        max_items: int = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Process items from an iterator.
        
        Args:
            items: Source iterable.
            processor: Transform function.
            filter_func: Optional filter predicate.
            max_items: Maximum items to process.
        
        Yields:
            Dict with item index, original value, processed result.
        """
        for idx, item in enumerate(items):
            if max_items is not None and idx >= max_items:
                break
            
            if filter_func is None or filter_func(item):
                try:
                    result = processor(item)
                    yield {"index": idx, "item": item, "result": result, "success": True}
                except Exception as e:
                    yield {"index": idx, "item": item, "error": str(e), "success": False}
    
    def batch_process(
        self,
        items: Iterable,
        processor: Callable[[List[Any]], List[Any]],
        max_items: int = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Process items in batches.
        
        Args:
            items: Source iterable.
            processor: Batch transform function.
            max_items: Maximum items to process.
        
        Yields:
            Dict with batch index, items, results.
        """
        batch = []
        batch_idx = 0
        
        for item in items:
            if max_items is not None and len(batch) >= max_items:
                break
            
            batch.append(item)
            
            if len(batch) >= self.batch_size:
                try:
                    results = processor(batch)
                    yield {"batch": batch_idx, "items": batch, "results": results, "success": True}
                except Exception as e:
                    yield {"batch": batch_idx, "items": batch, "error": str(e), "success": False}
                batch = []
                batch_idx += 1
        
        if batch:
            try:
                results = processor(batch)
                yield {"batch": batch_idx, "items": batch, "results": results, "success": True}
            except Exception as e:
                yield {"batch": batch_idx, "items": batch, "error": str(e), "success": False}


class RangeIterator:
    """Generate numeric sequences."""
    
    def __init__(self, start: int, stop: int, step: int = 1):
        """Initialize range iterator.
        
        Args:
            start: Start value.
            stop: Stop value (exclusive).
            step: Step value.
        """
        self.start = start
        self.stop = stop
        self.step = step
        self._current = start
    
    def __iter__(self):
        self._current = self.start
        return self
    
    def __next__(self) -> int:
        if self._current >= self.stop:
            raise StopIteration
        value = self._current
        self._current += self.step
        return value
    
    def to_list(self) -> List[int]:
        """Convert to list."""
        return list(range(self.start, self.stop, self.step))


class CycleIterator:
    """Cycle through an iterable indefinitely."""
    
    def __init__(self, items: List[Any]):
        """Initialize cycle iterator.
        
        Args:
            items: Items to cycle through.
        """
        self.items = items
        self._index = 0
    
    def __iter__(self):
        return self
    
    def __next__(self) -> Any:
        if not self.items:
            raise StopIteration
        value = self.items[self._index]
        self._index = (self._index + 1) % len(self.items)
        return value
    
    def reset(self) -> None:
        """Reset to beginning."""
        self._index = 0


class ChunkIterator:
    """Iterator that yields chunks of items."""
    
    def __init__(self, items: List[Any], chunk_size: int):
        """Initialize chunk iterator.
        
        Args:
            items: Items to chunk.
            chunk_size: Size of each chunk.
        """
        self.items = items
        self.chunk_size = chunk_size
        self._index = 0
    
    def __iter__(self):
        return self
    
    def __next__(self) -> List[Any]:
        if self._index >= len(self.items):
            raise StopIteration
        chunk = self.items[self._index:self._index + self.chunk_size]
        self._index += self.chunk_size
        return chunk


class EnumerateIterator:
    """Iterator that yields indexed items."""
    
    def __init__(self, items: Iterable, start: int = 0):
        """Initialize enumerate iterator.
        
        Args:
            items: Items to enumerate.
            start: Starting index.
        """
        self.items = items
        self.start = start
        self._iterator = iter(items)
        self._index = start
    
    def __iter__(self):
        self._iterator = iter(self.items)
        self._index = self.start
        return self
    
    def __next__(self) -> tuple:
        item = next(self._iterator)
        index = self._index
        self._index += 1
        return (index, item)


class IteratorAction(BaseAction):
    """Process items using an iterator pattern."""
    action_type = "iterator"
    display_name = "迭代器处理"
    description = "迭代处理数据项"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process items with iterator.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, processor_body, filter_body, max_items.
        
        Returns:
            ActionResult with processed results.
        """
        items = params.get('items', [])
        processor_body = params.get('processor_body', 'lambda x: x')
        filter_body = params.get('filter_body', None)
        max_items = params.get('max_items', None)
        batch_size = params.get('batch_size', 10)
        
        if not items:
            return ActionResult(success=False, message="items are required")
        
        try:
            processor = eval(f"lambda x: {processor_body}") if isinstance(processor_body, str) else processor_body
            
            filter_func = None
            if filter_body:
                filter_func = eval(f"lambda x: {filter_body}") if isinstance(filter_body, str) else filter_body
            
            iter_processor = IteratorProcessor(batch_size=batch_size)
            
            results = []
            success_count = 0
            fail_count = 0
            
            for item_result in iter_processor.process(items, processor, filter_func, max_items):
                results.append(item_result)
                if item_result.get('success'):
                    success_count += 1
                else:
                    fail_count += 1
            
            return ActionResult(
                success=True,
                message=f"Processed {len(results)} items: {success_count} success, {fail_count} failed",
                data={
                    "total": len(results),
                    "successful": success_count,
                    "failed": fail_count,
                    "results": results[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Iterator error: {str(e)}")


class RangeAction(BaseAction):
    """Generate a numeric range."""
    action_type = "range"
    display_name = "数值范围"
    description = "生成数值序列"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate range.
        
        Args:
            context: Execution context.
            params: Dict with keys: start, stop, step, as_list.
        
        Returns:
            ActionResult with range values.
        """
        start = params.get('start', 0)
        stop = params.get('stop', 10)
        step = params.get('step', 1)
        as_list = params.get('as_list', True)
        
        try:
            range_iter = RangeIterator(start, stop, step)
            
            if as_list:
                result = range_iter.to_list()
            else:
                result = list(range(start, stop, step))
            
            return ActionResult(
                success=True,
                message=f"Generated range [{start}, {stop}, {step}) with {len(result)} values",
                data={"range": result, "count": len(result)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Range error: {str(e)}")


class ChunkAction(BaseAction):
    """Split items into chunks."""
    action_type = "chunk_items"
    display_name = "分块"
    description = "将数据分块"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Chunk items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, chunk_size.
        
        Returns:
            ActionResult with chunks.
        """
        items = params.get('items', [])
        chunk_size = params.get('chunk_size', 10)
        
        if not items:
            return ActionResult(success=False, message="items are required")
        
        if chunk_size <= 0:
            return ActionResult(success=False, message="chunk_size must be positive")
        
        try:
            chunk_iter = ChunkIterator(list(items), chunk_size)
            chunks = list(chunk_iter)
            
            return ActionResult(
                success=True,
                message=f"Split {len(items)} items into {len(chunks)} chunks",
                data={"chunks": chunks, "chunk_count": len(chunks), "chunk_size": chunk_size}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Chunk error: {str(e)}")


class EnumerateAction(BaseAction):
    """Enumerate items with index."""
    action_type = "enumerate_items"
    display_name = "枚举"
    description = "带索引枚举"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Enumerate items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, start.
        
        Returns:
            ActionResult with enumerated items.
        """
        items = params.get('items', [])
        start = params.get('start', 0)
        
        if items is None:
            return ActionResult(success=False, message="items are required")
        
        try:
            enum_iter = EnumerateIterator(items, start)
            enumerated = list(enum_iter)
            
            return ActionResult(
                success=True,
                message=f"Enumerated {len(enumerated)} items",
                data={"enumerated": enumerated, "count": len(enumerated)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enumerate error: {str(e)}")


class CycleAction(BaseAction):
    """Cycle through items indefinitely."""
    action_type = "cycle"
    display_name = "循环"
    description = "无限循环数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cycle items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, count.
        
        Returns:
            ActionResult with cycled items.
        """
        items = params.get('items', [])
        count = params.get('count', 10)
        
        if not items:
            return ActionResult(success=False, message="items are required")
        
        try:
            cycle_iter = CycleIterator(list(items))
            cycled = [next(cycle_iter) for _ in range(count)]
            
            return ActionResult(
                success=True,
                message=f"Cycled {count} items from {len(items)} source items",
                data={"items": cycled, "count": count, "source_count": len(items)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cycle error: {str(e)}")


class BatchIteratorAction(BaseAction):
    """Process items in batches."""
    action_type = "batch_iterator"
    display_name = "批量迭代"
    description = "批量处理数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Batch process items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, batch_size, processor_body.
        
        Returns:
            ActionResult with batch results.
        """
        items = params.get('items', [])
        batch_size = params.get('batch_size', 10)
        processor_body = params.get('processor_body', 'lambda batch: batch')
        
        if not items:
            return ActionResult(success=False, message="items are required")
        
        try:
            processor = eval(f"lambda batch: {processor_body}") if isinstance(processor_body, str) else processor_body
            
            iter_processor = IteratorProcessor(batch_size=batch_size)
            
            batches = list(iter_processor.batch_process(items, processor))
            
            all_success = all(b.get('success', False) for b in batches)
            
            return ActionResult(
                success=all_success,
                message=f"Processed {len(items)} items in {len(batches)} batches",
                data={
                    "batches": batches,
                    "batch_count": len(batches),
                    "total_items": len(items)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch iterator error: {str(e)}")
