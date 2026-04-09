"""Input batch utilities for UI automation.

Provides utilities for batching input operations,
optimizing input sequences, and managing batch execution.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class BatchItem:
    """An item in an input batch."""
    item_type: str
    data: Dict[str, Any]
    timestamp_ms: float
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchResult:
    """Result of batch execution."""
    success: bool
    items_processed: int
    items_failed: int
    duration_ms: float
    errors: List[str] = field(default_factory=list)


class InputBatcher:
    """Batches input operations for optimized execution.
    
    Collects multiple input operations and executes them
    together for improved efficiency.
    """
    
    def __init__(
        self,
        max_batch_size: int = 50,
        max_wait_ms: float = 100.0,
        executor: Optional[Callable[[List[BatchItem]], BatchResult]] = None
    ) -> None:
        """Initialize the input batcher.
        
        Args:
            max_batch_size: Maximum items per batch.
            max_wait_ms: Maximum wait time before forcing batch.
            executor: Function to execute batch.
        """
        self.max_batch_size = max_batch_size
        self.max_wait_ms = max_wait_ms
        self.executor = executor
        self._batch: Deque[BatchItem] = deque()
        self._last_execution_ms: float = 0.0
    
    def add(
        self,
        item_type: str,
        data: Dict[str, Any],
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add an item to the batch.
        
        Args:
            item_type: Type of input item.
            data: Item data.
            priority: Priority for ordering.
            metadata: Additional metadata.
        """
        item = BatchItem(
            item_type=item_type,
            data=data,
            timestamp_ms=time.time() * 1000,
            priority=priority,
            metadata=metadata or {}
        )
        self._batch.append(item)
    
    def should_execute(self) -> bool:
        """Check if batch should be executed.
        
        Returns:
            True if batch should execute.
        """
        if len(self._batch) >= self.max_batch_size:
            return True
        
        if len(self._batch) == 0:
            return False
        
        elapsed = time.time() * 1000 - self._last_execution_ms
        if elapsed >= self.max_wait_ms:
            return True
        
        return False
    
    def execute(self) -> BatchResult:
        """Execute the current batch.
        
        Returns:
            Batch result.
        """
        if not self._batch:
            return BatchResult(
                success=True,
                items_processed=0,
                items_failed=0,
                duration_ms=0.0
            )
        
        start_ms = time.time() * 1000
        items = list(self._batch)
        self._batch.clear()
        
        items.sort(key=lambda x: -x.priority)
        
        if self.executor:
            result = self.executor(items)
        else:
            result = BatchResult(
                success=True,
                items_processed=len(items),
                items_failed=0,
                duration_ms=time.time() * 1000 - start_ms
            )
        
        self._last_execution_ms = time.time() * 1000
        return result
    
    def get_pending_count(self) -> int:
        """Get number of pending items.
        
        Returns:
            Number of pending items.
        """
        return len(self._batch)
    
    def clear(self) -> None:
        """Clear pending items."""
        self._batch.clear()


class BatchOptimizer:
    """Optimizes input batches for efficient execution.
    
    Analyzes batch items and reorders or consolidates
    them for better performance.
    """
    
    def __init__(self) -> None:
        """Initialize the batch optimizer."""
        self._consolidation_rules: Dict[str, Callable[[BatchItem, BatchItem], bool]] = {}
    
    def register_consolidation_rule(
        self,
        item_type: str,
        rule: Callable[[BatchItem, BatchItem], bool]
    ) -> None:
        """Register a consolidation rule for item type.
        
        Args:
            item_type: Type of items to consolidate.
            rule: Function that returns True if items can be consolidated.
        """
        self._consolidation_rules[item_type] = rule
    
    def optimize(
        self,
        items: List[BatchItem]
    ) -> List[BatchItem]:
        """Optimize a list of batch items.
        
        Args:
            items: Items to optimize.
            
        Returns:
            Optimized list of items.
        """
        consolidated = self._consolidate(items)
        reordered = self._reorder(consolidated)
        return reordered
    
    def _consolidate(
        self,
        items: List[BatchItem]
    ) -> List[BatchItem]:
        """Consolidate similar items.
        
        Args:
            items: Items to consolidate.
            
        Returns:
            Consolidated items.
        """
        result: List[BatchItem] = []
        
        for item in items:
            consolidated = False
            
            for existing in result:
                rule = self._consolidation_rules.get(item.item_type)
                if rule and rule(item, existing):
                    self._merge_items(existing, item)
                    consolidated = True
                    break
            
            if not consolidated:
                result.append(item)
        
        return result
    
    def _merge_items(self, target: BatchItem, source: BatchItem) -> None:
        """Merge source item into target.
        
        Args:
            target: Target item.
            source: Source item.
        """
        if "count" not in target.metadata:
            target.metadata["count"] = 1
        target.metadata["count"] += 1
        
        target.data.setdefault("merged", []).append(source.data)
    
    def _reorder(self, items: List[BatchItem]) -> List[BatchItem]:
        """Reorder items for optimal execution.
        
        Args:
            items: Items to reorder.
            
        Returns:
            Reordered items.
        """
        return sorted(items, key=lambda x: (-x.priority, x.timestamp_ms))


class SequenceBatcher:
    """Batches sequential input operations.
    
    Maintains operation sequences and executes them
    in order with proper timing.
    """
    
    def __init__(
        self,
        inter_item_delay_ms: float = 10.0,
        on_item: Optional[Callable[[BatchItem], None]] = None
    ) -> None:
        """Initialize the sequence batcher.
        
        Args:
            inter_item_delay_ms: Delay between items.
            on_item: Callback for each item execution.
        """
        self.inter_item_delay_ms = inter_item_delay_ms
        self.on_item = on_item
        self._sequence: List[BatchItem] = []
    
    def add_sequence_item(
        self,
        item_type: str,
        data: Dict[str, Any],
        delay_ms: Optional[float] = None
    ) -> None:
        """Add an item to the sequence.
        
        Args:
            item_type: Type of item.
            data: Item data.
            delay_ms: Delay after this item.
        """
        item = BatchItem(
            item_type=item_type,
            data=data,
            timestamp_ms=time.time() * 1000,
            metadata={"delay_ms": delay_ms or self.inter_item_delay_ms}
        )
        self._sequence.append(item)
    
    def execute_sequence(self) -> BatchResult:
        """Execute the sequence.
        
        Returns:
            Batch result.
        """
        start_ms = time.time() * 1000
        errors = []
        processed = 0
        failed = 0
        
        for item in self._sequence:
            try:
                if self.on_item:
                    self.on_item(item)
                processed += 1
                
                delay = item.metadata.get("delay_ms", self.inter_item_delay_ms)
                if delay > 0:
                    time.sleep(delay / 1000.0)
            except Exception as e:
                errors.append(str(e))
                failed += 1
        
        self._sequence.clear()
        
        return BatchResult(
            success=failed == 0,
            items_processed=processed,
            items_failed=failed,
            duration_ms=time.time() * 1000 - start_ms,
            errors=errors
        )
    
    def get_sequence_length(self) -> int:
        """Get number of items in sequence.
        
        Returns:
            Sequence length.
        """
        return len(self._sequence)
    
    def clear_sequence(self) -> None:
        """Clear the sequence."""
        self._sequence.clear()


class ParallelBatcher:
    """Batches parallel input operations.
    
    Executes independent operations concurrently
    for improved throughput.
    """
    
    def __init__(
        self,
        max_parallel: int = 5,
        executor: Optional[Callable[[BatchItem], Any]] = None
    ) -> None:
        """Initialize the parallel batcher.
        
        Args:
            max_parallel: Maximum parallel operations.
            executor: Function to execute single item.
        """
        self.max_parallel = max_parallel
        self.executor = executor
        self._pending: List[BatchItem] = []
    
    def add(self, item: BatchItem) -> None:
        """Add an item to the batch.
        
        Args:
            item: Item to add.
        """
        self._pending.append(item)
    
    def execute_parallel(self) -> Tuple[int, int, List[str]]:
        """Execute items in parallel.
        
        Returns:
            Tuple of (processed, failed, errors).
        """
        processed = 0
        failed = 0
        errors = []
        
        for item in self._pending:
            try:
                if self.executor:
                    self.executor(item)
                processed += 1
            except Exception as e:
                errors.append(str(e))
                failed += 1
        
        self._pending.clear()
        
        return (processed, failed, errors)
    
    def clear(self) -> None:
        """Clear pending items."""
        self._pending.clear()


def batch_by_type(
    items: List[BatchItem]
) -> Dict[str, List[BatchItem]]:
    """Group batch items by type.
    
    Args:
        items: Items to group.
        
    Returns:
        Dictionary mapping type to items.
    """
    groups: Dict[str, List[BatchItem]] = {}
    
    for item in items:
        if item.item_type not in groups:
            groups[item.item_type] = []
        groups[item.item_type].append(item)
    
    return groups


def prioritize_items(
    items: List[BatchItem],
    priority_func: Callable[[BatchItem], int]
) -> List[BatchItem]:
    """Prioritize items using a priority function.
    
    Args:
        items: Items to prioritize.
        priority_func: Function that returns priority.
        
    Returns:
        Prioritized items.
    """
    return sorted(items, key=priority_func, reverse=True)
