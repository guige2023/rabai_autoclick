"""API Bulk Action Module.

Provides bulk API operation support including batch uploads,
bulk updates, and bulk delete operations with progress tracking.

Example:
    >>> from actions.api.api_bulk_action import APIBulkAction
    >>> action = APIBulkAction()
    >>> results = await action.bulk_create(records)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
import threading
import uuid


class BulkOperation(Enum):
    """Bulk operation types."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    UPSERT = "upsert"


class BulkStatus(Enum):
    """Status of bulk operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class BulkItem:
    """Single item in bulk operation.
    
    Attributes:
        item_id: Unique item identifier
        operation: Operation type
        data: Item data
        status: Current status
        result: Result if completed
        error: Error message if failed
    """
    item_id: str
    operation: BulkOperation
    data: Dict[str, Any]
    status: str = "pending"
    result: Any = None
    error: Optional[str] = None


@dataclass
class BulkConfig:
    """Configuration for bulk operations.
    
    Attributes:
        batch_size: Items per batch
        max_concurrent: Max concurrent batches
        retry_failed: Retry failed items
        retry_count: Number of retries
        continue_on_error: Continue on individual errors
    """
    batch_size: int = 50
    max_concurrent: int = 3
    retry_failed: bool = True
    retry_count: int = 3
    continue_on_error: bool = True


@dataclass
class BulkResult:
    """Result of bulk operation.
    
    Attributes:
        job_id: Unique job identifier
        status: Overall status
        total: Total items
        succeeded: Succeeded count
        failed: Failed count
        results: Individual results
        duration: Operation duration
    """
    job_id: str
    status: BulkStatus
    total: int
    succeeded: int
    failed: int
    results: List[BulkItem] = field(default_factory=list)
    duration: float = 0.0
    errors: List[str] = field(default_factory=list)


class APIBulkAction:
    """Bulk operation handler for API requests.
    
    Handles bulk create, update, delete operations with
    batching, concurrency, and error handling.
    
    Attributes:
        config: Bulk operation configuration
        _active_jobs: Active bulk jobs
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[BulkConfig] = None,
    ) -> None:
        """Initialize bulk action.
        
        Args:
            config: Bulk operation configuration
        """
        self.config = config or BulkConfig()
        self._active_jobs: Dict[str, BulkResult] = {}
        self._lock = threading.RLock()
    
    async def bulk_create(
        self,
        items: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> BulkResult:
        """Bulk create items.
        
        Args:
            items: List of items to create
            progress_callback: Optional progress callback
        
        Returns:
            BulkResult with operation details
        """
        bulk_items = [
            BulkItem(
                item_id=str(uuid.uuid4()),
                operation=BulkOperation.CREATE,
                data=item,
            )
            for item in items
        ]
        
        return await self._execute_bulk(bulk_items, progress_callback)
    
    async def bulk_update(
        self,
        items: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> BulkResult:
        """Bulk update items.
        
        Args:
            items: List of items to update
            progress_callback: Optional progress callback
        
        Returns:
            BulkResult with operation details
        """
        bulk_items = [
            BulkItem(
                item_id=item.get("id", str(uuid.uuid4())),
                operation=BulkOperation.UPDATE,
                data=item,
            )
            for item in items
        ]
        
        return await self._execute_bulk(bulk_items, progress_callback)
    
    async def bulk_delete(
        self,
        item_ids: List[str],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> BulkResult:
        """Bulk delete items.
        
        Args:
            item_ids: List of item IDs to delete
            progress_callback: Optional progress callback
        
        Returns:
            BulkResult with operation details
        """
        bulk_items = [
            BulkItem(
                item_id=item_id,
                operation=BulkOperation.DELETE,
                data={"id": item_id},
            )
            for item_id in item_ids
        ]
        
        return await self._execute_bulk(bulk_items, progress_callback)
    
    async def _execute_bulk(
        self,
        items: List[BulkItem],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> BulkResult:
        """Execute bulk operation.
        
        Args:
            items: List of bulk items
            progress_callback: Optional progress callback
        
        Returns:
            BulkResult with operation details
        """
        import time
        start_time = time.time()
        
        job_id = str(uuid.uuid4())
        
        result = BulkResult(
            job_id=job_id,
            status=BulkStatus.IN_PROGRESS,
            total=len(items),
            succeeded=0,
            failed=0,
        )
        
        with self._lock:
            self._active_jobs[job_id] = result
        
        batches = self._create_batches(items)
        
        for batch_idx, batch in enumerate(batches):
            batch_results = await self._process_batch(batch)
            
            for item, batch_result in zip(batch, batch_results):
                if batch_result["success"]:
                    item.status = "completed"
                    item.result = batch_result["data"]
                    result.succeeded += 1
                else:
                    item.status = "failed"
                    item.error = batch_result["error"]
                    result.failed += 1
                    result.errors.append(f"{item.item_id}: {batch_result['error']}")
                
                result.results.append(item)
            
            if progress_callback:
                completed = sum(
                    1 for r in result.results
                    if r.status == "completed" or r.status == "failed"
                )
                progress_callback(completed, len(items))
        
        if result.failed == 0:
            result.status = BulkStatus.COMPLETED
        elif result.succeeded == 0:
            result.status = BulkStatus.FAILED
        else:
            result.status = BulkStatus.PARTIAL
        
        result.duration = time.time() - start_time
        
        with self._lock:
            if job_id in self._active_jobs:
                del self._active_jobs[job_id]
        
        return result
    
    def _create_batches(self, items: List[BulkItem]) -> List[List[BulkItem]]:
        """Create batches from items.
        
        Args:
            items: List of items
        
        Returns:
            List of batches
        """
        batches = []
        for i in range(0, len(items), self.config.batch_size):
            batches.append(items[i:i + self.config.batch_size])
        return batches
    
    async def _process_batch(
        self,
        batch: List[BulkItem],
    ) -> List[Dict[str, Any]]:
        """Process a batch of items.
        
        Args:
            batch: List of items in batch
        
        Returns:
            List of results
        """
        tasks = [self._process_single(item) for item in batch]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_single(self, item: BulkItem) -> Dict[str, Any]:
        """Process a single bulk item.
        
        Args:
            item: Bulk item to process
        
        Returns:
            Result dictionary
        """
        await asyncio.sleep(0.01)
        
        return {
            "success": True,
            "data": {"id": item.item_id, "created": True},
        }
    
    async def get_job_status(self, job_id: str) -> Optional[BulkResult]:
        """Get status of bulk job.
        
        Args:
            job_id: Job identifier
        
        Returns:
            BulkResult if found
        """
        with self._lock:
            return self._active_jobs.get(job_id)
