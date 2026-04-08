"""Batch Operation Action Module.

Provides batch processing with chunking
and error handling.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BatchResult:
    """Batch processing result."""
    batch_id: str
    total: int
    successful: int
    failed: int
    errors: List[Dict] = field(default_factory=list)


class BatchOperationManager:
    """Manages batch operations."""

    def __init__(self):
        self._batches: Dict[str, BatchResult] = {}

    def process_batch(
        self,
        items: List[Any],
        processor: Callable,
        chunk_size: int = 100
    ) -> BatchResult:
        """Process items in batches."""
        batch_id = f"batch_{int(time.time() * 1000)}"

        successful = 0
        failed = 0
        errors = []

        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]

            for j, item in enumerate(chunk):
                try:
                    processor(item)
                    successful += 1
                except Exception as e:
                    failed += 1
                    errors.append({
                        "index": i + j,
                        "item": str(item)[:100],
                        "error": str(e)
                    })

        result = BatchResult(
            batch_id=batch_id,
            total=len(items),
            successful=successful,
            failed=failed,
            errors=errors
        )

        self._batches[batch_id] = result
        return result

    def get_result(self, batch_id: str) -> Optional[BatchResult]:
        """Get batch result."""
        return self._batches.get(batch_id)


class BatchOperationAction(BaseAction):
    """Action for batch operations."""

    def __init__(self):
        super().__init__("batch_operation")
        self._manager = BatchOperationManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute batch operation."""
        try:
            operation = params.get("operation", "process")

            if operation == "process":
                return self._process(params)
            elif operation == "get":
                return self._get(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _process(self, params: Dict) -> ActionResult:
        """Process batch."""
        def default_processor(item):
            return item

        result = self._manager.process_batch(
            items=params.get("items", []),
            processor=params.get("processor") or default_processor,
            chunk_size=params.get("chunk_size", 100)
        )

        return ActionResult(success=True, data={
            "batch_id": result.batch_id,
            "total": result.total,
            "successful": result.successful,
            "failed": result.failed,
            "error_count": len(result.errors)
        })

    def _get(self, params: Dict) -> ActionResult:
        """Get batch result."""
        result = self._manager.get_result(params.get("batch_id", ""))
        if not result:
            return ActionResult(success=False, message="Batch not found")

        return ActionResult(success=True, data={
            "batch_id": result.batch_id,
            "total": result.total,
            "successful": result.successful,
            "failed": result.failed,
            "errors": result.errors[:10]
        })
