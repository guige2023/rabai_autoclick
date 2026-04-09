"""
Bulk operation action for batch data processing.

Provides chunking, parallel processing, and progress tracking.
"""

from typing import Any, Callable, Optional
import time
import threading


class BulkOperationAction:
    """Bulk operation processor with chunking and parallelism."""

    def __init__(
        self,
        chunk_size: int = 100,
        max_parallel: int = 4,
        continue_on_error: bool = True,
    ) -> None:
        """
        Initialize bulk operation processor.

        Args:
            chunk_size: Items per chunk
            max_parallel: Maximum parallel workers
            continue_on_error: Continue processing on error
        """
        self.chunk_size = chunk_size
        self.max_parallel = max_parallel
        self.continue_on_error = continue_on_error
        self._active_operations: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute bulk operation.

        Args:
            params: Dictionary containing:
                - operation: 'process', 'status', 'cancel'
                - items: List of items to process
                - processor: Processing function
                - operation_id: Optional operation identifier

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "process")

        if operation == "process":
            return self._process_bulk(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "cancel":
            return self._cancel_operation(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _process_bulk(self, params: dict[str, Any]) -> dict[str, Any]:
        """Process items in bulk."""
        items = params.get("items", [])
        processor = params.get("processor")
        operation_id = params.get("operation_id", f"bulk_{int(time.time())}")

        if not items:
            return {"success": False, "error": "No items to process"}

        chunks = self._create_chunks(items)
        total_chunks = len(chunks)

        self._active_operations[operation_id] = {
            "total_items": len(items),
            "total_chunks": total_chunks,
            "processed_chunks": 0,
            "processed_items": 0,
            "failed_items": 0,
            "status": "running",
            "started_at": time.time(),
        }

        results = []
        failed = []

        for i, chunk in enumerate(chunks):
            chunk_result, chunk_failed = self._process_chunk(
                chunk, processor, operation_id
            )
            results.extend(chunk_result)
            failed.extend(chunk_failed)

            with self._lock:
                self._active_operations[operation_id]["processed_chunks"] = i + 1
                self._active_operations[operation_id]["processed_items"] += len(
                    chunk_result
                )
                self._active_operations[operation_id]["failed_items"] += len(
                    chunk_failed
                )

            if chunk_failed and not self.continue_on_error:
                break

        self._active_operations[operation_id]["status"] = "completed"
        self._active_operations[operation_id]["completed_at"] = time.time()

        return {
            "success": True,
            "operation_id": operation_id,
            "total_items": len(items),
            "processed": len(results),
            "failed": len(failed),
            "results": results,
            "errors": failed,
        }

    def _create_chunks(self, items: list[Any]) -> list[list[Any]]:
        """Split items into chunks."""
        return [
            items[i : i + self.chunk_size] for i in range(0, len(items), self.chunk_size)
        ]

    def _process_chunk(
        self, chunk: list[Any], processor: Any, operation_id: str
    ) -> tuple[list[Any], list[dict[str, Any]]]:
        """Process single chunk of items."""
        results = []
        failed = []

        for item in chunk:
            try:
                if callable(processor):
                    result = processor(item)
                else:
                    result = item
                results.append(result)
            except Exception as e:
                failed.append({"item": str(item), "error": str(e)})

        return results, failed

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get bulk operation status."""
        operation_id = params.get("operation_id", "")

        if operation_id and operation_id in self._active_operations:
            op = self._active_operations[operation_id]
            progress = (
                op["processed_items"] / op["total_items"] * 100
                if op["total_items"] > 0
                else 0
            )
            return {
                "success": True,
                "operation_id": operation_id,
                "status": op["status"],
                "total_items": op["total_items"],
                "processed_items": op["processed_items"],
                "failed_items": op["failed_items"],
                "progress_percent": round(progress, 2),
                "started_at": op.get("started_at"),
                "completed_at": op.get("completed_at"),
            }
        elif not operation_id:
            return {
                "success": True,
                "active_operations": list(self._active_operations.keys()),
            }
        return {"success": False, "error": "Operation not found"}

    def _cancel_operation(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cancel bulk operation."""
        operation_id = params.get("operation_id", "")

        if operation_id in self._active_operations:
            self._active_operations[operation_id]["status"] = "cancelled"
            self._active_operations[operation_id]["cancelled_at"] = time.time()
            return {"success": True, "operation_id": operation_id}
        return {"success": False, "error": "Operation not found"}

    def get_active_operations(self) -> list[str]:
        """Get list of active operation IDs."""
        return [
            op_id
            for op_id, op in self._active_operations.items()
            if op["status"] == "running"
        ]
