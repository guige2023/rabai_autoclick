"""Bulk operation action module for RabAI AutoClick.

Provides bulk operations:
- BulkProcessor: Process items in bulk
- BatchExecutor: Execute operations in batches
- BulkImport: Import data in bulk
- BulkExport: Export data in bulk
- BulkUpdate: Update records in bulk
- BulkDelete: Delete records in bulk
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class BulkResult:
    """Result of bulk operation."""
    total: int
    successful: int
    failed: int
    duration: float
    errors: List[Dict[str, Any]]


@dataclass
class BatchResult:
    """Result of batch operation."""
    batch_index: int
    size: int
    successful: int
    failed: int
    duration: float
    errors: List[Dict[str, Any]]


class BulkProcessor:
    """Process items in bulk."""

    def __init__(
        self,
        batch_size: int = 100,
        max_workers: int = 4,
        stop_on_error: bool = False,
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.stop_on_error = stop_on_error
        self._results: List[Any] = []

    def process(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        on_error: Optional[Callable[[Exception, Any], None]] = None,
    ) -> BulkResult:
        """Process items in bulk."""
        start_time = time.time()
        total = len(items)
        successful = 0
        failed = 0
        errors = []

        batches = self._create_batches(items)

        for batch_index, batch in enumerate(batches):
            batch_start = time.time()
            batch_successful = 0
            batch_failed = 0
            batch_errors = []

            for item in batch:
                try:
                    result = processor(item)
                    self._results.append(result)
                    successful += 1
                    batch_successful += 1
                except Exception as e:
                    failed += 1
                    batch_failed += 1
                    error_info = {
                        "item": str(item)[:100],
                        "error": str(e),
                        "batch_index": batch_index,
                    }
                    errors.append(error_info)
                    batch_errors.append(error_info)

                    if on_error:
                        on_error(e, item)

                    if self.stop_on_error:
                        break

            batch_duration = time.time() - batch_start

            if self.max_workers > 1 and batch_index < len(batches) - 1:
                continue

        duration = time.time() - start_time
        return BulkResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            errors=errors,
        )

    def process_parallel(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        on_error: Optional[Callable[[Exception, Any], None]] = None,
    ) -> BulkResult:
        """Process items in parallel."""
        start_time = time.time()
        total = len(items)
        successful = 0
        failed = 0
        errors = []
        results_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {executor.submit(processor, item): item for item in items}

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    with results_lock:
                        self._results.append(result)
                        successful += 1
                except Exception as e:
                    failed += 1
                    error_info = {"item": str(item)[:100], "error": str(e)}
                    errors.append(error_info)

                    if on_error:
                        on_error(e, item)

        duration = time.time() - start_time
        return BulkResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            errors=errors,
        )

    def _create_batches(self, items: List[Any]) -> List[List[Any]]:
        """Create batches from items."""
        return [items[i:i+self.batch_size] for i in range(0, len(items), self.batch_size)]

    def get_results(self) -> List[Any]:
        """Get processing results."""
        return self._results


class BatchExecutor:
    """Execute operations in batches."""

    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size

    def execute(
        self,
        items: List[Any],
        executor: Callable[[List[Any]], List[Any]],
        on_batch_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> Tuple[List[Any], List[BatchResult]]:
        """Execute items in batches."""
        batches = self._create_batches(items)
        all_results = []
        batch_results = []

        for i, batch in enumerate(batches):
            start_time = time.time()

            try:
                batch_results_list = executor(batch)
                all_results.extend(batch_results_list)
                duration = time.time() - start_time

                batch_result = BatchResult(
                    batch_index=i,
                    size=len(batch),
                    successful=len(batch_results_list),
                    failed=0,
                    duration=duration,
                    errors=[],
                )
            except Exception as e:
                duration = time.time() - start_time
                batch_result = BatchResult(
                    batch_index=i,
                    size=len(batch),
                    successful=0,
                    failed=len(batch),
                    duration=duration,
                    errors=[{"error": str(e)}],
                )

            batch_results.append(batch_result)

            if on_batch_complete:
                on_batch_complete(batch_result)

        return all_results, batch_results

    def _create_batches(self, items: List[Any]) -> List[List[Any]]:
        """Create batches from items."""
        return [items[i:i+self.batch_size] for i in range(0, len(items), self.batch_size)]


class BulkImportExport:
    """Bulk import and export operations."""

    @staticmethod
    def bulk_import(
        source: List[Any],
        importer: Callable[[Any], bool],
        batch_size: int = 100,
    ) -> BulkResult:
        """Import data in bulk."""
        start_time = time.time()
        total = len(source)
        successful = 0
        failed = 0
        errors = []

        for i in range(0, total, batch_size):
            batch = source[i:i+batch_size]
            for item in batch:
                try:
                    if importer(item):
                        successful += 1
                    else:
                        failed += 1
                        errors.append({"item": str(item)[:100], "error": "Import returned False"})
                except Exception as e:
                    failed += 1
                    errors.append({"item": str(item)[:100], "error": str(e)})

        duration = time.time() - start_time
        return BulkResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            errors=errors,
        )

    @staticmethod
    def bulk_export(
        items: List[Any],
        exporter: Callable[[Any], bool],
        batch_size: int = 100,
    ) -> BulkResult:
        """Export data in bulk."""
        start_time = time.time()
        total = len(items)
        successful = 0
        failed = 0
        errors = []

        for i in range(0, total, batch_size):
            batch = items[i:i+batch_size]
            for item in batch:
                try:
                    if exporter(item):
                        successful += 1
                    else:
                        failed += 1
                        errors.append({"item": str(item)[:100], "error": "Export returned False"})
                except Exception as e:
                    failed += 1
                    errors.append({"item": str(item)[:100], "error": str(e)})

        duration = time.time() - start_time
        return BulkResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            errors=errors,
        )


class BulkUpdateDelete:
    """Bulk update and delete operations."""

    @staticmethod
    def bulk_update(
        items: List[Dict],
        updater: Callable[[Dict], bool],
        key_field: str = "id",
        batch_size: int = 100,
    ) -> BulkResult:
        """Update records in bulk."""
        start_time = time.time()
        total = len(items)
        successful = 0
        failed = 0
        errors = []

        for i in range(0, total, batch_size):
            batch = items[i:i+batch_size]
            for item in batch:
                try:
                    if updater(item):
                        successful += 1
                    else:
                        failed += 1
                        errors.append({
                            "key": item.get(key_field),
                            "error": "Update returned False",
                        })
                except Exception as e:
                    failed += 1
                    errors.append({
                        "key": item.get(key_field),
                        "error": str(e),
                    })

        duration = time.time() - start_time
        return BulkResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            errors=errors,
        )

    @staticmethod
    def bulk_delete(
        item_ids: List[Any],
        deleter: Callable[[Any], bool],
        batch_size: int = 100,
    ) -> BulkResult:
        """Delete records in bulk."""
        start_time = time.time()
        total = len(item_ids)
        successful = 0
        failed = 0
        errors = []

        for i in range(0, total, batch_size):
            batch = item_ids[i:i+batch_size]
            for item_id in batch:
                try:
                    if deleter(item_id):
                        successful += 1
                    else:
                        failed += 1
                        errors.append({
                            "id": str(item_id),
                            "error": "Delete returned False",
                        })
                except Exception as e:
                    failed += 1
                    errors.append({
                        "id": str(item_id),
                        "error": str(e),
                    })

        duration = time.time() - start_time
        return BulkResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            errors=errors,
        )


class BulkOperationAction(BaseAction):
    """Bulk operation action."""
    action_type = "bulk_operation"
    display_name = "批量操作"
    description = "批量数据处理操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "process")
            items = params.get("items", [])

            if operation == "process":
                return self._process_bulk(items, params)
            elif operation == "process_parallel":
                return self._process_parallel(items, params)
            elif operation == "batch":
                return self._batch_execute(items, params)
            elif operation == "import":
                return self._bulk_import(items, params)
            elif operation == "export":
                return self._bulk_export(items, params)
            elif operation == "update":
                return self._bulk_update(items, params)
            elif operation == "delete":
                return self._bulk_delete(items, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Bulk operation error: {str(e)}")

    def _process_bulk(self, items: List[Any], params: Dict) -> ActionResult:
        """Process items in bulk."""
        processor_func = params.get("processor")
        if not processor_func:
            return ActionResult(success=False, message="processor is required")

        batch_size = params.get("batch_size", 100)
        max_workers = params.get("max_workers", 1)
        stop_on_error = params.get("stop_on_error", False)

        bulk_proc = BulkProcessor(
            batch_size=batch_size,
            max_workers=max_workers,
            stop_on_error=stop_on_error,
        )

        result = bulk_proc.process(items, processor_func)

        return ActionResult(
            success=result.failed == 0,
            message=f"Bulk process: {result.successful}/{result.total} successful in {result.duration:.2f}s",
            data={
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration": result.duration,
                "errors": result.errors[:10],
            },
        )

    def _process_parallel(self, items: List[Any], params: Dict) -> ActionResult:
        """Process items in parallel."""
        processor_func = params.get("processor")
        if not processor_func:
            return ActionResult(success=False, message="processor is required")

        max_workers = params.get("max_workers", 4)

        bulk_proc = BulkProcessor(max_workers=max_workers)
        result = bulk_proc.process_parallel(items, processor_func)

        return ActionResult(
            success=result.failed == 0,
            message=f"Parallel process: {result.successful}/{result.total} successful",
            data={
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration": result.duration,
            },
        )

    def _batch_execute(self, items: List[Any], params: Dict) -> ActionResult:
        """Execute in batches."""
        executor_func = params.get("executor")
        if not executor_func:
            return ActionResult(success=False, message="executor is required")

        batch_size = params.get("batch_size", 50)
        batch_exec = BatchExecutor(batch_size=batch_size)

        results, batch_results = batch_exec.execute(items, executor_func)

        return ActionResult(
            success=True,
            message=f"Batch execute: {len(batch_results)} batches",
            data={
                "total_items": len(items),
                "batches": len(batch_results),
                "results": results,
            },
        )

    def _bulk_import(self, items: List[Any], params: Dict) -> ActionResult:
        """Bulk import."""
        importer_func = params.get("importer")
        if not importer_func:
            return ActionResult(success=False, message="importer is required")

        batch_size = params.get("batch_size", 100)
        result = BulkImportExport.bulk_import(items, importer_func, batch_size)

        return ActionResult(
            success=result.failed == 0,
            message=f"Import: {result.successful}/{result.total} successful",
            data={"result": result},
        )

    def _bulk_export(self, items: List[Any], params: Dict) -> ActionResult:
        """Bulk export."""
        exporter_func = params.get("exporter")
        if not exporter_func:
            return ActionResult(success=False, message="exporter is required")

        batch_size = params.get("batch_size", 100)
        result = BulkImportExport.bulk_export(items, exporter_func, batch_size)

        return ActionResult(
            success=result.failed == 0,
            message=f"Export: {result.successful}/{result.total} successful",
            data={"result": result},
        )

    def _bulk_update(self, items: List[Dict], params: Dict) -> ActionResult:
        """Bulk update."""
        updater_func = params.get("updater")
        if not updater_func:
            return ActionResult(success=False, message="updater is required")

        key_field = params.get("key_field", "id")
        batch_size = params.get("batch_size", 100)
        result = BulkUpdateDelete.bulk_update(items, updater_func, key_field, batch_size)

        return ActionResult(
            success=result.failed == 0,
            message=f"Update: {result.successful}/{result.total} successful",
            data={"result": result},
        )

    def _bulk_delete(self, items: List[Any], params: Dict) -> ActionResult:
        """Bulk delete."""
        deleter_func = params.get("deleter")
        if not deleter_func:
            return ActionResult(success=False, message="deleter is required")

        batch_size = params.get("batch_size", 100)
        result = BulkUpdateDelete.bulk_delete(items, deleter_func, batch_size)

        return ActionResult(
            success=result.failed == 0,
            message=f"Delete: {result.successful}/{result.total} successful",
            data={"result": result},
        )
