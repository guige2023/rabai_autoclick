"""Automation Batch Action Module.

Batches multiple automation actions into a single transaction with
atomic execution, rollback support, and partial failure handling.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class BatchItem:
    item_id: str
    action_type: str
    params: Dict[str, Any]
    order: int
    enabled: bool = True
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class BatchResult:
    item_id: str
    success: bool
    duration_ms: float
    output: Any
    error: Optional[str] = None
    retries: int = 0


@dataclass
class BatchConfig:
    atomic: bool = False
    stop_on_first_error: bool = False
    parallel: bool = False
    max_parallel_workers: int = 5
    rollback_on_failure: bool = False


class AutomationBatchAction:
    """Batches automation actions with transaction semantics."""

    def __init__(self, config: Optional[BatchConfig] = None) -> None:
        self._config = config or BatchConfig()
        self._items: List[BatchItem] = []
        self._results: List[BatchResult] = []
        self._rollback_stack: List[Tuple[str, Callable]] = []
        self._snapshots: Dict[str, Any] = {}

    def add_item(
        self,
        item_id: str,
        action_type: str,
        params: Dict[str, Any],
        order: int = 0,
        enabled: bool = True,
        max_retries: int = 3,
    ) -> None:
        item = BatchItem(
            item_id=item_id,
            action_type=action_type,
            params=params,
            order=order,
            enabled=enabled,
            max_retries=max_retries,
        )
        self._items.append(item)
        self._items.sort(key=lambda x: x.order)

    def remove_item(self, item_id: str) -> bool:
        for i, item in enumerate(self._items):
            if item.item_id == item_id:
                self._items.pop(i)
                return True
        return False

    def register_rollback(self, action_id: str, rollback_fn: Callable) -> None:
        self._rollback_stack.append((action_id, rollback_fn))

    def take_snapshot(self, key: str, data: Any) -> None:
        self._snapshots[key] = data

    def execute(
        self,
        executor: Callable[[str, Dict[str, Any]], Any],
    ) -> Tuple[bool, List[BatchResult]]:
        self._results.clear()
        success = True
        if self._config.parallel:
            return self._execute_parallel(executor)
        for item in self._items:
            if not item.enabled:
                continue
            result = self._execute_item(item, executor)
            self._results.append(result)
            if not result.success:
                if self._config.stop_on_first_error:
                    success = False
                    break
                if self._config.atomic:
                    success = False
                    self._rollback()
                    break
        return success, self._results

    def _execute_item(
        self,
        item: BatchItem,
        executor: Callable,
    ) -> BatchResult:
        start = time.time()
        for attempt in range(item.max_retries + 1):
            try:
                output = executor(item.action_type, item.params)
                return BatchResult(
                    item_id=item.item_id,
                    success=True,
                    duration_ms=(time.time() - start) * 1000,
                    output=output,
                    retries=attempt,
                )
            except Exception as e:
                if attempt < item.max_retries:
                    item.retry_count = attempt + 1
                    logger.debug(f"Retrying {item.item_id} (attempt {attempt + 1})")
                    continue
                return BatchResult(
                    item_id=item.item_id,
                    success=False,
                    duration_ms=(time.time() - start) * 1000,
                    output=None,
                    error=str(e),
                    retries=attempt,
                )
        return BatchResult(
            item_id=item.item_id,
            success=False,
            duration_ms=(time.time() - start) * 1000,
            output=None,
            error="Max retries exceeded",
        )

    def _execute_parallel(
        self,
        executor: Callable,
    ) -> Tuple[bool, List[BatchResult]]:
        import threading
        results_lock = threading.Lock()
        results: List[BatchResult] = []
        items = [item for item in self._items if item.enabled]
        total = len(items)
        completed = 0

        def worker(item: BatchItem):
            nonlocal completed
            result = self._execute_item(item, executor)
            with results_lock:
                results.append(result)
                completed += 1
            if not result.success and self._config.stop_on_first_error:
                pass

        threads = []
        for item in items:
            while threading.active_count() > self._config.max_parallel_workers:
                time.sleep(0.01)
            t = threading.Thread(target=worker, args=(item,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        self._results = sorted(results, key=lambda r: r.item_id)
        return all(r.success for r in self._results), self._results

    def _rollback(self) -> int:
        rolled_back = 0
        for action_id, rollback_fn in reversed(self._rollback_stack):
            try:
                rollback_fn()
                rolled_back += 1
                logger.info(f"Rolled back {action_id}")
            except Exception as e:
                logger.error(f"Rollback failed for {action_id}: {e}")
        return rolled_back

    def get_pending_count(self) -> int:
        return sum(1 for item in self._items if item.enabled)

    def get_results(self) -> List[BatchResult]:
        return list(self._results)

    def clear_items(self) -> None:
        self._items.clear()
        self._results.clear()
        self._rollback_stack.clear()

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._results)
        successful = sum(1 for r in self._results if r.success)
        failed = sum(1 for r in self._results if not r.success)
        return {
            "total_items": len(self._items),
            "enabled_items": self.get_pending_count(),
            "completed": total,
            "successful": successful,
            "failed": failed,
            "success_rate": successful / total if total > 0 else 0,
            "total_duration_ms": sum(r.duration_ms for r in self._results),
        }
