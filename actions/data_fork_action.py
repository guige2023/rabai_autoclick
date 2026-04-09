"""Data Fork Action Module.

Splits data streams into multiple parallel branches with configurable
routing rules, fan-out control, and result aggregation.
"""

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class ForkBranch:
    branch_id: str
    name: str
    filter_fn: Optional[Callable[[Any], bool]] = None
    transform_fn: Optional[Callable[[Any], Any]] = None
    processor_fn: Optional[Callable[[Any], Any]] = None
    max_buffer: int = 1000
    _buffer: List[Any] = field(default_factory=list)


@dataclass
class ForkConfig:
    max_concurrent: int = 10
    timeout_per_branch: float = 30.0
    fail_fast: bool = False
    aggregate_results: bool = True
    fanout_mode: str = "all"


@dataclass
class ForkResult:
    branch_id: str
    success: bool
    items_processed: int
    duration_ms: float
    error: Optional[str] = None


class DataForkAction:
    """Splits data into multiple parallel processing branches."""

    def __init__(self, config: Optional[ForkConfig] = None) -> None:
        self._config = config or ForkConfig()
        self._branches: Dict[str, ForkBranch] = {}
        self._results: Dict[str, List[Any]] = defaultdict(list)
        self._lock = threading.RLock()
        self._stats = {"total_forked": 0, "total_aggregated": 0}

    def add_branch(
        self,
        branch_id: str,
        name: str,
        filter_fn: Optional[Callable[[Any], bool]] = None,
        transform_fn: Optional[Callable[[Any], Any]] = None,
        processor_fn: Optional[Callable[[Any], Any]] = None,
        max_buffer: int = 1000,
    ) -> None:
        branch = ForkBranch(
            branch_id=branch_id,
            name=name,
            filter_fn=filter_fn,
            transform_fn=transform_fn,
            processor_fn=processor_fn,
            max_buffer=max_buffer,
        )
        self._branches[branch_id] = branch

    def remove_branch(self, branch_id: str) -> bool:
        return self._branches.pop(branch_id, None) is not None

    def fork(
        self,
        data: List[Any],
        wait_for_results: bool = True,
    ) -> Dict[str, List[Any]]:
        self._results.clear()
        threads = []
        for branch in self._branches.values():
            t = threading.Thread(target=self._process_branch, args=(branch, data))
            t.start()
            threads.append(t)
            if len(threads) >= self._config.max_concurrent:
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.join()
        self._stats["total_forked"] += len(data)
        if self._config.aggregate_results:
            self._stats["total_aggregated"] += sum(
                len(r) for r in self._results.values()
            )
        return dict(self._results)

    def _process_branch(self, branch: ForkBranch, data: List[Any]) -> None:
        processed = 0
        for item in data:
            if branch.filter_fn and not branch.filter_fn(item):
                continue
            try:
                if branch.transform_fn:
                    item = branch.transform_fn(item)
                if branch.processor_fn:
                    result = branch.processor_fn(item)
                    if result is not None:
                        with self._lock:
                            if len(branch._buffer) < branch.max_buffer:
                                branch._buffer.append(result)
                                self._results[branch.branch_id].append(result)
                else:
                    with self._lock:
                        if len(branch._buffer) < branch.max_buffer:
                            branch._buffer.append(item)
                            self._results[branch.branch_id].append(item)
                processed += 1
            except Exception as e:
                logger.error(f"Branch {branch.branch_id} error: {e}")
                if self._config.fail_fast:
                    break
        logger.debug(f"Branch {branch.branch_id} processed {processed} items")

    def get_branch_results(self, branch_id: str) -> List[Any]:
        return list(self._results.get(branch_id, []))

    def get_all_results(self) -> Dict[str, List[Any]]:
        return dict(self._results)

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "branch_count": len(self._branches),
            "branches": {
                bid: {
                    "buffer_size": len(br._buffer),
                    "max_buffer": br.max_buffer,
                }
                for bid, br in self._branches.items()
            },
        }

    def clear_results(self) -> None:
        self._results.clear()
        for branch in self._branches.values():
            branch._buffer.clear()

    def list_branches(self) -> List[Dict[str, Any]]:
        return [
            {
                "branch_id": b.branch_id,
                "name": b.name,
                "has_filter": b.filter_fn is not None,
                "has_transform": b.transform_fn is not None,
                "has_processor": b.processor_fn is not None,
                "buffer_size": len(b._buffer),
                "max_buffer": b.max_buffer,
            }
            for b in self._branches.values()
        ]
