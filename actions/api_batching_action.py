"""API Batching Action.

Batches API requests and responses for efficiency.
"""
from typing import Any, Callable, Dict, List, Optional, Generic, TypeVar
from dataclasses import dataclass, field
import time


T = TypeVar("T")


@dataclass
class BatchItem:
    item_id: str
    request: Any
    added_at: float
    callback: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchResult:
    batch_id: str
    items: List[Any]
    responses: List[Any]
    duration_ms: float
    errors: List[str]


class APIBatchingAction(Generic[T]):
    """Batches API requests for efficiency."""

    def __init__(
        self,
        batch_size: int = 100,
        flush_interval_sec: float = 1.0,
        batch_fn: Optional[Callable[[List[Any]], List[Any]]] = None,
    ) -> None:
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.batch_fn = batch_fn
        self.buffer: List[BatchItem] = []
        self._last_flush = time.time()

    def add(
        self,
        item_id: str,
        request: Any,
        callback: Optional[Callable[[Any], None]] = None,
        **metadata,
    ) -> None:
        item = BatchItem(
            item_id=item_id,
            request=request,
            added_at=time.time(),
            callback=callback,
            metadata=metadata,
        )
        self.buffer.append(item)

    def flush(self) -> Optional[BatchResult]:
        if not self.buffer:
            return None
        start = time.time()
        requests = [item.request for item in self.buffer]
        if self.batch_fn:
            responses = self.batch_fn(requests)
        else:
            responses = requests
        errors = []
        for item, response in zip(self.buffer, responses):
            if item.callback:
                try:
                    item.callback(response)
                except Exception as e:
                    errors.append(f"{item.item_id}: {str(e)}")
        result = BatchResult(
            batch_id=f"batch_{int(start*1000)}",
            items=requests,
            responses=responses,
            duration_ms=(time.time() - start) * 1000,
            errors=errors,
        )
        self.buffer.clear()
        self._last_flush = time.time()
        return result

    def is_flush_needed(self) -> bool:
        if len(self.buffer) >= self.batch_size:
            return True
        if time.time() - self._last_flush >= self.flush_interval_sec:
            return True
        return False

    def get_buffer_size(self) -> int:
        return len(self.buffer)
