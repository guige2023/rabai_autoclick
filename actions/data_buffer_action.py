"""Data Buffer Action Module.

Implements a configurable in-memory data buffer with flush policies,
batch sizing, watermark controls, and backpressure signaling.
"""

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class FlushPolicy:
    max_size: int = 1000
    max_age_sec: float = 60.0
    max_bytes: int = 10 * 1024 * 1024
    min_items: int = 1


@dataclass
class BufferStats:
    items_received: int = 0
    items_flushed: int = 0
    flush_count: int = 0
    bytes_received: int = 0
    bytes_flushed: int = 0
    last_flush: Optional[float] = None
    backpressure_events: int = 0


class DataBufferAction:
    """Thread-safe data buffer with configurable flush policies."""

    def __init__(
        self,
        name: str = "default",
        flush_policy: Optional[FlushPolicy] = None,
        flush_callback: Optional[Callable[[List[Any]], None]] = None,
    ) -> None:
        self.name = name
        self._policy = flush_policy or FlushPolicy()
        self._flush_callback = flush_callback
        self._buffer: deque = deque(maxlen=self._policy.max_size)
        self._lock = threading.RLock()
        self._stats = BufferStats()
        self._closed = False

    def put(self, item: Any, size_bytes: int = 0) -> bool:
        if self._closed:
            return False
        with self._lock:
            if len(self._buffer) >= self._policy.max_size:
                self._stats.backpressure_events += 1
                logger.warning(f"Buffer {self.name} full, backpressure triggered")
                return False
            self._buffer.append(item)
            self._stats.items_received += 1
            self._stats.bytes_received += size_bytes or self._estimate_size(item)
            if self._should_flush():
                self._flush()
        return True

    def put_batch(self, items: List[Any]) -> int:
        if self._closed:
            return 0
        accepted = 0
        with self._lock:
            for item in items:
                if len(self._buffer) < self._policy.max_size:
                    self._buffer.append(item)
                    self._stats.items_received += 1
                    accepted += 1
                else:
                    self._stats.backpressure_events += 1
                    break
            if self._should_flush():
                self._flush()
        return accepted

    def _should_flush(self) -> bool:
        if len(self._buffer) == 0:
            return False
        if len(self._buffer) >= self._policy.max_size:
            return True
        if self._stats.bytes_received >= self._policy.max_bytes:
            return True
        if self._policy.max_age_sec > 0:
            last = self._stats.last_flush or time.time()
            if time.time() - last >= self._policy.max_age_sec:
                return True
        return False

    def _flush(self) -> None:
        if not self._buffer:
            return
        items = list(self._buffer)
        size = sum(self._estimate_size(i) for i in items)
        self._buffer.clear()
        self._stats.items_flushed += len(items)
        self._stats.bytes_flushed += size
        self._stats.flush_count += 1
        self._stats.bytes_received = 0
        self._stats.last_flush = time.time()
        logger.debug(
            f"Flushed {len(items)} items ({size} bytes) from buffer {self.name}"
        )
        if self._flush_callback:
            try:
                self._flush_callback(items)
            except Exception as e:
                logger.error(f"Flush callback failed: {e}")

    def flush(self) -> int:
        with self._lock:
            count = len(self._buffer)
            self._flush()
            return count

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "current_size": len(self._buffer),
                "max_size": self._policy.max_size,
                "utilization": len(self._buffer) / self._policy.max_size
                if self._policy.max_size > 0
                else 0,
                "items_received": self._stats.items_received,
                "items_flushed": self._stats.items_flushed,
                "flush_count": self._stats.flush_count,
                "bytes_received": self._stats.bytes_received,
                "bytes_flushed": self._stats.bytes_flushed,
                "backpressure_events": self._stats.backpressure_events,
                "last_flush": self._stats.last_flush,
            }

    def close(self) -> int:
        self._closed = True
        return self.flush()

    def is_backpressured(self) -> bool:
        with self._lock:
            return len(self._buffer) >= int(self._policy.max_size * 0.9)

    def _estimate_size(self, item: Any) -> int:
        try:
            return len(str(item).encode("utf-8"))
        except Exception:
            return 64
