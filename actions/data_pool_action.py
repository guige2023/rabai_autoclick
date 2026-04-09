"""Data Pool Action Module.

Implements object pooling for reusable data items with lifecycle
management, pre-warming, and automatic cleanup of stale entries.
"""

import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class PooledItem:
    item_id: str
    item: Any
    created_at: float
    last_used: float
    use_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PoolConfig:
    max_size: int = 100
    min_size: int = 10
    max_idle_sec: float = 300.0
    max_use_count: int = 1000
    prewarm: bool = True
    auto_shrink: bool = True
    shrink_interval_sec: float = 60.0


class DataPoolAction:
    """Object pool for reusable data items with lifecycle management."""

    def __init__(
        self,
        name: str = "default",
        config: Optional[PoolConfig] = None,
        factory_fn: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.name = name
        self._config = config or PoolConfig()
        self._factory_fn = factory_fn
        self._pool: Dict[str, PooledItem] = {}
        self._available: set = set()
        self._in_use: set = set()
        self._lock = threading.RLock()
        self._stats = {
            "total_acquired": 0,
            "total_released": 0,
            "total_created": 0,
            "total_destroyed": 0,
            "pool_hits": 0,
            "pool_misses": 0,
        }

    def acquire(self) -> Optional[Any]:
        with self._lock:
            self._stats["total_acquired"] += 1
            if self._available:
                item_id = self._available.pop()
                pooled = self._pool[item_id]
                pooled.last_used = time.time()
                pooled.use_count += 1
                self._in_use.add(item_id)
                self._stats["pool_hits"] += 1
                return pooled.item
            if len(self._pool) < self._config.max_size:
                return self._create_item()
            self._stats["pool_misses"] += 1
            return None

    def release(self, item: Any) -> bool:
        with self._lock:
            for item_id, pooled in self._pool.items():
                if pooled.item is item and item_id in self._in_use:
                    pooled.last_used = time.time()
                    self._in_use.remove(item_id)
                    self._available.add(item_id)
                    self._stats["total_released"] += 1
                    self._maybe_shrink()
                    return True
            return False

    def _create_item(self) -> Optional[Any]:
        if self._factory_fn is None:
            return None
        try:
            item = self._factory_fn()
            item_id = f"item_{self._stats['total_created']}_{int(time.time() * 1000)}"
            pooled = PooledItem(
                item_id=item_id,
                item=item,
                created_at=time.time(),
                last_used=time.time(),
            )
            self._pool[item_id] = pooled
            self._available.add(item_id)
            self._stats["total_created"] += 1
            return item
        except Exception as e:
            logger.error(f"Pool factory failed: {e}")
            return None

    def prewarm(self, count: Optional[int] = None) -> int:
        if self._factory_fn is None:
            return 0
        target = count or self._config.min_size
        current = len(self._pool)
        created = 0
        for _ in range(target - current):
            if len(self._pool) >= self._config.max_size:
                break
            if self._create_item() is not None:
                created += 1
        logger.info(f"Prewarmed pool {self.name} with {created} items")
        return created

    def _maybe_shrink(self) -> int:
        if not self._config.auto_shrink:
            return 0
        target_size = self._config.min_size
        available_count = len(self._available)
        if available_count <= target_size:
            return 0
        to_remove = available_count - target_size
        removed = 0
        now = time.time()
        for item_id in list(self._available):
            pooled = self._pool[item_id]
            idle_time = now - pooled.last_used
            if idle_time > self._config.max_idle_sec or pooled.use_count >= self._config.max_use_count:
                self._destroy_item(item_id)
                removed += 1
                if removed >= to_remove:
                    break
        return removed

    def _destroy_item(self, item_id: str) -> None:
        if item_id in self._pool:
            del self._pool[item_id]
        self._available.discard(item_id)
        self._in_use.discard(item_id)
        self._stats["total_destroyed"] += 1

    def clear(self) -> int:
        with self._lock:
            count = len(self._pool)
            self._pool.clear()
            self._available.clear()
            self._in_use.clear()
            return count

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                **self._stats,
                "pool_size": len(self._pool),
                "available": len(self._available),
                "in_use": len(self._in_use),
                "max_size": self._config.max_size,
                "utilization": len(self._in_use) / len(self._pool) if self._pool else 0,
                "hit_rate": self._stats["pool_hits"] / max(1, self._stats["total_acquired"]),
            }

    def get_idle_items(self) -> List[Dict[str, Any]]:
        with self._lock:
            now = time.time()
            return [
                {
                    "item_id": p.item_id,
                    "idle_sec": now - p.last_used,
                    "use_count": p.use_count,
                }
                for item_id in self._available
                for p in [self._pool[item_id]]
            ]
