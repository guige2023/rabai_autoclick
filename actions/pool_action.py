"""Object pool action module for RabAI AutoClick.

Provides object pool operations:
- PoolAcquireAction: Acquire object from pool
- PoolReleaseAction: Release object back to pool
- PoolStatsAction: Get pool statistics
- PoolResizeAction: Resize pool
"""

import threading
import time
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class PooledObject:
    """Represents a pooled object."""
    obj_id: str
    obj: Any
    in_use: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_used: datetime = field(default_factory=datetime.utcnow)
    use_count: int = 0


class ObjectPool:
    """Thread-safe object pool."""
    def __init__(self, factory: Optional[Callable] = None, max_size: int = 10):
        self._factory = factory or (lambda: None)
        self._max_size = max_size
        self._pool: List[PooledObject] = []
        self._lock = threading.RLock()
        self._total_acquired = 0
        self._total_released = 0

    def acquire(self, timeout: Optional[float] = None) -> Optional[Any]:
        start = time.time()
        while True:
            with self._lock:
                for pooled in self._pool:
                    if not pooled.in_use:
                        pooled.in_use = True
                        pooled.last_used = datetime.utcnow()
                        pooled.use_count += 1
                        self._total_acquired += 1
                        return pooled.obj

                if len(self._pool) < self._max_size:
                    obj = self._factory()
                    pooled = PooledObject(obj_id=str(id(obj)), obj=obj, in_use=True)
                    self._pool.append(pooled)
                    self._total_acquired += 1
                    return obj

            if timeout and (time.time() - start) >= timeout:
                return None
            time.sleep(0.01)

    def release(self, obj: Any) -> bool:
        with self._lock:
            for pooled in self._pool:
                if pooled.obj is obj and pooled.in_use:
                    pooled.in_use = False
                    pooled.last_used = datetime.utcnow()
                    self._total_released += 1
                    return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            in_use = sum(1 for p in self._pool if p.in_use)
            available = len(self._pool) - in_use
            return {
                "max_size": self._max_size,
                "total_objects": len(self._pool),
                "in_use": in_use,
                "available": available,
                "total_acquired": self._total_acquired,
                "total_released": self._total_released
            }


_pools: Dict[str, ObjectPool] = {}
_pools_lock = threading.Lock()


class PoolAcquireAction(BaseAction):
    """Acquire object from pool."""
    action_type = "pool_acquire"
    display_name = "获取池对象"
    description = "从对象池获取对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pool_name = params.get("pool_name", "default")
            timeout = params.get("timeout", 30.0)

            with _pools_lock:
                if pool_name not in _pools:
                    _pools[pool_name] = ObjectPool()

            obj = _pools[pool_name].acquire(timeout=timeout)

            if obj is None:
                return ActionResult(success=False, message=f"Failed to acquire from pool '{pool_name}' (timeout)")

            return ActionResult(
                success=True,
                message=f"Acquired from pool '{pool_name}'",
                data={"pool_name": pool_name, "acquired": True}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pool acquire failed: {str(e)}")


class PoolReleaseAction(BaseAction):
    """Release object back to pool."""
    action_type = "pool_release"
    display_name = "释放池对象"
    description = "释放对象回对象池"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pool_name = params.get("pool_name", "default")
            obj = params.get("obj", None)

            if pool_name not in _pools:
                return ActionResult(success=False, message=f"Pool '{pool_name}' not found")

            if obj is None:
                return ActionResult(success=False, message="obj is required")

            released = _pools[pool_name].release(obj)

            return ActionResult(
                success=released,
                message=f"Released to pool '{pool_name}': {released}",
                data={"pool_name": pool_name, "released": released}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pool release failed: {str(e)}")


class PoolStatsAction(BaseAction):
    """Get pool statistics."""
    action_type = "pool_stats"
    display_name = "池统计"
    description = "获取对象池统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pool_name = params.get("pool_name", None)

            if pool_name:
                if pool_name not in _pools:
                    return ActionResult(success=False, message=f"Pool '{pool_name}' not found")
                stats = _pools[pool_name].get_stats()
            else:
                all_stats = {name: pool.get_stats() for name, pool in _pools.items()}
                return ActionResult(
                    success=True,
                    message=f"{len(all_stats)} pools",
                    data={"pools": all_stats, "count": len(all_stats)}
                )

            return ActionResult(
                success=True,
                message=f"Pool '{pool_name}' stats",
                data=stats
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pool stats failed: {str(e)}")


class PoolResizeAction(BaseAction):
    """Resize pool."""
    action_type = "pool_resize"
    display_name = "调整池大小"
    description = "调整对象池大小"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pool_name = params.get("pool_name", "default")
            new_size = params.get("new_size", 10)

            with _pools_lock:
                if pool_name not in _pools:
                    _pools[pool_name] = ObjectPool(max_size=new_size)
                else:
                    _pools[pool_name]._max_size = new_size

            return ActionResult(
                success=True,
                message=f"Pool '{pool_name}' resized to {new_size}",
                data={"pool_name": pool_name, "new_size": new_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pool resize failed: {str(e)}")
