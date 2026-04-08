"""Object Pool Action Module.

Provides object pool for resource
reuse and management.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class PooledObject:
    """Pooled object."""
    object_id: str
    obj: Any
    in_use: bool = False
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)


class ObjectPool:
    """Object pool implementation."""
    def __init__(self, pool_id: str, factory: Callable):
        self.pool_id = pool_id
        self._factory = factory
        self._objects: List[PooledObject] = []
        self._lock = threading.RLock()
        self._min_size = 0
        self._max_size = 10

    def acquire(self) -> Any:
        """Acquire object from pool."""
        with self._lock:
            for pooled in self._objects:
                if not pooled.in_use:
                    pooled.in_use = True
                    pooled.last_used = time.time()
                    return pooled.obj

            if len(self._objects) < self._max_size:
                new_obj = self._factory()
                pooled = PooledObject(
                    object_id=f"pooled_{len(self._objects)}",
                    obj=new_obj,
                    in_use=True
                )
                self._objects.append(pooled)
                return new_obj

            raise RuntimeError("Pool exhausted")

    def release(self, obj: Any) -> bool:
        """Release object back to pool."""
        with self._lock:
            for pooled in self._objects:
                if pooled.obj is obj:
                    pooled.in_use = False
                    pooled.last_used = time.time()
                    return True
        return False

    def get_stats(self) -> Dict:
        """Get pool statistics."""
        with self._lock:
            total = len(self._objects)
            in_use = sum(1 for p in self._objects if p.in_use)
            available = total - in_use

            return {
                "total": total,
                "in_use": in_use,
                "available": available,
                "max_size": self._max_size
            }


class ObjectPoolManager:
    """Manages object pools."""

    def __init__(self):
        self._pools: Dict[str, ObjectPool] = {}

    def create_pool(self, pool_id: str, factory: Callable) -> str:
        """Create object pool."""
        self._pools[pool_id] = ObjectPool(pool_id, factory)
        return pool_id

    def get_pool(self, pool_id: str) -> Optional[ObjectPool]:
        """Get pool."""
        return self._pools.get(pool_id)


class ObjectPoolAction(BaseAction):
    """Action for object pool operations."""

    def __init__(self):
        super().__init__("object_pool")
        self._manager = ObjectPoolManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute object pool action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "acquire":
                return self._acquire(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create pool."""
        pool_id = self._manager.create_pool(
            params.get("pool_id", ""),
            params.get("factory") or (lambda: {})
        )
        return ActionResult(success=True, data={"pool_id": pool_id})

    def _acquire(self, params: Dict) -> ActionResult:
        """Acquire object."""
        pool = self._manager.get_pool(params.get("pool_id", ""))
        if not pool:
            return ActionResult(success=False, message="Pool not found")

        obj = pool.acquire()
        return ActionResult(success=True, data={"acquired": True})

    def _release(self, params: Dict) -> ActionResult:
        """Release object."""
        pool = self._manager.get_pool(params.get("pool_id", ""))
        if not pool:
            return ActionResult(success=False, message="Pool not found")

        success = pool.release(params.get("obj"))
        return ActionResult(success=success)

    def _stats(self, params: Dict) -> ActionResult:
        """Get stats."""
        pool = self._manager.get_pool(params.get("pool_id", ""))
        if not pool:
            return ActionResult(success=False, message="Pool not found")

        return ActionResult(success=True, data=pool.get_stats())
