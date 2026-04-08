"""Pool action module for RabAI AutoClick.

Provides object pool pattern implementation:
- Pool: Generic object pool
- PooledObject: Wrapper for pooled objects
- ObjectFactory: Factory for creating objects
- PoolManager: Manages multiple pools
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")


@dataclass
class PooledObject:
    """Pooled object wrapper."""
    object_id: str
    obj: Any
    created_at: float
    last_used: float
    use_count: int = 0
    is_valid: bool = True


class ObjectFactory(Generic[T]):
    """Factory for creating objects."""

    def __init__(self, factory_fn: Callable[[], T]):
        self._factory_fn = factory_fn

    def create(self) -> T:
        """Create a new object."""
        return self._factory_fn()

    def validate(self, obj: T) -> bool:
        """Validate an object."""
        return True

    def reset(self, obj: T) -> None:
        """Reset an object for reuse."""
        pass


class Pool(Generic[T]):
    """Generic object pool."""

    def __init__(
        self,
        factory: ObjectFactory[T],
        min_size: int = 0,
        max_size: int = 10,
        idle_timeout: float = 300.0,
    ):
        self._factory = factory
        self._min_size = min_size
        self._max_size = max_size
        self._idle_timeout = idle_timeout

        self._available: List[PooledObject] = []
        self._in_use: Dict[str, PooledObject] = {}
        self._lock = threading.RLock()

        self._total_created = 0
        self._total_destroyed = 0

        for _ in range(min_size):
            self._create_object()

    def acquire(self, timeout: Optional[float] = None) -> Optional[T]:
        """Acquire an object from the pool."""
        start_time = time.time()

        while True:
            obj = self._try_acquire()
            if obj is not None:
                return obj

            if self._can_create():
                new_obj = self._create_object()
                if new_obj is not None:
                    return new_obj

            if timeout is not None and time.time() - start_time >= timeout:
                return None

            time.sleep(0.01)

    def release(self, obj: T) -> None:
        """Release an object back to the pool."""
        with self._lock:
            for obj_id, pooled in list(self._in_use.items()):
                if pooled.obj is obj:
                    self._in_use.pop(obj_id)

                    if pooled.is_valid:
                        pooled.last_used = time.time()
                        self._available.append(pooled)
                    else:
                        self._destroy_object(pooled)
                    return

    def _try_acquire(self) -> Optional[T]:
        """Try to acquire without blocking."""
        with self._lock:
            self._cleanup_idle()

            if self._available:
                pooled = self._available.pop(0)
                if self._factory.validate(pooled.obj):
                    pooled.use_count += 1
                    pooled.last_used = time.time()
                    self._in_use[pooled.object_id] = pooled
                    return pooled.obj
                else:
                    self._destroy_object(pooled)
                    return self._try_acquire()

        return None

    def _can_create(self) -> bool:
        """Check if we can create a new object."""
        with self._lock:
            total = len(self._available) + len(self._in_use)
            return total < self._max_size

    def _create_object(self) -> Optional[T]:
        """Create a new object."""
        with self._lock:
            total = len(self._available) + len(self._in_use)
            if total >= self._max_size:
                return None

            try:
                obj = self._factory.create()
                pooled = PooledObject(
                    object_id=str(uuid.uuid4()),
                    obj=obj,
                    created_at=time.time(),
                    last_used=time.time(),
                )
                pooled.use_count = 1
                self._available.append(pooled)
                self._total_created += 1
                return obj
            except Exception:
                return None

    def _destroy_object(self, pooled: PooledObject) -> None:
        """Destroy a pooled object."""
        if hasattr(pooled.obj, "close"):
            try:
                pooled.obj.close()
            except Exception:
                pass
        self._total_destroyed += 1

    def _cleanup_idle(self) -> None:
        """Clean up idle objects."""
        now = time.time()
        to_remove = []

        for pooled in self._available:
            if now - pooled.last_used > self._idle_timeout:
                total = len(self._available) + len(self._in_use)
                if total > self._min_size:
                    to_remove.append(pooled)

        for pooled in to_remove:
            self._available.remove(pooled)
            self._destroy_object(pooled)

    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        with self._lock:
            return {
                "available": len(self._available),
                "in_use": len(self._in_use),
                "total": len(self._available) + len(self._in_use),
                "min_size": self._min_size,
                "max_size": self._max_size,
                "total_created": self._total_created,
                "total_destroyed": self._total_destroyed,
            }

    def clear(self) -> None:
        """Clear the pool."""
        with self._lock:
            for pooled in self._available:
                self._destroy_object(pooled)
            self._available.clear()
            self._in_use.clear()


class PoolManager:
    """Manages multiple pools."""

    def __init__(self):
        self._pools: Dict[str, Pool] = {}
        self._lock = threading.RLock()

    def create_pool(
        self,
        name: str,
        factory_fn: Callable[[], Any],
        min_size: int = 0,
        max_size: int = 10,
        idle_timeout: float = 300.0,
    ) -> Pool:
        """Create a new pool."""
        with self._lock:
            factory = ObjectFactory(factory_fn)
            pool = Pool(factory, min_size, max_size, idle_timeout)
            self._pools[name] = pool
            return pool

    def get_pool(self, name: str) -> Optional[Pool]:
        """Get a pool by name."""
        with self._lock:
            return self._pools.get(name)

    def remove_pool(self, name: str) -> bool:
        """Remove a pool."""
        with self._lock:
            if name in self._pools:
                self._pools[name].clear()
                del self._pools[name]
                return True
            return False

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all pools."""
        with self._lock:
            return {name: pool.get_stats() for name, pool in self._pools.items()}


class PoolAction(BaseAction):
    """Pool pattern action."""
    action_type = "pool"
    display_name = "对象池模式"
    description = "对象池管理"

    def __init__(self):
        super().__init__()
        self._manager = PoolManager()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "acquire")

            if operation == "create":
                return self._create_pool(params)
            elif operation == "acquire":
                return self._acquire(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear_pool(params)
            elif operation == "list":
                return self._list_pools()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Pool error: {str(e)}")

    def _create_pool(self, params: Dict[str, Any]) -> ActionResult:
        """Create a pool."""
        name = params.get("name")
        factory_fn = params.get("factory_fn")
        min_size = params.get("min_size", 0)
        max_size = params.get("max_size", 10)
        idle_timeout = params.get("idle_timeout", 300.0)

        if not name:
            return ActionResult(success=False, message="name is required")

        if not callable(factory_fn):
            def default_factory():
                return {"id": str(uuid.uuid4()), "created": time.time()}
            factory_fn = default_factory

        pool = self._manager.create_pool(name, factory_fn, min_size, max_size, idle_timeout)

        return ActionResult(success=True, message=f"Pool created: {name}", data={"name": name})

    def _acquire(self, params: Dict[str, Any]) -> ActionResult:
        """Acquire from pool."""
        name = params.get("name")
        timeout = params.get("timeout")

        if not name:
            return ActionResult(success=False, message="name is required")

        pool = self._manager.get_pool(name)
        if not pool:
            return ActionResult(success=False, message=f"Pool not found: {name}")

        obj = pool.acquire(timeout=timeout)

        if obj is None:
            return ActionResult(success=False, message="Failed to acquire object (pool full or timeout)")

        return ActionResult(success=True, message="Object acquired", data={"object": obj})

    def _release(self, params: Dict[str, Any]) -> ActionResult:
        """Release object to pool."""
        name = params.get("name")
        obj = params.get("object")

        if not name:
            return ActionResult(success=False, message="name is required")

        pool = self._manager.get_pool(name)
        if not pool:
            return ActionResult(success=False, message=f"Pool not found: {name}")

        pool.release(obj)

        return ActionResult(success=True, message="Object released")

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get pool statistics."""
        name = params.get("name")

        if name:
            pool = self._manager.get_pool(name)
            if not pool:
                return ActionResult(success=False, message=f"Pool not found: {name}")
            stats = pool.get_stats()
            return ActionResult(success=True, message="Pool stats", data={"name": name, "stats": stats})
        else:
            all_stats = self._manager.get_all_stats()
            return ActionResult(success=True, message="All pool stats", data={"pools": all_stats})

    def _clear_pool(self, params: Dict[str, Any]) -> ActionResult:
        """Clear a pool."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        success = self._manager.remove_pool(name)
        return ActionResult(success=success, message="Pool cleared" if success else "Pool not found")

    def _list_pools(self) -> ActionResult:
        """List all pools."""
        all_stats = self._manager.get_all_stats()
        return ActionResult(success=True, message=f"{len(all_stats)} pools", data={"pools": list(all_stats.keys())})
