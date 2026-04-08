"""Bulkhead Action Module.

Provides bulkhead isolation pattern for
resource partitioning and fault isolation.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BulkheadPartition:
    """A bulkhead partition."""
    name: str
    max_concurrent: int
    max_queue_size: int
    current_load: int = 0
    queue_size: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class BulkheadManager:
    """Manages bulkhead partitions."""

    def __init__(self):
        self._partitions: Dict[str, BulkheadPartition] = {}

    def create_partition(
        self,
        name: str,
        max_concurrent: int = 10,
        max_queue_size: int = 100
    ) -> None:
        """Create a bulkhead partition."""
        self._partitions[name] = BulkheadPartition(
            name=name,
            max_concurrent=max_concurrent,
            max_queue_size=max_queue_size
        )

    def acquire(self, partition_name: str, timeout: float = 30.0) -> bool:
        """Acquire a slot in partition."""
        partition = self._partitions.get(partition_name)
        if not partition:
            return True

        start = time.time()

        while True:
            with partition.lock:
                if partition.current_load < partition.max_concurrent:
                    partition.current_load += 1
                    return True

                if partition.queue_size >= partition.max_queue_size:
                    return False

                partition.queue_size += 1

            if time.time() - start >= timeout:
                with partition.lock:
                    partition.queue_size = max(0, partition.queue_size - 1)
                return False

            time.sleep(0.1)

    def release(self, partition_name: str) -> None:
        """Release a slot in partition."""
        partition = self._partitions.get(partition_name)
        if not partition:
            return

        with partition.lock:
            if partition.current_load > 0:
                partition.current_load -= 1

    def get_stats(self, partition_name: str) -> Optional[Dict]:
        """Get partition statistics."""
        partition = self._partitions.get(partition_name)
        if not partition:
            return None

        return {
            "name": partition.name,
            "max_concurrent": partition.max_concurrent,
            "current_load": partition.current_load,
            "available": partition.max_concurrent - partition.current_load,
            "max_queue_size": partition.max_queue_size,
            "queue_size": partition.queue_size
        }


class BulkheadAction(BaseAction):
    """Action for bulkhead operations."""

    def __init__(self):
        super().__init__("bulkhead")
        self._manager = BulkheadManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute bulkhead action."""
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
        """Create partition."""
        self._manager.create_partition(
            name=params.get("name", ""),
            max_concurrent=params.get("max_concurrent", 10),
            max_queue_size=params.get("max_queue_size", 100)
        )
        return ActionResult(success=True)

    def _acquire(self, params: Dict) -> ActionResult:
        """Acquire slot."""
        acquired = self._manager.acquire(
            params.get("name", ""),
            params.get("timeout", 30.0)
        )
        return ActionResult(success=acquired, data={"acquired": acquired})

    def _release(self, params: Dict) -> ActionResult:
        """Release slot."""
        self._manager.release(params.get("name", ""))
        return ActionResult(success=True)

    def _stats(self, params: Dict) -> ActionResult:
        """Get stats."""
        stats = self._manager.get_stats(params.get("name", ""))
        if not stats:
            return ActionResult(success=False, message="Partition not found")
        return ActionResult(success=True, data=stats)
