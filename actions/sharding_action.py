"""Sharding Action Module.

Provides horizontal sharding for
data distribution.
"""

import hashlib
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Shard:
    """Shard definition."""
    shard_id: str
    name: str
    handler: Optional[Callable] = None


class ShardingManager:
    """Manages sharding."""

    def __init__(self):
        self._shards: List[Shard] = []
        self._data: Dict[str, Any] = {}

    def add_shard(
        self,
        name: str,
        handler: Optional[Callable] = None
    ) -> str:
        """Add a shard."""
        shard_id = f"shard_{len(self._shards)}"
        self._shards.append(Shard(
            shard_id=shard_id,
            name=name,
            handler=handler
        ))
        return shard_id

    def get_shard_for_key(self, key: str) -> int:
        """Get shard index for key."""
        if not self._shards:
            return 0

        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_val % len(self._shards)

    def put(self, key: str, value: Any) -> int:
        """Put value with sharding."""
        shard_idx = self.get_shard_for_key(key)

        if not hasattr(self, '_shard_data'):
            self._shard_data: List[Dict[str, Any]] = [{} for _ in self._shards]

        self._shard_data[shard_idx][key] = value
        return shard_idx

    def get(self, key: str) -> Optional[Any]:
        """Get value by key."""
        shard_idx = self.get_shard_for_key(key)

        if not hasattr(self, '_shard_data'):
            return None

        return self._shard_data[shard_idx].get(key)

    def get_shard_stats(self) -> List[Dict]:
        """Get shard statistics."""
        if not hasattr(self, '_shard_data'):
            return [{"shard_id": s.shard_id, "size": 0} for s in self._shards]

        return [
            {
                "shard_id": self._shards[i].shard_id,
                "name": self._shards[i].name,
                "size": len(self._shard_data[i]) if i < len(self._shards) else 0
            }
            for i in range(len(self._shards))
        ]


class ShardingAction(BaseAction):
    """Action for sharding operations."""

    def __init__(self):
        super().__init__("sharding")
        self._manager = ShardingManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute sharding action."""
        try:
            operation = params.get("operation", "add_shard")

            if operation == "add_shard":
                return self._add_shard(params)
            elif operation == "put":
                return self._put(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _add_shard(self, params: Dict) -> ActionResult:
        """Add shard."""
        shard_id = self._manager.add_shard(params.get("name", ""))
        return ActionResult(success=True, data={"shard_id": shard_id})

    def _put(self, params: Dict) -> ActionResult:
        """Put value."""
        shard_idx = self._manager.put(
            params.get("key", ""),
            params.get("value")
        )
        return ActionResult(success=True, data={"shard_index": shard_idx})

    def _get(self, params: Dict) -> ActionResult:
        """Get value."""
        value = self._manager.get(params.get("key", ""))
        return ActionResult(success=value is not None, data={"value": value})

    def _stats(self, params: Dict) -> ActionResult:
        """Get stats."""
        return ActionResult(success=True, data={"shards": self._manager.get_shard_stats()})
