"""Sharding action module for RabAI AutoClick.

Provides data sharding strategies for distributed databases
and caches with range, hash, and directory-based sharding.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ShardKeyType(Enum):
    """Shard key types."""
    HASH = "hash"
    RANGE = "range"
    DIRECTORY = "directory"


@dataclass
class Shard:
    """Represents a data shard."""
    shard_id: str
    name: str
    shard_key_start: Any = None
    shard_key_end: Any = None
    nodes: List[str] = field(default_factory=list)
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ShardConfig:
    """Configuration for sharding strategy."""
    strategy: ShardKeyType = ShardKeyType.HASH
    num_shards: int = 4
    shard_function: Optional[str] = None  # Custom function name


class ShardingManager:
    """Manages data sharding across multiple shards."""
    
    def __init__(self):
        self._shards: Dict[str, Shard] = {}
        self._directory: Dict[str, str] = {}  # key -> shard_id
        self._config: Optional[ShardConfig] = None
        self._shard_functions: Dict[str, Callable] = {}
    
    def configure(self, config: ShardConfig) -> None:
        """Configure sharding strategy."""
        self._config = config
    
    def add_shard(self, shard: Shard) -> None:
        """Add a shard."""
        self._shards[shard.shard_id] = shard
    
    def remove_shard(self, shard_id: str) -> bool:
        """Remove a shard."""
        if shard_id in self._shards:
            del self._shards[shard_id]
            # Remove from directory
            self._directory = {k: v for k, v in self._directory.items() if v != shard_id}
            return True
        return False
    
    def register_shard_function(self, name: str, func: Callable) -> None:
        """Register a custom shard function."""
        self._shard_functions[name] = func
    
    def _compute_hash_shard(self, key: Any) -> str:
        """Compute shard using hash."""
        if not self._config:
            raise ValueError("Sharding not configured")
        
        key_str = str(key)
        hash_val = hash(key_str)
        shard_index = hash_val % self._config.num_shards
        return f"shard_{shard_index}"
    
    def _compute_range_shard(self, key: Any) -> Optional[str]:
        """Compute shard using range."""
        for shard_id, shard in self._shards.items():
            if shard.shard_key_start is not None and shard.shard_key_end is not None:
                if shard.shard_key_start <= key < shard.shard_key_end:
                    return shard_id
            elif shard.shard_key_end is not None:
                if key < shard.shard_key_end:
                    return shard_id
            elif shard.shard_key_start is not None:
                if key >= shard.shard_key_start:
                    return shard_id
        return None
    
    def _compute_directory_shard(self, key: Any) -> Optional[str]:
        """Look up shard from directory."""
        return self._directory.get(str(key))
    
    def compute_shard(self, key: Any) -> Optional[str]:
        """Compute shard for a given key."""
        if not self._config:
            return None
        
        if self._config.strategy == ShardKeyType.HASH:
            return self._compute_hash_shard(key)
        elif self._config.strategy == ShardKeyType.RANGE:
            return self._compute_range_shard(key)
        elif self._config.strategy == ShardKeyType.DIRECTORY:
            return self._compute_directory_shard(key)
        
        return None
    
    def get_shard(self, shard_id: str) -> Optional[Shard]:
        """Get shard by ID."""
        return self._shards.get(shard_id)
    
    def map_to_shard(self, key: Any, shard_id: str) -> None:
        """Explicitly map a key to a shard."""
        self._directory[str(key)] = shard_id
    
    def get_shard_for_key(self, key: Any) -> Optional[Shard]:
        """Get shard object for a key."""
        shard_id = self.compute_shard(key)
        return self._shards.get(shard_id)
    
    def get_keys_for_shard(self, shard_id: str) -> List[str]:
        """Get all keys mapped to a shard."""
        return [k for k, sid in self._directory.items() if sid == shard_id]
    
    def rebalance_shards(self, target_num_shards: int) -> Dict[str, str]:
        """Rebalance keys across new shard count.
        
        Returns mapping of old_shard -> new_shard for each key.
        """
        old_config = self._config
        self._config = ShardConfig(
            strategy=ShardKeyType.HASH,
            num_shards=target_num_shards
        )
        
        # Rebuild directory with new shard count
        new_directory: Dict[str, str] = {}
        for key, old_shard_id in self._directory.items():
            new_shard_id = self._compute_hash_shard(key)
            new_directory[key] = new_shard_id
        
        self._directory = new_directory
        self._config = old_config
        
        return self._directory
    
    def list_shards(self) -> List[Shard]:
        """List all shards."""
        return list(self._shards.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get sharding statistics."""
        keys_per_shard = {}
        for shard_id in self._shards:
            keys_per_shard[shard_id] = len(self.get_keys_for_shard(shard_id))
        
        return {
            "strategy": self._config.strategy.value if self._config else None,
            "total_shards": len(self._shards),
            "total_keys": len(self._directory),
            "keys_per_shard": keys_per_shard
        }


class ShardingAction(BaseAction):
    """Data sharding for distributed storage.
    
    Supports hash, range, and directory-based sharding strategies.
    """
    action_type = "sharding"
    display_name = "数据分片"
    description = "数据分片策略，支持哈希、范围和目录分片"
    
    def __init__(self):
        super().__init__()
        self._manager = ShardingManager()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sharding operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "configure":
                return self._configure(params)
            elif operation == "add_shard":
                return self._add_shard(params)
            elif operation == "remove_shard":
                return self._remove_shard(params)
            elif operation == "get_shard":
                return self._get_shard(params)
            elif operation == "map_key":
                return self._map_key(params)
            elif operation == "rebalance":
                return self._rebalance(params)
            elif operation == "list_shards":
                return self._list_shards(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure sharding."""
        config = ShardConfig(
            strategy=ShardKeyType(params.get("strategy", "hash")),
            num_shards=params.get("num_shards", 4)
        )
        self._manager.configure(config)
        return ActionResult(success=True, message="Configured")
    
    def _add_shard(self, params: Dict[str, Any]) -> ActionResult:
        """Add a shard."""
        shard = Shard(
            shard_id=params.get("shard_id", ""),
            name=params.get("name", ""),
            shard_key_start=params.get("shard_key_start"),
            shard_key_end=params.get("shard_key_end"),
            weight=params.get("weight", 1)
        )
        self._manager.add_shard(shard)
        return ActionResult(success=True, message=f"Shard '{shard.shard_id}' added")
    
    def _remove_shard(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a shard."""
        shard_id = params.get("shard_id", "")
        removed = self._manager.remove_shard(shard_id)
        return ActionResult(success=removed, message="Removed" if removed else "Not found")
    
    def _get_shard(self, params: Dict[str, Any]) -> ActionResult:
        """Get shard for a key."""
        key = params.get("key", "")
        shard = self._manager.get_shard_for_key(key)
        if not shard:
            return ActionResult(success=False, message="No shard found")
        return ActionResult(success=True, message=f"Shard: {shard.shard_id}",
                         data={"shard_id": shard.shard_id, "name": shard.name})
    
    def _map_key(self, params: Dict[str, Any]) -> ActionResult:
        """Map a key to a shard."""
        key = params.get("key", "")
        shard_id = params.get("shard_id", "")
        self._manager.map_to_shard(key, shard_id)
        return ActionResult(success=True, message=f"Key mapped to '{shard_id}'")
    
    def _rebalance(self, params: Dict[str, Any]) -> ActionResult:
        """Rebalance shards."""
        target = params.get("target_shards", 4)
        mapping = self._manager.rebalance_shards(target)
        return ActionResult(success=True, message=f"Rebalanced to {target} shards",
                         data={"keys_mapped": len(mapping)})
    
    def _list_shards(self, params: Dict[str, Any]) -> ActionResult:
        """List all shards."""
        shards = self._manager.list_shards()
        return ActionResult(success=True, message=f"{len(shards)} shards",
                         data={"shards": [{"shard_id": s.shard_id, "name": s.name} for s in shards]})
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get sharding stats."""
        stats = self._manager.get_stats()
        return ActionResult(success=True, message="Stats retrieved", data=stats)
