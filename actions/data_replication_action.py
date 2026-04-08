"""
Data Replication Action Module

Provides data replication, synchronization, and consistency management.
"""
from typing import Any, Optional, Callable, Literal
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
import asyncio
import hashlib


class ReplicationMode(Enum):
    """Replication modes."""
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
    SEMI_SYNCHRONOUS = "semi_synchronous"


class ConsistencyLevel(Enum):
    """Consistency levels."""
    ONE = "one"
    QUORUM = "quorum"
    ALL = "all"
    LOCAL_QUORUM = "local_quorum"
    EACH_QUORUM = "each_quorum"


@dataclass
class ReplicaNode:
    """A replica node."""
    node_id: str
    endpoint: str
    priority: int = 1
    healthy: bool = True
    last_heartbeat: Optional[datetime] = None
    replication_latency_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplicationConfig:
    """Configuration for replication."""
    replication_factor: int = 3
    consistency_level: ConsistencyLevel = ConsistencyLevel.QUORUM
    mode: ReplicationMode = ReplicationMode.ASYNCHRONOUS
    retry_attempts: int = 3
    timeout_seconds: float = 10.0
    write_ack_required: bool = True


@dataclass
class ReplicationRecord:
    """A record to be replicated."""
    record_id: str
    key: str
    value: Any
    version: int
    timestamp: datetime
    ttl_seconds: Optional[float] = None
    source_node: Optional[str] = None


@dataclass
class ReplicationResult:
    """Result of replication operation."""
    success: bool
    record_id: str
    replicas_written: int
    replicas_failed: int
    duration_ms: float
    errors: list[str] = field(default_factory=list)
    confirmations: list[str] = field(default_factory=list)


@dataclass
class SyncConflict:
    """A synchronization conflict."""
    record_id: str
    key: str
    local_version: int
    remote_version: int
    local_value: Any
    remote_value: Any
    resolution_strategy: str = "latest"
    resolved_value: Optional[Any] = None
    resolved_at: Optional[datetime] = None


class DataReplicationAction:
    """Main data replication action handler."""
    
    def __init__(self):
        self._nodes: dict[str, ReplicaNode] = {}
        self._replication_config = ReplicationConfig()
        self._write_buffers: dict[str, list[ReplicationRecord]] = defaultdict(list)
        self._sync_state: dict[str, dict] = {}
        self._conflict_resolvers: dict[str, Callable] = {
            "latest": self._resolve_latest,
            "first": self._resolve_first,
            "manual": self._resolve_manual
        }
        self._stats: dict[str, Any] = defaultdict(int)
    
    def add_node(self, node: ReplicaNode) -> "DataReplicationAction":
        """Add a replica node."""
        self._nodes[node.node_id] = node
        return self
    
    def remove_node(self, node_id: str) -> bool:
        """Remove a replica node."""
        if node_id in self._nodes:
            del self._nodes[node_id]
            return True
        return False
    
    def set_replication_config(self, config: ReplicationConfig):
        """Set replication configuration."""
        self._replication_config = config
    
    async def replicate_write(
        self,
        record: ReplicationRecord
    ) -> ReplicationResult:
        """
        Replicate a write operation to all replica nodes.
        
        Args:
            record: Record to replicate
            
        Returns:
            ReplicationResult with outcome
        """
        start_time = datetime.now()
        success_count = 0
        failed_count = 0
        errors = []
        confirmations = []
        
        healthy_nodes = [
            n for n in self._nodes.values()
            if n.healthy
        ]
        
        # Determine required acknowledgements
        required_acks = self._get_required_acks()
        
        if len(healthy_nodes) < self._replication_config.replication_factor:
            return ReplicationResult(
                success=False,
                record_id=record.record_id,
                replicas_written=0,
                replicas_failed=len(healthy_nodes),
                duration_ms=0,
                errors=["Insufficient healthy replicas"]
            )
        
        # Select nodes for replication
        target_nodes = healthy_nodes[:self._replication_config.replication_factor]
        
        if self._replication_config.mode == ReplicationMode.SYNCHRONOUS:
            # Wait for all replicas
            for node in target_nodes:
                try:
                    success = await self._write_to_node(node, record)
                    
                    if success:
                        success_count += 1
                        confirmations.append(node.node_id)
                    else:
                        failed_count += 1
                        errors.append(f"Failed to write to {node.node_id}")
                
                except Exception as e:
                    failed_count += 1
                    errors.append(f"{node.node_id}: {str(e)}")
            
            success = success_count >= required_acks
            
        elif self._replication_config.mode == ReplicationMode.ASYNCHRONOUS:
            # Write to all in background
            tasks = [
                self._write_to_node(node, record)
                for node in target_nodes
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for node, result in zip(target_nodes, results):
                if isinstance(result, Exception):
                    failed_count += 1
                    errors.append(f"{node.node_id}: {str(result)}")
                elif result:
                    success_count += 1
                    confirmations.append(node.node_id)
            
            success = success_count > 0
        
        else:  # SEMI_SYNCHRONOUS
            # Write to one synchronously, rest async
            if target_nodes:
                primary = target_nodes[0]
                try:
                    primary_success = await self._write_to_node(primary, record)
                    if primary_success:
                        confirmations.append(primary.node_id)
                        success_count = 1
                        
                        # Fire async writes for rest
                        for node in target_nodes[1:]:
                            asyncio.create_task(self._write_to_node(node, record))
                    
                except Exception as e:
                    errors.append(f"Primary write failed: {e}")
            
            success = success_count >= 1
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        self._stats["replication_attempts"] += 1
        if success:
            self._stats["successful_replications"] += 1
        else:
            self._stats["failed_replications"] += 1
        
        return ReplicationResult(
            success=success,
            record_id=record.record_id,
            replicas_written=success_count,
            replicas_failed=failed_count,
            duration_ms=duration_ms,
            errors=errors,
            confirmations=confirmations
        )
    
    async def _write_to_node(self, node: ReplicaNode, record: ReplicationRecord) -> bool:
        """Write a record to a specific node."""
        # Simulate write with latency
        await asyncio.sleep(node.replication_latency_ms / 1000)
        
        # Update sync state
        self._sync_state[record.record_id] = {
            "node_id": node.node_id,
            "version": record.version,
            "timestamp": datetime.now()
        }
        
        return True
    
    def _get_required_acks(self) -> int:
        """Get number of required acknowledgements based on consistency level."""
        level = self._replication_config.consistency_level
        rf = self._replication_config.replication_factor
        
        if level == ConsistencyLevel.ONE:
            return 1
        elif level == ConsistencyLevel.QUORUM:
            return (rf // 2) + 1
        elif level == ConsistencyLevel.ALL:
            return rf
        elif level == ConsistencyLevel.LOCAL_QUORUM:
            return max(1, rf // 2)
        elif level == ConsistencyLevel.EACH_QUORUM:
            return rf
        return rf
    
    async def replicate_batch(
        self,
        records: list[ReplicationRecord],
        parallel: bool = True
    ) -> list[ReplicationResult]:
        """Replicate multiple records."""
        if parallel:
            tasks = [self.replicate_write(record) for record in records]
            return await asyncio.gather(*tasks, return_exceptions=True)
        else:
            results = []
            for record in records:
                result = await self.replicate_write(record)
                results.append(result)
            return results
    
    async def resolve_conflict(
        self,
        conflict: SyncConflict
    ) -> Any:
        """Resolve a synchronization conflict."""
        resolver = self._conflict_resolvers.get(
            conflict.resolution_strategy,
            self._resolve_latest
        )
        
        resolved_value = await resolver(conflict)
        
        conflict.resolved_value = resolved_value
        conflict.resolved_at = datetime.now()
        
        return resolved_value
    
    async def _resolve_latest(self, conflict: SyncConflict) -> Any:
        """Resolve by latest timestamp."""
        if conflict.local_timestamp > conflict.remote_timestamp:
            return conflict.local_value
        return conflict.remote_value
    
    async def _resolve_first(self, conflict: SyncConflict) -> Any:
        """Resolve by first version."""
        if conflict.local_version < conflict.remote_version:
            return conflict.local_value
        return conflict.remote_value
    
    async def _resolve_manual(self, conflict: SyncConflict) -> Any:
        """Manual resolution - returns remote by default."""
        return conflict.remote_value
    
    async def sync_nodes(
        self,
        source_node_id: str,
        target_node_ids: list[str]
    ) -> dict[str, Any]:
        """Synchronize data between nodes."""
        source = self._nodes.get(source_node_id)
        if not source:
            return {"error": f"Source node {source_node_id} not found"}
        
        sync_results = {}
        
        for target_id in target_node_ids:
            target = self._nodes.get(target_id)
            if not target:
                sync_results[target_id] = {"error": "Node not found"}
                continue
            
            # Simulate sync
            await asyncio.sleep(0.1)
            
            sync_results[target_id] = {
                "status": "synced",
                "records_synced": 0,
                "conflicts": 0
            }
        
        return sync_results
    
    async def get_replication_status(self) -> dict[str, Any]:
        """Get current replication status."""
        healthy_nodes = [n for n in self._nodes.values() if n.healthy]
        
        return {
            "total_nodes": len(self._nodes),
            "healthy_nodes": len(healthy_nodes),
            "replication_factor": self._replication_config.replication_factor,
            "consistency_level": self._replication_config.consistency_level.value,
            "mode": self._replication_config.mode.value,
            "nodes": [
                {
                    "id": n.node_id,
                    "endpoint": n.endpoint,
                    "healthy": n.healthy,
                    "latency_ms": n.replication_latency_ms
                }
                for n in self._nodes.values()
            ]
        }
    
    def register_conflict_resolver(
        self,
        strategy: str,
        resolver: Callable[[SyncConflict], Any]
    ) -> "DataReplicationAction":
        """Register a custom conflict resolver."""
        self._conflict_resolvers[strategy] = resolver
        return self
    
    def get_stats(self) -> dict[str, Any]:
        """Get replication statistics."""
        return dict(self._stats)
