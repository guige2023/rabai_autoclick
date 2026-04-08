"""API Partitioner Action.

Partitions API requests across multiple backends or workers.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import hashlib


@dataclass
class Partition:
    partition_id: str
    backend: str
    weight: float = 1.0
    healthy: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIPartitionerAction:
    """Partitions API requests across multiple backends."""

    def __init__(self, strategy: str = "round_robin") -> None:
        self.strategy = strategy
        self.partitions: Dict[str, Partition] = {}
        self._index = 0
        self._request_counts: Dict[str, int] = {}

    def add_partition(self, partition: Partition) -> None:
        self.partitions[partition.partition_id] = partition
        self._request_counts[partition.partition_id] = 0

    def remove_partition(self, partition_id: str) -> None:
        self.partitions.pop(partition_id, None)
        self._request_counts.pop(partition_id, None)

    def set_healthy(self, partition_id: str, healthy: bool) -> None:
        if partition_id in self.partitions:
            self.partitions[partition_id].healthy = healthy

    def get_partition(self, key: Optional[str] = None) -> Optional[Partition]:
        healthy = [p for p in self.partitions.values() if p.healthy]
        if not healthy:
            return None
        if self.strategy == "round_robin":
            partition = healthy[self._index % len(healthy)]
            self._index += 1
            return partition
        elif self.strategy == "weighted":
            total_weight = sum(p.weight for p in healthy)
            r = (hashlib.md5((key or str(time.time())).encode()).digest()[0] / 255.0) * total_weight
            cumulative = 0.0
            for p in healthy:
                cumulative += p.weight
                if r <= cumulative:
                    return p
            return healthy[-1]
        elif self.strategy == "least_load":
            return min(healthy, key=lambda p: self._request_counts.get(p.partition_id, 0))
        return healthy[0]

    def record_request(self, partition_id: str) -> None:
        if partition_id in self._request_counts:
            self._request_counts[partition_id] += 1

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_partitions": len(self.partitions),
            "healthy": sum(1 for p in self.partitions.values() if p.healthy),
            "strategy": self.strategy,
            "request_counts": dict(self._request_counts),
        }
