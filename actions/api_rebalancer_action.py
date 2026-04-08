"""API Rebalancer Action.

Rebalances API traffic across partitions based on load.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import time


@dataclass
class LoadInfo:
    partition_id: str
    request_count: int = 0
    error_count: int = 0
    avg_latency_ms: float = 0.0
    last_updated: float = field(default_factory=time.time)


class APIRebalancerAction:
    """Rebalances API traffic across partitions."""

    def __init__(
        self,
        target_load: Optional[float] = None,
        rebalance_threshold: float = 0.2,
        min_partitions: int = 1,
    ) -> None:
        self.target_load = target_load
        self.rebalance_threshold = rebalance_threshold
        self.min_partitions = min_partitions
        self.load_info: Dict[str, LoadInfo] = {}
        self.partition_weights: Dict[str, float] = {}

    def update_load(self, partition_id: str, request_count: int, error_count: int = 0, avg_latency_ms: float = 0.0) -> None:
        self.load_info[partition_id] = LoadInfo(
            partition_id=partition_id,
            request_count=request_count,
            error_count=error_count,
            avg_latency_ms=avg_latency_ms,
            last_updated=time.time(),
        )

    def calculate_weights(self) -> Dict[str, float]:
        if not self.load_info:
            return {}
        if self.target_load is None:
            total = sum(li.request_count for li in self.load_info.values())
            if total == 0:
                return {pid: 1.0 for pid in self.load_info}
            return {pid: total / max(li.request_count, 1) for pid, li in self.load_info.items()}
        avg_load = sum(li.request_count for li in self.load_info.values()) / len(self.load_info)
        target = self.target_load if self.target_load else avg_load
        weights = {}
        for pid, li in self.load_info.items():
            if li.request_count == 0:
                weights[pid] = 1.0
            else:
                weights[pid] = target / li.request_count
        return weights

    def needs_rebalance(self) -> bool:
        weights = self.calculate_weights()
        if not weights:
            return False
        avg = sum(weights.values()) / len(weights)
        max_dev = max(abs(w - avg) / avg for w in weights.values())
        return max_dev > self.rebalance_threshold

    def get_rebalance_plan(self) -> Dict[str, float]:
        return self.calculate_weights()

    def get_stats(self) -> Dict[str, Any]:
        return {
            "partitions": len(self.load_info),
            "weights": self.partition_weights,
            "needs_rebalance": self.needs_rebalance(),
        }
