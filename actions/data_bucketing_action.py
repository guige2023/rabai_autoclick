"""Data Bucketing Action.

Distributes data into buckets based on keys or ranges.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass
import hashlib


T = TypeVar("T")


@dataclass
class Bucket:
    bucket_id: str
    items: List[Any]
    metadata: Dict[str, Any]


class DataBucketingAction(Generic[T]):
    """Distributes data into buckets based on keys."""

    def __init__(
        self,
        num_buckets: int = 10,
        key_fn: Optional[Callable[[T], str]] = None,
    ) -> None:
        self.num_buckets = num_buckets
        self.key_fn = key_fn or (lambda x: str(x))
        self.buckets: Dict[int, List[T]] = {i: [] for i in range(num_buckets)}
        self.bucket_metadata: Dict[int, Dict[str, Any]] = {i: {} for i in range(num_buckets)}

    def _get_bucket_index(self, item: T) -> int:
        key = self.key_fn(item)
        h = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return h % self.num_buckets

    def add(self, item: T) -> int:
        idx = self._get_bucket_index(item)
        self.buckets[idx].append(item)
        return idx

    def add_batch(self, items: List[T]) -> Dict[int, int]:
        counts = {}
        for item in items:
            idx = self.add(item)
            counts[idx] = counts.get(idx, 0) + 1
        return counts

    def get_bucket(self, bucket_id: int) -> List[T]:
        return self.buckets.get(bucket_id, [])

    def get_all_buckets(self) -> Dict[int, List[T]]:
        return dict(self.buckets)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "num_buckets": self.num_buckets,
            "total_items": sum(len(b) for b in self.buckets.values()),
            "bucket_sizes": {i: len(self.buckets[i]) for i in range(self.num_buckets)},
            "avg_size": sum(len(b) for b in self.buckets.values()) / self.num_buckets,
        }
