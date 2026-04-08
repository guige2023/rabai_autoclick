"""Data Slicing Action.

Slices data into chunks, windows, or partitions for processing.
"""
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from dataclasses import dataclass


T = TypeVar("T")


@dataclass
class Slice:
    index: int
    data: List[Any]
    start: int
    end: int
    metadata: Dict[str, Any]


class DataSlicingAction:
    """Slices data into chunks/windows for batch processing."""

    def __init__(self) -> None:
        self.stats = {"total_slices": 0, "total_items": 0}

    def chunk(
        self,
        data: List[T],
        chunk_size: int,
        drop_last: bool = False,
    ) -> List[List[T]]:
        if chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            if drop_last and len(chunk) < chunk_size:
                continue
            chunks.append(chunk)
            self.stats["total_slices"] += 1
            self.stats["total_items"] += len(chunk)
        return chunks

    def window(
        self,
        data: List[T],
        window_size: int,
        step: Optional[int] = None,
    ) -> List[List[T]]:
        if step is None:
            step = window_size
        windows = []
        for i in range(0, len(data) - window_size + 1, step):
            windows.append(data[i : i + window_size])
            self.stats["total_slices"] += 1
            self.stats["total_items"] += window_size
        return windows

    def stratified_slice(
        self,
        data: List[Dict[str, Any]],
        num_slices: int,
        stratify_by: str,
    ) -> List[List[Dict[str, Any]]]:
        if num_slices <= 0:
            raise ValueError("num_slices must be positive")
        buckets: Dict[str, List] = {}
        for item in data:
            key = str(item.get(stratify_by, "unknown"))
            buckets.setdefault(key, []).append(item)
        slices: List[List] = [[] for _ in range(num_slices)]
        for bucket_items in buckets.values():
            for i, item in enumerate(bucket_items):
                slices[i % num_slices].append(item)
        return slices

    def iterate_chunks(
        self,
        data: List[T],
        chunk_size: int,
    ) -> Iterator[Slice]:
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            yield Slice(
                index=i // chunk_size,
                data=chunk,
                start=i,
                end=min(i + chunk_size, len(data)),
                metadata={"is_first": i == 0, "is_last": i + chunk_size >= len(data)},
            )
            self.stats["total_slices"] += 1
            self.stats["total_items"] += len(chunk)

    def get_stats(self) -> Dict[str, Any]:
        return dict(self.stats)
