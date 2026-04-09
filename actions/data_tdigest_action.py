"""Data TDigest Action Module.

Provides t-digest based approximate quantile computation for large
datasets with support for streaming data, merge operations, and
accurate percentile estimates at distribution tails.
"""

from __future__ import annotations

import logging
import math
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class Centroid:
    """A centroid in t-digest structure."""
    mean: float
    weight: float = 1.0


class TDigest:
    """T-Digest data structure for quantile estimation."""

    MAX_BUFFER_SIZE = 100

    def __init__(self, compression: float = 100.0):
        self._compression = compression
        self._centroids: List[Centroid] = []
        self._buffer: List[float] = []
        self._min_value: Optional[float] = None
        self._max_value: Optional[float] = None
        self._total_weight: float = 0.0
        self._count: int = 0

    def _compression_factor(self) -> float:
        """Calculate compression factor based on compression parameter."""
        return 4.0 * math.log(self._compression)

    def add(self, value: float, weight: float = 1.0):
        """Add a value to the digest."""
        self._count += 1
        self._total_weight += weight

        if self._min_value is None or value < self._min_value:
            self._min_value = value
        if self._max_value is None or value > self._max_value:
            self._max_value = value

        self._buffer.append(value)
        if len(self._buffer) >= self.MAX_BUFFER_SIZE:
            self._merge_buffer()

    def _merge_buffer(self):
        """Merge buffered values into centroids."""
        if not self._buffer:
            return

        self._buffer.sort()
        self._centroids.sort(key=lambda c: c.mean)

        for value in self._buffer:
            self._insert_centroid(Centroid(mean=value, weight=1.0))

        self._compress()
        self._buffer.clear()

    def _insert_centroid(self, centroid: Centroid):
        """Insert a centroid into the list maintaining order."""
        if not self._centroids:
            self._centroids.append(centroid)
            return

        for i, c in enumerate(self._centroids):
            if centroid.mean < c.mean:
                self._centroids.insert(i, centroid)
                return

        self._centroids.append(centroid)

    def _compress(self):
        """Compress centroids to maintain size bound."""
        if len(self._centroids) <= self._compression:
            return

        scale = self._calculate_scale()
        new_centroids: List[Centroid] = []

        for centroid in self._centroids:
            if not new_centroids:
                new_centroids.append(centroid)
                continue

            last = new_centroids[-1]
            combined_weight = last.weight + centroid.weight

            if combined_weight <= scale:
                new_mean = (last.mean * last.weight + centroid.mean * centroid.weight) / combined_weight
                last.mean = new_mean
                last.weight = combined_weight
            else:
                new_centroids.append(centroid)

        self._centroids = new_centroids

    def _calculate_scale(self) -> float:
        """Calculate the scale factor for compression."""
        return self._compression_factor() * 2.0

    def quantile(self, q: float) -> float:
        """Estimate the q-th quantile of the distribution."""
        if q < 0 or q > 1:
            raise ValueError(f"Quantile must be between 0 and 1, got {q}")

        if not self._centroids and not self._buffer:
            raise ValueError("No data in t-digest")

        if self._buffer:
            self._merge_buffer()

        if len(self._centroids) == 1:
            return self._centroids[0].mean

        if q == 0.0:
            return self._min_value or 0.0
        if q == 1.0:
            return self._max_value or 0.0

        sorted_centroids = sorted(self._centroids, key=lambda c: c.mean)
        total_weight = sum(c.weight for c in sorted_centroids)
        rank = q * total_weight

        cumulative_weight = 0.0
        for i, centroid in enumerate(sorted_centroids):
            cumulative_weight += centroid.weight
            next_weight = sorted_centroids[i + 1].weight if i + 1 < len(sorted_centroids) else 0.0

            left_boundary = (cumulative_weight - centroid.weight) / total_weight
            right_boundary = (cumulative_weight + next_weight) / total_weight

            if left_boundary <= q <= right_boundary:
                t = (q - left_boundary) / max(right_boundary - left_boundary, 1e-10)
                return centroid.mean + t * (sorted_centroids[i + 1].mean - centroid.mean) if i + 1 < len(sorted_centroids) else centroid.mean

        return sorted_centroids[-1].mean

    def cdf(self, x: float) -> float:
        """Estimate the CDF (cumulative distribution function) at x."""
        if not self._centroids:
            if not self._buffer:
                return 0.0 if x < self._min_value else 1.0 if x > self._max_value else 0.5
            return 0.0

        sorted_centroids = sorted(self._centroids, key=lambda c: c.mean)
        total_weight = sum(c.weight for c in sorted_centroids)

        if x <= sorted_centroids[0].mean:
            weight_below = 0.0
        else:
            weight_below = sum(
                c.weight for c in sorted_centroids
                if c.mean < x
            )

        return min(1.0, max(0.0, weight_below / total_weight))

    def merge(self, other: "TDigest") -> "TDigest":
        """Merge another t-digest into this one."""
        merged = TDigest(self._compression)

        for centroid in self._centroids:
            merged._centroids.append(Centroid(mean=centroid.mean, weight=centroid.weight))

        for centroid in other._centroids:
            merged._insert_centroid(Centroid(mean=centroid.mean, weight=centroid.weight))

        for value in self._buffer:
            merged._buffer.append(value)
        for value in other._buffer:
            merged._buffer.append(value)

        merged._total_weight = self._total_weight + other._total_weight
        merged._count = self._count + other._count
        merged._min_value = min(
            self._min_value or float('inf'),
            other._min_value or float('inf')
        ) if (self._min_value is not None or other._min_value is not None) else None
        merged._max_value = max(
            self._max_value or float('-inf'),
            other._max_value or float('-inf')
        ) if (self._max_value is not None or other._max_value is not None) else None

        merged._merge_buffer()
        return merged

    def summary(self) -> Dict[str, Any]:
        """Get summary statistics of the digest."""
        return {
            "count": self._count,
            "total_weight": self._total_weight,
            "centroid_count": len(self._centroids),
            "buffer_size": len(self._buffer),
            "min": self._min_value,
            "max": self._max_value,
            "compression": self._compression
        }


class DataTDigestAction(BaseAction):
    """Action for t-digest quantile computation."""

    def __init__(self):
        super().__init__(name="data_tdigest")
        self._digests: Dict[str, TDigest] = {}
        self._lock = threading.Lock()
        self._default_compression = 100.0

    def create_digest(
        self,
        digest_id: str,
        compression: float = 100.0
    ) -> ActionResult:
        """Create a new t-digest."""
        try:
            with self._lock:
                if digest_id in self._digests:
                    return ActionResult(success=False, error=f"Digest {digest_id} already exists")

                self._digests[digest_id] = TDigest(compression)
                return ActionResult(success=True, data={"digest_id": digest_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add(
        self,
        digest_id: str,
        value: float,
        weight: float = 1.0
    ) -> ActionResult:
        """Add a value to a digest."""
        try:
            with self._lock:
                if digest_id not in self._digests:
                    self._digests[digest_id] = TDigest(self._default_compression)

                self._digests[digest_id].add(value, weight)
                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_batch(
        self,
        digest_id: str,
        values: List[float],
        weights: Optional[List[float]] = None
    ) -> ActionResult:
        """Add multiple values to a digest."""
        try:
            with self._lock:
                if digest_id not in self._digests:
                    self._digests[digest_id] = TDigest(self._default_compression)

                weights = weights or [1.0] * len(values)
                for value, weight in zip(values, weights):
                    self._digests[digest_id].add(value, weight)

                return ActionResult(success=True, data={"count": len(values)})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def quantile(
        self,
        digest_id: str,
        q: float
    ) -> ActionResult:
        """Get quantile estimate from a digest."""
        try:
            with self._lock:
                if digest_id not in self._digests:
                    return ActionResult(success=False, error=f"Digest {digest_id} not found")

                value = self._digests[digest_id].quantile(q)
                return ActionResult(success=True, data={"quantile": q, "value": value})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def quantiles(
        self,
        digest_id: str,
        qs: List[float]
    ) -> ActionResult:
        """Get multiple quantile estimates."""
        try:
            with self._lock:
                if digest_id not in self._digests:
                    return ActionResult(success=False, error=f"Digest {digest_id} not found")

                result = {}
                for q in qs:
                    try:
                        result[str(q)] = self._digests[digest_id].quantile(q)
                    except ValueError:
                        result[str(q)] = None

                return ActionResult(success=True, data={"quantiles": result})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def percentile_summary(
        self,
        digest_id: str
    ) -> ActionResult:
        """Get a summary with common percentiles."""
        try:
            with self._lock:
                if digest_id not in self._digests:
                    return ActionResult(success=False, error=f"Digest {digest_id} not found")

                percentiles = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
                summary = self._digests[digest_id].summary()

                result = {}
                for p in percentiles:
                    try:
                        result[f"p{int(p * 100)}"] = self._digests[digest_id].quantile(p)
                    except ValueError:
                        result[f"p{int(p * 100)}"] = None

                return ActionResult(success=True, data={
                    "summary": summary,
                    "percentiles": result
                })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def merge_digests(
        self,
        target_id: str,
        source_ids: List[str]
    ) -> ActionResult:
        """Merge multiple digests into a target digest."""
        try:
            with self._lock:
                if target_id not in self._digests:
                    self._digests[target_id] = TDigest(self._default_compression)

                for source_id in source_ids:
                    if source_id in self._digests:
                        self._digests[target_id] = self._digests[target_id].merge(
                            self._digests[source_id]
                        )

                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute t-digest operation."""
        try:
            action = params.get("action", "add")

            if action == "add":
                return self.add(
                    params["digest_id"],
                    params["value"],
                    params.get("weight", 1.0)
                )
            elif action == "add_batch":
                return self.add_batch(
                    params["digest_id"],
                    params["values"],
                    params.get("weights")
                )
            elif action == "quantile":
                return self.quantile(
                    params["digest_id"],
                    params["q"]
                )
            elif action == "percentiles":
                return self.percentile_summary(params["digest_id"])
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
