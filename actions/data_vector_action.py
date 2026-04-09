"""
Data Vector Action Module.

Provides vector operations including mathematical operations,
distance calculations, similarity measures, and vector indexing
for machine learning and similarity search applications.

Author: RabAi Team
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional


class DistanceMetric(Enum):
    """Vector distance/similarity metrics."""
    EUCLIDEAN = "euclidean"
    COSINE = "cosine"
    DOT_PRODUCT = "dot_product"
    MANHATTAN = "manhattan"
    CHEBYSHEV = "chebyshev"
    JACCARD = "jaccard"


@dataclass
class Vector:
    """N-dimensional vector representation."""
    data: list[float]
    id: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def dim(self) -> int:
        """Vector dimension."""
        return len(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, index: int) -> float:
        return self.data[index]

    def __add__(self, other: Vector) -> Vector:
        if self.dim != other.dim:
            raise ValueError(f"Dimension mismatch: {self.dim} vs {other.dim}")
        return Vector(data=[a + b for a, b in zip(self.data, other.data)])

    def __sub__(self, other: Vector) -> Vector:
        if self.dim != other.dim:
            raise ValueError(f"Dimension mismatch: {self.dim} vs {other.dim}")
        return Vector(data=[a - b for a, b in zip(self.data, other.data)])

    def __mul__(self, scalar: float) -> Vector:
        return Vector(data=[x * scalar for x in self.data])

    def __rmul__(self, scalar: float) -> Vector:
        return self * scalar

    def magnitude(self) -> float:
        """Compute magnitude (L2 norm)."""
        return math.sqrt(sum(x * x for x in self.data))

    def normalize(self) -> Vector:
        """Return normalized vector (unit length)."""
        mag = self.magnitude()
        if mag == 0:
            return Vector(data=[0.0] * self.dim)
        return Vector(data=[x / mag for x in self.data])


def euclidean_distance(a: Vector, b: Vector) -> float:
    """Compute Euclidean distance between two vectors."""
    if a.dim != b.dim:
        raise ValueError(f"Dimension mismatch: {a.dim} vs {b.dim}")
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a.data, b.data)))


def cosine_similarity(a: Vector, b: Vector) -> float:
    """Compute cosine similarity between two vectors."""
    if a.dim != b.dim:
        raise ValueError(f"Dimension mismatch: {a.dim} vs {b.dim}")
    dot = sum(x * y for x, y in zip(a.data, b.data))
    mag_a = a.magnitude()
    mag_b = b.magnitude()
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def cosine_distance(a: Vector, b: Vector) -> float:
    """Compute cosine distance (1 - similarity)."""
    return 1.0 - cosine_similarity(a, b)


def manhattan_distance(a: Vector, b: Vector) -> float:
    """Compute Manhattan (L1) distance."""
    if a.dim != b.dim:
        raise ValueError(f"Dimension mismatch: {a.dim} vs {b.dim}")
    return sum(abs(x - y) for x, y in zip(a.data, b.data))


def chebyshev_distance(a: Vector, b: Vector) -> float:
    """Compute Chebyshev (L-infinity) distance."""
    if a.dim != b.dim:
        raise ValueError(f"Dimension mismatch: {a.dim} vs {b.dim}")
    return max(abs(x - y) for x, y in zip(a.data, b.data))


def dot_product(a: Vector, b: Vector) -> float:
    """Compute dot product of two vectors."""
    if a.dim != b.dim:
        raise ValueError(f"Dimension mismatch: {a.dim} vs {b.dim}")
    return sum(x * y for x, y in zip(a.data, b.data))


def vector_distance(a: Vector, b: Vector, metric: DistanceMetric = DistanceMetric.EUCLIDEAN) -> float:
    """Compute distance using specified metric."""
    if metric == DistanceMetric.EUCLIDEAN:
        return euclidean_distance(a, b)
    elif metric == DistanceMetric.COSINE:
        return cosine_distance(a, b)
    elif metric == DistanceMetric.DOT_PRODUCT:
        return -dot_product(a, b)
    elif metric == DistanceMetric.MANHATTAN:
        return manhattan_distance(a, b)
    elif metric == DistanceMetric.CHEBYSHEV:
        return chebyshev_distance(a, b)
    raise ValueError(f"Unknown metric: {metric}")


class VectorStore:
    """In-memory vector storage with indexing."""

    def __init__(self, metric: DistanceMetric = DistanceMetric.EUCLIDEAN, thread_safe: bool = False):
        self.metric = metric
        self._vectors: list[Vector] = []
        self._lock = threading.RLock() if thread_safe else None

    def add(self, vector: Vector) -> int:
        """Add a vector to the store."""
        with self._get_lock():
            self._vectors.append(vector)
            return len(self._vectors) - 1

    def get(self, index: int) -> Optional[Vector]:
        """Get vector by index."""
        with self._get_lock():
            if 0 <= index < len(self._vectors):
                return self._vectors[index]
            return None

    def search(
        self,
        query: Vector,
        k: int = 5,
        filter_func: Optional[Callable[[Vector], bool]] = None,
    ) -> list[tuple[Vector, float]]:
        """Search for k nearest neighbors."""
        candidates = self._vectors
        if filter_func:
            candidates = [v for v in candidates if filter_func(v)]

        distances = [(v, vector_distance(query, v, self.metric)) for v in candidates]
        distances.sort(key=lambda x: x[1])
        return distances[:k]

    def search_by_id(self, vector_id: str, k: int = 5) -> list[tuple[Vector, float]]:
        """Search for similar vectors by ID."""
        target = next((v for v in self._vectors if v.id == vector_id), None)
        if not target:
            return []
        return self.search(target, k)

    def remove(self, index: int) -> bool:
        """Remove vector by index."""
        with self._get_lock():
            if 0 <= index < len(self._vectors):
                self._vectors.pop(index)
                return True
            return False

    def __len__(self) -> int:
        return len(self._vectors)

    def _get_lock(self):
        return self._lock or type("", (), {"__enter__": lambda s: None, "__exit__": lambda s, *a: None})()


class VectorIndex:
    """Simple flat index for vector search (brute force)."""

    def __init__(self, metric: DistanceMetric = DistanceMetric.EUCLIDEAN):
        self.metric = metric
        self._vectors: dict[str, Vector] = {}

    def upsert(self, vector_id: str, data: list[float], metadata: Optional[dict[str, Any]] = None) -> None:
        """Insert or update a vector."""
        self._vectors[vector_id] = Vector(data=data, id=vector_id, metadata=metadata or {})

    def search(self, query_data: list[float], k: int = 5) -> list[tuple[str, float, dict]]:
        """Search for k nearest neighbors by data."""
        query = Vector(data=query_data)
        results = []
        for vid, vec in self._vectors.items():
            dist = vector_distance(query, vec, self.metric)
            results.append((vid, dist, vec.metadata))
        results.sort(key=lambda x: x[1])
        return results[:k]

    def get(self, vector_id: str) -> Optional[Vector]:
        """Get vector by ID."""
        return self._vectors.get(vector_id)

    def delete(self, vector_id: str) -> bool:
        """Delete vector by ID."""
        if vector_id in self._vectors:
            del self._vectors[vector_id]
            return True
        return False

    def __len__(self) -> int:
        return len(self._vectors)


class VectorMath:
    """Vector mathematical operations."""

    @staticmethod
    def add(a: Vector, b: Vector) -> Vector:
        return a + b

    @staticmethod
    def subtract(a: Vector, b: Vector) -> Vector:
        return a - b

    @staticmethod
    def scale(v: Vector, scalar: float) -> Vector:
        return v * scalar

    @staticmethod
    def mean(vectors: list[Vector]) -> Vector:
        """Compute element-wise mean."""
        if not vectors:
            raise ValueError("Cannot compute mean of empty list")
        dim = vectors[0].dim
        return Vector(
            data=[sum(v[i] for v in vectors) / len(vectors) for i in range(dim)]
        )

    @staticmethod
    def centroid(vectors: list[Vector]) -> Vector:
        """Compute centroid of vectors."""
        return VectorMath.mean(vectors)

    @staticmethod
    def variance(vectors: list[Vector]) -> float:
        """Compute total variance."""
        if not vectors:
            return 0.0
        c = VectorMath.centroid(vectors)
        return sum(euclidean_distance(v, c) ** 2 for v in vectors) / len(vectors)

    @staticmethod
    def standard_deviation(vectors: list[Vector]) -> float:
        """Compute standard deviation from centroid."""
        return math.sqrt(VectorMath.variance(vectors))


def create_vector(data: list[float], id: Optional[str] = None, metadata: Optional[dict] = None) -> Vector:
    """Create a new vector."""
    return Vector(data=data, id=id, metadata=metadata or {})


def batch_create_vectors(data_list: list[list[float]]) -> list[Vector]:
    """Create multiple vectors."""
    return [Vector(data=d) for d in data_list]


async def demo():
    """Demo vector operations."""
    v1 = create_vector([1.0, 2.0, 3.0], id="v1")
    v2 = create_vector([4.0, 5.0, 6.0], id="v2")

    print(f"v1 + v2 = {(v1 + v2).data}")
    print(f"v1 magnitude: {v1.magnitude():.4f}")
    print(f"cosine similarity: {cosine_similarity(v1, v2):.4f}")

    store = VectorStore(metric=DistanceMetric.COSINE)
    store.add(v1)
    store.add(v2)
    v3 = create_vector([1.1, 2.1, 3.1], id="v3")
    store.add(v3)

    results = store.search(v1, k=2)
    print(f"Nearest to v1: {[(r.id, d) for r, d in results]}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo())
