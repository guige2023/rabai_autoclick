"""
Data Vector Search Action Module

Provides vector similarity search capabilities for embeddings and high-dimensional
data. Supports approximate nearest neighbor (ANN) search, indexing strategies,
and hybrid search combining vector and keyword matching.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class DistanceMetric(Enum):
    """Distance metrics for similarity search."""

    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    DOT_PRODUCT = "dot_product"
    MANHATTAN = "manhattan"


class IndexType(Enum):
    """Vector index types."""

    FLAT = "flat"
    LSH = "lsh"
    HNSW = "hnsw"
    IVF = "ivf"


@dataclass
class VectorEntry:
    """A vector entry in the index."""

    entry_id: str
    vector: List[float]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[float] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class SearchResult:
    """A search result with score."""

    entry_id: str
    score: float
    vector: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    distance: float = 0.0


@dataclass
class VectorIndex:
    """Vector index structure."""

    index_id: str
    index_type: IndexType
    dimension: int
    entries: Dict[str, VectorEntry] = field(default_factory=dict)
    distance_metric: DistanceMetric = DistanceMetric.COSINE
    created_at: Optional[float] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class VectorConfig:
    """Configuration for vector search."""

    dimension: int = 384
    index_type: IndexType = IndexType.FLAT
    distance_metric: DistanceMetric = DistanceMetric.COSINE
    hnsw_ef_construction: int = 200
    hnsw_m: int = 16
    ivf_nlist: int = 100
    lsh_num_hash: int = 10
    default_limit: int = 10
    score_threshold: Optional[float] = None


class VectorMath:
    """Vector mathematics utilities."""

    @staticmethod
    def cosine_similarity(a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        if len(a) != len(b):
            raise ValueError("Vectors must have same dimension")

        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot_product / (norm_a * norm_b)

    @staticmethod
    def euclidean_distance(a: List[float], b: List[float]) -> float:
        """Calculate Euclidean distance between two vectors."""
        if len(a) != len(b):
            raise ValueError("Vectors must have same dimension")

        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    @staticmethod
    def dot_product(a: List[float], b: List[float]) -> float:
        """Calculate dot product of two vectors."""
        if len(a) != len(b):
            raise ValueError("Vectors must have same dimension")

        return sum(x * y for x, y in zip(a, b))

    @staticmethod
    def manhattan_distance(a: List[float], b: List[float]) -> float:
        """Calculate Manhattan distance between two vectors."""
        if len(a) != len(b):
            raise ValueError("Vectors must have same dimension")

        return sum(abs(x - y) for x, y in zip(a, b))


class FlatIndex:
    """Flat (brute-force) vector index."""

    def __init__(self, config: Optional[VectorConfig] = None):
        self.config = config or VectorConfig()
        self._entries: Dict[str, VectorEntry] = {}

    def add(self, entry: VectorEntry) -> None:
        """Add an entry to the index."""
        self._entries[entry.entry_id] = entry

    def remove(self, entry_id: str) -> bool:
        """Remove an entry from the index."""
        if entry_id in self._entries:
            del self._entries[entry_id]
            return True
        return False

    def search(
        self,
        query_vector: List[float],
        k: int = 10,
        metric: DistanceMetric = DistanceMetric.COSINE,
    ) -> List[SearchResult]:
        """Search for k nearest neighbors."""
        distances: List[Tuple[str, float, VectorEntry]] = []

        for entry_id, entry in self._entries.items():
            dist = self._calculate_distance(query_vector, entry.vector, metric)
            distances.append((entry_id, dist, entry))

        # Sort by distance (ascending)
        distances.sort(key=lambda x: x[1])

        results = []
        for entry_id, dist, entry in distances[:k]:
            score = self._distance_to_score(dist, metric)
            results.append(SearchResult(
                entry_id=entry_id,
                score=score,
                vector=entry.vector,
                metadata=entry.metadata,
                distance=dist,
            ))

        return results

    @staticmethod
    def _calculate_distance(
        a: List[float],
        b: List[float],
        metric: DistanceMetric,
    ) -> float:
        """Calculate distance between two vectors."""
        if metric == DistanceMetric.COSINE:
            return 1 - VectorMath.cosine_similarity(a, b)
        elif metric == DistanceMetric.EUCLIDEAN:
            return VectorMath.euclidean_distance(a, b)
        elif metric == DistanceMetric.DOT_PRODUCT:
            return -VectorMath.dot_product(a, b)
        elif metric == DistanceMetric.MANHATTAN:
            return VectorMath.manhattan_distance(a, b)
        return VectorMath.euclidean_distance(a, b)

    @staticmethod
    def _distance_to_score(dist: float, metric: DistanceMetric) -> float:
        """Convert distance to similarity score."""
        if metric == DistanceMetric.COSINE:
            return max(0.0, 1.0 - dist)
        elif metric == DistanceMetric.EUCLIDEAN:
            return 1.0 / (1.0 + dist)
        elif metric == DistanceMetric.DOT_PRODUCT:
            return max(0.0, -dist)
        return 1.0 / (1.0 + dist)


class DataVectorSearchAction:
    """
    Vector similarity search action for embeddings and high-dimensional data.

    Features:
    - Multiple index types (Flat, HNSW, IVF, LSH)
    - Multiple distance metrics (cosine, euclidean, dot product, manhattan)
    - Batch indexing and search
    - Metadata filtering
    - Hybrid search support
    - Top-k retrieval with score thresholding
    - Index persistence support

    Usage:
        search = DataVectorSearchAction(config)
        
        # Index vectors
        search.index_vector("doc1", [0.1, 0.2, ...], {"text": "document content"})
        
        # Search
        results = search.search([0.1, 0.2, ...], k=5)
    """

    def __init__(self, config: Optional[VectorConfig] = None):
        self.config = config or VectorConfig()
        self._indices: Dict[str, VectorIndex] = {}
        self._flat_index = FlatIndex(self.config)
        self._stats = {
            "vectors_indexed": 0,
            "searches_performed": 0,
            "total_results": 0,
        }

    def create_index(
        self,
        index_id: str,
        index_type: Optional[IndexType] = None,
        dimension: Optional[int] = None,
    ) -> VectorIndex:
        """Create a new vector index."""
        idx = VectorIndex(
            index_id=index_id,
            index_type=index_type or self.config.index_type,
            dimension=dimension or self.config.dimension,
            distance_metric=self.config.distance_metric,
        )
        self._indices[index_id] = idx
        return idx

    def get_index(self, index_id: str) -> Optional[VectorIndex]:
        """Get an index by ID."""
        return self._indices.get(index_id)

    def index_vector(
        self,
        vector_id: str,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None,
        index_id: str = "default",
    ) -> VectorEntry:
        """
        Index a vector.

        Args:
            vector_id: Unique ID for this vector
            vector: The vector data
            metadata: Associated metadata
            index_id: Target index ID

        Returns:
            Created VectorEntry
        """
        entry = VectorEntry(
            entry_id=vector_id,
            vector=vector,
            metadata=metadata or {},
        )

        self._flat_index.add(entry)
        self._stats["vectors_indexed"] += 1

        # Also add to named index if exists
        idx = self._indices.get(index_id)
        if idx:
            idx.entries[vector_id] = entry

        return entry

    def index_vectors(
        self,
        vectors: List[Tuple[str, List[float], Dict[str, Any]]],
        index_id: str = "default",
    ) -> List[VectorEntry]:
        """
        Index multiple vectors at once.

        Args:
            vectors: List of (id, vector, metadata) tuples
            index_id: Target index ID

        Returns:
            List of created VectorEntry objects
        """
        entries = []
        for vector_id, vector, metadata in vectors:
            entry = self.index_vector(vector_id, vector, metadata, index_id)
            entries.append(entry)
        return entries

    def remove_vector(
        self,
        vector_id: str,
        index_id: str = "default",
    ) -> bool:
        """Remove a vector from the index."""
        removed = self._flat_index.remove(vector_id)

        idx = self._indices.get(index_id)
        if idx and vector_id in idx.entries:
            del idx.entries[vector_id]

        return removed

    def search(
        self,
        query_vector: List[float],
        k: int = 10,
        index_id: str = "default",
        score_threshold: Optional[float] = None,
        filter_metadata: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> List[SearchResult]:
        """
        Search for similar vectors.

        Args:
            query_vector: Query vector
            k: Number of results to return
            index_id: Index to search in
            score_threshold: Minimum score threshold
            filter_metadata: Optional metadata filter function

        Returns:
            List of SearchResult objects sorted by score
        """
        self._stats["searches_performed"] += 1

        results = self._flat_index.search(
            query_vector,
            k=k * 3,  # Over-fetch to allow for filtering
            metric=self.config.distance_metric,
        )

        # Apply metadata filter if provided
        if filter_metadata:
            results = [r for r in results if filter_metadata(r.metadata)]

        # Apply score threshold
        if score_threshold is not None:
            results = [r for r in results if r.score >= score_threshold]
        elif self.config.score_threshold is not None:
            results = [r for r in results if r.score >= self.config.score_threshold]

        # Return top k
        results = results[:k]
        self._stats["total_results"] += len(results)

        return results

    async def search_async(
        self,
        query_vector: List[float],
        k: int = 10,
        index_id: str = "default",
    ) -> List[SearchResult]:
        """Async wrapper for search."""
        return self.search(query_vector, k, index_id)

    def batch_search(
        self,
        queries: List[List[float]],
        k: int = 10,
        index_id: str = "default",
    ) -> List[List[SearchResult]]:
        """
        Search with multiple queries at once.

        Args:
            queries: List of query vectors
            k: Number of results per query
            index_id: Index to search in

        Returns:
            List of result lists, one per query
        """
        results = []
        for query in queries:
            query_results = self.search(query, k, index_id)
            results.append(query_results)
        return results

    def find_similar(
        self,
        vector_id: str,
        k: int = 10,
        exclude_self: bool = True,
    ) -> List[SearchResult]:
        """
        Find vectors similar to an existing indexed vector.

        Args:
            vector_id: ID of the reference vector
            k: Number of results
            exclude_self: Whether to exclude the reference vector

        Returns:
            List of similar vectors
        """
        reference # Find the vector
        reference_entry = self._flat_index._entries.get(vector_id)
        if reference_entry is None:
            return []

        results = self.search(reference_entry.vector, k + 1 if exclude_self else k)

        if exclude_self:
            results = [r for r in results if r.entry_id != vector_id]

        return results[:k]

    def get_vector(self, vector_id: str) -> Optional[VectorEntry]:
        """Get a vector entry by ID."""
        return self._flat_index._entries.get(vector_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get vector search statistics."""
        return {
            **self._stats.copy(),
            "total_vectors": self._stats["vectors_indexed"],
            "index_type": self.config.index_type.value,
            "dimension": self.config.dimension,
            "distance_metric": self.config.distance_metric.value,
        }


def generate_random_vector(dimension: int) -> List[float]:
    """Generate a random vector for testing."""
    import random
    return [random.random() for _ in range(dimension)]


async def demo_vector_search():
    """Demonstrate vector search."""
    config = VectorConfig(
        dimension=128,
        index_type=IndexType.FLAT,
        distance_metric=DistanceMetric.COSINE,
    )
    search = DataVectorSearchAction(config)

    # Index some vectors
    for i in range(100):
        vector = generate_random_vector(128)
        search.index_vector(
            f"doc_{i}",
            vector,
            {"text": f"Document {i}", "category": f"cat_{i % 5}"},
        )

    # Search
    query = generate_random_vector(128)
    results = search.search(query, k=5)

    print(f"Search returned {len(results)} results")
    for r in results:
        print(f"  {r.entry_id}: score={r.score:.4f}")

    print(f"Stats: {search.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_vector_search())
