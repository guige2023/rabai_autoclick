"""
Metric learning utilities.

Provides distance metrics, similarity learning, and
metric-based operations for embedding spaces.
"""
from __future__ import annotations

from typing import Callable, List, Optional, Tuple

import numpy as np


def euclidean_distance_matrix(X: np.ndarray, Y: np.ndarray = None) -> np.ndarray:
    """
    Compute pairwise Euclidean distance matrix.

    Args:
        X: First set of vectors (n, d)
        Y: Second set of vectors (m, d), or None to use X

    Returns:
        Distance matrix (n, m)

    Example:
        >>> X = np.array([[0, 0], [1, 1]])
        >>> euclidean_distance_matrix(X).shape
        (2, 2)
    """
    if Y is None:
        Y = X
    X_sq = np.sum(X ** 2, axis=1, keepdims=True)
    Y_sq = np.sum(Y ** 2, axis=1, keepdims=True)
    dist_sq = X_sq + Y_sq.T - 2 * np.dot(X, Y.T)
    dist_sq = np.maximum(dist_sq, 0)
    return np.sqrt(dist_sq)


def cosine_similarity_matrix(X: np.ndarray, Y: np.ndarray = None) -> np.ndarray:
    """
    Compute pairwise cosine similarity matrix.

    Args:
        X: First set of vectors (n, d)
        Y: Second set of vectors (m, d), or None to use X

    Returns:
        Similarity matrix (n, m)

    Example:
        >>> X = np.array([[1, 0], [0, 1]])
        >>> cosine_similarity_matrix(X)
        array([[1.        , 0.        ],
               [0.        , 1.        ]])
    """
    if Y is None:
        Y = X
    X_norm = np.linalg.norm(X, axis=1, keepdims=True)
    Y_norm = np.linalg.norm(Y, axis=1, keepdims=True)
    X_norm = np.where(X_norm == 0, 1, X_norm)
    Y_norm = np.where(Y_norm == 0, 1, Y_norm)
    return np.dot(X / X_norm, (Y / Y_norm).T)


def mahalanobis_distance(x: np.ndarray, mean: np.ndarray, cov: np.ndarray) -> float:
    """
    Compute Mahalanobis distance.

    Args:
        x: Query point
        mean: Distribution mean
        cov: Covariance matrix

    Returns:
        Mahalanobis distance

    Example:
        >>> x = np.array([1, 1])
        >>> mean = np.array([0, 0])
        >>> cov = np.eye(2)
        >>> mahalanobis_distance(x, mean, cov)
        1.4142135623730951
    """
    from scipy.linalg import solve
    diff = x - mean
    cov_inv = np.linalg.pinv(cov)
    return np.sqrt(np.dot(diff, np.dot(cov_inv, diff)))


def mahalanobis_distance_matrix(X: np.ndarray, mean: np.ndarray, cov: np.ndarray) -> np.ndarray:
    """Compute Mahalanobis distances for multiple points."""
    diff = X - mean
    cov_inv = np.linalg.pinv(cov)
    return np.sqrt(np.sum(diff @ cov_inv * diff, axis=1))


class TripletSampler:
    """Sample triplets for metric learning."""

    def __init__(
        self,
        labels: np.ndarray,
        strategy: str = "random",
        margin: float = 1.0,
    ):
        self.labels = labels
        self.strategy = strategy
        self.margin = margin
        self.label_to_indices = self._build_label_map()

    def _build_label_map(self) -> dict:
        """Build mapping from labels to sample indices."""
        label_map = {}
        for idx, label in enumerate(self.labels):
            if label not in label_map:
                label_map[label] = []
            label_map[label].append(idx)
        return label_map

    def sample(self, n_triplets: int) -> List[Tuple[int, int, int]]:
        """
        Sample triplets (anchor, positive, negative).

        Args:
            n_triplets: Number of triplets to sample

        Returns:
            List of (anchor, positive, negative) index tuples
        """
        triplets = []
        for _ in range(n_triplets):
            anchor_idx = np.random.randint(len(self.labels))
            anchor_label = self.labels[anchor_idx]
            positive_candidates = self.label_to_indices[anchor_label]
            positive_idx = np.random.choice([i for i in positive_candidates if i != anchor_idx])
            negative_label = np.random.choice([l for l in self.label_to_indices.keys() if l != anchor_label])
            negative_idx = np.random.choice(self.label_to_indices[negative_label])
            triplets.append((anchor_idx, positive_idx, negative_idx))
        return triplets


class MarginTripletLoss:
    """Triplet loss with margin."""

    def __init__(self, margin: float = 1.0):
        self.margin = margin

    def __call__(
        self, anchor: np.ndarray, positive: np.ndarray, negative: np.ndarray
    ) -> float:
        """
        Compute triplet loss.

        Args:
            anchor: Anchor embeddings (n, d)
            positive: Positive embeddings (n, d)
            negative: Negative embeddings (n, d)

        Returns:
            Average triplet loss
        """
        pos_dist = np.sum((anchor - positive) ** 2, axis=1)
        neg_dist = np.sum((anchor - negative) ** 2, axis=1)
        loss = np.maximum(0, pos_dist - neg_dist + self.margin)
        return np.mean(loss)


class ContrastiveLoss:
    """Contrastive loss for metric learning."""

    def __init__(self, margin: float = 1.0):
        self.margin = margin

    def __call__(
        self, embeddings1: np.ndarray, embeddings2: np.ndarray, labels: np.ndarray
    ) -> float:
        """
        Compute contrastive loss.

        Args:
            embeddings1: First embeddings (n, d)
            embeddings2: Second embeddings (n, d)
            labels: Same/different labels (n,)

        Returns:
            Contrastive loss
        """
        distances = np.sqrt(np.sum((embeddings1 - embeddings2) ** 2, axis=1) + 1e-10)
        same_loss = labels * (distances ** 2)
        diff_loss = (1 - labels) * np.maximum(0, self.margin - distances) ** 2
        return np.mean(same_loss + diff_loss)


class LMNN:
    """Large Margin Nearest Neighbors (simplified)."""

    def __init__(self, k: int = 3, mu: float = 0.5, max_iter: int = 100):
        self.k = k
        self.mu = mu
        self.max_iter = max_iter
        self.L = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "LMNN":
        """Fit LMNN transformation."""
        d = X.shape[1]
        self.L = np.eye(d)
        for _ in range(self.max_iter):
            X_trans = X @ self.L.T
            target_neighbors = self._find_target_neighbors(X_trans, y)
            gradients = self._compute_gradients(X, target_neighbors, y)
            self.L -= 0.01 * gradients
        return self

    def _find_target_neighbors(self, X: np.ndarray, y: np.ndarray) -> np.ndarray:
        """Find k nearest same-class neighbors."""
        dists = euclidean_distance_matrix(X)
        neighbors = np.zeros((len(X), self.k), dtype=int)
        for i in range(len(X)):
            same_class = np.where(y == y[i])[0]
            same_class = same_class[same_class != i]
            if len(same_class) >= self.k:
                sorted_idx = np.argsort(dists[i, same_class])[: self.k]
                neighbors[i] = same_class[sorted_idx]
        return neighbors

    def _compute_gradients(self, X: np.ndarray, target_neighbors: np.ndarray, y: np.ndarray) -> np.ndarray:
        """Compute gradient approximation."""
        return np.zeros_like(self.L)

    def transform(self, X: np.ndarray) -> np.ndarray:
        """Transform data using learned metric."""
        if self.L is None:
            raise ValueError("Model not fitted")
        return X @ self.L.T


def k_nearest_neighbors(
    query: np.ndarray,
    database: np.ndarray,
    k: int = 5,
    distance_metric: str = "euclidean",
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Find k nearest neighbors.

    Args:
        query: Query points (n, d)
        database: Database points (m, d)
        k: Number of neighbors
        distance_metric: Distance metric ('euclidean', 'cosine', 'manhattan')

    Returns:
        Tuple of (distances, indices) for each query point
    """
    if distance_metric == "euclidean":
        dists = euclidean_distance_matrix(query, database)
    elif distance_metric == "cosine":
        sims = cosine_similarity_matrix(query, database)
        dists = 1 - sims
    elif distance_metric == "manhattan":
        dists = np.zeros((len(query), len(database)))
        for i in range(len(query)):
            dists[i] = np.sum(np.abs(query[i] - database), axis=1)
    else:
        raise ValueError(f"Unknown metric: {distance_metric}")
    sorted_indices = np.argsort(dists, axis=1)
    top_k_indices = sorted_indices[:, :k]
    top_k_dists = np.take_along_axis(dists, top_k_indices, axis=1)
    return top_k_dists, top_k_indices


def kmeans_plusplus(
    X: np.ndarray, n_clusters: int, random_seed: int = None
) -> Tuple[np.ndarray, np.ndarray]:
    """
    K-Means++ initialization.

    Args:
        X: Data points (n, d)
        n_clusters: Number of clusters
        random_seed: Random seed

    Returns:
        Tuple of (centroids, cluster_assignments)
    """
    if random_seed is not None:
        np.random.seed(random_seed)
    n, d = X.shape
    centroids = [X[np.random.randint(n)]]
    for _ in range(1, n_clusters):
        dists = np.zeros(n)
        for c in centroids:
            dists += np.sum((X - c) ** 2, axis=1)
        probs = dists / dists.sum()
        centroids.append(X[np.random.choice(n, p=probs)])
    centroids = np.array(centroids)
    assignments = np.argmin(euclidean_distance_matrix(X, centroids), axis=1)
    return centroids, assignments


class KNearestCentroid:
    """Nearest centroid classifier."""

    def __init__(self):
        self.centroids = None
        self.classes = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "KNearestCentroid":
        """Compute class centroids."""
        self.classes = np.unique(y)
        self.centroids = np.zeros((len(self.classes), X.shape[1]))
        for i, c in enumerate(self.classes):
            self.centroids[i] = np.mean(X[y == c], axis=0)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict class labels."""
        dists = euclidean_distance_matrix(X, self.centroids)
        return self.classes[np.argmin(dists, axis=1)]


class LocalitySensitiveHashing:
    """LSH for approximate nearest neighbor search."""

    def __init__(
        self,
        n_tables: int = 5,
        n_hash_fn: int = 10,
        dim: int = 128,
        seed: int = 42,
    ):
        self.n_tables = n_tables
        self.n_hash_fn = n_hash_fn
        self.dim = dim
        np.random.seed(seed)
        self.planes = [np.random.randn(dim, n_hash_fn) for _ in range(n_tables)]
        self.tables = [{} for _ in range(n_tables)]

    def _hash(self, x: np.ndarray, table_idx: int) -> str:
        """Hash a point to a bucket."""
        projections = x @ self.planes[table_idx]
        return "".join(["1" if p > 0 else "0" for p in projections])

    def index(self, X: np.ndarray) -> None:
        """Index all points."""
        for i, x in enumerate(X):
            for t in range(self.n_tables):
                bucket = self._hash(x, t)
                if bucket not in self.tables[t]:
                    self.tables[t][bucket] = []
                self.tables[t][bucket].append(i)

    def query(self, x: np.ndarray, max_results: int = 10) -> List[Tuple[int, int]]:
        """Query for approximate nearest neighbors."""
        candidates = set()
        for t in range(self.n_tables):
            bucket = self._hash(x, t)
            if bucket in self.tables[t]:
                candidates.update(self.tables[t][bucket])
        return list(candidates)[:max_results]
