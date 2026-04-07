"""
Clustering algorithms and utilities.

Provides K-means, hierarchical clustering, DBSCAN, and silhouette scoring.
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from typing import Callable


Point = list[float]
Points = list[Point]


def euclidean_distance(a: Point, b: Point) -> float:
    """Euclidean distance between two points."""
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def manhattan_distance(a: Point, b: Point) -> float:
    """Manhattan (L1) distance between two points."""
    return sum(abs(x - y) for x, y in zip(a, b))


def cosine_distance(a: Point, b: Point) -> float:
    """Cosine distance (1 - cosine similarity) between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 1.0
    return 1.0 - dot / (norm_a * norm_b)


class KMeans:
    """K-means clustering algorithm."""

    def __init__(
        self,
        k: int = 3,
        max_iter: int = 100,
        tol: float = 1e-4,
        distance: Callable[[Point, Point], float] | None = None,
        seed: int | None = None,
    ):
        self.k = k
        self.max_iter = max_iter
        self.tol = tol
        self.distance = distance or euclidean_distance
        self.centroids: list[Point] = []
        self.labels: list[int] = []
        self._rng = random.Random(seed)

    def fit(self, points: Points) -> list[int]:
        """
        Fit K-means to points.

        Args:
            points: List of n-dimensional points

        Returns:
            List of cluster labels (0 to k-1) for each point.
        """
        n = len(points)
        if n == 0:
            return []

        # Initialize centroids randomly
        indices = self._rng.sample(range(n), min(self.k, n))
        self.centroids = [list(points[i]) for i in indices]

        self.labels = [0] * n

        for _ in range(self.max_iter):
            # Assign points to nearest centroid
            new_labels = []
            for p in points:
                dists = [self.distance(p, c) for c in self.centroids]
                new_labels.append(math.argmin(dists))

            # Compute new centroids
            new_centroids: list[Point | None] = [None] * self.k
            counts = [0] * self.k
            for p, label in zip(points, new_labels):
                if new_centroids[label] is None:
                    new_centroids[label] = [0.0] * len(p)
                for i, v in enumerate(p):
                    new_centroids[label][i] += v
                counts[label] += 1

            for j in range(self.k):
                if counts[j] > 0:
                    new_centroids[j] = [v / counts[j] for v in new_centroids[j]]

            # Check convergence
            shift = 0.0
            for c, nc in zip(self.centroids, new_centroids):
                if nc is None:
                    continue
                shift = max(shift, self.distance(c, nc))

            self.centroids = [c for c in new_centroids if c is not None]
            self.labels = new_labels

            if shift < self.tol:
                break

        # Handle empty clusters - reassign to nearest
        empty_clusters = set(range(self.k)) - set(self.labels)
        for i, label in enumerate(self.labels):
            if label in empty_clusters and self.centroids:
                dists = [self.distance(points[i], c) for c in self.centroids]
                self.labels[i] = math.argmin(dists)

        return self.labels


class DBSCAN:
    """DBSCAN density-based clustering."""

    def __init__(
        self,
        eps: float = 0.5,
        min_samples: int = 5,
        distance: Callable[[Point, Point], float] | None = None,
    ):
        self.eps = eps
        self.min_samples = min_samples
        self.distance = distance or euclidean_distance
        self.labels: list[int] = []
        self.noise_label: int = -1

    def fit(self, points: Points) -> list[int]:
        """
        Fit DBSCAN to points.

        Args:
            points: List of n-dimensional points

        Returns:
            List of cluster labels (-1 for noise, >=0 for cluster id).
        """
        n = len(points)
        self.labels = [self.noise_label] * n
        cluster_id = 0

        def region_query(idx: int) -> list[int]:
            return [i for i in range(n) if i != idx and self.distance(points[i], points[idx]) <= self.eps]

        for i in range(n):
            if self.labels[i] != self.noise_label:
                continue
            neighbors = region_query(i)
            if len(neighbors) < self.min_samples:
                continue
            # Start a new cluster
            self.labels[i] = cluster_id
            seed_set = set(neighbors)
            seed_set.discard(i)
            while seed_set:
                j = seed_set.pop()
                if self.labels[j] == self.noise_label:
                    self.labels[j] = cluster_id
                if self.labels[j] != self.noise_label and self.labels[j] != cluster_id:
                    continue
                self.labels[j] = cluster_id
                j_neighbors = region_query(j)
                if len(j_neighbors) >= self.min_samples:
                    seed_set.update(j_neighbors)
            cluster_id += 1

        return self.labels


class HierarchicalClustering:
    """Agglomerative hierarchical clustering."""

    def __init__(
        self,
        n_clusters: int = 3,
        linkage: str = "single",
        distance: Callable[[Point, Point], float] | None = None,
    ):
        self.n_clusters = n_clusters
        self.linkage = linkage
        self.distance = distance or euclidean_distance
        self.labels: list[int] = []

    def fit(self, points: Points) -> list[int]:
        """
        Agglomerative hierarchical clustering with minimax linkage.

        Args:
            points: List of n-dimensional points

        Returns:
            Cluster labels for each point.
        """
        n = len(points)
        if n == 0:
            return []
        if self.n_clusters >= n:
            self.labels = list(range(n))
            return self.labels

        clusters: list[set[int]] = [{i} for i in range(n)]
        dist_matrix: dict[tuple[int, int], float] = {}

        def get_dist(i: int, j: int) -> float:
            key = (min(i, j), max(i, j))
            if key not in dist_matrix:
                dist_matrix[key] = self.distance(points[i], points[j])
            return dist_matrix[key]

        while len(clusters) > self.n_clusters:
            min_dist = float("inf")
            merge = (0, 1)
            for i in range(len(clusters)):
                for j in range(i + 1, len(clusters)):
                    if self.linkage == "single":
                        d = min(get_dist(a, b) for a in clusters[i] for b in clusters[j])
                    elif self.linkage == "complete":
                        d = max(get_dist(a, b) for a in clusters[i] for b in clusters[j])
                    else:  # average
                        d = sum(get_dist(a, b) for a in clusters[i] for b in clusters[j]) / (len(clusters[i]) * len(clusters[j]))
                    if d < min_dist:
                        min_dist = d
                        merge = (i, j)

        i, j = merge
            clusters[i] = clusters[i] | clusters[j]
            del clusters[j]

        label_map = {cid: label for label, cluster in enumerate(clusters) for cid in cluster}
        self.labels = [label_map[i] for i in range(n)]
        return self.labels


def silhouette_score(points: Points, labels: list[int]) -> float:
    """
    Compute the silhouette score for clustered data.

    Args:
        points: List of n-dimensional points
        labels: Cluster labels

    Returns:
        Mean silhouette coefficient in [-1, 1].
    """
    n = len(points)
    if n < 2:
        return 0.0

    unique_labels = set(labels)
    if len(unique_labels) < 2:
        return 0.0
    if len(unique_labels) == n:
        return 0.0

    dist_cache: dict[tuple[int, int], float] = {}

    def get_dist(i: int, j: int) -> float:
        key = (min(i, j), max(i, j))
        if key not in dist_cache:
            dist_cache[key] = euclidean_distance(points[i], points[j])
        return dist_cache[key]

    a = [0.0] * n
    b = [0.0] * n

    for i in range(n):
        label_i = labels[i]
        same = [j for j in range(n) if j != i and labels[j] == label_i]
        other = [j for j in range(n) if labels[j] != label_i]
        if same:
            a[i] = sum(get_dist(i, j) for j in same) / len(same)
        if other:
            by_label: dict[int, list[int]] = defaultdict(list)
            for j in other:
                by_label[labels[j]].append(j)
            b[i] = min(sum(get_dist(i, j) for j in grp) / len(grp) for grp in by_label.values())

    scores = []
    for i in range(n):
        s = b[i] - a[i]
        d = max(a[i], b[i])
        if d > 0:
            scores.append(s / d)
        else:
            scores.append(0.0)

    return sum(scores) / n
