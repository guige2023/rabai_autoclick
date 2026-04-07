"""
Advanced clustering algorithms v2.

Extends clustering_utils.py with GMM, spectral clustering,
OPTICS, mean shift, and cluster validation indices.
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from typing import Callable


Point = list[float]
Points = list[Point]


def euclidean_dist(a: Point, b: Point) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


class GaussianMixtureModel:
    """GMM clustering using EM algorithm."""

    def __init__(
        self,
        n_clusters: int = 3,
        max_iter: int = 100,
        tol: float = 1e-4,
        seed: int | None = None,
    ):
        self.k = n_clusters
        self.max_iter = max_iter
        self.tol = tol
        self.rng = random.Random(seed)
        self.means: list[Point] = []
        self.covariances: list[list[list[float]]] = []
        self.weights: list[float] = []
        self.labels: list[int] = []

    def _init_params(self, points: Points) -> None:
        n = len(points)
        indices = self.rng.sample(range(n), self.k)
        self.means = [list(points[i]) for i in indices]
        dim = len(points[0])
        var_init = sum(euclidean_dist(p, self.means[0]) for p in points) / n
        self.covariances = [
            [[var_init if i == j else 0.0 for j in range(dim)] for i in range(dim)]
            for _ in range(self.k)
        ]
        self.weights = [1.0 / self.k] * self.k

    def _gaussian_pdf(
        self, x: Point, mean: Point, cov: list[list[float]]
    ) -> float:
        dim = len(x)
        diff = [x[i] - mean[i] for i in range(dim)]
        # Compute (x-mu)^T cov^-1 (x-mu)
        cov_inv = [[0.0] * dim for _ in range(dim)]
        det = sum(cov[i][i] for i in range(dim))
        if abs(det) < 1e-12:
            det = 1e-12
        for i in range(dim):
            cov_inv[i][i] = 1.0 / (cov[i][i] + 1e-12)
        mahal = sum(diff[i] * cov_inv[i][j] * diff[j] for i in range(dim) for j in range(dim))
        norm_const = 1.0 / ((2.0 * math.pi) ** (dim / 2) * math.sqrt(abs(det)) + 1e-12)
        return norm_const * math.exp(-0.5 * mahal)

    def fit(self, points: Points) -> list[int]:
        self._init_params(points)
        n = len(points)
        responsibilities: list[list[float]] = [[0.0] * self.k for _ in range(n)]

        for _ in range(self.max_iter):
            # E-step
            for i in range(n):
                for j in range(self.k):
                    responsibilities[i][j] = (
                        self.weights[j] * self._gaussian_pdf(points[i], self.means[j], self.covariances[j])
                    )
                row_sum = sum(responsibilities[i])
                if row_sum > 0:
                    for j in range(self.k):
                        responsibilities[i][j] /= row_sum

            # M-step
            for j in range(self.k):
                N_k = sum(responsibilities[i][j] for i in range(n))
                if N_k < 1e-12:
                    continue
                self.weights[j] = N_k / n
                new_mean = [0.0] * len(points[0])
                for i in range(n):
                    for d in range(len(points[0])):
                        new_mean[d] += responsibilities[i][j] * points[i][d]
                self.means[j] = [m / N_k for m in new_mean]

            # Check convergence
            diff = 0.0
            for i in range(n):
                for j in range(self.k):
                    new_resp = self.weights[j] * self._gaussian_pdf(points[i], self.means[j], self.covariances[j])
                    diff += abs(new_resp - responsibilities[i][j])
            if diff < self.tol:
                break

        # Final labels
        self.labels = [0] * n
        for i in range(n):
            self.labels[i] = max(range(self.k), key=lambda j: responsibilities[i][j])
        return self.labels


class SpectralClustering:
    """Spectral clustering using graph Laplacian."""

    def __init__(self, n_clusters: int = 3, sigma: float = 1.0):
        self.k = n_clusters
        self.sigma = sigma
        self.labels: list[int] = []

    def _affinity_matrix(self, points: Points) -> list[list[float]]:
        n = len(points)
        A: list[list[float]] = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(i + 1, n):
                d = euclidean_dist(points[i], points[j])
                w = math.exp(-d * d / (2 * self.sigma ** 2))
                A[i][j] = w
                A[j][i] = w
        return A

    def fit(self, points: Points) -> list[int]:
        n = len(points)
        A = self._affinity_matrix(points)
        D = [sum(A[i]) for i in range(n)]
        # Normalized Laplacian: I - D^-1/2 * A * D^-1/2
        L: list[list[float]] = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                if D[i] > 0 and D[j] > 0:
                    L[i][j] = -A[i][j] / math.sqrt(D[i] * D[j])
                if i == j:
                    L[i][j] = 1.0 + L[i][j]
        # Power iteration for top k eigenvectors
        k = min(self.k, n)
        eigenvectors = self._power_iteration(L, k)
        # K-means on eigenvectors
        from clustering_utils import KMeans
        km = KMeans(k=k)
        self.labels = km.fit(eigenvectors)
        return self.labels

    def _power_iteration(self, A: list[list[float]], k: int) -> list[list[float]]:
        n = len(A)
        vectors = [[1.0 / math.sqrt(n)] * n for _ in range(k)]
        for _ in range(50):
            new_vectors: list[list[float]] = []
            for vec in vectors:
                new_v = [sum(A[i][j] * vec[j] for j in range(n)) for i in range(n)]
                norm = math.sqrt(sum(x * x for x in new_v))
                if norm > 1e-12:
                    new_v = [x / norm for x in new_v]
                new_vectors.append(new_v)
            vectors = new_vectors
        return vectors


class MeanShift:
    """Mean shift clustering."""

    def __init__(self, bandwidth: float = 1.0, max_iter: int = 100):
        self.bandwidth = bandwidth
        self.max_iter = max_iter
        self.centroids: list[Point] = []
        self.labels: list[int] = []

    def fit(self, points: Points) -> list[int]:
        n = len(points)
        centroids = [list(p) for p in points]
        for _ in range(self.max_iter):
            new_centroids: list[Point] = []
            for i, c in enumerate(centroids):
                numerator = [0.0] * len(c)
                denom = 0.0
                for p in points:
                    d = euclidean_dist(c, p)
                    if d < self.bandwidth:
                        weight = math.exp(-d * d / (2 * self.bandwidth ** 2))
                        for j in range(len(c)):
                            numerator[j] += weight * p[j]
                        denom += weight
                if denom > 1e-12:
                    new_c = [numerator[j] / denom for j in range(len(c))]
                else:
                    new_c = list(c)
                new_centroids.append(new_c)
            centroids = new_centroids
        # Merge nearby centroids
        merged: list[Point] = []
        for c in centroids:
            found = False
            for m in merged:
                if euclidean_dist(c, m) < self.bandwidth / 2:
                    found = True
                    break
            if not found:
                merged.append(c)
        self.centroids = merged
        self.labels = [0] * n
        for i, p in enumerate(points):
            best_idx = 0
            best_dist = float("inf")
            for j, c in enumerate(self.centroids):
                d = euclidean_dist(p, c)
                if d < best_dist:
                    best_dist = d
                    best_idx = j
            self.labels[i] = best_idx
        return self.labels


class OPTICS:
    """OPTICS density-based clustering."""

    def __init__(self, eps: float = 5.0, min_samples: int = 5):
        self.eps = eps
        self.min_samples = min_samples
        self.labels: list[int] = []

    def fit(self, points: Points) -> list[int]:
        n = len(points)
        dist_matrix: dict[tuple[int, int], float] = {}
        for i in range(n):
            for j in range(i + 1, n):
                d = euclidean_dist(points[i], points[j])
                dist_matrix[(i, j)] = d
                dist_matrix[(j, i)] = d

        def core_distance(i: int) -> float:
            neighbors = sorted(dist_matrix[(i, j)] for j in range(n) if j != i)
            if len(neighbors) < self.min_samples:
                return float("inf")
            return neighbors[self.min_samples - 1]

        reachability = [float("inf")] * n
        ordered: list[int] = []
        processed = [False] * n

        for i in range(n):
            if processed[i]:
                continue
            processed[i] = True
            ordered.append(i)
            neighbors = [j for j in range(n) if j != i and dist_matrix[(i, j)] <= self.eps]
            if core_distance(i) < float("inf"):
                for j in neighbors:
                    dist_ij = dist_matrix[(i, j)]
                    new_dist = max(core_distance(i), dist_ij)
                    if reachability[j] == float("inf"):
                        reachability[j] = new_dist

        # Simple extraction: DBSCAN-like on reachability plot
        self.labels = [0] * n
        cluster_id = 0
        i = 0
        while i < n:
            if reachability[i] == float("inf"):
                self.labels[i] = -1  # noise
            else:
                start = i
                while i < n and reachability[i] != float("inf") and reachability[i] <= self.eps:
                    self.labels[ordered[i]] = cluster_id
                    i += 1
                cluster_id += 1
                continue
            i += 1
        return self.labels


def davies_bouldin_index(points: Points, labels: list[int]) -> float:
    """
    Davies-Bouldin index (lower is better).

    Measures within-cluster scatter vs between-cluster separation.
    """
    unique_labels = set(labels)
    k = len(unique_labels)
    if k < 2:
        return 0.0

    n = len(points)
    dim = len(points[0])
    centroids: dict[int, Point] = {}
    for i, l in enumerate(labels):
        if l not in centroids:
            centroids[l] = [0.0] * dim
        for d in range(dim):
            centroids[l][d] += points[i][d]
    counts: dict[int, int] = {}
    for l in labels:
        counts[l] = counts.get(l, 0) + 1
    for l in centroids:
        if counts[l] > 0:
            centroids[l] = [c / counts[l] for c in centroids[l]]

    # Scatter
    scatter: dict[int, float] = {}
    for l in unique_labels:
        if counts[l] > 1:
            sc = sum(euclidean_dist(points[i], centroids[l]) for i in range(n) if labels[i] == l) / counts[l]
            scatter[l] = sc
        else:
            scatter[l] = 0.0

    # DB index
    db = 0.0
    for i in unique_labels:
        max_ratio = 0.0
        for j in unique_labels:
            if i != j:
                d_ij = euclidean_dist(centroids[i], centroids[j])
                if d_ij > 1e-12:
                    ratio = (scatter[i] + scatter[j]) / d_ij
                    max_ratio = max(max_ratio, ratio)
        db += max_ratio
    return db / k


def calinski_harabasz_index(points: Points, labels: list[int]) -> float:
    """
    Calinski-Harabasz index (higher is better).

    Ratio of between-cluster to within-cluster variance.
    """
    n = len(points)
    dim = len(points[0])
    k = len(set(labels))
    if k < 2 or n <= k:
        return 0.0

    grand_mean = [sum(points[i][d] for i in range(n)) / n for d in range(dim)]

    # Between-cluster variance
    counts: dict[int, int] = {}
    for l in labels:
        counts[l] = counts.get(l, 0) + 1

    centroids: dict[int, Point] = {}
    for i, l in enumerate(labels):
        if l not in centroids:
            centroids[l] = [0.0] * dim
        for d in range(dim):
            centroids[l][d] += points[i][d]
    for l in centroids:
        centroids[l] = [c / counts[l] for c in centroids[l]]

    B = 0.0
    for l in unique_labels := set(labels):
        B += counts[l] * sum((centroids[l][d] - grand_mean[d]) ** 2 for d in range(dim))

    # Within-cluster variance
    W = 0.0
    for i in range(n):
        l = labels[i]
        W += sum((points[i][d] - centroids[l][d]) ** 2 for d in range(dim))

    if W < 1e-12:
        return float("inf")
    return (B / (k - 1)) / (W / (n - k))
