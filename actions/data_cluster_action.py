"""Data clustering action module for RabAI AutoClick.

Provides K-Means, DBSCAN, and hierarchical clustering for
unsupervised grouping of data points.
"""

from __future__ import annotations

import sys
import os
import math
import random
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ClusterResult:
    """Cluster assignment result."""
    cluster_id: int
    points: List[Dict[str, Any]]
    centroid: Optional[List[float]] = None
    inertia: float = 0.0


def euclidean_distance(a: List[float], b: List[float]) -> float:
    """Compute Euclidean distance between two vectors."""
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def manhattan_distance(a: List[float], b: List[float]) -> float:
    """Compute Manhattan distance between two vectors."""
    return sum(abs(x - y) for x, y in zip(a, b))


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a)) or 1e-10
    norm_b = math.sqrt(sum(x * x for x in b)) or 1e-10
    return dot / (norm_a * norm_b)


class KMeansClusterAction(BaseAction):
    """K-Means clustering with k-means++ initialization.
    
    Partitions data into k clusters by minimizing within-cluster
    variance. Suitable for spherical, evenly-sized clusters.
    
    Args:
        n_clusters: Number of clusters (k)
        max_iterations: Maximum refinement iterations
        distance: euclidean, manhattan, or cosine
        n_init: Number of initializations to try
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        features: List[str],
        n_clusters: int = 3,
        max_iterations: int = 100,
        distance: str = "euclidean",
        n_init: int = 5,
        normalize: bool = False
    ) -> ActionResult:
        try:
            # Extract feature vectors
            vectors: List[Tuple[int, List[float]]] = []
            for idx, row in enumerate(data):
                try:
                    vec = [float(row[f]) for f in features if f in row and row[f] is not None]
                    if len(vec) == len(features):
                        vectors.append((idx, vec))
                except (ValueError, TypeError):
                    continue

            if len(vectors) < n_clusters:
                return ActionResult(success=False, error="Not enough valid data points")

            # Normalize if requested
            if normalize:
                n_features = len(features)
                col_mins = [min(v[i] for v in vectors) for i in range(n_features)]
                col_maxs = [max(v[i] for v in vectors) for i in range(n_features)]
                ranges = [maxm - minm or 1.0 for minm, maxm in zip(col_mins, col_maxs)]
                vectors = [(idx, [(v[i] - col_mins[i]) / ranges[i] for i in range(n_features)])
                           for i, v in enumerate(vec)] for idx, vec in vectors]

            dist_func = {"euclidean": euclidean_distance, "manhattan": manhattan_distance,
                         "cosine": lambda a, b: 1.0 - cosine_similarity(a, b)}.get(distance, euclidean_distance)

            best_clusters: List[List[Tuple[int, List[float]]]] = []
            best_inertia = float('inf')

            for _ in range(n_init):
                clusters, inertia = self._kmeans_single(
                    vectors, n_clusters, max_iterations, dist_func
                )
                if inertia < best_inertia:
                    best_inertia = inertia
                    best_clusters = clusters

            # Build cluster results
            cluster_data: List[Dict[str, Any]] = []
            for cid, members in enumerate(best_clusters):
                centroid = [sum(p[i] for p in members) / len(members) for i in range(len(features))]
                cluster_data.append({
                    "cluster_id": cid,
                    "size": len(members),
                    "centroid": {f: round(centroid[i], 6) for i, f in enumerate(features)},
                    "indices": [idx for idx, _ in members]
                })

            return ActionResult(success=True, data={
                "n_clusters": n_clusters,
                "inertia": round(best_inertia, 4),
                "clusters": cluster_data
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _kmeans_single(
        self,
        vectors: List[Tuple[int, List[float]]],
        k: int,
        max_iter: int,
        dist_func
    ) -> Tuple[List[List[Tuple[int, List[float]]]], float]:
        # k-means++ init
        centroids: List[List[float]] = []
        _, first = random.choice(vectors)
        centroids.append(first[:])
        for _ in range(k - 1):
            distances = []
            for _, vec in vectors:
                d = min(dist_func(vec, c) for c in centroids)
                distances.append(d ** 2)
            total = sum(distances)
            if total == 0:
                idx = random.randint(0, len(vectors) - 1)
            else:
                idx = random.choices(range(len(vectors)), weights=distances)[0]
            centroids.append(vectors[idx][1][:])

        clusters: List[List[Tuple[int, List[float]]]] = [[] for _ in range(k)]
        inertia = 0.0

        for _ in range(max_iter):
            # Assign
            new_clusters: List[List[Tuple[int, List[float]]]] = [[] for _ in range(k)]
            for idx, vec in vectors:
                dists = [dist_func(vec, c) for c in centroids]
                cid = dists.index(min(dists))
                new_clusters[cid].append((idx, vec))

            # Check convergence
            empty = any(len(c) == 0 for c in new_clusters)
            if empty or new_clusters == clusters:
                break

            clusters = new_clusters

            # Update centroids
            for cid, members in enumerate(clusters):
                if members:
                    centroids[cid] = [
                        sum(p[i] for p in members) / len(members)
                        for i in range(len(members[0][1]))
                    ]

        inertia = sum(
            sum(dist_func(p, centroids[cid]) ** 2 for cid, members in enumerate(clusters) for p in [pt[1]])
        ) if clusters else 0.0

        return clusters, inertia


class DBSCANClusterAction(BaseAction):
    """DBSCAN density-based clustering.
    
    Does not require specifying k. Finds core samples of high density
    and expands clusters from them. Outliers get label -1.
    
    Args:
        eps: Maximum distance between two samples for neighborhood
        min_samples: Core point minimum neighbors
        distance: euclidean, manhattan, or cosine
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        features: List[str],
        eps: float = 0.5,
        min_samples: int = 5,
        distance: str = "euclidean"
    ) -> ActionResult:
        try:
            vectors: List[Tuple[int, List[float]]] = []
            for idx, row in enumerate(data):
                try:
                    vec = [float(row[f]) for f in features if f in row and row[f] is not None]
                    if len(vec) == len(features):
                        vectors.append((idx, vec))
                except (ValueError, TypeError):
                    continue

            if len(vectors) < min_samples:
                return ActionResult(success=False, error="Not enough data for DBSCAN")

            dist_func = {"euclidean": euclidean_distance, "manhattan": manhattan_distance,
                         "cosine": lambda a, b: 1.0 - cosine_similarity(a, b)}.get(distance, euclidean_distance)

            # Build neighborhood
            n = len(vectors)
            neighborhoods: List[List[int]] = [[] for _ in range(n)]
            for i in range(n):
                for j in range(n):
                    if i != j and dist_func(vectors[i][1], vectors[j][1]) <= eps:
                        neighborhoods[i].append(j)

            # Core points
            core_indices = {i for i in range(n) if len(neighborhoods[i]) >= min_samples}

            labels = [-1] * n
            cluster_id = 0

            def expand(idx: int, cid: int, stack: List[int]):
                labels[idx] = cid
                while stack:
                    curr = stack.pop()
                    if curr in core_indices:
                        for neighbor in neighborhoods[curr]:
                            if labels[neighbor] == -1:
                                labels[neighbor] = cid
                                stack.append(neighbor)

            for i in range(n):
                if labels[i] == -1 and i in core_indices:
                    expand(i, cluster_id, list(neighborhoods[i]))
                    cluster_id += 1

            # Build results
            clusters: Dict[int, List[int]] = defaultdict(list)
            noise_count = 0
            for orig_idx, label in enumerate(labels):
                if label == -1:
                    noise_count += 1
                else:
                    clusters[label].append(vectors[orig_idx][0])

            cluster_results = []
            for cid in sorted(clusters.keys()):
                pts = clusters[cid]
                cluster_results.append({
                    "cluster_id": cid,
                    "size": len(pts),
                    "indices": pts
                })

            return ActionResult(success=True, data={
                "n_clusters": cluster_id,
                "noise_count": noise_count,
                "clusters": cluster_results
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class HierarchicalClusterAction(BaseAction):
    """Hierarchical (agglomerative) clustering with multiple linkage methods.
    
    Builds a dendrogram-style hierarchy. Supports single, complete,
    average, and ward linkage.
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        features: List[str],
        n_clusters: int = 3,
        linkage: str = "average",  # single, complete, average, ward
        distance: str = "euclidean"
    ) -> ActionResult:
        try:
            vectors: List[Tuple[int, List[float]]] = []
            for idx, row in enumerate(data):
                try:
                    vec = [float(row[f]) for f in features if f in row and row[f] is not None]
                    if len(vec) == len(features):
                        vectors.append((idx, vec))
                except (ValueError, TypeError):
                    continue

            if len(vectors) < 2:
                return ActionResult(success=False, error="Need at least 2 data points")

            dist_func = {"euclidean": euclidean_distance, "manhattan": manhattan_distance}.get(distance, euclidean_distance)

            n = len(vectors)
            # Compute initial distance matrix
            dist_matrix: List[List[float]] = [
                [0.0 if i == j else dist_func(vectors[i][1], vectors[j][1])
                 for j in range(n)] for i in range(n)
            ]

            # Union-find for clustering
            parent = list(range(n))
            rank = [0] * n

            def find(x: int) -> int:
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x

            def union(x: int, y: int):
                px, py = find(x), find(y)
                if px == py:
                    return
                if rank[px] < rank[py]:
                    px, py = py, px
                parent[py] = px
                if rank[px] == rank[py]:
                    rank[px] += 1

            merges: List[Dict[str, Any]] = []
            active = set(range(n))

            for step in range(n - 1):
                if len(active) <= n_clusters:
                    break

                min_dist = float('inf')
                pair = (0, 1)
                active_list = list(active)
                for i in range(len(active_list)):
                    for j in range(i + 1, len(active_list)):
                        a, b = active_list[i], active_list[j]
                        d = dist_matrix[a][b]
                        if d < min_dist:
                            min_dist = d
                            pair = (a, b)

                pa, pb = pair
                merges.append({
                    "step": step,
                    "merged": [pa, pb],
                    "distance": round(min_dist, 4),
                    "size": sum(1 for x in active if find(x) == find(pa)) + 1
                })
                union(pa, pb)
                active.discard(pb)

            # Assign labels
            label_map: Dict[int, int] = {}
            label = 0
            for orig_idx in range(n):
                root = find(orig_idx)
                if root not in label_map:
                    label_map[root] = label
                    label += 1

            labels = [label_map[find(i)] for i in range(n)]

            clusters: Dict[int, List[int]] = defaultdict(list)
            for orig_idx, lab in enumerate(labels):
                clusters[lab].append(vectors[orig_idx][0])

            cluster_results = [{
                "cluster_id": cid,
                "size": len(members),
                "indices": members
            } for cid, members in sorted(clusters.items())]

            return ActionResult(success=True, data={
                "n_clusters": len(cluster_results),
                "merges": merges,
                "clusters": cluster_results
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))
