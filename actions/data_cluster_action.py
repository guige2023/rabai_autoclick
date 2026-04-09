"""Data Cluster Action.

Performs clustering analysis on datasets using multiple algorithms
including K-Means, DBSCAN, and hierarchical clustering.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


class ClusteringAlgorithm(Enum):
    """Supported clustering algorithms."""
    KMEANS = "kmeans"
    DBSCAN = "dbscan"
    HIERARCHICAL = "hierarchical"
    SPECTRAL = "spectral"


@dataclass
class ClusterResult:
    """Result of a clustering operation."""
    algorithm: str
    n_clusters: int
    labels: np.ndarray
    centers: Optional[np.ndarray] = None
    inertia: Optional[float] = None
    n_noise: Optional[int] = None
    metric: str = "euclidean"


@dataclass
class ClusterStats:
    """Statistics for a single cluster."""
    cluster_id: int
    size: int
    centroid: Optional[np.ndarray] = None
    variance: Optional[float] = None
    diameter: Optional[float] = None
    density: Optional[float] = None


class DataClusterAction:
    """Clustering analysis for datasets."""

    def __init__(self, random_state: int = 42) -> None:
        self.random_state = random_state

    def kmeans(
        self,
        data: np.ndarray,
        n_clusters: int,
        max_iter: int = 300,
        n_init: int = 10,
    ) -> ClusterResult:
        """Perform K-Means clustering."""
        from sklearn.cluster import KMeans

        kmeans = KMeans(
            n_clusters=n_clusters,
            max_iter=max_iter,
            n_init=n_init,
            random_state=self.random_state,
        )
        labels = kmeans.fit_predict(data)

        return ClusterResult(
            algorithm="kmeans",
            n_clusters=n_clusters,
            labels=labels,
            centers=kmeans.cluster_centers_,
            inertia=kmeans.inertia_,
        )

    def dbscan(
        self,
        data: np.ndarray,
        eps: float = 0.5,
        min_samples: int = 5,
        metric: str = "euclidean",
    ) -> ClusterResult:
        """Perform DBSCAN clustering."""
        from sklearn.cluster import DBSCAN

        dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric=metric)
        labels = dbscan.fit_predict(data)

        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = list(labels).count(-1)

        return ClusterResult(
            algorithm="dbscan",
            n_clusters=n_clusters,
            labels=labels,
            n_noise=n_noise,
            metric=metric,
        )

    def hierarchical(
        self,
        data: np.ndarray,
        n_clusters: int,
        method: str = "ward",
    ) -> ClusterResult:
        """Perform hierarchical agglomerative clustering."""
        from sklearn.cluster import AgglomerativeClustering

        hier = AgglomerativeClustering(
            n_clusters=n_clusters,
            metric="euclidean",
            linkage=method,
        )
        labels = hier.fit_predict(data)

        return ClusterResult(
            algorithm="hierarchical",
            n_clusters=n_clusters,
            labels=labels,
        )

    def spectral(
        self,
        data: np.ndarray,
        n_clusters: int,
    ) -> ClusterResult:
        """Perform spectral clustering."""
        from sklearn.cluster import SpectralClustering

        spectral = SpectralClustering(
            n_clusters=n_clusters,
            random_state=self.random_state,
            affinity="nearest_neighbors",
        )
        labels = spectral.fit_predict(data)

        return ClusterResult(
            algorithm="spectral",
            n_clusters=n_clusters,
            labels=labels,
        )

    def cluster(
        self,
        data: np.ndarray,
        algorithm: ClusteringAlgorithm,
        **kwargs: Any,
    ) -> ClusterResult:
        """Perform clustering with specified algorithm."""
        if algorithm == ClusteringAlgorithm.KMEANS:
            return self.kmeans(data, **kwargs)
        elif algorithm == ClusteringAlgorithm.DBSCAN:
            return self.dbscan(data, **kwargs)
        elif algorithm == ClusteringAlgorithm.HIERARCHICAL:
            return self.hierarchical(data, **kwargs)
        elif algorithm == ClusteringAlgorithm.SPECTRAL:
            return self.spectral(data, **kwargs)
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")

    def get_cluster_stats(self, data: np.ndarray, result: ClusterResult) -> List[ClusterStats]:
        """Calculate statistics for each cluster."""
        stats = []
        unique_labels = set(result.labels)

        for label in unique_labels:
            if label == -1:
                continue
            cluster_points = data[result.labels == label]

            centroid = cluster_points.mean(axis=0) if len(cluster_points) > 0 else None
            variance = float(np.var(cluster_points)) if len(cluster_points) > 0 else 0.0

            if len(cluster_points) > 1:
                pairwise_dist = np.linalg.norm(
                    cluster_points[:, np.newaxis] - cluster_points[np.newaxis, :],
                    axis=2,
                )
                diameter = float(np.max(pairwise_dist))
            else:
                diameter = 0.0

            density = len(cluster_points) / (diameter + 1e-10)

            stats.append(ClusterStats(
                cluster_id=int(label),
                size=len(cluster_points),
                centroid=centroid,
                variance=variance,
                diameter=diameter,
                density=density,
            ))

        return stats

    def elbow_method(
        self,
        data: np.ndarray,
        max_k: int = 10,
    ) -> List[Tuple[int, float]]:
        """Find optimal k using elbow method."""
        from sklearn.cluster import KMeans

        inertias = []
        for k in range(1, max_k + 1):
            kmeans = KMeans(n_clusters=k, random_state=self.random_state)
            kmeans.fit(data)
            inertias.append((k, float(kmeans.inertia_)))

        return inertias

    def silhouette_scores(
        self,
        data: np.ndarray,
        range_k: range,
    ) -> List[Tuple[int, float]]:
        """Calculate silhouette scores for range of k."""
        from sklearn.metrics import silhouette_score

        scores = []
        for k in range_k:
            if k < 2:
                continue
            labels = self.kmeans(data, n_clusters=k).labels
            score = silhouette_score(data, labels)
            scores.append((k, float(score)))

        return scores

    def assign_clusters_to_df(
        self,
        df: pd.DataFrame,
        result: ClusterResult,
        prefix: str = "cluster",
    ) -> pd.DataFrame:
        """Add cluster labels to a dataframe."""
        df_result = df.copy()
        df_result[f"{prefix}_label"] = result.labels
        return df_result
