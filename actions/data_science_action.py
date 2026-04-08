"""Data science and ML operations action module for RabAI AutoClick.

Provides:
- DataScienceAction: Common ML operations
- DataClusteringAction: Clustering algorithms
- DataSimilarityAction: Similarity computation
- DataSamplingAction: Statistical sampling
- DataStatisticsAction: Statistical analysis
"""

import time
import math
import random
import json
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataScienceAction(BaseAction):
    """Common ML and data science operations."""
    action_type = "data_science"
    display_name = "数据科学"
    description = "常用数据科学操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compute")
            data = params.get("data", [])
            features = params.get("features", [])

            if operation == "normalize":
                normalized = self._min_max_normalize(data)
                return ActionResult(success=True, data={"normalized": normalized}, message=f"Normalized {len(data)} values")

            elif operation == "standardize":
                standardized = self._standardize(data)
                return ActionResult(success=True, data={"standardized": standardized}, message=f"Standardized {len(data)} values")

            elif operation == "describe":
                if not data:
                    return ActionResult(success=False, message="data is empty")
                numeric_data = [float(x) for x in data if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(".", "").replace("-", "").isdigit())]
                if not numeric_data:
                    return ActionResult(success=True, data={"count": len(data), "message": "no numeric data"})
                mean = sum(numeric_data) / len(numeric_data)
                variance = sum((x - mean) ** 2 for x in numeric_data) / len(numeric_data)
                sorted_data = sorted(numeric_data)
                return ActionResult(
                    success=True,
                    data={
                        "count": len(numeric_data),
                        "mean": round(mean, 6),
                        "std": round(math.sqrt(variance), 6),
                        "min": min(numeric_data),
                        "max": max(numeric_data),
                        "median": sorted_data[len(sorted_data) // 2],
                        "q25": sorted_data[len(sorted_data) // 4],
                        "q75": sorted_data[3 * len(sorted_data) // 4]
                    }
                )

            elif operation == "histogram":
                bins = params.get("bins", 10)
                if not data:
                    return ActionResult(success=False, message="data is empty")
                numeric_data = [float(x) for x in data if isinstance(x, (int, float))]
                if not numeric_data:
                    return ActionResult(success=False, message="no numeric data")
                min_val = min(numeric_data)
                max_val = max(numeric_data)
                bin_width = (max_val - min_val) / bins if bins > 0 else 1
                histogram = [0] * bins
                for val in numeric_data:
                    bin_idx = min(int((val - min_val) / bin_width), bins - 1) if bin_width > 0 else 0
                    histogram[bin_idx] += 1
                return ActionResult(
                    success=True,
                    data={"histogram": histogram, "bins": bins, "bin_width": round(bin_width, 4)}
                )

            elif operation == "correlation":
                x = params.get("x", [])
                y = params.get("y", [])
                if len(x) != len(y) or len(x) < 2:
                    return ActionResult(success=False, message="x and y must have same length and at least 2 points")
                corr = self._pearson_correlation(x, y)
                return ActionResult(success=True, data={"correlation": round(corr, 6)}, message=f"Correlation: {corr:.4f}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data science error: {str(e)}")

    def _min_max_normalize(self, data: List[float]) -> List[float]:
        if not data:
            return []
        min_val, max_val = min(data), max(data)
        if max_val == min_val:
            return [0.5] * len(data)
        return [(x - min_val) / (max_val - min_val) for x in data]

    def _standardize(self, data: List[float]) -> List[float]:
        if not data:
            return []
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        std = math.sqrt(variance) if variance > 0 else 1
        return [(x - mean) / std for x in data]

    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n)) / n
        std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x) / n)
        std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y) / n)
        if std_x == 0 or std_y == 0:
            return 0
        return cov / (std_x * std_y)


class DataClusteringAction(BaseAction):
    """Clustering algorithms for data grouping."""
    action_type = "data_clustering"
    display_name = "数据聚类"
    description = "聚类算法"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "cluster")
            data = params.get("data", [])
            algorithm = params.get("algorithm", "kmeans")
            n_clusters = params.get("n_clusters", 3)

            if operation == "cluster":
                if not data:
                    return ActionResult(success=False, message="data is required")

                if algorithm == "kmeans":
                    clusters, centroids = self._kmeans(data, n_clusters)
                elif algorithm == "dbscan":
                    eps = params.get("eps", 0.5)
                    min_samples = params.get("min_samples", 2)
                    clusters, centroids = self._dbscan(data, eps, min_samples)
                elif algorithm == "hierarchical":
                    clusters, centroids = self._hierarchical_clustering(data, n_clusters)
                else:
                    return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

                return ActionResult(
                    success=True,
                    data={
                        "algorithm": algorithm,
                        "n_clusters": len(set(clusters)),
                        "cluster_assignments": clusters,
                        "centroids": centroids
                    },
                    message=f"Clustered {len(data)} points into {len(set(clusters))} clusters"
                )

            elif operation == "elbow":
                data_sample = data[:min(len(data), 100)] if data else []
                max_k = min(params.get("max_k", 10), len(data_sample) // 2)
                inertias = []
                for k in range(1, max_k + 1):
                    clusters, _ = self._kmeans(data_sample, k)
                    inertia = self._compute_inertia(data_sample, clusters, n_clusters)
                    inertias.append(inertia)
                return ActionResult(success=True, data={"k_values": list(range(1, max_k + 1)), "inertias": inertias})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Clustering error: {str(e)}")

    def _kmeans(self, data: List[List[float]], k: int, max_iter: int = 50) -> Tuple[List[int], List[List[float]]]:
        if not data or k <= 0:
            return [], []
        if len(data) <= k:
            return list(range(len(data))), data

        centroids = data[:k]
        assignments = [0] * len(data)

        for _ in range(max_iter):
            new_assignments = []
            for point in data:
                distances = [self._euclidean_distance(point, c) for c in centroids]
                new_assignments.append(distances.index(min(distances)))

            if new_assignments == assignments:
                break
            assignments = new_assignments

            for i in range(k):
                cluster_points = [data[j] for j in range(len(data)) if assignments[j] == i]
                if cluster_points:
                    centroids[i] = [sum(p[d] for p in cluster_points) / len(cluster_points) for d in range(len(data[0]))]

        return assignments, centroids

    def _dbscan(self, data: List[List[float]], eps: float, min_samples: int) -> Tuple[List[int], List[List[float]]]:
        labels = [-1] * len(data)
        cluster_id = 0
        for i, point in enumerate(data):
            if labels[i] != -1:
                continue
            neighbors = [j for j, p in enumerate(data) if self._euclidean_distance(point, p) <= eps]
            if len(neighbors) < min_samples:
                continue
            labels[i] = cluster_id
            self._expand_cluster(data, labels, i, neighbors, cluster_id, eps, min_samples)
            cluster_id += 1
        centroids = self._compute_centroids(data, labels)
        return labels, centroids

    def _expand_cluster(self, data: List, labels: List, idx: int, neighbors: List, cluster_id: int, eps: float, min_samples: int):
        labels[idx] = cluster_id
        i = 0
        while i < len(neighbors):
            neighbor = neighbors[i]
            if labels[neighbor] == -1:
                labels[neighbor] = cluster_id
                new_neighbors = [j for j, p in enumerate(data) if self._euclidean_distance(data[neighbor], p) <= eps]
                if len(new_neighbors) >= min_samples:
                    neighbors.extend(new_neighbors)
            elif labels[neighbor] == -1:
                labels[neighbor] = cluster_id
            i += 1

    def _hierarchical_clustering(self, data: List[List[float]], k: int) -> Tuple[List[int], List[List[float]]]:
        if not data or k <= 0:
            return [], []
        n = len(data)
        clusters = list(range(n))
        while len(set(clusters)) > k:
            min_dist = float("inf")
            merge = (0, 1)
            for i in range(n):
                for j in range(i + 1, n):
                    if clusters[i] != clusters[j]:
                        dist = self._euclidean_distance(data[i], data[j])
                        if dist < min_dist:
                            min_dist = dist
                            merge = (i, j)
            target = clusters[merge[0]]
            for i in range(n):
                if clusters[i] == target:
                    clusters[i] = clusters[merge[1]]
        return clusters, self._compute_centroids(data, clusters)

    def _euclidean_distance(self, a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((a[i] - b[i]) ** 2 for i in range(len(a))))

    def _compute_inertia(self, data: List[List[float]], assignments: List[int], k: int) -> float:
        centroids = self._compute_centroids(data, list(range(k)))
        inertia = 0
        for i, point in enumerate(data):
            inertia += self._euclidean_distance(point, centroids[assignments[i]]) ** 2
        return inertia

    def _compute_centroids(self, data: List[List[float]], labels: List[int]) -> List[List[float]]:
        unique_labels = sorted(set(labels))
        if -1 in unique_labels:
            unique_labels.remove(-1)
        centroids = []
        for label in unique_labels:
            points = [data[i] for i in range(len(data)) if labels[i] == label]
            if points:
                dim = len(data[0])
                centroids.append([sum(p[d] for p in points) / len(points) for d in range(dim)])
            else:
                centroids.append([0] * len(data[0]))
        return centroids


class DataSimilarityAction(BaseAction):
    """Similarity computation between data points."""
    action_type = "data_similarity"
    display_name = "数据相似度"
    description = "相似度计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "similarity")
            a = params.get("a")
            b = params.get("b")
            data = params.get("data", [])
            metric = params.get("metric", "cosine")

            if operation == "similarity":
                if a is None or b is None:
                    return ActionResult(success=False, message="a and b required")

                if metric == "cosine":
                    sim = self._cosine_similarity(a, b)
                elif metric == "euclidean":
                    sim = self._euclidean_distance(a, b)
                elif metric == "manhattan":
                    sim = self._manhattan_distance(a, b)
                elif metric == "jaccard":
                    sim = self._jaccard_similarity(a, b)
                elif metric == "pearson":
                    sim = self._pearson_correlation(a, b)
                else:
                    return ActionResult(success=False, message=f"Unknown metric: {metric}")

                return ActionResult(
                    success=True,
                    data={"similarity": round(sim, 6), "metric": metric},
                    message=f"{metric}: {sim:.4f}"
                )

            elif operation == "nearest_neighbors":
                target = params.get("target")
                k = params.get("k", 5)
                if not target or not data:
                    return ActionResult(success=False, message="target and data required")

                distances = []
                for i, point in enumerate(data):
                    dist = self._euclidean_distance(target, point)
                    distances.append({"index": i, "distance": dist})

                distances.sort(key=lambda x: x["distance"])
                return ActionResult(
                    success=True,
                    data={"neighbors": distances[:k], "k": k}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Similarity error: {str(e)}")

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0 or norm_b == 0:
            return 0
        return dot / (norm_a * norm_b)

    def _euclidean_distance(self, a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    def _manhattan_distance(self, a: List[float], b: List[float]) -> float:
        return sum(abs(x - y) for x, y in zip(a, b))

    def _jaccard_similarity(self, a: List, b: List) -> float:
        set_a = set(a)
        set_b = set(b)
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union > 0 else 0

    def _pearson_correlation(self, a: List[float], b: List[float]) -> float:
        n = len(a)
        if n < 2:
            return 0
        mean_a = sum(a) / n
        mean_b = sum(b) / n
        cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n)) / n
        std_a = math.sqrt(sum((x - mean_a) ** 2 for x in a) / n)
        std_b = math.sqrt(sum((x - mean_b) ** 2 for x in b) / n)
        if std_a == 0 or std_b == 0:
            return 0
        return cov / (std_a * std_b)


class DataSamplingAction(BaseAction):
    """Statistical sampling methods."""
    action_type = "data_sampling"
    display_name = "数据采样"
    description = "统计采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sample")
            data = params.get("data", [])
            n = params.get("n", 10)

            if operation == "sample":
                method = params.get("method", "random")
                if not data:
                    return ActionResult(success=False, message="data is required")

                if method == "random":
                    sampled = random.sample(data, min(n, len(data)))
                elif method == "systematic":
                    step = max(1, len(data) // n)
                    sampled = data[::step][:n]
                elif method == "stratified":
                    sampled = self._stratified_sample(data, n)
                elif method == "bootstrap":
                    sampled = random.choices(data, k=n)
                else:
                    return ActionResult(success=False, message=f"Unknown method: {method}")

                return ActionResult(
                    success=True,
                    data={"sampled": sampled, "original_size": len(data), "sample_size": len(sampled), "method": method}
                )

            elif operation == "split":
                test_size = params.get("test_size", 0.2)
                shuffled = list(data)
                random.shuffle(shuffled)
                split_idx = int(len(shuffled) * (1 - test_size))
                train = shuffled[:split_idx]
                test = shuffled[split_idx:]
                return ActionResult(
                    success=True,
                    data={"train": train, "test": test, "train_size": len(train), "test_size": len(test)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Sampling error: {str(e)}")

    def _stratified_sample(self, data: List, n: int) -> List:
        if not data:
            return []
        if len(data) <= n:
            return data
        labels = params.get("strata_field", "")
        if not labels:
            return random.sample(data, n)
        groups = {}
        for item in data:
            group_key = item.get(labels, "unknown") if isinstance(item, dict) else "unknown"
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(item)
        sampled = []
        for group_key, group_data in groups.items():
            group_n = max(1, int(n * len(group_data) / len(data)))
            sampled.extend(random.sample(group_data, min(group_n, len(group_data))))
        return sampled


class DataStatisticsAction(BaseAction):
    """Statistical analysis."""
    action_type = "data_statistics"
    display_name = "统计分析"
    description = "统计分析"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "analyze")
            data = params.get("data", [])

            if operation == "analyze":
                if not data:
                    return ActionResult(success=False, message="data is required")

                numeric_data = [float(x) for x in data if isinstance(x, (int, float)) or (isinstance(x, str) and self._is_numeric(x))]
                if not numeric_data:
                    return ActionResult(success=False, message="No numeric data found")

                sorted_data = sorted(numeric_data)
                n = len(numeric_data)
                mean = sum(numeric_data) / n
                variance = sum((x - mean) ** 2 for x in numeric_data) / n
                std = math.sqrt(variance)

                return ActionResult(
                    success=True,
                    data={
                        "count": n,
                        "mean": round(mean, 6),
                        "variance": round(variance, 6),
                        "std": round(std, 6),
                        "min": min(numeric_data),
                        "max": max(numeric_data),
                        "median": sorted_data[n // 2],
                        "q1": sorted_data[n // 4],
                        "q3": sorted_data[3 * n // 4],
                        "iqr": round(sorted_data[3 * n // 4] - sorted_data[n // 4], 6),
                        "skewness": round(self._skewness(numeric_data, mean, std), 6),
                        "kurtosis": round(self._kurtosis(numeric_data, mean, std), 6)
                    }
                )

            elif operation == "ttest":
                x = params.get("x", [])
                y = params.get("y", [])
                if len(x) < 2 or len(y) < 2:
                    return ActionResult(success=False, message="Both samples need at least 2 values")
                t_stat, p_value = self._ttest(x, y)
                return ActionResult(success=True, data={"t_statistic": round(t_stat, 6), "p_value": round(p_value, 6)})

            elif operation == "chi_square":
                observed = params.get("observed", [])
                expected = params.get("expected", [])
                if len(observed) != len(expected):
                    return ActionResult(success=False, message="observed and expected must have same length")
                chi2 = sum((observed[i] - expected[i]) ** 2 / expected[i] for i in range(len(observed)) if expected[i] != 0)
                return ActionResult(success=True, data={"chi_square": round(chi2, 6), "df": len(observed) - 1})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Statistics error: {str(e)}")

    def _is_numeric(self, s: str) -> bool:
        try:
            float(s)
            return True
        except:
            return False

    def _skewness(self, data: List[float], mean: float, std: float) -> float:
        if std == 0 or len(data) < 3:
            return 0
        n = len(data)
        return (sum((x - mean) ** 3 for x in data) / n) / (std ** 3)

    def _kurtosis(self, data: List[float], mean: float, std: float) -> float:
        if std == 0 or len(data) < 4:
            return 0
        n = len(data)
        return (sum((x - mean) ** 4 for x in data) / n) / (std ** 4) - 3

    def _ttest(self, x: List[float], y: List[float]) -> Tuple[float, float]:
        mean_x, mean_y = sum(x) / len(x), sum(y) / len(y)
        var_x = sum((xi - mean_x) ** 2 for xi in x) / (len(x) - 1) if len(x) > 1 else 0
        var_y = sum((yi - mean_y) ** 2 for yi in y) / (len(y) - 1) if len(y) > 1 else 0
        pooled_se = math.sqrt(var_x / len(x) + var_y / len(y))
        if pooled_se == 0:
            return 0, 1
        t = (mean_x - mean_y) / pooled_se
        df = len(x) + len(y) - 2
        p_value = 1.0 - min(0.99, self._t_cdf(abs(t), df))
        return t, p_value

    def _t_cdf(self, t: float, df: int) -> float:
        import math
        x = df / (df + t * t)
        return 1 - 0.5 * (x ** (df / 2))
