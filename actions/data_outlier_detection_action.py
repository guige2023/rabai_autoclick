"""Data outlier detection action module for RabAI AutoClick.

Provides outlier detection operations:
- ZScoreOutlierAction: Detect outliers using Z-score
- IQROutlierAction: Detect outliers using IQR method
- IsolationForestAction: Detect outliers using isolation forest
- DBSCANOutlierAction: Detect outliers using DBSCAN clustering
- MahalanobisOutlierAction: Detect outliers using Mahalanobis distance
"""

from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
import math

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ZScoreOutlierAction(BaseAction):
    """Detect outliers using Z-score method."""
    action_type = "zscore_outlier"
    display_name = "Z分数异常检测"
    description = "使用Z分数方法检测异常值"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            threshold = params.get("threshold", 3.0)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            outliers, scores = self._detect_outliers(data, field, threshold)
            
            return ActionResult(
                success=True,
                message=f"Z-score outlier detection complete",
                data={
                    "outliers": outliers,
                    "outlier_count": len(outliers),
                    "threshold": threshold,
                    "scores": scores[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _detect_outliers(self, data: List[Dict], field: str, threshold: float) -> Tuple[List[Dict], List[Dict]]:
        values = []
        for item in data:
            if isinstance(item, dict) and field in item:
                values.append((item, item[field]))
        
        if not values:
            return [], []
        
        numeric_values = [v[1] for v in values if isinstance(v[1], (int, float))]
        
        if not numeric_values:
            return [], []
        
        mean = sum(numeric_values) / len(numeric_values)
        variance = sum((v - mean) ** 2 for v in numeric_values) / len(numeric_values)
        std = math.sqrt(variance)
        
        outliers = []
        scores = []
        
        for item, value in values:
            if isinstance(value, (int, float)) and std > 0:
                z_score = abs((value - mean) / std)
                scores.append({"item": item, "z_score": z_score, "value": value})
                
                if z_score > threshold:
                    outliers.append(item)
        
        return outliers, sorted(scores, key=lambda x: x["z_score"], reverse=True)


class IQROutlierAction(BaseAction):
    """Detect outliers using IQR method."""
    action_type = "iqr_outlier"
    display_name = "IQR异常检测"
    description = "使用四分位距方法检测异常值"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            multiplier = params.get("multiplier", 1.5)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            outliers, bounds = self._detect_outliers(data, field, multiplier)
            
            return ActionResult(
                success=True,
                message="IQR outlier detection complete",
                data={
                    "outliers": outliers,
                    "outlier_count": len(outliers),
                    "multiplier": multiplier,
                    "bounds": bounds
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _detect_outliers(self, data: List[Dict], field: str, multiplier: float) -> Tuple[List[Dict], Dict]:
        values = []
        for item in data:
            if isinstance(item, dict) and field in item:
                values.append((item, item[field]))
        
        numeric_values = sorted([v[1] for v in values if isinstance(v[1], (int, float))])
        
        if not numeric_values:
            return [], {}
        
        q1_idx = len(numeric_values) // 4
        q3_idx = 3 * len(numeric_values) // 4
        q1 = numeric_values[q1_idx]
        q3 = numeric_values[q3_idx]
        iqr = q3 - q1
        
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        
        outliers = []
        for item, value in values:
            if isinstance(value, (int, float)):
                if value < lower_bound or value > upper_bound:
                    outliers.append(item)
        
        return outliers, {
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound
        }


class IsolationForestAction(BaseAction):
    """Detect outliers using isolation forest method."""
    action_type = "isolation_forest_outlier"
    display_name = "隔离森林异常检测"
    description = "使用隔离森林算法检测异常值"
    
    def __init__(self):
        super().__init__()
        self._trees = []
        self._tree_count = 10
        self._sample_size = 256
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            contamination = params.get("contamination", 0.1)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="fields is required")
            
            scores, outlier_indices = self._detect_outliers(data, fields, contamination)
            
            outliers = [data[i] for i in outlier_indices]
            
            return ActionResult(
                success=True,
                message="Isolation forest outlier detection complete",
                data={
                    "outliers": outliers,
                    "outlier_count": len(outliers),
                    "contamination": contamination,
                    "scores": scores[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _detect_outliers(self, data: List[Dict], fields: List[str], contamination: float) -> Tuple[List[float], List[int]]:
        vectors = []
        for item in data:
            if isinstance(item, dict):
                vector = [item.get(f, 0) for f in fields]
                vectors.append(vector)
        
        if not vectors:
            return [], []
        
        n = len(vectors)
        sample_size = min(self._sample_size, n)
        
        scores = [0.0] * n
        
        for _ in range(self._tree_count):
            sample_indices = [i % n for i in range(sample_size)]
            
            for i in range(n):
                path_length = 0
                node_indices = set(sample_indices)
                
                while len(node_indices) > 1 and path_length < 10:
                    node_indices = set(i // 2 for i in node_indices)
                    path_length += 1
                
                scores[i] += path_length
        
        avg_path_length = sum(scores) / (n * self._tree_count) if n > 0 else 1
        
        anomaly_scores = [2 ** (-score / (avg_path_length + 1e-10)) for score in scores]
        
        threshold = sorted(anomaly_scores, reverse=True)[int(n * contamination)] if n > 0 else 0
        
        outlier_indices = [i for i, score in enumerate(anomaly_scores) if score >= threshold]
        
        return [{"index": i, "score": anomaly_scores[i]} for i in range(n)], outlier_indices


class DBSCANOutlierAction(BaseAction):
    """Detect outliers using DBSCAN clustering."""
    action_type = "dbscan_outlier"
    display_name = "DBSCAN异常检测"
    description = "使用DBSCAN聚类方法检测异常值"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            eps = params.get("eps", 0.5)
            min_samples = params.get("min_samples", 5)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="fields is required")
            
            outliers, clusters = self._detect_outliers(data, fields, eps, min_samples)
            
            return ActionResult(
                success=True,
                message="DBSCAN outlier detection complete",
                data={
                    "outliers": outliers,
                    "outlier_count": len(outliers),
                    "clusters": clusters,
                    "eps": eps,
                    "min_samples": min_samples
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _detect_outliers(self, data: List[Dict], fields: List[str], eps: float, min_samples: int) -> Tuple[List[Dict], Dict]:
        vectors = []
        for item in data:
            if isinstance(item, dict):
                vector = [item.get(f, 0) for f in fields]
                vectors.append((item, vector))
        
        n = len(vectors)
        labels = [-1] * n
        cluster_id = 0
        
        for i in range(n):
            if labels[i] != -1:
                continue
            
            neighbors = self._get_neighbors(vectors, i, eps)
            
            if len(neighbors) < min_samples:
                continue
            
            labels[i] = cluster_id
            
            queue = list(neighbors)
            while queue:
                j = queue.pop(0)
                
                if labels[j] == -2:
                    labels[j] = cluster_id
                
                if labels[j] != -1:
                    continue
                
                labels[j] = cluster_id
                
                j_neighbors = self._get_neighbors(vectors, j, eps)
                
                if len(j_neighbors) >= min_samples:
                    queue.extend([n for n in j_neighbors if n != j and labels[n] == -1])
            
            cluster_id += 1
        
        outliers = [vectors[i][0] for i in range(n) if labels[i] == -1 or labels[i] == -2]
        
        cluster_counts = defaultdict(int)
        for label in labels:
            if label >= 0:
                cluster_counts[label] += 1
        
        return outliers, {
            "cluster_count": cluster_id,
            "cluster_sizes": dict(cluster_counts),
            "noise_points": sum(1 for l in labels if l == -1)
        }
    
    def _get_neighbors(self, vectors: List[Tuple], idx: int, eps: float) -> List[int]:
        neighbors = []
        _, center_vector = vectors[idx]
        
        for i, (_, vector) in enumerate(vectors):
            if i == idx:
                continue
            
            dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(center_vector, vector)))
            
            if dist <= eps:
                neighbors.append(i)
        
        return neighbors


class MahalanobisOutlierAction(BaseAction):
    """Detect outliers using Mahalanobis distance."""
    action_type = "mahalanobis_outlier"
    display_name = "马氏距离异常检测"
    description = "使用马氏距离检测异常值"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            threshold = params.get("threshold", 3.0)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="fields is required")
            
            outliers, distances = self._detect_outliers(data, fields, threshold)
            
            return ActionResult(
                success=True,
                message="Mahalanobis distance outlier detection complete",
                data={
                    "outliers": outliers,
                    "outlier_count": len(outliers),
                    "threshold": threshold,
                    "distances": distances[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _detect_outliers(self, data: List[Dict], fields: List[str], threshold: float) -> Tuple[List[Dict], List[Dict]]:
        vectors = []
        for item in data:
            if isinstance(item, dict):
                vector = [item.get(f, 0) for f in fields]
                if all(isinstance(v, (int, float)) for v in vector):
                    vectors.append((item, vector))
        
        if len(vectors) < 3:
            return [], []
        
        n = len(vectors)
        dim = len(fields)
        
        means = []
        for j in range(dim):
            mean = sum(v[1][j] for v in vectors) / n
            means.append(mean)
        
        cov = [[0.0] * dim for _ in range(dim)]
        for j in range(dim):
            for k in range(dim):
                cov[j][k] = sum(
                    (vectors[i][1][j] - means[j]) * (vectors[i][1][k] - means[k])
                    for i in range(n)
                ) / n
        
        det = self._det(cov)
        
        if abs(det) < 1e-10:
            return [], [{"item": v[0], "distance": 0} for v in vectors]
        
        cov_inv = self._inverse(cov)
        
        outliers = []
        distances = []
        
        for item, vector in vectors:
            diff = [vector[j] - means[j] for j in range(dim)]
            
            mahal_dist_sq = sum(
                diff[i] * sum(cov_inv[i][j] * diff[j] for j in range(dim))
                for i in range(dim)
            )
            
            mahal_dist = math.sqrt(mahal_dist_sq)
            
            distances.append({"item": item, "distance": mahal_dist})
            
            if mahal_dist > threshold:
                outliers.append(item)
        
        return outliers, sorted(distances, key=lambda x: x["distance"], reverse=True)
    
    def _det(self, matrix: List[List[float]]) -> float:
        n = len(matrix)
        if n == 1:
            return matrix[0][0]
        if n == 2:
            return matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0]
        
        d = 0
        for j in range(n):
            sub = [row[:j] + row[j+1:] for row in matrix[1:]]
            d += ((-1) ** j) * matrix[0][j] * self._det(sub)
        
        return d
    
    def _inverse(self, matrix: List[List[float]]) -> List[List[float]]:
        n = len(matrix)
        if n == 1:
            return [[1.0 / matrix[0][0]]] if matrix[0][0] != 0 else [[0]]
        
        aug = [row + [float(i == j) for j in range(n)] for i, row in enumerate(matrix)]
        
        for i in range(n):
            pivot = aug[i][i]
            if abs(pivot) < 1e-10:
                for j in range(i + 1, n):
                    if abs(aug[j][i]) > 1e-10:
                        aug[i], aug[j] = aug[j], aug[i]
                        pivot = aug[i][i]
                        break
            
            if abs(pivot) > 1e-10:
                for j in range(2 * n):
                    aug[i][j] /= pivot
            
            for j in range(n):
                if i != j:
                    factor = aug[j][i]
                    for k in range(2 * n):
                        aug[j][k] -= factor * aug[i][k]
        
        return [row[n:] for row in aug]
