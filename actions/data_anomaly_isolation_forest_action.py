"""Data Anomaly Isolation Forest Action.

Isolation Forest algorithm for anomaly detection in high-dimensional
data. Supports scoring and prediction.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math
import random


@dataclass
class IsolationTreeNode:
    left: Optional["IsolationTreeNode"] = None
    right: Optional["IsolationTreeNode"] = None
    feature_index: Optional[int] = None
    threshold: Optional[float] = None
    size: int = 0
    is_leaf: bool = False
    anomaly_score: Optional[float] = None


class IsolationForest:
    """Isolation Forest for anomaly detection."""

    def __init__(
        self,
        n_estimators: int = 100,
        max_samples: int = 256,
        max_depth: int = 10,
        seed: Optional[int] = None,
    ) -> None:
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.max_depth = max_depth
        self.rng = random.Random(seed)
        self._trees: List[IsolationTreeNode] = []
        self._c: float = 0.0
        self._fitted: bool = False

    def _c_factor(self, n: int) -> float:
        if n <= 1:
            return 1.0
        return 2.0 * (math.log(n - 1) + 0.5772156649) - 2.0 * (n - 1) / n

    def _build_tree(
        self,
        data: List[List[float]],
        depth: int,
    ) -> IsolationTreeNode:
        n = len(data)
        dim = len(data[0]) if data else 0
        if depth >= self.max_depth or n <= 1 or dim == 0:
            node = IsolationTreeNode(size=n, is_leaf=True)
            return node
        feat_idx = self.rng.randint(0, dim - 1)
        col = [row[feat_idx] for row in data]
        min_val, max_val = min(col), max(col)
        if min_val == max_val:
            node = IsolationTreeNode(size=n, is_leaf=True)
            return node
        threshold = min_val + self.rng.random() * (max_val - min_val)
        left_data = [row for row in data if row[feat_idx] < threshold]
        right_data = [row for row in data if row[feat_idx] >= threshold]
        node = IsolationTreeNode(
            feature_index=feat_idx,
            threshold=threshold,
            size=n,
            is_leaf=False,
        )
        node.left = self._build_tree(left_data, depth + 1)
        node.right = self._build_tree(right_data, depth + 1)
        return node

    def _path_length(self, point: List[float], node: IsolationTreeNode, depth: int) -> float:
        if node.is_leaf or depth >= self.max_depth:
            return depth + self._c_factor(node.size)
        feat_idx = node.feature_index or 0
        threshold = node.threshold or 0.0
        if point[feat_idx] < threshold:
            return self._path_length(point, node.left, depth + 1)
        else:
            return self._path_length(point, node.right, depth + 1)

    def _avg_path_length(self, n: int) -> float:
        if n <= 1:
            return 0.0
        if n == 2:
            return 1.0
        return self._c_factor(n)

    def fit(self, data: List[List[float]]) -> "IsolationForest":
        self.rng.seed(self.n_estimators if self._fitted else 0)
        self._c = self._c_factor(len(data)) if data else 1.0
        self._trees = []
        for _ in range(self.n_estimators):
            sample = data[: self.max_samples]
            if len(sample) > 0:
                self.rng.shuffle(sample)
            if len(sample) < self.max_samples and len(data) > self.max_samples:
                sample = [self.rng.choice(data) for _ in range(self.max_samples)]
            tree = self._build_tree(sample, 0)
            self._trees.append(tree)
        self._fitted = True
        return self

    def anomaly_score(self, point: List[float]) -> float:
        if not self._trees:
            return 0.0
        avg_h = sum(self._path_length(point, tree, 0) for tree in self._trees) / len(self._trees)
        score = math.pow(2, -avg_h / self._c)
        return score

    def predict(self, point: List[float], threshold: float = 0.5) -> bool:
        return self.anomaly_score(point) >= threshold

    def score_samples(self, data: List[List[float]]) -> List[float]:
        return [self.anomaly_score(point) for point in data]


class DataAnomalyIsolationForestAction:
    """Isolation Forest anomaly detection."""

    def __init__(
        self,
        n_estimators: int = 100,
        max_samples: int = 256,
        max_depth: int = 10,
        threshold: float = 0.5,
        seed: Optional[int] = None,
    ) -> None:
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.max_depth = max_depth
        self.threshold = threshold
        self.forest: Optional[IsolationForest] = None
        self._is_fitted = False

    def fit(self, data: List[List[float]]) -> "DataAnomalyIsolationForestAction":
        self.forest = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            max_depth=self.max_depth,
            seed=seed,
        )
        self.forest.fit(data)
        self._is_fitted = True
        return self

    def detect(
        self,
        data: List[List[float]],
        threshold: Optional[float] = None,
    ) -> List[bool]:
        thresh = threshold or self.threshold
        if not self._is_fitted:
            self.fit(data)
        return [self.forest.anomaly_score(point) >= thresh for point in data]

    def score(self, data: List[List[float]]) -> List[float]:
        if not self._is_fitted:
            self.fit(data)
        return self.forest.score_samples(data)

    def anomaly_report(self, data: List[List[float]]) -> Dict[str, Any]:
        scores = self.score(data)
        thresh = self.threshold
        anomalies = [s >= thresh for s in scores]
        return {
            "anomaly_scores": [round(s, 4) for s in scores],
            "anomaly_mask": anomalies,
            "anomaly_count": sum(anomalies),
            "anomaly_rate": sum(anomalies) / len(anomalies) if anomalies else 0.0,
            "mean_score": round(sum(scores) / len(scores), 4),
            "max_score": round(max(scores), 4),
        }
