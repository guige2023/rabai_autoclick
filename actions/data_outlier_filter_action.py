"""Data Outlier Filter Action Module.

Provides outlier detection and filtering using statistical methods,
clustering, and custom criteria for data cleaning.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class OutlierMethod(Enum):
    ZSCORE = "zscore"
    IQR = "iqr"
    MAD = "mad"
    MODIFIED_ZSCORE = "modified_zscore"
    DBSCAN = "dbscan"
    ISOLATION_FOREST = "isolation_forest"
    CUSTOM = "custom"


@dataclass
class OutlierResult:
    index: int
    value: Any
    score: float
    is_outlier: bool
    method: OutlierMethod
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FilterConfig:
    method: OutlierMethod = OutlierMethod.ZSCORE
    threshold: float = 3.0
    contamination: float = 0.1
    window_size: Optional[int] = None
    custom_fn: Optional[Callable[[Any], float]] = None
    return_mask: bool = False


@dataclass
class FilterStats:
    total_count: int
    outlier_count: int
    inlier_count: int
    outlier_percentage: float
    method: OutlierMethod
    duration_ms: float


class OutlierFilter:
    def __init__(self, config: Optional[FilterConfig] = None):
        self.config = config or FilterConfig()
        self._stats: Optional[FilterStats] = None

    def fit_transform(self, data: List[float]) -> List[float]:
        results = self.detect_outliers(data)
        return [v for i, v in enumerate(data) if not results[i].is_outlier]

    def transform(self, data: List[float]) -> List[float]:
        results = self.detect_outliers(data)
        return [v for i, v in enumerate(data) if not results[i].is_outlier]

    def detect_outliers(self, data: List[float]) -> List[OutlierResult]:
        import time
        start = time.time()

        if self.config.method == OutlierMethod.ZSCORE:
            results = self._zscore_outliers(data)
        elif self.config.method == OutlierMethod.IQR:
            results = self._iqr_outliers(data)
        elif self.config.method == OutlierMethod.MAD:
            results = self._mad_outliers(data)
        elif self.config.method == OutlierMethod.MODIFIED_ZSCORE:
            results = self._modified_zscore_outliers(data)
        elif self.config.method == OutlierMethod.DBSCAN:
            results = self._dbscan_outliers(data)
        elif self.config.method == OutlierMethod.ISOLATION_FOREST:
            results = self._isolation_forest_outliers(data)
        elif self.config.method == OutlierMethod.CUSTOM:
            results = self._custom_outliers(data)
        else:
            results = [OutlierResult(i, v, 0.0, False, self.config.method) for i, v in enumerate(data)]

        duration_ms = (time.time() - start) * 1000
        outlier_count = sum(1 for r in results if r.is_outlier)

        self._stats = FilterStats(
            total_count=len(data),
            outlier_count=outlier_count,
            inlier_count=len(data) - outlier_count,
            outlier_percentage=outlier_count / len(data) * 100 if data else 0.0,
            method=self.config.method,
            duration_ms=duration_ms,
        )

        return results

    def _zscore_outliers(self, data: List[float]) -> List[OutlierResult]:
        if len(data) < 2:
            return [OutlierResult(i, v, 0.0, False, OutlierMethod.ZSCORE) for i, v in enumerate(data)]

        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        std = math.sqrt(variance) if variance > 0 else 1e-6

        results = []
        for i, value in enumerate(data):
            zscore = abs((value - mean) / std)
            is_outlier = zscore > self.config.threshold
            results.append(OutlierResult(
                index=i,
                value=value,
                score=zscore,
                is_outlier=is_outlier,
                method=OutlierMethod.ZSCORE,
                details={"zscore": zscore, "mean": mean, "std": std},
            ))
        return results

    def _iqr_outliers(self, data: List[float]) -> List[OutlierResult]:
        sorted_data = sorted(enumerate(data), key=lambda x: x[1])
        n = len(sorted_data)
        q1_val = sorted_data[n // 4][1]
        q3_val = sorted_data[3 * n // 4][1]
        iqr = q3_val - q1_val
        lower_bound = q1_val - 1.5 * iqr
        upper_bound = q3_val + 1.5 * iqr

        results = []
        for i, value in enumerate(data):
            is_outlier = value < lower_bound or value > upper_bound
            score = 0.0
            if value < lower_bound:
                score = (lower_bound - value) / iqr if iqr > 0 else 0.0
            elif value > upper_bound:
                score = (value - upper_bound) / iqr if iqr > 0 else 0.0

            results.append(OutlierResult(
                index=i,
                value=value,
                score=abs(score),
                is_outlier=is_outlier,
                method=OutlierMethod.IQR,
                details={"q1": q1_val, "q3": q3_val, "iqr": iqr, "lower": lower_bound, "upper": upper_bound},
            ))
        return results

    def _mad_outliers(self, data: List[float]) -> List[OutlierResult]:
        sorted_data = sorted(data)
        n = len(sorted_data)
        median = sorted_data[n // 2]
        mad = sorted([abs(v - median) for v in data])[n // 2]
        mad_multiplier = 0.6745

        results = []
        for i, value in enumerate(data):
            modified_zscore = mad_multiplier * abs(value - median) / mad if mad > 0 else 0.0
            is_outlier = modified_zscore > self.config.threshold
            results.append(OutlierResult(
                index=i,
                value=value,
                score=modified_zscore,
                is_outlier=is_outlier,
                method=OutlierMethod.MAD,
                details={"median": median, "mad": mad},
            ))
        return results

    def _modified_zscore_outliers(self, data: List[float]) -> List[OutlierResult]:
        return self._mad_outliers(data)

    def _dbscan_outliers(self, data: List[float]) -> List[OutlierResult]:
        if len(data) < 3:
            return [OutlierResult(i, v, 0.0, False, OutlierMethod.DBSCAN) for i, v in enumerate(data)]

        eps = self.config.threshold * self._compute_avg_distance(data)
        min_samples = max(2, int(len(data) * 0.1))

        core_indices = []
        for i in range(len(data)):
            neighbors = sum(1 for j in range(len(data)) if i != j and abs(data[i] - data[j]) < eps)
            if neighbors >= min_samples:
                core_indices.append(i)

        cluster = [-1] * len(data)
        cluster_id = 0
        for i in core_indices:
            if cluster[i] != -1:
                continue
            cluster[i] = cluster_id
            queue = [i]
            while queue:
                current = queue.pop(0)
                if current in core_indices:
                    for j in range(len(data)):
                        if abs(data[current] - data[j]) < eps and cluster[j] == -1:
                            cluster[j] = cluster_id
                            queue.append(j)
            cluster_id += 1

        outlier_indices = set(i for i in range(len(data)) if cluster[i] == -1)

        results = []
        for i, value in enumerate(data):
            results.append(OutlierResult(
                index=i,
                value=value,
                score=1.0 if i in outlier_indices else 0.0,
                is_outlier=i in outlier_indices,
                method=OutlierMethod.DBSCAN,
                details={"cluster": cluster[i], "is_core": i in core_indices},
            ))
        return results

    def _isolation_forest_outliers(self, data: List[float]) -> List[OutlierResult]:
        n = len(data)
        avg_depth = self._compute_avg_isolation_depth(n)
        threshold = -0.5 * math.log(self.config.contamination) if self.config.contamination > 0 else 0.5

        results = []
        for i, value in enumerate(data):
            depth = self._compute_isolation_depth(value, data)
            score = 2 ** (-depth / avg_depth) if avg_depth > 0 else 0.0
            is_outlier = score > threshold
            results.append(OutlierResult(
                index=i,
                value=value,
                score=score,
                is_outlier=is_outlier,
                method=OutlierMethod.ISOLATION_FOREST,
                details={"isolation_depth": depth},
            ))
        return results

    def _custom_outliers(self, data: List[float]) -> List[OutlierResult]:
        if not self.config.custom_fn:
            return [OutlierResult(i, v, 0.0, False, OutlierMethod.CUSTOM) for i, v in enumerate(data)]

        results = []
        for i, value in enumerate(data):
            score = self.config.custom_fn(value)
            is_outlier = score > self.config.threshold
            results.append(OutlierResult(
                index=i,
                value=value,
                score=score,
                is_outlier=is_outlier,
                method=OutlierMethod.CUSTOM,
            ))
        return results

    def _compute_avg_distance(self, data: List[float]) -> float:
        if len(data) < 2:
            return 1.0
        total = sum(abs(data[i] - data[j]) for i in range(len(data)) for j in range(i + 1, len(data)))
        n = len(data)
        return 2 * total / (n * (n - 1)) if n > 1 else 1.0

    def _compute_isolation_depth(self, value: float, data: List[float]) -> float:
        import random
        tree_size = min(256, len(data))
        sample = random.sample(data, tree_size)
        sample.sort()

        depth = 0
        lower, upper = 0, len(sample) - 1
        while lower <= upper and value < sample[upper] and value > sample[lower]:
            mid = (lower + upper) // 2
            if sample[mid] < value:
                lower = mid + 1
            else:
                upper = mid - 1
            depth += 1
            if depth > 10:
                break

        return depth

    def _compute_avg_isolation_depth(self, n: int) -> float:
        if n <= 1:
            return 0.0
        return 2 * (math.log(n - 1) + 0.57721566) - 2 * (n - 1) / n if n > 1 else 0.0

    def get_stats(self) -> Optional[FilterStats]:
        return self._stats
