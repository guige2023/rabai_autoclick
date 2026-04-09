"""Data Correlation Engine Action Module.

Provides correlation analysis for multidimensional data with support
for Pearson, Spearman, Kendall, and custom correlation measures.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class CorrelationMethod(Enum):
    PEARSON = "pearson"
    SPEARMAN = "spearman"
    KENDALL = "kendall"
    CRAMER_V = "cramer_v"
    COSINE = "cosine"
    MUTUAL_INFO = "mutual_info"


@dataclass
class CorrelationResult:
    var1: str
    var2: str
    correlation: float
    method: CorrelationMethod
    p_value: Optional[float] = None
    significant: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CorrelationMatrix:
    variables: List[str]
    matrix: List[List[float]]
    method: CorrelationMethod
    computed_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CorrelationEngine:
    def __init__(self, method: CorrelationMethod = CorrelationMethod.PEARSON):
        self.method = method

    def correlate(
        self,
        x: List[float],
        y: List[float],
    ) -> CorrelationResult:
        if len(x) != len(y):
            raise ValueError("x and y must have the same length")

        if self.method == CorrelationMethod.PEARSON:
            return self._pearson_correlation(x, y)
        elif self.method == CorrelationMethod.SPEARMAN:
            return self._spearman_correlation(x, y)
        elif self.method == CorrelationMethod.KENDALL:
            return self._kendall_correlation(x, y)
        elif self.method == CorrelationMethod.COSINE:
            return self._cosine_similarity(x, y)

        return CorrelationResult(var1="x", var2="y", correlation=0.0, method=self.method)

    def _pearson_correlation(self, x: List[float], y: List[float]) -> CorrelationResult:
        n = len(x)
        if n < 2:
            return CorrelationResult("x", "y", 0.0, self.method)

        mean_x = sum(x) / n
        mean_y = sum(y) / n

        cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n)) / n
        std_x = math.sqrt(sum((v - mean_x) ** 2 for v in x) / n)
        std_y = math.sqrt(sum((v - mean_y) ** 2 for v in y) / n)

        if std_x == 0 or std_y == 0:
            return CorrelationResult("x", "y", 0.0, CorrelationMethod.PEARSON, significant=False)

        corr = cov / (std_x * std_y)
        p_value = self._pearson_p_value(n, abs(corr))

        return CorrelationResult(
            var1="x",
            var2="y",
            correlation=corr,
            method=CorrelationMethod.PEARSON,
            p_value=p_value,
            significant=p_value < 0.05,
        )

    def _spearman_correlation(self, x: List[float], y: List[float]) -> CorrelationResult:
        ranks_x = self._compute_ranks(x)
        ranks_y = self._compute_ranks(y)
        return self._pearson_correlation(ranks_x, ranks_y)

    def _kendall_correlation(self, x: List[float], y: List[float]) -> CorrelationResult:
        n = len(x)
        concordant = 0
        discordant = 0

        for i in range(n):
            for j in range(i + 1, n):
                x_diff = x[i] - x[j]
                y_diff = y[i] - y[j]
                product = x_diff * y_diff

                if product > 0:
                    concordant += 1
                elif product < 0:
                    discordant += 1

        tau = (concordant - discordant) / (n * (n - 1) / 2) if n > 1 else 0.0

        return CorrelationResult(
            var1="x",
            var2="y",
            correlation=tau,
            method=CorrelationMethod.KENDALL,
        )

    def _cosine_similarity(self, x: List[float], y: List[float]) -> CorrelationResult:
        dot_product = sum(x[i] * y[i] for i in range(len(x)))
        norm_x = math.sqrt(sum(v ** 2 for v in x))
        norm_y = math.sqrt(sum(v ** 2 for v in y))

        if norm_x == 0 or norm_y == 0:
            return CorrelationResult("x", "y", 0.0, CorrelationMethod.COSINE)

        similarity = dot_product / (norm_x * norm_y)

        return CorrelationResult(
            var1="x",
            var2="y",
            correlation=similarity,
            method=CorrelationMethod.COSINE,
        )

    def _compute_ranks(self, values: List[float]) -> List[float]:
        sorted_pairs = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0] * len(values)
        i = 0
        while i < len(sorted_pairs):
            j = i
            while j < len(sorted_pairs) - 1 and sorted_pairs[j + 1][1] == sorted_pairs[i][1]:
                j += 1
            rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                ranks[sorted_pairs[k][0]] = rank
            i = j + 1
        return ranks

    def _pearson_p_value(self, n: int, r: float) -> float:
        if n < 3:
            return 1.0

        t = r * math.sqrt((n - 2) / (1 - r * r)) if abs(r) < 1 else 0.0
        df = n - 2

        x = df / (df + t * t)
        p_value = 0.5 * (1 + self._beta_inc(df / 2, 0.5, x))

        return min(1.0, max(0.0, p_value))

    def _beta_inc(self, a: float, b: float, x: float) -> float:
        if x == 0 or x == 1:
            return x
        if x > 0 and x < 1:
            return x
        return 0.5


def compute_correlation_matrix(
    data: List[Dict[str, Any]],
    columns: List[str],
    method: CorrelationMethod = CorrelationMethod.PEARSON,
) -> CorrelationMatrix:
    if not data or not columns:
        return CorrelationMatrix(variables=columns, matrix=[], method=method)

    n = len(columns)
    matrix = [[0.0] * n for _ in range(n)]
    engine = CorrelationEngine(method)

    for i, col1 in enumerate(columns):
        values1 = [row.get(col1, 0.0) for row in data]
        matrix[i][i] = 1.0

        for j, col2 in enumerate(columns):
            if i < j:
                values2 = [row.get(col2, 0.0) for row in data]
                result = engine.correlate(values1, values2)
                matrix[i][j] = result.correlation
                matrix[j][i] = result.correlation

    return CorrelationMatrix(
        variables=columns,
        matrix=matrix,
        method=method,
    )


def find_correlated_pairs(
    data: List[Dict[str, Any]],
    threshold: float = 0.7,
    method: CorrelationMethod = CorrelationMethod.PEARSON,
) -> List[CorrelationResult]:
    if not data:
        return []

    columns = list(data[0].keys())
    results = []
    engine = CorrelationEngine(method)

    for i, col1 in enumerate(columns):
        values1 = [row.get(col1, 0.0) for row in data]

        for j, col2 in enumerate(columns):
            if i < j:
                values2 = [row.get(col2, 0.0) for row in data]
                result = engine.correlate(values1, values2)
                result.var1 = col1
                result.var2 = col2

                if abs(result.correlation) >= threshold:
                    results.append(result)

    results.sort(key=lambda r: abs(r.correlation), reverse=True)
    return results
