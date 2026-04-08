"""Data Correlation Action.

Computes correlations between data fields and features.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass
import math


T = TypeVar("T")


@dataclass
class CorrelationResult:
    field_a: str
    field_b: str
    correlation: float
    method: str
    p_value: Optional[float] = None


class DataCorrelationAction:
    """Computes correlations between data fields."""

    def __init__(self) -> None:
        self.field_data: Dict[str, List[float]] = {}

    def add_field(self, name: str, values: List[float]) -> None:
        self.field_data[name] = values

    def pearson(
        self,
        field_a: str,
        field_b: str,
    ) -> Optional[CorrelationResult]:
        a = self.field_data.get(field_a)
        b = self.field_data.get(field_b)
        if not a or not b or len(a) != len(b):
            return None
        n = len(a)
        mean_a = sum(a) / n
        mean_b = sum(b) / n
        cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b)) / n
        std_a = math.sqrt(sum((x - mean_a) ** 2 for x in a) / n)
        std_b = math.sqrt(sum((y - mean_b) ** 2 for y in b) / n)
        if std_a == 0 or std_b == 0:
            return CorrelationResult(field_a=field_a, field_b=field_b, correlation=0.0, method="pearson")
        corr = cov / (std_a * std_b)
        return CorrelationResult(field_a=field_a, field_b=field_b, correlation=corr, method="pearson")

    def spearman(
        self,
        field_a: str,
        field_b: str,
    ) -> Optional[CorrelationResult]:
        a = self.field_data.get(field_a)
        b = self.field_data.get(field_b)
        if not a or not b or len(a) != len(b):
            return None
        a_ranks = self._rank(a)
        b_ranks = self._rank(b)
        return self.pearson("_rank_a", "_rank_b")

    def _rank(self, values: List[float]) -> List[float]:
        sorted_vals = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0.0] * len(values)
        for rank, (orig_idx, _) in enumerate(sorted_vals, 1):
            ranks[orig_idx] = rank
        return ranks

    def correlation_matrix(
        self,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, float]]:
        field_list = fields or list(self.field_data.keys())
        matrix = {}
        for fa in field_list:
            matrix[fa] = {}
            for fb in field_list:
                result = self.pearson(fa, fb)
                matrix[fa][fb] = result.correlation if result else 0.0
        return matrix

    def get_top_correlations(
        self,
        field_name: str,
        n: int = 5,
        positive: bool = True,
    ) -> List[Tuple[str, float]]:
        field_list = [f for f in self.field_data.keys() if f != field_name]
        correlations = []
        for other in field_list:
            result = self.pearson(field_name, other)
            if result:
                correlations.append((other, result.correlation))
        if positive:
            correlations = [(f, c) for f, c in correlations if c > 0]
        correlations.sort(key=lambda x: x[1], reverse=positive)
        return correlations[:n]
