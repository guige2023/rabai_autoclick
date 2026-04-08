"""
Data Correlation Action Module.

Computes correlations between data features,
supports Pearson, Spearman, and custom correlation methods.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass
import logging
import math

logger = logging.getLogger(__name__)


class CorrelationMethod(Enum):
    """Correlation method types."""
    PEARSON = "pearson"
    SPEARMAN = "spearman"
    KENDALL = "kendall"


@dataclass
class CorrelationResult:
    """Result of correlation computation."""
    variable_a: str
    variable_b: str
    correlation: float
    method: str
    p_value: Optional[float] = None
    significant: bool = False


class DataCorrelationAction:
    """
    Correlation analysis between data features.

    Computes pairwise correlations using
    Pearson, Spearman, or Kendall methods.

    Example:
        corr = DataCorrelationAction()
        result = corr.correlate(data, "x", "y", method=CORRELATION_METHOD.PEARSON)
    """

    def __init__(
        self,
        significance_level: float = 0.05,
    ) -> None:
        self.significance_level = significance_level

    def correlate(
        self,
        data: list[dict],
        var_a: str,
        var_b: str,
        method: str = "pearson",
    ) -> Optional[CorrelationResult]:
        """Compute correlation between two variables."""
        values_a = [row.get(var_a) for row in data if var_a in row]
        values_b = [row.get(var_b) for row in data if var_b in row]

        valid_pairs = [
            (a, b) for a, b in zip(values_a, values_b)
            if a is not None and b is not None
        ]

        if len(valid_pairs) < 3:
            return None

        arr_a = [p[0] for p in valid_pairs]
        arr_b = [p[1] for p in valid_pairs]

        if method == "pearson":
            correlation = self._pearson(arr_a, arr_b)
        elif method == "spearman":
            correlation = self._spearman(arr_a, arr_b)
        elif method == "kendall":
            correlation = self._kendall(arr_a, arr_b)
        else:
            correlation = self._pearson(arr_a, arr_b)

        p_value = self._approximate_p_value(correlation, len(valid_pairs))

        return CorrelationResult(
            variable_a=var_a,
            variable_b=var_b,
            correlation=correlation,
            method=method,
            p_value=p_value,
            significant=p_value < self.significance_level if p_value else False,
        )

    def correlation_matrix(
        self,
        data: list[dict],
        variables: list[str],
        method: str = "pearson",
    ) -> dict[str, dict[str, float]]:
        """Compute correlation matrix for multiple variables."""
        matrix: dict[str, dict[str, float]] = {}

        for var_a in variables:
            matrix[var_a] = {}
            for var_b in variables:
                if var_a == var_b:
                    matrix[var_a][var_b] = 1.0
                elif var_b in matrix:
                    matrix[var_a][var_b] = matrix[var_b][var_a]
                else:
                    result = self.correlate(data, var_a, var_b, method)
                    matrix[var_a][var_b] = result.correlation if result else 0.0

        return matrix

    def find_strong_correlations(
        self,
        data: list[dict],
        variables: list[str],
        threshold: float = 0.7,
        method: str = "pearson",
    ) -> list[CorrelationResult]:
        """Find all correlations above threshold."""
        results = []

        for i, var_a in enumerate(variables):
            for var_b in variables[i + 1:]:
                result = self.correlate(data, var_a, var_b, method)
                if result and abs(result.correlation) >= threshold:
                    results.append(result)

        results.sort(key=lambda x: abs(x.correlation), reverse=True)
        return results

    def _pearson(self, x: list, y: list) -> float:
        """Compute Pearson correlation coefficient."""
        n = len(x)
        if n == 0:
            return 0.0

        mean_x = sum(x) / n
        mean_y = sum(y) / n

        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        denominator = denom_x * denom_y

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def _spearman(self, x: list, y: list) -> float:
        """Compute Spearman rank correlation."""
        ranks_x = self._rank(x)
        ranks_y = self._rank(y)

        return self._pearson(ranks_x, ranks_y)

    def _kendall(self, x: list, y: list) -> float:
        """Compute Kendall tau correlation."""
        n = len(x)
        if n < 2:
            return 0.0

        concordant = 0
        discordant = 0

        for i in range(n):
            for j in range(i + 1, n):
                sign_x = math.copysign(1, x[i] - x[j])
                sign_y = math.copysign(1, y[i] - y[j])
                product = sign_x * sign_y

                if product > 0:
                    concordant += 1
                elif product < 0:
                    discordant += 1

        return (concordant - discordant) / (n * (n - 1) / 2)

    def _rank(self, values: list) -> list[float]:
        """Compute ranks for values."""
        sorted_pairs = sorted(enumerate(values), key=lambda p: p[1])
        ranks = [0] * len(values)

        for rank, (idx, _) in enumerate(sorted_pairs, start=1):
            ranks[idx] = rank

        return ranks

    def _approximate_p_value(
        self,
        correlation: float,
        n: int,
    ) -> Optional[float]:
        """Approximate p-value for correlation."""
        if n < 3:
            return None

        t = correlation * math.sqrt((n - 2) / (1 - correlation ** 2 + 1e-10))
        df = n - 2

        p_value = self._t_distribution_pvalue(abs(t), df)
        return p_value * 2

    @staticmethod
    def _t_distribution_pvalue(t: float, df: int) -> float:
        """Approximate p-value from t-distribution."""
        x = df / (df + t * t)
        return x ** (df / 2)
