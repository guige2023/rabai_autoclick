"""Data Correlation Matrix Action.

Computes correlation matrices (Pearson, Spearman, Kendall) for
datasets, with heatmap-friendly output and feature selection.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class CorrelationResult:
    variable1: str
    variable2: str
    coefficient: float
    p_value: float
    method: str
    significant: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "var1": self.variable1,
            "var2": self.variable2,
            "coefficient": round(self.coefficient, 4),
            "p_value": round(self.p_value, 4),
            "method": self.method,
            "significant": self.significant,
        }


class DataCorrelationMatrixAction:
    """Computes pairwise correlations between variables."""

    def _rank(self, values: List[float]) -> List[float]:
        sorted_pairs = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0.0] * len(values)
        for rank, (idx, _) in enumerate(sorted_pairs, 1):
            ranks[idx] = rank
        return ranks

    def pearson(
        self,
        x: List[float],
        y: List[float],
    ) -> Tuple[float, float]:
        n = len(x)
        if n != len(y) or n < 2:
            return 0.0, 1.0
        mx = sum(x) / n
        my = sum(y) / n
        num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
        sx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
        sy = math.sqrt(sum((yi - my) ** 2 for yi in y))
        if sx == 0 or sy == 0:
            return 0.0, 1.0
        r = num / (sx * sy)
        # t-statistic
        t = r * math.sqrt((n - 2) / max(1 - r**2, 1e-10))
        # Approximate p-value
        p = math.exp(-abs(t) * 0.5 + math.log(1 / max(abs(t), 0.1)))
        p = min(max(p, 0.0), 1.0)
        return r, p

    def spearman(self, x: List[float], y: List[float]) -> Tuple[float, float]:
        n = len(x)
        if n != len(y) or n < 2:
            return 0.0, 1.0
        rx = self._rank(x)
        ry = self._rank(y)
        return self.pearson(rx, ry)

    def kendall(self, x: List[float], y: List[float]) -> Tuple[float, float]:
        n = len(x)
        if n != len(y) or n < 2:
            return 0.0, 1.0
        concordant = 0
        discordant = 0
        tied_x = 0
        tied_y = 0
        for i in range(n):
            for j in range(i + 1, n):
                x_diff = x[i] - x[j]
                y_diff = y[i] - y[j]
                product = x_diff * y_diff
                if product != 0:
                    if product > 0:
                        concordant += 1
                    else:
                        discordant += 1
                else:
                    if x_diff == 0:
                        tied_x += 1
                    if y_diff == 0:
                        tied_y += 1
        n_pairs = n * (n - 1) // 2
        tau = (concordant - discordant) / math.sqrt(
            max(n_pairs - tied_x, 1) * max(n_pairs - tied_y, 1)
        )
        # Approximate p-value
        se = math.sqrt((4 * n + 10) / (9 * n * (n - 1)))
        z = abs(tau) / se if se > 0 else 0.0
        p = math.exp(-1.2 * z)
        return tau, min(p, 1.0)

    def correlation_matrix(
        self,
        data: Dict[str, List[float]],
        method: str = "pearson",
        alpha: float = 0.05,
    ) -> Dict[str, Any]:
        variables = list(data.keys())
        n_vars = len(variables)
        matrix: List[List[float]] = [[0.0] * n_vars for _ in range(n_vars)]
        results: List[CorrelationResult] = []
        for i in range(n_vars):
            for j in range(n_vars):
                if i == j:
                    matrix[i][j] = 1.0
                    continue
                if j < i:
                    matrix[i][j] = matrix[j][i]
                    continue
                x, y = data[variables[i]], data[variables[j]]
                if method == "spearman":
                    coef, p = self.spearman(x, y)
                elif method == "kendall":
                    coef, p = self.kendall(x, y)
                else:
                    coef, p = self.pearson(x, y)
                matrix[i][j] = coef
                results.append(CorrelationResult(
                    variable1=variables[i],
                    variable2=variables[j],
                    coefficient=coef,
                    p_value=p,
                    method=method,
                    significant=p < alpha,
                ))
        return {
            "variables": variables,
            "matrix": matrix,
            "results": [r.as_dict() for r in results],
        }

    def top_correlations(
        self,
        corr_results: List[CorrelationResult],
        n: int = 10,
        absolute: bool = True,
    ) -> List[CorrelationResult]:
        key = (lambda r: abs(r.coefficient)) if absolute else (lambda r: r.coefficient)
        return sorted(corr_results, key=key, reverse=True)[:n]
