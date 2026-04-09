"""
Correlation analysis module for measuring statistical relationships.

Provides Pearson, Spearman, and Kendall correlation coefficients,
plus correlation matrices and significance testing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class CorrelationType(Enum):
    """Types of correlation coefficients."""
    PEARSON = auto()
    SPEARMAN = auto()
    KENDALL = auto()
    CRAMER_V = auto()


@dataclass
class CorrelationResult:
    """Result of correlation analysis between two variables."""
    coefficient: float
    p_value: float
    method: CorrelationType
    sample_size: int
    confidence_level: float
    is_significant: bool
    interpretation: str


@dataclass
class CorrelationMatrix:
    """Correlation matrix for multiple variables."""
    variables: list[str]
    matrix: list[list[float]]
    method: CorrelationType
    is_symmetric: bool


class CorrelationAnalyzer:
    """
    Analyzes correlations between variables using multiple methods.
    
    Example:
        analyzer = CorrelationAnalyzer()
        x = [1, 2, 3, 4, 5]
        y = [2, 4, 5, 4, 5]
        result = analyzer.correlate(x, y, method=CorrelationType.PEARSON)
    """

    def __init__(self, default_alpha: float = 0.05) -> None:
        """
        Initialize correlation analyzer.
        
        Args:
            default_alpha: Default significance level for hypothesis tests.
        """
        self.default_alpha = default_alpha

    def correlate(
        self,
        x: list[float],
        y: list[float],
        method: CorrelationType = CorrelationType.PEARSON,
        alpha: Optional[float] = None
    ) -> CorrelationResult:
        """
        Compute correlation between two variables.
        
        Args:
            x: First variable values.
            y: Second variable values (must be same length as x).
            method: Correlation method to use.
            alpha: Significance level (defaults to instance default).
            
        Returns:
            CorrelationResult with coefficient and statistics.
            
        Raises:
            ValueError: If x and y have different lengths or are empty.
        """
        if len(x) != len(y):
            raise ValueError("x and y must have the same length")
        if len(x) < 3:
            raise ValueError("Need at least 3 data points")
        if alpha is None:
            alpha = self.default_alpha

        methods: dict[CorrelationType, callable] = {
            CorrelationType.PEARSON: self._pearson_correlation,
            CorrelationType.SPEARMAN: self._spearman_correlation,
            CorrelationType.KENDALL: self._kendall_correlation,
        }

        computer = methods.get(method)
        if computer is None:
            raise ValueError(f"Unknown correlation method: {method}")

        coef = computer(x, y)
        p_value = self._compute_p_value(coef, len(x), method)
        is_significant = p_value < alpha
        interpretation = self._interpret_coefficient(coef)

        return CorrelationResult(
            coefficient=round(coef, 6),
            p_value=round(p_value, 6),
            method=method,
            sample_size=len(x),
            confidence_level=round(1 - alpha, 2),
            is_significant=is_significant,
            interpretation=interpretation
        )

    def _mean(self, data: list[float]) -> float:
        """Compute mean."""
        return sum(data) / len(data)

    def _pearson_correlation(self, x: list[float], y: list[float]) -> float:
        """Compute Pearson correlation coefficient."""
        n = len(x)
        mean_x = self._mean(x)
        mean_y = self._mean(y)

        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        sum_sq_x = sum((xi - mean_x) ** 2 for xi in x)
        sum_sq_y = sum((yi - mean_y) ** 2 for yi in y)

        denominator = math.sqrt(sum_sq_x * sum_sq_y)
        if denominator < 1e-10:
            return 0.0

        return numerator / denominator

    def _rank_data(self, data: list[float]) -> list[float]:
        """Rank data with average ties."""
        sorted_with_idx = sorted(enumerate(data), key=lambda t: t[1])
        ranks = [0.0] * len(data)
        i = 0

        while i < n := len(data):
            j = i
            while j < n and sorted_with_idx[j][1] == sorted_with_idx[i][1]:
                j += 1
            avg_rank = (i + j - 1) / 2.0 + 1  # 1-indexed
            for k in range(i, j):
                ranks[sorted_with_idx[k][0]] = avg_rank
            i = j

        return ranks

    def _spearman_correlation(self, x: list[float], y: list[float]) -> float:
        """Compute Spearman rank correlation."""
        ranked_x = self._rank_data(x)
        ranked_y = self._rank_data(y)
        return self._pearson_correlation(ranked_x, ranked_y)

    def _kendall_correlation(self, x: list[float], y: list[float]) -> float:
        """Compute Kendall tau correlation."""
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

        total = n * (n - 1) / 2
        if total < 1e-10:
            return 0.0

        return (concordant - discordant) / total

    def _compute_p_value(
        self,
        r: float,
        n: int,
        method: CorrelationType
    ) -> float:
        """Approximate p-value for correlation coefficient."""
        # Fisher's z-transformation approximation
        if method == CorrelationType.PEARSON:
            if abs(r) >= 1.0:
                return 0.0 if r != 1.0 else 1.0
            # t-statistic: t = r * sqrt(n-2) / sqrt(1-r^2)
            df = n - 2
            if df <= 0 or (1 - r * r) < 1e-10:
                return 0.001
            t_stat = r * math.sqrt(df) / math.sqrt(1 - r * r)
            # Approximate p-value using normal distribution for large samples
            # |t| > 2 is roughly p < 0.05
            return max(0.001, min(0.999, 2.0 / (abs(t_stat) + 1)))

        # For non-parametric methods, use a simpler heuristic
        if abs(r) > 0.9:
            return 0.001
        elif abs(r) > 0.7:
            return 0.01
        elif abs(r) > 0.5:
            return 0.05
        elif abs(r) > 0.3:
            return 0.1
        else:
            return 0.5

    def _interpret_coefficient(self, r: float) -> str:
        """Provide interpretation of correlation strength."""
        abs_r = abs(r)
        if abs_r >= 0.9:
            strength = "very strong"
        elif abs_r >= 0.7:
            strength = "strong"
        elif abs_r >= 0.5:
            strength = "moderate"
        elif abs_r >= 0.3:
            strength = "weak"
        else:
            strength = "very weak/negligible"

        direction = "positive" if r >= 0 else "negative"
        return f"{strength} {direction} correlation"

    def compute_matrix(
        self,
        data: dict[str, list[float]],
        method: CorrelationType = CorrelationType.PEARSON
    ) -> CorrelationMatrix:
        """
        Compute correlation matrix for multiple variables.
        
        Args:
            data: Dictionary mapping variable names to their values.
            method: Correlation method to use.
            
        Returns:
            CorrelationMatrix with all pairwise correlations.
        """
        variables = list(data.keys())
        n_vars = len(variables)
        matrix = [[0.0] * n_vars for _ in range(n_vars)]

        for i, var1 in enumerate(variables):
            for j, var2 in enumerate(variables):
                if i == j:
                    matrix[i][j] = 1.0
                elif j > i:
                    result = self.correlate(data[var1], data[var2], method)
                    matrix[i][j] = result.coefficient
                    matrix[j][i] = result.coefficient

        return CorrelationMatrix(
            variables=variables,
            matrix=matrix,
            method=method,
            is_symmetric=True
        )

    def find_strongest_correlations(
        self,
        data: dict[str, list[float]],
        threshold: float = 0.7,
        method: CorrelationType = CorrelationType.PEARSON
    ) -> list[tuple[str, str, float]]:
        """
        Find variable pairs with strong correlations.
        
        Args:
            data: Dictionary mapping variable names to their values.
            threshold: Absolute correlation threshold.
            method: Correlation method to use.
            
        Returns:
            List of (var1, var2, correlation) tuples above threshold.
        """
        matrix = self.compute_matrix(data, method)
        variables = matrix.variables
        results = []

        for i, var1 in enumerate(variables):
            for j, var2 in enumerate(variables):
                if j > i:
                    coef = matrix.matrix[i][j]
                    if abs(coef) >= threshold:
                        results.append((var1, var2, coef))

        results.sort(key=lambda x: abs(x[2]), reverse=True)
        return results

    def partial_correlation(
        self,
        x: list[float],
        y: list[float],
        z: list[float]
    ) -> float:
        """
        Compute partial correlation between x and y, controlling for z.
        
        Args:
            x: First variable.
            y: Second variable.
            z: Control variable.
            
        Returns:
            Partial correlation coefficient.
        """
        # Residualize x and y with respect to z
        def residualize(a: list[float], b: list[float]) -> list[float]:
            n = len(a)
            mean_a = self._mean(a)
            mean_b = self._mean(b)
            b_centered = [bi - mean_b for bi in b]

            numerator = sum(ai * bi for ai, bi in zip(a, b_centered))
            denominator = sum(bi * bi for bi in b_centered)

            if denominator < 1e-10:
                return [ai - mean_a for ai in a]

            slope = numerator / denominator
            return [ai - mean_a - slope * (bi - mean_b) for ai, bi in zip(a, b)]

        x_resid = residualize(x, z)
        y_resid = residualize(y, z)

        return self._pearson_correlation(x_resid, y_resid)
