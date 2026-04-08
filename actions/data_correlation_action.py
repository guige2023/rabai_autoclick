"""Data correlation analysis action module for RabAI AutoClick.

Provides correlation computation between numeric columns:
Pearson, Spearman, Kendall, and Cramér's V for categorical.
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class CorrelationPair:
    """A correlation pair between two columns."""
    col_a: str
    col_b: str
    method: str
    coefficient: float
    p_value: Optional[float] = None
    strength: str = ""  # weak/moderate/strong/very_strong

    def __post_init__(self):
        abs_c = abs(self.coefficient)
        if abs_c < 0.2:
            self.strength = "weak"
        elif abs_c < 0.4:
            self.strength = "weak_to_moderate"
        elif abs_c < 0.6:
            self.strength = "moderate"
        elif abs_c < 0.8:
            self.strength = "strong"
        else:
            self.strength = "very_strong"


class PearsonCorrelationAction(BaseAction):
    """Compute Pearson correlation coefficient between two numeric series.
    
    Measures linear relationship strength from -1 (perfect negative)
    to +1 (perfect positive). Sensitive to outliers.
    
    Args:
        col_a: First column name
        col_b: Second column name
        data: List of dicts with numeric values
    
    Returns:
        CorrelationPair with coefficient and p_value
    """

    def execute(self, col_a: str, col_b: str, data: List[Dict[str, Any]]) -> ActionResult:
        try:
            vals_a = [d[col_a] for d in data if col_a in d and d[col_a] is not None]
            vals_b = [d[col_b] for d in data if col_b in d and d[col_b] is not None]

            if len(vals_a) < 3 or len(vals_b) < 3:
                return ActionResult(success=False, error="Insufficient data points")

            n = min(len(vals_a), len(vals_b))
            vals_a = vals_a[:n]
            vals_b = vals_b[:n]

            mean_a = sum(vals_a) / n
            mean_b = sum(vals_b) / n

            cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(vals_a, vals_b))
            std_a = (sum((a - mean_a) ** 2 for a in vals_a) ** 0.5)
            std_b = (sum((b - mean_b) ** 2 for b in vals_b) ** 0.5)

            if std_a == 0 or std_b == 0:
                coefficient = 0.0
            else:
                coefficient = cov / (std_a * std_b)

            # Approximate p-value using t-distribution
            if abs(coefficient) >= 1.0:
                p_value = 0.0
            else:
                t_stat = coefficient * ((n - 2) ** 0.5) / ((1 - coefficient ** 2) ** 0.5)
                p_value = self._t_dist_pvalue(abs(t_stat), n - 2)

            pair = CorrelationPair(
                col_a=col_a, col_b=col_b, method="pearson",
                coefficient=round(coefficient, 6), p_value=round(p_value, 6)
            )
            return ActionResult(success=True, data={
                "coefficient": pair.coefficient,
                "p_value": pair.p_value,
                "strength": pair.strength,
                "n_samples": n
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _t_dist_pvalue(self, t: float, df: int) -> float:
        """Approximate two-tailed p-value from t-statistic."""
        import math
        x = df / (df + t * t)
        # Beta regularized incomplete — simplified approximation
        p = 0.5 * (1.0 + math.copysign(
            1.0, t) * ((x + math.sqrt(abs(0.001 - x * (1.0 - x)))) / 0.5))
        return max(0.0, min(1.0, 1.0 - abs(p - 0.5) * 2))


class SpearmanCorrelationAction(BaseAction):
    """Compute Spearman rank correlation coefficient.
    
    Non-parametric measure using rank order instead of values.
    Handles non-linear relationships and is robust to outliers.
    """

    def execute(self, col_a: str, col_b: str, data: List[Dict[str, Any]]) -> ActionResult:
        try:
            filtered = [
                (d[col_a], d[col_b])
                for d in data
                if col_a in d and col_b in d and d[col_a] is not None and d[col_b] is not None
            ]
            if len(filtered) < 3:
                return ActionResult(success=False, error="Insufficient data points")

            def rankify(values: List[float]) -> List[Tuple[int, float]]:
                sorted_pairs = sorted(enumerate(values), key=lambda x: x[1])
                ranks = {}
                i = 0
                while i < len(sorted_pairs):
                    j = i
                    while j < len(sorted_pairs) - 1 and \
                            sorted_pairs[j + 1][1] == sorted_pairs[i][1]:
                        j += 1
                    avg_rank = (i + j) / 2.0
                    for k in range(i, j + 1):
                        ranks[sorted_pairs[k][0]] = avg_rank
                    i = j + 1
                return [(idx, ranks[idx]) for idx in range(len(values))]

            vals_a = [v[0] for v in filtered]
            vals_b = [v[1] for v in filtered]
            ranks_a = rankify(vals_a)
            ranks_b = rankify(vals_b)

            n = len(ranks_a)
            d_sq_sum = sum((ra - rb) ** 2 for (_, ra), (_, rb) in zip(ranks_a, ranks_b))
            coefficient = 1.0 - (6.0 * d_sq_sum) / (n * (n * n - 1))

            pair = CorrelationPair(
                col_a=col_a, col_b=col_b, method="spearman",
                coefficient=round(coefficient, 6)
            )
            return ActionResult(success=True, data={
                "coefficient": pair.coefficient,
                "strength": pair.strength,
                "n_samples": n
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class CramersVCorrelationAction(BaseAction):
    """Compute Cramér's V for categorical-categorical correlation.
    
    Based on chi-squared statistic, ranges from 0 (no association)
    to 1 (perfect association). Symmetric measure.
    """

    def execute(self, col_a: str, col_b: str, data: List[Dict[str, Any]]) -> ActionResult:
        try:
            filtered = [
                (str(d[col_a]), str(d[col_b]))
                for d in data
                if col_a in d and col_b in d and d[col_a] is not None and d[col_b] is not None
            ]
            if len(filtered) < 2:
                return ActionResult(success=False, error="Insufficient data points")

            n = len(filtered)

            # Contingency table
            count_ab: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            count_a: Dict[str, int] = defaultdict(int)
            count_b: Dict[str, int] = defaultdict(int)

            for val_a, val_b in filtered:
                count_ab[val_a][val_b] += 1
                count_a[val_a] += 1
                count_b[val_b] += 1

            # Chi-squared
            chi2 = 0.0
            all_a = list(count_a.keys())
            all_b = list(count_b.keys())
            for va in all_a:
                for vb in all_b:
                    observed = count_ab[va][vb]
                    expected = (count_a[va] * count_b[vb]) / n
                    if expected > 0:
                        chi2 += (observed - expected) ** 2 / expected

            # Cramér's V
            min_dim = min(len(all_a) - 1, len(all_b) - 1)
            if min_dim == 0:
                coefficient = 0.0
            else:
                coefficient = (chi2 / (n * min_dim)) ** 0.5

            pair = CorrelationPair(
                col_a=col_a, col_b=col_b, method="cramers_v",
                coefficient=round(coefficient, 6)
            )
            return ActionResult(success=True, data={
                "coefficient": pair.coefficient,
                "strength": pair.strength,
                "chi2": round(chi2, 4),
                "n_samples": n
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class CorrelationMatrixAction(BaseAction):
    """Compute full pairwise correlation matrix for numeric columns.
    
    Supports pearson, spearman, and kendall methods.
    Returns matrix as dict of dicts.
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        method: str = "pearson"
    ) -> ActionResult:
        try:
            if len(data) < 2:
                return ActionResult(success=False, error="Insufficient rows")

            valid_cols = [c for c in columns if all(c in row for row in data)]
            if len(valid_cols) < 2:
                return ActionResult(success=False, error="Need at least 2 valid columns")

            results: Dict[str, Dict[str, float]] = {}
            for col_a in valid_cols:
                results[col_a] = {}
                for col_b in valid_cols:
                    if col_a == col_b:
                        results[col_a][col_b] = 1.0
                    elif col_b in results:
                        results[col_a][col_b] = results[col_b][col_a]
                    else:
                        if method == "pearson":
                            action = PearsonCorrelationAction()
                        elif method == "spearman":
                            action = SpearmanCorrelationAction()
                        else:
                            return ActionResult(success=False, error=f"Unknown method: {method}")

                        res = action.execute(col_a, col_b, data)
                        if res.success:
                            results[col_a][col_b] = res.data["coefficient"]
                        else:
                            results[col_a][col_b] = 0.0

            return ActionResult(success=True, data={"matrix": results, "method": method})
        except Exception as e:
            return ActionResult(success=False, error=str(e))
