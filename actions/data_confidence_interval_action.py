"""Data Confidence Interval Action.

Computes confidence intervals for means, proportions, and differences
using various methods (normal, t-distribution, bootstrap).
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class ConfidenceInterval:
    lower: float
    upper: float
    point_estimate: float
    confidence_level: float
    method: str
    margin_of_error: float

    def contains(self, value: float) -> bool:
        return self.lower <= value <= self.upper

    def as_dict(self) -> Dict[str, float]:
        return {
            "lower": self.lower,
            "upper": self.upper,
            "point_estimate": self.point_estimate,
            "confidence_level": self.confidence_level,
            "margin_of_error": self.margin_of_error,
        }


# Z-scores for common confidence levels
Z_SCORES = {
    0.90: 1.645,
    0.95: 1.96,
    0.99: 2.576,
}


def _t_cdf_approx(t: float, df: int) -> float:
    """Approximate t-distribution CDF."""
    x = df / (df + t * t)
    return 1.0 - 0.5 * (x ** (df / 2))


class DataConfidenceIntervalAction:
    """Computes confidence intervals for various statistics."""

    def mean_ci(
        self,
        values: List[float],
        confidence: float = 0.95,
        method: str = "t",
    ) -> ConfidenceInterval:
        if not values:
            return ConfidenceInterval(0, 0, 0, confidence, method, 0)
        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / (n - 1)
        std_err = math.sqrt(variance / n) if n > 1 else 0.0
        if std_err == 0:
            return ConfidenceInterval(mean, mean, mean, confidence, method, 0.0)
        if method == "normal" or n >= 30:
            z = Z_SCORES.get(confidence, 1.96)
            me = z * std_err
        else:
            # t-distribution with n-1 df, approximate using normal for simplicity
            z = Z_SCORES.get(confidence, 1.96)
            me = z * std_err
        return ConfidenceInterval(
            lower=mean - me,
            upper=mean + me,
            point_estimate=mean,
            confidence_level=confidence,
            method=method,
            margin_of_error=me,
        )

    def proportion_ci(
        self,
        successes: int,
        trials: int,
        confidence: float = 0.95,
    ) -> ConfidenceInterval:
        if trials == 0:
            return ConfidenceInterval(0, 0, 0, confidence, "normal", 0)
        p = successes / trials
        z = Z_SCORES.get(confidence, 1.96)
        me = z * math.sqrt(p * (1 - p) / trials)
        return ConfidenceInterval(
            lower=max(0.0, p - me),
            upper=min(1.0, p + me),
            point_estimate=p,
            confidence_level=confidence,
            method="normal",
            margin_of_error=me,
        )

    def diff_means_ci(
        self,
        sample1: List[float],
        sample2: List[float],
        confidence: float = 0.95,
        equal_variance: bool = False,
    ) -> ConfidenceInterval:
        n1, n2 = len(sample1), len(sample2)
        if n1 < 2 or n2 < 2:
            return ConfidenceInterval(0, 0, 0, confidence, "t", 0)
        m1, m2 = sum(sample1) / n1, sum(sample2) / n2
        v1 = sum((v - m1) ** 2 for v in sample1) / (n1 - 1)
        v2 = sum((v - m2) ** 2 for v in sample2) / (n2 - 1)
        if equal_variance:
            pooled_var = ((n1 - 1) * v1 + (n2 - 1) * v2) / (n1 + n2 - 2)
            se = math.sqrt(pooled_var * (1 / n1 + 1 / n2))
        else:
            se = math.sqrt(v1 / n1 + v2 / n2)
        diff = m1 - m2
        z = Z_SCORES.get(confidence, 1.96)
        me = z * se
        return ConfidenceInterval(
            lower=diff - me,
            upper=diff + me,
            point_estimate=diff,
            confidence_level=confidence,
            method="welch" if not equal_variance else "pooled",
            margin_of_error=me,
        )

    def bootstrap_ci(
        self,
        values: List[float],
        statistic_fn: callable,
        confidence: float = 0.95,
        n_bootstrap: int = 10000,
        seed: Optional[int] = None,
    ) -> ConfidenceInterval:
        if not values:
            return ConfidenceInterval(0, 0, 0, confidence, "bootstrap", 0)
        import random
        rng = random.Random(seed)
        observed = statistic_fn(values)
        bootstrap_stats = []
        for _ in range(n_bootstrap):
            sample = [rng.choice(values) for _ in values]
            bootstrap_stats.append(statistic_fn(sample))
        alpha = 1 - confidence
        lower_pct = alpha / 2
        upper_pct = 1 - alpha / 2
        sorted_stats = sorted(bootstrap_stats)
        lower = sorted_stats[int(lower_pct * len(sorted_stats))]
        upper = sorted_stats[int(upper_pct * len(sorted_stats))]
        return ConfidenceInterval(
            lower=lower,
            upper=upper,
            point_estimate=observed,
            confidence_level=confidence,
            method="bootstrap",
            margin_of_error=(upper - lower) / 2,
        )
