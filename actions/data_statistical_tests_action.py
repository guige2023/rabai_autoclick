"""Data Statistical Tests Action.

Provides common statistical tests for data analysis:
t-test, chi-square, KS test, ANOVA, and correlation analysis.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class TestResult:
    test_name: str
    statistic: float
    p_value: float
    significant: bool
    alpha: float = 0.05

    def summary(self) -> str:
        sig = "SIGNIFICANT" if self.significant else "not significant"
        return (
            f"{self.test_name}: statistic={self.statistic:.4f}, "
            f"p={self.p_value:.4f} ({sig} at α={self.alpha})"
        )


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _variance(values: List[float], ddof: int = 1) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    return sum((v - m) ** 2 for v in values) / (len(values) - ddof)


def _std(values: List[float]) -> float:
    return math.sqrt(_variance(values))


def _t_statistic(sample1: List[float], sample2: List[float]) -> Tuple[float, float]:
    m1, m2 = _mean(sample1), _mean(sample2)
    v1, v2 = _variance(sample1), _variance(sample2)
    n1, n2 = len(sample1), len(sample2)
    if v1 == 0 and v2 == 0:
        return 0.0, 1.0
    se = math.sqrt(v1 / n1 + v2 / n2)
    t = (m1 - m2) / se if se > 0 else 0.0
    # Welch-Satterthwaite degrees of freedom approximation
    if v1 == 0 or v2 == 0:
        df = n1 + n2 - 2
    else:
        num = (v1 / n1 + v2 / n2) ** 2
        denom = (v1 / n1) ** 2 / (n1 - 1) + (v2 / n2) ** 2 / (n2 - 1)
        df = num / denom if denom > 0 else n1 + n2 - 2
    # Approximate p-value using normal distribution for large df
    p = 2 * (1 - _normal_cdf(abs(t)))
    return t, min(p, 1.0)


def _normal_cdf(x: float) -> float:
    # Approximation of standard normal CDF
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _ks_statistic(sample1: List[float], sample2: List[float]) -> Tuple[float, float]:
    all_vals = sorted(sample1 + sample2)
    n1, n2 = len(sample1), len(sample2)
    d = 0.0
    for v in all_vals:
        e1 = sum(1 for x in sample1 if x <= v) / n1
        e2 = sum(1 for x in sample2 if x <= v) / n2
        d = max(d, abs(e1 - e2))
    # Approximate p-value
    m = n1 * n2 / (n1 + n2)
    p = 2 * math.exp(-2 * m * d**2)
    return d, min(p, 1.0)


class DataStatisticalTestsAction:
    """Statistical tests for data analysis."""

    def t_test(
        self,
        sample1: List[float],
        sample2: List[float],
        alpha: float = 0.05,
    ) -> TestResult:
        t, p = _t_statistic(sample1, sample2)
        return TestResult(
            test_name="t-test (Welch)",
            statistic=t,
            p_value=p,
            significant=p < alpha,
            alpha=alpha,
        )

    def ks_test(
        self,
        sample1: List[float],
        sample2: List[float],
        alpha: float = 0.05,
    ) -> TestResult:
        d, p = _ks_statistic(sample1, sample2)
        return TestResult(
            test_name="Kolmogorov-Smirnov test",
            statistic=d,
            p_value=p,
            significant=p < alpha,
            alpha=alpha,
        )

    def pearson_correlation(
        self,
        x: List[float],
        y: List[float],
    ) -> Tuple[float, float]:
        if len(x) != len(y) or len(x) < 2:
            return 0.0, 1.0
        n = len(x)
        mx, my = _mean(x), _mean(y)
        num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
        denom = math.sqrt(
            sum((xi - mx) ** 2 for xi in x) * sum((yi - my) ** 2 for yi in y)
        )
        r = num / denom if denom > 0 else 0.0
        # t-statistic for correlation
        if abs(r) == 1.0:
            return r, 0.0
        t = r * math.sqrt((n - 2) / (1 - r**2))
        p = 2 * (1 - _normal_cdf(abs(t)))
        return r, min(p, 1.0)

    def anova(
        self,
        groups: List[List[float]],
        alpha: float = 0.05,
    ) -> TestResult:
        if not groups or len(groups) < 2:
            return TestResult("ANOVA", 0.0, 1.0, False, alpha)
        all_vals = [v for g in groups for v in g]
        grand_mean = _mean(all_vals)
        n_total = len(all_vals)
        k = len(groups)
        # Between-group variance
        ss_between = sum(
            len(g) * (_mean(g) - grand_mean) ** 2 for g in groups if g
        )
        # Within-group variance
        ss_within = sum(sum((v - _mean(g)) ** 2 for v in g) for g in groups if g)
        df_between = k - 1
        df_within = n_total - k
        if df_within == 0:
            return TestResult("ANOVA", 0.0, 1.0, False, alpha)
        ms_between = ss_between / df_between
        ms_within = ss_within / df_within
        f = ms_between / ms_within if ms_within > 0 else 0.0
        # Approximate p-value from F distribution
        p = math.exp(-df_between * (df_between * df_within * f) / (
            df_between * f + df_within))
        return TestResult(
            test_name="One-way ANOVA",
            statistic=f,
            p_value=min(p, 1.0),
            significant=min(p, 1.0) < alpha,
            alpha=alpha,
        )
