"""Decimal action v3 - advanced statistics and probability.

Extended decimal utilities for statistics, probability,
distributions, and hypothesis testing.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from typing import Callable, Sequence

__all__ = [
    "decimal_combinations",
    "decimal_permutations",
    "decimal_factorial",
    "decimal_binomial",
    "decimal_gamma",
    "decimal_beta",
    "decimal_erf",
    "decimal_erfc",
    "decimal_normal_pdf",
    "decimal_normal_cdf",
    "decimal_normal_quantile",
    "decimal_poisson_pmf",
    "decimal_poisson_cdf",
    "decimal_binomial_pmf",
    "decimal_binomial_cdf",
    "decimal_exponential_pdf",
    "decimal_exponential_cdf",
    "decimal_uniform_pdf",
    "decimal_uniform_cdf",
    "decimal_chi_square_quantile",
    "decimal_t_quantile",
    "decimal_f_quantile",
    "decimal_z_test",
    "decimal_t_test",
    "decimal_confidence_interval",
    "decimal_correlation_significance",
    "decimal_regression",
    "DecimalProbability",
    "DecimalDistribution",
]


def decimal_combinations(n: int, k: int) -> Decimal:
    """Compute n choose k (combinations).

    Args:
        n: Total items.
        k: Items to choose.

    Returns:
        Number of combinations.
    """
    if k < 0 or k > n:
        return Decimal(0)
    if k == 0 or k == n:
        return Decimal(1)
    k = min(k, n - k)
    result = Decimal(1)
    for i in range(k):
        result = result * Decimal(n - i) / Decimal(i + 1)
    return result


def decimal_permutations(n: int, k: int) -> Decimal:
    """Compute permutations P(n, k).

    Args:
        n: Total items.
        k: Items to arrange.

    Returns:
        Number of permutations.
    """
    if k < 0 or k > n:
        return Decimal(0)
    result = Decimal(1)
    for i in range(k):
        result *= Decimal(n - i)
    return result


def decimal_factorial(n: int) -> Decimal:
    """Compute n! using decimal.

    Args:
        n: Non-negative integer.

    Returns:
        n factorial.
    """
    if n < 0:
        raise ValueError("n must be >= 0")
    if n <= 1:
        return Decimal(1)
    result = Decimal(1)
    for i in range(2, n + 1):
        result *= Decimal(i)
    return result


def decimal_binomial(n: int, k: int, p: Decimal | float) -> Decimal:
    """Binomial probability mass function.

    Args:
        n: Number of trials.
        k: Number of successes.
        p: Success probability.

    Returns:
        P(X = k).
    """
    return decimal_combinations(n, k) * Decimal(str(p)) ** k * (Decimal(1) - Decimal(str(p))) ** (n - k)


def decimal_gamma(x: Decimal | float) -> Decimal:
    """Gamma function approximation.

    Args:
        x: Input value.

    Returns:
        Gamma(x) approximation.
    """
    import math
    if isinstance(x, Decimal):
        x = float(x)
    if x <= 0:
        raise ValueError("x must be positive for gamma")
    if x == int(x) and x <= 170:
        return Decimal(math.factorial(int(x) - 1))
    val = math.sqrt(2 * math.pi / x) * (x / math.e) ** x
    return Decimal(str(val))


def decimal_beta(a: Decimal | float, b: Decimal | float) -> Decimal:
    """Beta function B(a, b).

    Args:
        a: First parameter.
        b: Second parameter.

    Returns:
        Beta function value.
    """
    import math
    if isinstance(a, Decimal):
        a = float(a)
    if isinstance(b, Decimal):
        b = float(b)
    if a <= 0 or b <= 0:
        raise ValueError("a and b must be positive")
    ga = decimal_gamma(Decimal(a))
    gb = decimal_gamma(Decimal(b))
    gab = decimal_gamma(Decimal(a + b))
    return ga * gb / gab


def _approx_erf(x: float) -> float:
    """Approximate error function."""
    import math
    if abs(x) > 1:
        return 1 if x > 0 else -1
    t = 1 / (1 + 0.5 * abs(x))
    tau = t * math.exp(-x * x - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.170872)))))))))
    return 1 - tau


def decimal_erf(x: Decimal | float) -> Decimal:
    """Error function erf(x)."""
    if isinstance(x, Decimal):
        x = float(x)
    return Decimal(str(_approx_erf(x)))


def decimal_erfc(x: Decimal | float) -> Decimal:
    """Complementary error function erfc(x)."""
    return Decimal(1) - decimal_erf(x)


def decimal_normal_pdf(x: Decimal | float, mu: Decimal | float = 0, sigma: Decimal | float = 1) -> Decimal:
    """Normal probability density function.

    Args:
        x: Value.
        mu: Mean.
        sigma: Standard deviation.

    Returns:
        PDF value.
    """
    import math
    if isinstance(x, Decimal):
        x = float(x)
    if isinstance(mu, Decimal):
        mu = float(mu)
    if isinstance(sigma, Decimal):
        sigma = float(sigma)
    if sigma <= 0:
        raise ValueError("sigma must be > 0")
    coeff = Decimal(1) / Decimal(str(math.sqrt(2 * math.pi) * sigma))
    exponent = Decimal(str(-0.5 * ((x - mu) / sigma) ** 2))
    return coeff * (Decimal(str(math.e)) ** exponent)


def decimal_normal_cdf(x: Decimal | float, mu: Decimal | float = 0, sigma: Decimal | float = 1) -> Decimal:
    """Normal cumulative distribution function."""
    return (Decimal(1) + decimal_erf((Decimal(x) - Decimal(mu)) / (Decimal(sigma) * Decimal(1.41421356237)))) / Decimal(2)


def decimal_normal_quantile(p: Decimal | float, mu: Decimal | float = 0, sigma: Decimal | float = 1) -> Decimal:
    """Inverse normal CDF (quantile function)."""
    import math
    if isinstance(p, Decimal):
        p = float(p)
    if isinstance(mu, Decimal):
        mu = float(mu)
    if isinstance(sigma, Decimal):
        sigma = float(sigma)
    if p <= 0 or p >= 1:
        raise ValueError("p must be 0 < p < 1")
    q = p - 0.5
    r = q * q
    z = q * ((((-0.302649546 + (-0.14818467 + (-0.01181916 + (-0.0113486 + -0.0050212) * r) * r) * r) * r) * r) / (((0.198291 + (0.116850 + (0.044846 + (0.003912 + 0.000226) * r) * r) * r) * r) + Decimal(1)
    return Decimal(str(mu + sigma * z))


def decimal_poisson_pmf(k: int, lambda_: Decimal | float) -> Decimal:
    """Poisson probability mass function.

    Args:
        k: Number of events.
        lambda_: Expected value (lambda).

    Returns:
        P(X = k).
    """
    import math
    if k < 0:
        return Decimal(0)
    if isinstance(lambda_, Decimal):
        lambda_ = float(lambda_)
    log_p = -lambda_ + k * math.log(lambda_) - math.lgamma(k + 1)
    return Decimal(str(math.exp(log_p)))


def decimal_poisson_cdf(k: int, lambda_: Decimal | float) -> Decimal:
    """Poisson cumulative distribution."""
    if k < 0:
        return Decimal(0)
    total = Decimal(0)
    for i in range(k + 1):
        total += decimal_poisson_pmf(i, lambda_)
    return total


def decimal_binomial_pmf(k: int, n: int, p: Decimal | float) -> Decimal:
    """Binomial PMF (alias for decimal_binomial)."""
    return decimal_binomial(n, k, p)


def decimal_binomial_cdf(k: int, n: int, p: Decimal | float) -> Decimal:
    """Binomial CDF."""
    total = Decimal(0)
    for i in range(k + 1):
        total += decimal_binomial(n, i, p)
    return total


def decimal_exponential_pdf(x: Decimal | float, lambda_: Decimal | float) -> Decimal:
    """Exponential PDF."""
    if isinstance(x, Decimal):
        x = float(x)
    if isinstance(lambda_, Decimal):
        lambda_ = float(lambda_)
    if x < 0 or lambda_ <= 0:
        return Decimal(0)
    return Decimal(str(lambda_ * math.exp(-lambda_ * x)))


def decimal_exponential_cdf(x: Decimal | float, lambda_: Decimal | float) -> Decimal:
    """Exponential CDF."""
    if isinstance(x, Decimal):
        x = float(x)
    if isinstance(lambda_, Decimal):
        lambda_ = float(lambda_)
    if x < 0:
        return Decimal(0)
    return Decimal(str(1 - math.exp(-lambda_ * x)))


def decimal_uniform_pdf(x: Decimal | float, a: Decimal | float = 0, b: Decimal | float = 1) -> Decimal:
    """Uniform PDF."""
    if isinstance(x, Decimal):
        x = float(x)
    if isinstance(a, Decimal):
        a = float(a)
    if isinstance(b, Decimal):
        b = float(b)
    if a <= x <= b:
        return Decimal(1) / Decimal(str(b - a))
    return Decimal(0)


def decimal_uniform_cdf(x: Decimal | float, a: Decimal | float = 0, b: Decimal | float = 1) -> Decimal:
    """Uniform CDF."""
    if isinstance(x, Decimal):
        x = float(x)
    if isinstance(a, Decimal):
        a = float(a)
    if isinstance(b, Decimal):
        b = float(b)
    if x < a:
        return Decimal(0)
    if x >= b:
        return Decimal(1)
    return Decimal(str((x - a) / (b - a)))


def decimal_chi_square_quantile(p: Decimal | float, df: int) -> Decimal:
    """Chi-square quantile approximation."""
    import math
    if isinstance(p, Decimal):
        p = float(p)
    if df <= 0:
        raise ValueError("df must be positive")
    if p <= 0 or p >= 1:
        raise ValueError("p must be 0 < p < 1")
    x = df * (1 - 2 / (9 * df) + decimal_normal_quantile(p) * math.sqrt(2 / (9 * df))) ** 3
    return Decimal(str(max(0, x)))


def decimal_t_quantile(p: Decimal | float, df: int) -> Decimal:
    """Student's t quantile approximation."""
    import math
    if isinstance(p, Decimal):
        p = float(p)
    if df <= 0:
        raise ValueError("df must be positive")
    z = float(decimal_normal_quantile(p))
    cdf_t = z + (z ** 3 + z) / (4 * df) + (5 * z ** 5 + 16 * z ** 3 + 3 * z) / (96 * df ** 2)
    return Decimal(str(cdf_t))


def decimal_f_quantile(p: Decimal | float, df1: int, df2: int) -> Decimal:
    """F distribution quantile approximation."""
    import math
    if isinstance(p, Decimal):
        p = float(p)
    if df1 <= 0 or df2 <= 0:
        raise ValueError("df1 and df2 must be positive")
    if p <= 0 or p >= 1:
        raise ValueError("p must be 0 < p < 1")
    x = df1 * (1 - 2 / (9 * df1) + decimal_normal_quantile(p) * math.sqrt(2 / (9 * df1))) ** 3
    y = df2 * (1 - 2 / (9 * df2)) ** 3
    return Decimal(str(max(0, x / y)))


def decimal_z_test(x: Decimal | float, mu: Decimal | float, sigma: Decimal | float, alternative: str = "two-sided") -> Decimal:
    """Z-test statistic.

    Args:
        x: Sample mean.
        mu: Population mean.
        sigma: Population std dev.
        alternative: 'two-sided', 'less', 'greater'.

    Returns:
        Z-test statistic.
    """
    x_d = Decimal(str(x)) if not isinstance(x, Decimal) else x
    mu_d = Decimal(str(mu)) if not isinstance(mu, Decimal) else mu
    sigma_d = Decimal(str(sigma)) if not isinstance(sigma, Decimal) else sigma
    if sigma_d == 0:
        raise ValueError("sigma cannot be 0")
    z = (x_d - mu_d) / sigma_d
    return z


def decimal_t_test(sample: Sequence[Decimal | float], mu: Decimal | float = 0, alternative: str = "two-sided") -> Decimal:
    """One-sample t-test statistic.

    Args:
        sample: Sample values.
        mu: Population mean under null hypothesis.
        alternative: 'two-sided', 'less', 'greater'.

    Returns:
        t-statistic.
    """
    n = len(sample)
    if n < 2:
        raise ValueError("Need at least 2 samples")
    vals = [Decimal(str(v)) if not isinstance(v, Decimal) else v for v in sample]
    mu_d = Decimal(str(mu)) if not isinstance(mu, Decimal) else mu
    mean = sum(vals) / n
    variance = sum((v - mean) ** 2 for v in vals) / (n - 1)
    if variance == 0:
        raise ValueError("Variance is zero")
    se = (variance / Decimal(n)) ** Decimal(0.5)
    return (mean - mu_d) / se


def decimal_confidence_interval(data: Sequence[Decimal | float], confidence: float = 0.95) -> tuple[Decimal, Decimal]:
    """Confidence interval for mean.

    Args:
        data: Sample data.
        confidence: Confidence level (e.g., 0.95 for 95%).

    Returns:
        (lower, upper) bounds.
    """
    n = len(data)
    vals = [Decimal(str(v)) if not isinstance(v, Decimal) else v for v in data]
    mean = sum(vals) / n
    variance = sum((v - mean) ** 2 for v in vals) / (n - 1)
    se = (variance / Decimal(n)) ** Decimal(0.5)
    t = float(decimal_t_quantile(Decimal((1 + confidence) / 2), n - 1))
    margin = Decimal(str(t)) * se
    return (mean - margin, mean + margin)


def decimal_correlation_significance(r: Decimal | float, n: int) -> Decimal:
    """Calculate significance of correlation.

    Args:
        r: Correlation coefficient.
        n: Sample size.

    Returns:
        p-value approximation.
    """
    if isinstance(r, Decimal):
        r = float(r)
    if n <= 2:
        raise ValueError("Need n > 2")
    t = r * ((n - 2) / (1 - r * r)) ** 0.5
    return Decimal(str(abs(float(decimal_erfc(abs(t) / math.sqrt(2))))))


def decimal_regression(x_vals: Sequence[Decimal | float], y_vals: Sequence[Decimal | float]) -> dict:
    """Simple linear regression.

    Args:
        x_vals: X values.
        y_vals: Y values.

    Returns:
        Dict with slope, intercept, r_squared.
    """
    from decimal_action import decimal_linreg
    if len(x_vals) != len(y_vals):
        raise ValueError("Length mismatch")
    m, b = decimal_linreg(x_vals, y_vals)
    x = [Decimal(str(v)) if not isinstance(v, Decimal) else v for v in x_vals]
    y = [Decimal(str(v)) if not isinstance(v, Decimal) else v for v in y_vals]
    y_mean = sum(y) / len(y)
    ss_res = sum((yi - (m * xi + b)) ** 2 for xi, yi in zip(x, y))
    ss_tot = sum((yi - y_mean) ** 2 for yi in y)
    r_squared = Decimal(1) - ss_res / ss_tot if ss_tot != 0 else Decimal(0)
    return {"slope": m, "intercept": b, "r_squared": r_squared}


class DecimalProbability:
    """Probability calculations with decimal precision."""

    @staticmethod
    def combinations(n: int, k: int) -> Decimal:
        return decimal_combinations(n, k)

    @staticmethod
    def permutations(n: int, k: int) -> Decimal:
        return decimal_permutations(n, k)

    @staticmethod
    def factorial(n: int) -> Decimal:
        return decimal_factorial(n)

    @staticmethod
    def binomial(n: int, k: int, p: Decimal | float) -> Decimal:
        return decimal_binomial(n, k, p)


class DecimalDistribution:
    """Statistical distributions with decimal precision."""

    @staticmethod
    def normal_pdf(x: Decimal | float, mu: Decimal | float = 0, sigma: Decimal | float = 1) -> Decimal:
        return decimal_normal_pdf(x, mu, sigma)

    @staticmethod
    def normal_cdf(x: Decimal | float, mu: Decimal | float = 0, sigma: Decimal | float = 1) -> Decimal:
        return decimal_normal_cdf(x, mu, sigma)

    @staticmethod
    def poisson_pmf(k: int, lambda_: Decimal | float) -> Decimal:
        return decimal_poisson_pmf(k, lambda_)

    @staticmethod
    def exponential_pdf(x: Decimal | float, lambda_: Decimal | float) -> Decimal:
        return decimal_exponential_pdf(x, lambda_)
