"""sstatistics action extensions for rabai_autoclick.

Provides statistics operations, distribution helpers,
statistical tests, and data analysis utilities.
"""

from __future__ import annotations

import math
import statistics
import random
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

__all__ = [
    "mean",
    "median",
    "mode",
    "stdev",
    "variance",
    "pstdev",
    "pvariance",
    "quantile",
    "percentile",
    "range_",
    "midrange",
    "sum_",
    "product",
    "harmonic_mean",
    "geometric_mean",
    "rms",
    "mean_abs_deviation",
    "median_abs_deviation",
    "coefficient_of_variation",
    "skewness",
    "kurtosis",
    "normal_cdf",
    "normal_pdf",
    "z_score",
    "standardize",
    "min_max_scale",
    "min_max_unscale",
    "z_score_normalize",
    "log_normal",
    "exponential",
    "uniform_distribution",
    "binomial",
    "poisson",
    "chi_squared",
    "correlation",
    "covariance",
    "spearman_correlation",
    "pearson_correlation",
    "linear_regression",
    "moving_average",
    "exponential_moving_average",
    "weighted_mean",
    "hypothesize",
    "t_test",
    "confidence_interval",
    "sample_size",
    "bootstrap",
    "resample",
    "histogram",
    "frequency",
    "count_if",
    "cumsum",
    "cumprod",
    "diff",
    "pct_change",
    "rolling",
    "describe",
    "summary",
    "StatsResult",
    "Distribution",
    "Bernoulli",
    "NormalDistribution",
    "UniformDistribution",
    "ExponentialDistribution",
]


T = TypeVar("T")


def mean(data: List[float]) -> float:
    """Calculate arithmetic mean.

    Args:
        data: List of numbers.

    Returns:
        Arithmetic mean.
    """
    if not data:
        raise ValueError("Cannot compute mean of empty data")
    return statistics.mean(data)


def median(data: List[float]) -> float:
    """Calculate median.

    Args:
        data: List of numbers.

    Returns:
        Median value.
    """
    if not data:
        raise ValueError("Cannot compute median of empty data")
    return statistics.median(data)


def mode(data: List[float]) -> float:
    """Calculate mode (most common value).

    Args:
        data: List of numbers.

    Returns:
        Mode value.
    """
    if not data:
        raise ValueError("Cannot compute mode of empty data")
    return statistics.mode(data)


def stdev(data: List[float]) -> float:
    """Calculate sample standard deviation.

    Args:
        data: List of numbers.

    Returns:
        Sample standard deviation.
    """
    if len(data) < 2:
        raise ValueError("Need at least 2 data points for stdev")
    return statistics.stdev(data)


def variance(data: List[float]) -> float:
    """Calculate sample variance.

    Args:
        data: List of numbers.

    Returns:
        Sample variance.
    """
    if len(data) < 2:
        raise ValueError("Need at least 2 data points for variance")
    return statistics.variance(data)


def pstdev(data: List[float]) -> float:
    """Calculate population standard deviation.

    Args:
        data: List of numbers.

    Returns:
        Population standard deviation.
    """
    if not data:
        raise ValueError("Cannot compute pstdev of empty data")
    return statistics.pstdev(data)


def pvariance(data: List[float]) -> float:
    """Calculate population variance.

    Args:
        data: List of numbers.

    Returns:
        Population variance.
    """
    if not data:
        raise ValueError("Cannot compute pvariance of empty data")
    return statistics.pvariance(data)


def quantile(data: List[float], n: int = 4) -> List[float]:
    """Calculate quantiles of data.

    Args:
        data: List of numbers.
        n: Number of quantiles (4 for quartiles).

    Returns:
        List of quantile values.
    """
    if not data:
        raise ValueError("Cannot compute quantile of empty data")
    return statistics.quantiles(data, n=n)


def percentile(data: List[float], p: float) -> float:
    """Calculate percentile value.

    Args:
        data: List of numbers.
        p: Percentile (0-100).

    Returns:
        Value at the p-th percentile.
    """
    if not data:
        raise ValueError("Cannot compute percentile of empty data")
    if p < 0 or p > 100:
        raise ValueError("Percentile must be between 0 and 100")
    return statistics.quantile(data, p / 100.0)


def range_(data: List[float]) -> float:
    """Calculate range (max - min).

    Args:
        data: List of numbers.

    Returns:
        Range of data.
    """
    if not data:
        raise ValueError("Cannot compute range of empty data")
    return max(data) - min(data)


def midrange(data: List[float]) -> float:
    """Calculate midrange (mean of max and min).

    Args:
        data: List of numbers.

    Returns:
        Midrange value.
    """
    if not data:
        raise ValueError("Cannot compute midrange of empty data")
    return (max(data) + min(data)) / 2.0


def sum_(data: List[float]) -> float:
    """Calculate sum of data.

    Args:
        data: List of numbers.

    Returns:
        Sum of all values.
    """
    return sum(data)


def product(data: List[float]) -> float:
    """Calculate product of all values.

    Args:
        data: List of numbers.

    Returns:
        Product of all values.
    """
    result = 1.0
    for x in data:
        result *= x
    return result


def harmonic_mean(data: List[float]) -> float:
    """Calculate harmonic mean.

    Args:
        data: List of numbers.

    Returns:
        Harmonic mean.
    """
    if not data:
        raise ValueError("Cannot compute harmonic_mean of empty data")
    return statistics.harmonic_mean(data)


def geometric_mean(data: List[float]) -> float:
    """Calculate geometric mean.

    Args:
        data: List of numbers.

    Returns:
        Geometric mean.
    """
    if not data:
        raise ValueError("Cannot compute geometric_mean of empty data")
    return statistics.geometric_mean(data)


def rms(data: List[float]) -> float:
    """Calculate root mean square.

    Args:
        data: List of numbers.

    Returns:
        RMS value.
    """
    if not data:
        raise ValueError("Cannot compute rms of empty data")
    return math.sqrt(mean([x * x for x in data]))


def mean_abs_deviation(data: List[float]) -> float:
    """Calculate mean absolute deviation.

    Args:
        data: List of numbers.

    Returns:
        MAD from mean.
    """
    if not data:
        raise ValueError("Cannot compute MAD of empty data")
    m = mean(data)
    return mean([abs(x - m) for x in data])


def median_abs_deviation(data: List[float]) -> float:
    """Calculate median absolute deviation.

    Args:
        data: List of numbers.

    Returns:
        MAD from median.
    """
    if not data:
        raise ValueError("Cannot compute median_abs_deviation of empty data")
    m = median(data)
    return median([abs(x - m) for x in data])


def coefficient_of_variation(data: List[float]) -> float:
    """Calculate coefficient of variation.

    Args:
        data: List of numbers.

    Returns:
        CV as percentage.
    """
    if not data:
        raise ValueError("Cannot compute CV of empty data")
    m = mean(data)
    if m == 0:
        raise ValueError("Mean is zero, cannot compute CV")
    return (stdev(data) / abs(m)) * 100.0


def skewness(data: List[float]) -> float:
    """Calculate skewness (3rd moment).

    Args:
        data: List of numbers.

    Returns:
        Skewness coefficient.
    """
    if len(data) < 3:
        raise ValueError("Need at least 3 data points for skewness")
    m = mean(data)
    s = stdev(data)
    if s == 0:
        raise ValueError("Standard deviation is zero, cannot compute skewness")
    n = len(data)
    return (sum([((x - m) / s) ** 3 for x in data]) * n) / ((n - 1) * (n - 2))


def kurtosis(data: List[float]) -> float:
    """Calculate excess kurtosis (4th moment).

    Args:
        data: List of numbers.

    Returns:
        Excess kurtosis coefficient.
    """
    if len(data) < 4:
        raise ValueError("Need at least 4 data points for kurtosis")
    m = mean(data)
    s = stdev(data)
    if s == 0:
        raise ValueError("Standard deviation is zero, cannot compute kurtosis")
    n = len(data)
    return (sum([((x - m) / s) ** 4 for x in data]) * n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3)) - (3 * (n - 1) ** 2) / ((n - 2) * (n - 3))


def normal_pdf(x: float, mu: float = 0.0, sigma: float = 1.0) -> float:
    """Normal probability density function.

    Args:
        x: Value to evaluate.
        mu: Mean.
        sigma: Standard deviation.

    Returns:
        PDF value at x.
    """
    if sigma <= 0:
        raise ValueError("sigma must be positive")
    coef = 1.0 / (sigma * math.sqrt(2 * math.pi))
    exp = -0.5 * ((x - mu) / sigma) ** 2
    return coef * math.exp(exp)


def normal_cdf(x: float, mu: float = 0.0, sigma: float = 1.0) -> float:
    """Normal cumulative distribution function.

    Args:
        x: Value to evaluate.
        mu: Mean.
        sigma: Standard deviation.

    Returns:
        CDF value at x.
    """
    return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))


def z_score(x: float, mu: float, sigma: float) -> float:
    """Calculate z-score.

    Args:
        x: Raw value.
        mu: Mean.
        sigma: Standard deviation.

    Returns:
        Z-score.
    """
    if sigma == 0:
        raise ValueError("sigma cannot be zero")
    return (x - mu) / sigma


def standardize(data: List[float]) -> List[float]:
    """Standardize data (z-score normalization).

    Args:
        data: List of numbers.

    Returns:
        List of z-scores.
    """
    if not data:
        raise ValueError("Cannot standardize empty data")
    m = mean(data)
    s = stdev(data)
    if s == 0:
        raise ValueError("Cannot standardize: all values are identical")
    return [(x - m) / s for x in data]


def min_max_scale(data: List[float], min_val: float = 0.0, max_val: float = 1.0) -> List[float]:
    """Scale data to range [min_val, max_val].

    Args:
        data: List of numbers.
        min_val: Target minimum.
        max_val: Target maximum.

    Returns:
        Scaled data.
    """
    if not data:
        raise ValueError("Cannot scale empty data")
    d_min = min(data)
    d_max = max(data)
    if d_min == d_max:
        raise ValueError("Cannot scale: all values are identical")
    return [min_val + (x - d_min) / (d_max - d_min) * (max_val - min_val) for x in data]


def min_max_unscale(data: List[float], orig_min: float, orig_max: float) -> List[float]:
    """Reverse min-max scaling.

    Args:
        data: Scaled data.
        orig_min: Original minimum.
        orig_max: Original maximum.

    Returns:
        Unscaled data.
    """
    return [orig_min + x * (orig_max - orig_min) for x in data]


def z_score_normalize(data: List[float]) -> List[float]:
    """Alias for standardize."""
    return standardize(data)


def log_normal(mean: float, sigma: float) -> List[float]:
    """Generate log-normal distributed values.

    Args:
        mean: Target mean.
        sigma: Target standard deviation.

    Returns:
        List of values.
    """
    # Convert to normal parameters
    mu = math.log(mean ** 2 / math.sqrt(mean ** 2 + sigma ** 2))
    sigma_normal = math.sqrt(math.log(1 + sigma ** 2 / mean ** 2))
    return [random.lognormvariate(mu, sigma_normal)]


def exponential(lambda_: float) -> float:
    """Generate exponentially distributed value.

    Args:
        lambda_: Rate parameter (1/mean).

    Returns:
        Random exponentially distributed value.
    """
    if lambda_ <= 0:
        raise ValueError("lambda_ must be positive")
    return random.expovariate(lambda_)


def uniform_distribution(a: float, b: float) -> float:
    """Generate uniform distributed value.

    Args:
        a: Lower bound.
        b: Upper bound.

    Returns:
        Random value in [a, b].
    """
    return random.uniform(a, b)


def binomial(n: int, p: float) -> int:
    """Generate binomial distributed value.

    Args:
        n: Number of trials.
        p: Probability of success.

    Returns:
        Number of successes.
    """
    if n < 0:
        raise ValueError("n must be non-negative")
    if not 0 <= p <= 1:
        raise ValueError("p must be between 0 and 1")
    return random.binomialvariate(n, p)


def poisson(lambda_: float) -> int:
    """Generate Poisson distributed value.

    Args:
        lambda_: Expected number of events.

    Returns:
        Number of events.
    """
    if lambda_ <= 0:
        raise ValueError("lambda_ must be positive")
    return random.poissonvariate(lambda_)


def chi_squared(df: int) -> float:
    """Generate chi-squared distributed value.

    Args:
        df: Degrees of freedom.

    Returns:
        Random chi-squared value.
    """
    if df <= 0:
        raise ValueError("Degrees of freedom must be positive")
    return random.gammavariate(df / 2.0, 2.0)


def correlation(x: List[float], y: List[float]) -> float:
    """Calculate Pearson correlation coefficient.

    Args:
        x: First data series.
        y: Second data series.

    Returns:
        Correlation coefficient [-1, 1].
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")
    if len(x) < 2:
        raise ValueError("Need at least 2 data points")
    return pearson_correlation(x, y)


def pearson_correlation(x: List[float], y: List[float]) -> float:
    """Calculate Pearson correlation.

    Args:
        x: First data series.
        y: Second data series.

    Returns:
        Pearson r.
    """
    n = len(x)
    mx = mean(x)
    my = mean(y)
    sx = stdev(x)
    sy = stdev(y)
    if sx == 0 or sy == 0:
        raise ValueError("Standard deviation is zero for one series")
    return sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / ((n - 1) * sx * sy)


def spearman_correlation(x: List[float], y: List[float]) -> float:
    """Calculate Spearman rank correlation.

    Args:
        x: First data series.
        y: Second data series.

    Returns:
        Spearman rho.
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")
    # Rank the data
    def rank(data: List[float]) -> List[float]:
        sorted_data = sorted(enumerate(data), key=lambda p: p[1])
        ranks = [0] * len(data)
        for i, (idx, _) in enumerate(sorted_data):
            ranks[idx] = i + 1
        return ranks
    rx = rank(x)
    ry = rank(y)
    return pearson_correlation(rx, ry)


def covariance(x: List[float], y: List[float]) -> float:
    """Calculate covariance.

    Args:
        x: First data series.
        y: Second data series.

    Returns:
        Sample covariance.
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")
    if len(x) < 2:
        raise ValueError("Need at least 2 data points")
    mx = mean(x)
    my = mean(y)
    return sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / (len(x) - 1)


def linear_regression(x: List[float], y: List[float]) -> Tuple[float, float]:
    """Simple linear regression.

    Args:
        x: Independent variable.
        y: Dependent variable.

    Returns:
        Tuple of (slope, intercept).
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")
    if len(x) < 2:
        raise ValueError("Need at least 2 data points")
    mx = mean(x)
    my = mean(y)
    ssxx = sum((xi - mx) ** 2 for xi in x)
    ssxy = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    if ssxx == 0:
        raise ValueError("All x values are identical")
    slope = ssxy / ssxx
    intercept = my - slope * mx
    return slope, intercept


def moving_average(data: List[float], window: int) -> List[float]:
    """Calculate simple moving average.

    Args:
        data: List of values.
        window: Window size.

    Returns:
        List of moving averages.
    """
    if window < 1:
        raise ValueError("window must be at least 1")
    if not data:
        return []
    result = []
    for i in range(len(data)):
        start = max(0, i - window + 1)
        result.append(mean(data[start:i + 1]))
    return result


def exponential_moving_average(data: List[float], alpha: float = 0.3) -> List[float]:
    """Calculate exponential moving average.

    Args:
        data: List of values.
        alpha: Smoothing factor (0-1).

    Returns:
        List of EMAs.
    """
    if not 0 < alpha <= 1:
        raise ValueError("alpha must be between 0 and 1")
    if not data:
        return []
    result = [data[0]]
    for x in data[1:]:
        result.append(alpha * x + (1 - alpha) * result[-1])
    return result


def weighted_mean(values: List[float], weights: List[float]) -> float:
    """Calculate weighted mean.

    Args:
        values: List of values.
        weights: List of weights.

    Returns:
        Weighted mean.
    """
    if len(values) != len(weights):
        raise ValueError("values and weights must have same length")
    if not values:
        raise ValueError("Cannot compute weighted mean of empty data")
    total_weight = sum(weights)
    if total_weight == 0:
        raise ValueError("Total weight is zero")
    return sum(v * w for v, w in zip(values, weights)) / total_weight


def t_test(sample1: List[float], sample2: List[float]) -> float:
    """Two-sample t-test.

    Args:
        sample1: First sample.
        sample2: Second sample.

    Returns:
        T-statistic.
    """
    if len(sample1) < 2 or len(sample2) < 2:
        raise ValueError("Each sample needs at least 2 values")
    n1 = len(sample1)
    n2 = len(sample2)
    m1 = mean(sample1)
    m2 = mean(sample2)
    v1 = variance(sample1)
    v2 = variance(sample2)
    pooled_se = math.sqrt(v1 / n1 + v2 / n2)
    if pooled_se == 0:
        raise ValueError("Standard error is zero")
    return (m1 - m2) / pooled_se


def confidence_interval(data: List[float], confidence: float = 0.95) -> Tuple[float, float]:
    """Calculate confidence interval.

    Args:
        data: Sample data.
        confidence: Confidence level (default 95%).

    Returns:
        Tuple of (lower, upper) bounds.
    """
    if len(data) < 2:
        raise ValueError("Need at least 2 data points")
    m = mean(data)
    s = stdev(data)
    n = len(data)
    # Use normal approximation for large samples
    from statistics import NormalEnd
    z = 1.96 if confidence == 0.95 else 1.645  # Simplified
    margin = z * s / math.sqrt(n)
    return m - margin, m + margin


def sample_size(population_std: float, margin_of_error: float, confidence: float = 0.95) -> int:
    """Calculate required sample size.

    Args:
        population_std: Population standard deviation.
        margin_of_error: Desired margin of error.
        confidence: Confidence level.

    Returns:
        Required sample size.
    """
    if margin_of_error <= 0:
        raise ValueError("margin_of_error must be positive")
    z = 1.96 if confidence == 0.95 else 1.645  # Simplified
    return int(math.ceil((z * population_std / margin_of_error) ** 2))


def bootstrap(data: List[float], n_iterations: int = 1000) -> List[float]:
    """Bootstrap resampling.

    Args:
        data: Original sample.
        n_iterations: Number of resamples.

    Returns:
        List of bootstrap means.
    """
    if not data:
        raise ValueError("Cannot bootstrap empty data")
    results = []
    for _ in range(n_iterations):
        resample_data = random.choices(data, k=len(data))
        results.append(mean(resample_data))
    return results


def resample(data: List[float], size: Optional[int] = None) -> List[float]:
    """Resample with replacement.

    Args:
        data: Original data.
        size: Resample size (default same as original).

    Returns:
        Resampled data.
    """
    if not data:
        raise ValueError("Cannot resample empty data")
    n = size if size is not None else len(data)
    return random.choices(data, k=n)


def histogram(data: List[float], bins: int = 10) -> Tuple[List[float], List[int]]:
    """Create histogram.

    Args:
        data: Data to histogram.
        bins: Number of bins.

    Returns:
        Tuple of (bin_edges, frequencies).
    """
    if not data:
        raise ValueError("Cannot histogram empty data")
    if bins < 1:
        raise ValueError("bins must be at least 1")
    d_min = min(data)
    d_max = max(data)
    if d_min == d_max:
        return [d_min, d_max], [len(data)]
    bin_width = (d_max - d_min) / bins
    edges = [d_min + i * bin_width for i in range(bins + 1)]
    freqs = [0] * bins
    for x in data:
        idx = min(int((x - d_min) / bin_width), bins - 1)
        freqs[idx] += 1
    return edges, freqs


def frequency(data: List[float]) -> Dict[float, int]:
    """Count frequency of each value.

    Args:
        data: Data values.

    Returns:
        Dict mapping value to count.
    """
    freq: Dict[float, int] = {}
    for x in data:
        freq[x] = freq.get(x, 0) + 1
    return freq


def count_if(data: List[float], predicate: Callable[[float], bool]) -> int:
    """Count values matching predicate.

    Args:
        data: Data values.
        predicate: Condition function.

    Returns:
        Count of matching values.
    """
    return sum(1 for x in data if predicate(x))


def cumsum(data: List[float]) -> List[float]:
    """Cumulative sum.

    Args:
        data: Data values.

    Returns:
        List of cumulative sums.
    """
    result = []
    total = 0.0
    for x in data:
        total += x
        result.append(total)
    return result


def cumprod(data: List[float]) -> List[float]:
    """Cumulative product.

    Args:
        data: Data values.

    Returns:
        List of cumulative products.
    """
    result = []
    total = 1.0
    for x in data:
        total *= x
        result.append(total)
    return result


def diff(data: List[float]) -> List[float]:
    """Calculate differences between consecutive elements.

    Args:
        data: Data values.

    Returns:
        List of differences.
    """
    return [data[i] - data[i - 1] for i in range(1, len(data))]


def pct_change(data: List[float]) -> List[float]:
    """Calculate percentage change.

    Args:
        data: Data values.

    Returns:
        List of percentage changes.
    """
    return [0.0] + [(data[i] - data[i - 1]) / data[i - 1] * 100 for i in range(1, len(data))]


def rolling(data: List[float], window: int, func: Callable[[List[float]], float] = mean) -> List[float]:
    """Rolling window aggregation.

    Args:
        data: Data values.
        window: Window size.
        func: Aggregation function.

    Returns:
        List of rolling aggregations.
    """
    if window < 1:
        raise ValueError("window must be at least 1")
    if not data:
        return []
    return [func(data[max(0, i - window + 1):i + 1]) for i in range(len(data))]


def describe(data: List[float]) -> Dict[str, float]:
    """Get descriptive statistics.

    Args:
        data: Data values.

    Returns:
        Dict of statistics.
    """
    if not data:
        raise ValueError("Cannot describe empty data")
    return {
        "count": len(data),
        "mean": mean(data),
        "median": median(data),
        "stdev": stdev(data),
        "min": min(data),
        "max": max(data),
        "range": range_(data),
        "q25": quantile(data, 4)[0],
        "q75": quantile(data, 4)[-1],
    }


def summary(data: List[float]) -> str:
    """Get formatted summary string.

    Args:
        data: Data values.

    Returns:
        Formatted summary.
    """
    d = describe(data)
    return (
        f"n={d['count']}, "
        f"mean={d['mean']:.2f}, "
        f"std={d['stdev']:.2f}, "
        f"min={d['min']:.2f}, "
        f"median={d['median']:.2f}, "
        f"max={d['max']:.2f}"
    )


class StatsResult:
    """Container for statistical results."""

    def __init__(self, **kwargs: float) -> None:
        """Initialize with named statistics.

        Args:
            **kwargs: Statistics as keyword arguments.
        """
        self._data = kwargs

    def __getattr__(self, name: str) -> float:
        return self._data[name]

    def __repr__(self) -> str:
        return f"StatsResult({self._data})"

    def as_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return self._data.copy()


class Distribution:
    """Base class for probability distributions."""

    def pdf(self, x: float) -> float:
        """Probability density function."""
        raise NotImplementedError

    def cdf(self, x: float) -> float:
        """Cumulative distribution function."""
        raise NotImplementedError

    def sample(self) -> float:
        """Generate random sample."""
        raise NotImplementedError


class NormalDistribution(Distribution):
    """Normal/Gaussian distribution."""

    def __init__(self, mu: float = 0.0, sigma: float = 1.0) -> None:
        """Initialize normal distribution.

        Args:
            mu: Mean.
            sigma: Standard deviation.
        """
        self.mu = mu
        self.sigma = sigma

    def pdf(self, x: float) -> float:
        return normal_pdf(x, self.mu, self.sigma)

    def cdf(self, x: float) -> float:
        return normal_cdf(x, self.mu, self.sigma)

    def sample(self) -> float:
        return random.gauss(self.mu, self.sigma)


class UniformDistribution(Distribution):
    """Uniform distribution."""

    def __init__(self, a: float = 0.0, b: float = 1.0) -> None:
        """Initialize uniform distribution.

        Args:
            a: Lower bound.
            b: Upper bound.
        """
        self.a = a
        self.b = b

    def pdf(self, x: float) -> float:
        if self.a <= x <= self.b:
            return 1.0 / (self.b - self.a)
        return 0.0

    def cdf(self, x: float) -> float:
        if x < self.a:
            return 0.0
        if x > self.b:
            return 1.0
        return (x - self.a) / (self.b - self.a)

    def sample(self) -> float:
        return uniform_distribution(self.a, self.b)


class ExponentialDistribution(Distribution):
    """Exponential distribution."""

    def __init__(self, lambda_: float = 1.0) -> None:
        """Initialize exponential distribution.

        Args:
            lambda_: Rate parameter.
        """
        self.lambda_ = lambda_

    def pdf(self, x: float) -> float:
        if x < 0:
            return 0.0
        return self.lambda_ * math.exp(-self.lambda_ * x)

    def cdf(self, x: float) -> float:
        if x < 0:
            return 0.0
        return 1.0 - math.exp(-self.lambda_ * x)

    def sample(self) -> float:
        return exponential(self.lambda_)


class Bernoulli:
    """Bernoulli distribution helper."""

    def __init__(self, p: float) -> None:
        """Initialize Bernoulli distribution.

        Args:
            p: Probability of success.
        """
        if not 0 <= p <= 1:
            raise ValueError("p must be between 0 and 1")
        self.p = p

    def sample(self) -> int:
        """Generate Bernoulli trial.

        Returns:
            1 with probability p, 0 otherwise.
        """
        return 1 if random.random() < self.p else 0
