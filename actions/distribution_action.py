"""
Distribution analysis module for statistical distributions.

Provides tools for analyzing, fitting, and sampling from probability distributions.
Used in data analysis, simulation, and statistical testing workflows.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Optional


class DistributionType(Enum):
    """Supported distribution types."""
    NORMAL = auto()
    UNIFORM = auto()
    EXPONENTIAL = auto()
    POISSON = auto()
    BINOMIAL = auto()
    BERNOULLI = auto()
    GAMMA = auto()
    BETA = auto()
    WEIBULL = auto()
    LOG_NORMAL = auto()
    CUSTOM = auto()


@dataclass
class DistributionStats:
    """Statistical properties of a distribution."""
    mean: float
    variance: float
    std_dev: float
    skewness: float
    kurtosis: float
    min_value: float
    max_value: float
    median: float
    mode: float


class DistributionAnalyzer:
    """
    Analyzes and fits probability distributions to data.
    
    Example:
        analyzer = DistributionAnalyzer()
        data = [random.gauss(0, 1) for _ in range(1000)]
        stats = analyzer.compute_stats(data)
        best_fit = analyzer.fit_distribution(data)
    """

    def __init__(self, precision: int = 6) -> None:
        """
        Initialize the distribution analyzer.
        
        Args:
            precision: Decimal precision for floating point results.
        """
        self.precision = precision
        self._cache: dict[str, Any] = {}

    def compute_stats(self, data: list[float]) -> DistributionStats:
        """
        Compute comprehensive statistics for a dataset.
        
        Args:
            data: List of numeric values.
            
        Returns:
            DistributionStats with all computed metrics.
            
        Raises:
            ValueError: If data is empty or has fewer than 2 elements.
        """
        if not data:
            raise ValueError("Data cannot be empty")
        if len(data) < 2:
            raise ValueError("Need at least 2 data points")

        n = len(data)
        sorted_data = sorted(data)
        mean = sum(data) / n
        variance = sum((x - mean) ** 2 for x in data) / n
        std_dev = math.sqrt(variance)

        # Skewness (Fisher-Pearson)
        if std_dev > 0:
            skewness = sum((x - mean) ** 3 for x in data) / (n * std_dev ** 3)
            kurtosis = sum((x - mean) ** 4 for x in data) / (n * std_dev ** 4) - 3
        else:
            skewness = 0.0
            kurtosis = 0.0

        # Median
        mid = n // 2
        median = sorted_data[mid] if n % 2 == 1 else (sorted_data[mid - 1] + sorted_data[mid]) / 2

        # Mode (most common value)
        freq: dict[float, int] = {}
        for x in data:
            rounded = round(x, self.precision)
            freq[rounded] = freq.get(rounded, 0) + 1
        mode = max(freq, key=freq.get) if freq else mean

        return DistributionStats(
            mean=round(mean, self.precision),
            variance=round(variance, self.precision),
            std_dev=round(std_dev, self.precision),
            skewness=round(skewness, self.precision),
            kurtosis=round(kurtosis, self.precision),
            min_value=min(data),
            max_value=max(data),
            median=round(median, self.precision),
            mode=mode
        )

    def fit_distribution(
        self,
        data: list[float],
        distribution_types: Optional[list[DistributionType]] = None
    ) -> tuple[DistributionType, float]:
        """
        Fit the best matching distribution to the data.
        
        Args:
            data: List of numeric values.
            distribution_types: List of distributions to try. Defaults to common types.
            
        Returns:
            Tuple of (best_distribution, goodness_of_fit_score).
        """
        if distribution_types is None:
            distribution_types = [
                DistributionType.NORMAL,
                DistributionType.UNIFORM,
                DistributionType.EXPONENTIAL,
                DistributionType.LOG_NORMAL,
            ]

        stats = self.compute_stats(data)
        best_type = DistributionType.NORMAL
        best_score = float('inf')

        for dtype in distribution_types:
            score = self._compute_fit_score(data, stats, dtype)
            if score < best_score:
                best_score = score
                best_type = dtype

        return best_type, round(best_score, self.precision)

    def _compute_fit_score(
        self,
        data: list[float],
        stats: DistributionStats,
        dtype: DistributionType
    ) -> float:
        """Compute goodness-of-fit score for a distribution type."""
        scores: dict[DistributionType, Callable[[list[float], DistributionStats], float]] = {
            DistributionType.NORMAL: self._normal_fit_score,
            DistributionType.UNIFORM: self._uniform_fit_score,
            DistributionType.EXPONENTIAL: self._exponential_fit_score,
            DistributionType.LOG_NORMAL: self._log_normal_fit_score,
        }
        scorer = scores.get(dtype, self._normal_fit_score)
        return scorer(data, stats)

    def _normal_fit_score(self, data: list[float], stats: DistributionStats) -> float:
        """Score how well data fits a normal distribution."""
        # Kolmogorov-Smirnov inspired score
        n = len(data)
        sorted_data = sorted(data)
        mean, std = stats.mean, stats.std_dev
        if std == 0:
            return float('inf')
        max_diff = 0.0
        for i, x in enumerate(sorted_data):
            expected_cdf = (i + 0.5) / n
            actual_cdf = 0.5 * (1 + math.erf((x - mean) / (std * math.sqrt(2))))
            diff = abs(expected_cdf - actual_cdf)
            if diff > max_diff:
                max_diff = diff
        return max_diff

    def _uniform_fit_score(self, data: list[float], stats: DistributionStats) -> float:
        """Score how well data fits a uniform distribution."""
        range_size = stats.max_value - stats.min_value
        expected_variance = range_size ** 2 / 12
        return abs(stats.variance - expected_variance) / expected_variance

    def _exponential_fit_score(self, data: list[float], stats: DistributionStats) -> float:
        """Score how well data fits an exponential distribution."""
        if stats.mean == 0:
            return float('inf')
        # Check coefficient of variation (should be 1 for exponential)
        cv = stats.std_dev / abs(stats.mean)
        return abs(cv - 1.0)

    def _log_normal_fit_score(self, data: list[float], stats: DistributionStats) -> float:
        """Score how well data fits a log-normal distribution."""
        # Check if log-transformed data is normal
        try:
            log_data = [math.log(max(x, 1e-10)) for x in data]
            log_mean = sum(log_data) / len(log_data)
            log_var = sum((x - log_mean) ** 2 for x in log_data) / len(log_data)
            # Should have low kurtosis in log space
            return abs(stats.skewness - 1.0) + abs(stats.kurtosis)
        except (ValueError, ZeroDivisionError):
            return float('inf')

    def sample(
        self,
        dtype: DistributionType,
        size: int,
        params: Optional[dict[str, float]] = None
    ) -> list[float]:
        """
        Generate samples from a specified distribution.
        
        Args:
            dtype: Type of distribution to sample from.
            size: Number of samples to generate.
            params: Distribution parameters (e.g., {'mean': 0, 'std': 1}).
            
        Returns:
            List of sampled values.
        """
        if size <= 0:
            raise ValueError("Size must be positive")

        params = params or {}

        if dtype == DistributionType.NORMAL:
            mean = params.get('mean', 0.0)
            std = params.get('std', 1.0)
            return [random.gauss(mean, std) for _ in range(size)]

        if dtype == DistributionType.UNIFORM:
            low = params.get('low', 0.0)
            high = params.get('high', 1.0)
            return [random.uniform(low, high) for _ in range(size)]

        if dtype == DistributionType.EXPONENTIAL:
            rate = params.get('rate', 1.0)
            if rate <= 0:
                raise ValueError("Rate must be positive")
            return [random.expovariate(rate) for _ in range(size)]

        if dtype == DistributionType.BERNOULLI:
            p = params.get('p', 0.5)
            return [1.0 if random.random() < p else 0.0 for _ in range(size)]

        if dtype == DistributionType.BINOMIAL:
            n = int(params.get('n', 10))
            p = params.get('p', 0.5)
            return [float(random.randint(0, n)) if random.random() < p else 0.0 for _ in range(size)]

        # Default to normal
        return [random.gauss(0, 1) for _ in range(size)]

    def pdf(self, dtype: DistributionType, x: float, params: Optional[dict[str, float]] = None) -> float:
        """
        Compute probability density function value at x.
        
        Args:
            dtype: Distribution type.
            x: Point at which to evaluate the PDF.
            params: Distribution parameters.
            
        Returns:
            PDF value at x.
        """
        params = params or {}

        if dtype == DistributionType.NORMAL:
            mean = params.get('mean', 0.0)
            std = params.get('std', 1.0)
            if std <= 0:
                raise ValueError("Standard deviation must be positive")
            z = (x - mean) / std
            return math.exp(-0.5 * z * z) / (std * math.sqrt(2 * math.pi))

        if dtype == DistributionType.UNIFORM:
            low = params.get('low', 0.0)
            high = params.get('high', 1.0)
            return 1.0 / (high - low) if low <= x <= high else 0.0

        if dtype == DistributionType.EXPONENTIAL:
            rate = params.get('rate', 1.0)
            if rate <= 0:
                raise ValueError("Rate must be positive")
            return rate * math.exp(-rate * x) if x >= 0 else 0.0

        return 0.0

    def cdf(self, dtype: DistributionType, x: float, params: Optional[dict[str, float]] = None) -> float:
        """
        Compute cumulative distribution function at x.
        
        Args:
            dtype: Distribution type.
            x: Point at which to evaluate the CDF.
            params: Distribution parameters.
            
        Returns:
            CDF value at x.
        """
        params = params or {}

        if dtype == DistributionType.NORMAL:
            mean = params.get('mean', 0.0)
            std = params.get('std', 1.0)
            z = (x - mean) / (std * math.sqrt(2)) if std > 0 else 0
            return 0.5 * (1 + math.erf(z))

        if dtype == DistributionType.UNIFORM:
            low = params.get('low', 0.0)
            high = params.get('high', 1.0)
            if x < low:
                return 0.0
            if x > high:
                return 1.0
            return (x - low) / (high - low)

        if dtype == DistributionType.EXPONENTIAL:
            rate = params.get('rate', 1.0)
            if rate <= 0:
                raise ValueError("Rate must be positive")
            return 1.0 - math.exp(-rate * x) if x >= 0 else 0.0

        return 0.0
