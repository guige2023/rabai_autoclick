"""
Statistics Action Module

Provides statistical operations including measures of central tendency,
dispersion, correlation, and statistical distributions for data analysis.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import math
import random
import statistics
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

# Type variables
N = TypeVar("N", int, float)


class StatisticsAction:
    """
    Main statistics action handler providing statistical operations.
    
    This class wraps and extends Python's statistics module with
    additional utilities for data analysis and automation tasks.
    
    Attributes:
        None (all methods are static or class methods)
    """
    
    @staticmethod
    def mean(data: List[N]) -> float:
        """
        Calculate the arithmetic mean (average) of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Arithmetic mean
        
        Raises:
            StatisticsError: If data is empty
        
        Example:
            >>> StatisticsAction.mean([1, 2, 3, 4, 5])
            3.0
        """
        if not data:
            raise statistics.StatisticsError("mean requires at least one data point")
        return statistics.mean(data)
    
    @staticmethod
    def median(data: List[N]) -> float:
        """
        Calculate the median (middle value) of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Median value
        
        Raises:
            StatisticsError: If data is empty
        
        Example:
            >>> StatisticsAction.median([1, 2, 3, 4, 5])
            3
        """
        if not data:
            raise statistics.StatisticsError("median requires at least one data point")
        return statistics.median(data)
    
    @staticmethod
    def mode(data: List[Any]) -> Any:
        """
        Calculate the mode (most common value) of data.
        
        Args:
            data: List of values
        
        Returns:
            Most common value
        
        Raises:
            StatisticsError: If data is empty or has no mode
        
        Example:
            >>> StatisticsAction.mode([1, 2, 2, 3, 3, 3])
            3
        """
        if not data:
            raise statistics.StatisticsError("mode requires at least one data point")
        return statistics.mode(data)
    
    @staticmethod
    def multimode(data: List[Any]) -> List[Any]:
        """
        Calculate all modes (most common values) of data.
        
        Args:
            data: List of values
        
        Returns:
            List of most common values
        
        Example:
            >>> StatisticsAction.multimode([1, 1, 2, 2, 3])
            [1, 2]
        """
        if not data:
            raise statistics.StatisticsError("multimode requires at least one data point")
        return statistics.multimode(data)
    
    @staticmethod
    def stdev(data: List[N]) -> float:
        """
        Calculate the standard deviation of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Standard deviation
        
        Raises:
            StatisticsError: If data has fewer than 2 values
        
        Example:
            >>> StatisticsAction.stdev([2, 4, 4, 4, 5, 5, 7, 9])
            2.0
        """
        if len(data) < 2:
            raise statistics.StatisticsError("stdev requires at least 2 data points")
        return statistics.stdev(data)
    
    @staticmethod
    def pstdev(data: List[N]) -> float:
        """
        Calculate the population standard deviation of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Population standard deviation
        
        Example:
            >>> StatisticsAction.pstdev([2, 4, 4, 4, 5, 5, 7, 9])
            1.8708286933869707
        """
        if not data:
            raise statistics.StatisticsError("pstdev requires at least 1 data point")
        return statistics.pstdev(data)
    
    @staticmethod
    def variance(data: List[N]) -> float:
        """
        Calculate the variance of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Variance
        
        Raises:
            StatisticsError: If data has fewer than 2 values
        
        Example:
            >>> StatisticsAction.variance([2, 4, 4, 4, 5, 5, 7, 9])
            4.0
        """
        if len(data) < 2:
            raise statistics.StatisticsError("variance requires at least 2 data points")
        return statistics.variance(data)
    
    @staticmethod
    def pvariance(data: List[N]) -> float:
        """
        Calculate the population variance of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Population variance
        """
        if not data:
            raise statistics.StatisticsError("pvariance requires at least 1 data point")
        return statistics.pvariance(data)
    
    @staticmethod
    def quantiles(
        data: List[N],
        n: int = 4,
    ) -> List[float]:
        """
        Divide data into n continuous intervals with equal probability.
        
        Args:
            data: List of numeric values
            n: Number of quantiles (4 for quartiles, 100 for percentiles)
        
        Returns:
            List of quantile boundaries
        
        Example:
            >>> StatisticsAction.quantiles([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], n=4)
            [2.25, 5.5, 8.25]
        """
        if not data:
            raise statistics.StatisticsError("quantiles requires at least one data point")
        return statistics.quantiles(data, n=n)
    
    @staticmethod
    def percentile(
        data: List[N],
        p: float,
    ) -> float:
        """
        Calculate the p-th percentile of data.
        
        Args:
            data: List of numeric values
            p: Percentile to calculate (0-100)
        
        Returns:
            Value at the p-th percentile
        
        Example:
            >>> StatisticsAction.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50)
            5.5
        """
        if p < 0 or p > 100:
            raise ValueError("Percentile must be between 0 and 100")
        if not data:
            raise statistics.StatisticsError("percentile requires at least one data point")
        
        sorted_data = sorted(data)
        index = (p / 100) * (len(sorted_data) - 1)
        lower = int(math.floor(index))
        upper = int(math.ceil(index))
        
        if lower == upper:
            return sorted_data[lower]
        
        weight = index - lower
        return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight
    
    @staticmethod
    def quartile(data: List[N]) -> Tuple[float, float, float]:
        """
        Calculate the three quartiles of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Tuple of (Q1, Q2, Q3)
        
        Example:
            >>> StatisticsAction.quartile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            (3.25, 5.5, 7.75)
        """
        if not data:
            raise statistics.StatisticsError("quartile requires at least one data point")
        return statistics.quantiles(data, n=4)
    
    @staticmethod
    def interquartile_range(data: List[N]) -> float:
        """
        Calculate the interquartile range (IQR) of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            IQR (Q3 - Q1)
        
        Example:
            >>> StatisticsAction.interquartile_range([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            4.5
        """
        q1, q2, q3 = StatisticsAction.quartile(data)
        return q3 - q1
    
    @staticmethod
    def covariance(data1: List[N], data2: List[N]) -> float:
        """
        Calculate the covariance between two datasets.
        
        Args:
            data1: First list of numeric values
            data2: Second list of numeric values
        
        Returns:
            Covariance
        
        Raises:
            StatisticsError: If datasets have different lengths or < 2 values
        """
        if len(data1) != len(data2):
            raise statistics.StatisticsError("covariance requires datasets of equal length")
        if len(data1) < 2:
            raise statistics.StatisticsError("covariance requires at least 2 data points")
        return statistics.covariance(data1, data2)
    
    @staticmethod
    def correlation(data1: List[N], data2: List[N]) -> float:
        """
        Calculate the Pearson correlation coefficient between two datasets.
        
        Args:
            data1: First list of numeric values
            data2: Second list of numeric values
        
        Returns:
            Correlation coefficient (-1 to 1)
        
        Example:
            >>> StatisticsAction.correlation([1, 2, 3, 4, 5], [2, 4, 6, 8, 10])
            1.0
        """
        if len(data1) != len(data2):
            raise statistics.StatisticsError("correlation requires datasets of equal length")
        if len(data1) < 2:
            raise statistics.StatisticsError("correlation requires at least 2 data points")
        return statistics.correlation(data1, data2)
    
    @staticmethod
    def linear_regression(
        x: List[N],
        y: List[N],
    ) -> Tuple[float, float]:
        """
        Calculate linear regression parameters.
        
        Args:
            x: List of independent variable values
            y: List of dependent variable values
        
        Returns:
            Tuple of (slope, intercept)
        
        Example:
            >>> StatisticsAction.linear_regression([1, 2, 3, 4, 5], [2, 4, 6, 8, 10])
            (2.0, 0.0)
        """
        if len(x) != len(y):
            raise statistics.StatisticsError("linear_regression requires datasets of equal length")
        if len(x) < 2:
            raise statistics.StatisticsError("linear_regression requires at least 2 data points")
        return statistics.linear_regression(x, y)
    
    @staticmethod
    def geometric_mean(data: List[N]) -> float:
        """
        Calculate the geometric mean of data.
        
        Args:
            data: List of positive numeric values
        
        Returns:
            Geometric mean
        
        Example:
            >>> StatisticsAction.geometric_mean([1, 2, 3, 4, 5])
            2.605...
        """
        if not data:
            raise statistics.StatisticsError("geometric_mean requires at least one data point")
        return statistics.geometric_mean(data)
    
    @staticmethod
    def harmonic_mean(data: List[N]) -> float:
        """
        Calculate the harmonic mean of data.
        
        Args:
            data: List of numeric values (non-zero)
        
        Returns:
            Harmonic mean
        
        Example:
            >>> StatisticsAction.harmonic_mean([1, 2, 4])
            1.714...
        """
        if not data:
            raise statistics.StatisticsError("harmonic_mean requires at least one data point")
        return statistics.harmonic_mean(data)
    
    @staticmethod
    def median_grouped(data: List[N], interval: float = 1) -> float:
        """
        Calculate the median of grouped continuous data.
        
        Args:
            data: List of numeric values
            interval: Class interval size
        
        Returns:
            Grouped median
        """
        if not data:
            raise statistics.StatisticsError("median_grouped requires at least one data point")
        return statistics.median_grouped(data, interval=interval)
    
    @staticmethod
    def normal_dist(
        mu: float = 0,
        sigma: float = 1,
    ) -> statistics.NormalDist:
        """
        Create a NormalDist object with given parameters.
        
        Args:
            mu: Mean of the distribution
            sigma: Standard deviation
        
        Returns:
            NormalDist object
        """
        return statistics.NormalDist(mu, sigma)
    
    @staticmethod
    def sample(
        population: List[Any],
        k: int,
        *,
        replace: bool = False,
    ) -> List[Any]:
        """
        Return a random sample from the population.
        
        Args:
            population: List of items to sample from
            k: Number of items to sample
            replace: If True, sample with replacement
        
        Returns:
            List of sampled items
        
        Example:
            >>> StatisticsAction.sample([1, 2, 3, 4, 5], 3)
            [2, 5, 1]  # Random
        """
        if k < 0:
            raise ValueError("Sample size must be non-negative")
        if k > len(population) and not replace:
            raise ValueError("Sample size exceeds population without replacement")
        
        if replace:
            return random.choices(population, k=k)
        return random.sample(population, k=k)
    
    @staticmethod
    def shuffle(data: List[Any]) -> List[Any]:
        """
        Shuffle data randomly in place.
        
        Args:
            data: List to shuffle
        
        Returns:
            Shuffled list (same object)
        """
        result = data[:]
        random.shuffle(result)
        return result
    
    @staticmethod
    def zscore(value: float, mean: float, stdev: float) -> float:
        """
        Calculate the z-score of a value.
        
        Args:
            value: Value to calculate z-score for
            mean: Mean of the dataset
            stdev: Standard deviation of the dataset
        
        Returns:
            Z-score
        
        Example:
            >>> StatisticsAction.zscore(85, 70, 10)
            1.5
        """
        if stdev == 0:
            raise ValueError("Standard deviation cannot be zero")
        return (value - mean) / stdev
    
    @staticmethod
    def normalize(
        data: List[N],
        *,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> List[float]:
        """
        Normalize data to the range [0, 1].
        
        Args:
            data: List of numeric values
            min_val: Optional minimum value (uses data min if None)
            max_val: Optional maximum value (uses data max if None)
        
        Returns:
            List of normalized values
        
        Example:
            >>> StatisticsAction.normalize([0, 50, 100])
            [0.0, 0.5, 1.0]
        """
        if not data:
            return []
        
        min_val = min_val if min_val is not None else min(data)
        max_val = max_val if max_val is not None else max(data)
        
        if min_val == max_val:
            return [0.5] * len(data)
        
        return [(x - min_val) / (max_val - min_val) for x in data]
    
    @staticmethod
    def standardize(data: List[N]) -> List[float]:
        """
        Standardize data (z-score normalization).
        
        Args:
            data: List of numeric values
        
        Returns:
            List of standardized values (mean=0, stdev=1)
        
        Example:
            >>> StatisticsAction.standardize([1, 2, 3, 4, 5])
            [-1.414..., -0.707..., 0.0, 0.707..., 1.414...]
        """
        if not data:
            return []
        
        mean_val = statistics.mean(data)
        stdev_val = statistics.stdev(data)
        
        if stdev_val == 0:
            return [0.0] * len(data)
        
        return [(x - mean_val) / stdev_val for x in data]
    
    @staticmethod
    def summary(data: List[N]) -> Dict[str, Any]:
        """
        Calculate a comprehensive statistical summary of data.
        
        Args:
            data: List of numeric values
        
        Returns:
            Dictionary with statistical measures
        
        Example:
            >>> stats = StatisticsAction.summary([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            >>> stats['mean']
            5.5
        """
        if not data:
            return {}
        
        sorted_data = sorted(data)
        n = len(data)
        
        result = {
            "count": n,
            "min": min(data),
            "max": max(data),
            "sum": sum(data),
            "mean": statistics.mean(data),
            "median": statistics.median(data),
            "mode": StatisticsAction.mode(data) if data else None,
        }
        
        if n >= 2:
            result["stdev"] = statistics.stdev(data)
            result["variance"] = statistics.variance(data)
        
        if n >= 4:
            q1, q2, q3 = StatisticsAction.quartile(data)
            result["q1"] = q1
            result["q2"] = q2
            result["q3"] = q3
            result["iqr"] = q3 - q1
        
        return result


# Module-level convenience functions
def mean(data: List[N]) -> float:
    """Calculate arithmetic mean."""
    return StatisticsAction.mean(data)


def median(data: List[N]) -> float:
    """Calculate median."""
    return StatisticsAction.median(data)


def mode(data: List[Any]) -> Any:
    """Calculate mode."""
    return StatisticsAction.mode(data)


def stdev(data: List[N]) -> float:
    """Calculate standard deviation."""
    return StatisticsAction.stdev(data)


def variance(data: List[N]) -> float:
    """Calculate variance."""
    return StatisticsAction.variance(data)


def percentile(data: List[N], p: float) -> float:
    """Calculate percentile."""
    return StatisticsAction.percentile(data, p)


def correlation(data1: List[N], data2: List[N]) -> float:
    """Calculate correlation."""
    return StatisticsAction.correlation(data1, data2)


def summary(data: List[N]) -> Dict[str, Any]:
    """Calculate statistical summary."""
    return StatisticsAction.summary(data)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "StatisticsAction",
    "mean",
    "median",
    "mode",
    "stdev",
    "variance",
    "percentile",
    "correlation",
    "summary",
]
