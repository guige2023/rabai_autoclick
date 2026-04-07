"""
Ratio and proportion utilities for numeric calculations.

This module provides comprehensive ratio operations including:
- Ratio arithmetic (addition, subtraction, multiplication, division)
- Ratio simplification and normalization
- Ratio comparison and equality checks
- Proportion calculations and scaling
- Percentage conversions

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Any, Callable, Generator, List, Optional, Tuple, Union


@dataclass(frozen=True, order=True)
class Ratio:
    """
    Immutable ratio representation with exact arithmetic using fractions.
    
    A ratio represents a relationship between two numbers (a:b) where
    the second value is non-zero. Operations preserve exactness using
    Python's Fraction type internally.
    
    Attributes:
        numerator: The numerator of the ratio (first value).
        denominator: The denominator of the ratio (second value, must be non-zero).
        simplify: Whether to automatically simplify the ratio.
    
    Example:
        >>> ratio = Ratio(4, 8)
        >>> ratio.value
        Fraction(1, 2)
        >>> ratio.to_float()
        0.5
    """
    numerator: float = field(compare=True)
    denominator: float = field(compare=True)
    
    def __post_init__(self) -> None:
        if self.denominator == 0:
            raise ValueError(f"Denominator cannot be zero")
    
    @property
    def value(self) -> Fraction:
        """Return the exact fraction representation."""
        return Fraction(self.numerator).limit_denominator() / Fraction(self.denominator)
    
    @property
    def is_whole(self) -> bool:
        """Check if the ratio simplifies to a whole number."""
        return self.value.denominator == 1
    
    @property
    def is_proper(self) -> bool:
        """Check if the ratio is proper (numerator < denominator)."""
        return abs(self.numerator) < abs(self.denominator)
    
    @property
    def is_improper(self) -> bool:
        """Check if the ratio is improper (numerator >= denominator)."""
        return abs(self.numerator) >= abs(self.denominator)
    
    def to_float(self) -> float:
        """
        Convert the ratio to a floating-point number.
        
        Returns:
            The decimal representation of the ratio.
        """
        return float(self.numerator) / float(self.denominator)
    
    def to_percentage(self, decimals: int = 2) -> float:
        """
        Convert the ratio to a percentage.
        
        Args:
            decimals: Number of decimal places for rounding.
            
        Returns:
            The ratio expressed as a percentage (e.g., 0.5 -> 50.0).
        """
        percentage = self.to_float() * 100
        return round(percentage, decimals)
    
    def to_tuple(self) -> Tuple[int, int]:
        """
        Return the ratio as a simplified tuple of integers.
        
        Returns:
            A tuple (numerator, denominator) with the GCD removed.
        """
        frac = Fraction(self.numerator, self.denominator).limit_denominator()
        return (frac.numerator, frac.denominator)
    
    def inverse(self) -> Ratio:
        """
        Return the inverse (reciprocal) of the ratio.
        
        Returns:
            A new ratio with numerator and denominator swapped.
        """
        return Ratio(self.denominator, self.numerator)
    
    def scale(self, factor: float) -> Ratio:
        """
        Scale both parts of the ratio by a factor.
        
        Args:
            factor: The scaling factor.
            
        Returns:
            A new ratio with both parts multiplied by factor.
        """
        return Ratio(self.numerator * factor, self.denominator * factor)
    
    def __add__(self, other: Union[Ratio, float]) -> Ratio:
        """Add another ratio or float to this ratio."""
        if isinstance(other, Ratio):
            result = self.value + other.value
            return Ratio(result.numerator, result.denominator)
        else:
            result = self.value + Fraction(other)
            return Ratio(result.numerator, result.denominator)
    
    def __sub__(self, other: Union[Ratio, float]) -> Ratio:
        """Subtract another ratio or float from this ratio."""
        if isinstance(other, Ratio):
            result = self.value - other.value
            return Ratio(result.numerator, result.denominator)
        else:
            result = self.value - Fraction(other)
            return Ratio(result.numerator, result.denominator)
    
    def __mul__(self, other: Union[Ratio, float]) -> Ratio:
        """Multiply this ratio by another ratio or float."""
        if isinstance(other, Ratio):
            result = self.value * other.value
            return Ratio(result.numerator, result.denominator)
        else:
            result = self.value * Fraction(other)
            return Ratio(result.numerator, result.denominator)
    
    def __truediv__(self, other: Union[Ratio, float]) -> Ratio:
        """Divide this ratio by another ratio or float."""
        if isinstance(other, Ratio):
            result = self.value / other.value
            return Ratio(result.numerator, result.denominator)
        else:
            result = self.value / Fraction(other)
            return Ratio(result.numerator, result.denominator)
    
    def __repr__(self) -> str:
        """Return a string representation of the ratio."""
        return f"Ratio({self.numerator}:{self.denominator})"
    
    def __str__(self) -> str:
        """Return a human-readable string of the ratio."""
        return f"{self.numerator}:{self.denominator}"


@dataclass
class RatioSequence:
    """
    A sequence of ratios with operations like scaling and alignment.
    
    Example:
        >>> seq = RatioSequence([Ratio(1, 2), Ratio(2, 3), Ratio(3, 4)])
        >>> seq.common_denominator()
        12
        >>> seq.scale_to_sum(1.0)
    """
    ratios: List[Ratio] = field(default_factory=list)
    
    def common_denominator(self) -> int:
        """
        Find the least common multiple of all denominators.
        
        Returns:
            The LCM of all denominators.
        """
        def lcm(a: int, b: int) -> int:
            return abs(a * b) // math.gcd(a, b)
        
        result = 1
        for ratio in self.ratios:
            denom = ratio.value.denominator
            result = lcm(result, denom)
        
        return result
    
    def to_common_denominator(self) -> List[Tuple[int, int]]:
        """
        Convert all ratios to have a common denominator.
        
        Returns:
            List of (numerator, denominator) tuples with equal denominators.
        """
        if not self.ratios:
            return []
        
        common_denom = self.common_denominator()
        result = []
        
        for ratio in self.ratios:
            scale = common_denom // ratio.value.denominator
            new_num = ratio.value.numerator * scale
            result.append((new_num, common_denom))
        
        return result
    
    def scale_to_sum(self, target_sum: float) -> List[Ratio]:
        """
        Scale all ratios so they sum to a target value.
        
        Args:
            target_sum: The desired sum of all ratios.
            
        Returns:
            List of scaled ratios.
        """
        if not self.ratios:
            return []
        
        current_sum = sum(r.to_float() for r in self.ratios)
        
        if current_sum == 0:
            return self.ratios
        
        scale_factor = target_sum / current_sum
        
        return [r.scale(scale_factor) for r in self.ratios]
    
    def normalize(self) -> List[Ratio]:
        """
        Normalize ratios so they sum to 1.0.
        
        Returns:
            List of ratios that sum to exactly 1.0.
        """
        return self.scale_to_sum(1.0)
    
    def weighted_average(self, weights: List[float]) -> float:
        """
        Calculate the weighted average of the ratios.
        
        Args:
            weights: List of weights corresponding to each ratio.
            
        Returns:
            The weighted average.
            
        Raises:
            ValueError: If weights don't match ratios or sum to zero.
        """
        if len(weights) != len(self.ratios):
            raise ValueError("weights must have the same length as ratios")
        
        total_weight = sum(weights)
        if total_weight == 0:
            raise ValueError("weights cannot sum to zero")
        
        weighted_sum = sum(
            ratio.to_float() * weight
            for ratio, weight in zip(self.ratios, weights)
        )
        
        return weighted_sum / total_weight


def simplify_ratio(numerator: float, denominator: float) -> Tuple[int, int]:
    """
    Simplify a ratio to its lowest terms.
    
    Args:
        numerator: The numerator of the ratio.
        denominator: The denominator of the ratio.
        
    Returns:
        A tuple (simplified_numerator, simplified_denominator).
    """
    if denominator == 0:
        raise ValueError("Denominator cannot be zero")
    
    frac = Fraction(numerator, denominator).limit_denominator()
    return (frac.numerator, frac.denominator)


def ratio_to_percentage(numerator: float, denominator: float, decimals: int = 2) -> float:
    """
    Convert a ratio to a percentage.
    
    Args:
        numerator: The numerator of the ratio.
        denominator: The denominator of the ratio.
        decimals: Number of decimal places.
        
    Returns:
        The ratio as a percentage.
    """
    if denominator == 0:
        raise ValueError("Denominator cannot be zero")
    
    return round((numerator / denominator) * 100, decimals)


def percentage_to_ratio(percentage: float) -> Ratio:
    """
    Convert a percentage to a ratio.
    
    Args:
        percentage: The percentage value (e.g., 50 for 50%).
        
    Returns:
        A Ratio representing the percentage.
    """
    return Ratio(percentage, 100)


def proportion_scale(
    value: float,
    in_min: float,
    in_max: float,
    out_min: float,
    out_max: float
) -> float:
    """
    Scale a value from an input range to an output range (linear proportion).
    
    Args:
        value: The value to scale.
        in_min: Minimum of the input range.
        in_max: Maximum of the input range.
        out_min: Minimum of the output range.
        out_max: Maximum of the output range.
        
    Returns:
        The scaled value in the output range.
    """
    if math.isclose(in_min, in_max):
        raise ValueError(f"in_min ({in_min}) and in_max ({in_max}) cannot be equal")
    
    proportion = (value - in_min) / (in_max - in_min)
    return out_min + proportion * (out_max - out_min)


def continued_fraction(numerator: float, denominator: float, depth: int = 10) -> List[int]:
    """
    Compute the continued fraction representation of a ratio.
    
    Args:
        numerator: The numerator.
        denominator: The denominator.
        depth: Maximum depth of the continued fraction.
        
    Returns:
        List of integers representing the continued fraction.
    """
    if denominator == 0:
        raise ValueError("Denominator cannot be zero")
    
    result: List[int] = []
    n, d = int(numerator), int(denominator)
    
    for _ in range(depth):
        if d == 0:
            break
        result.append(n // d)
        n, d = d, n % d
    
    return result


def best_integer_ratio(value: float, max_denominator: int = 1000) -> Tuple[int, int]:
    """
    Find the best integer ratio approximating a float value.
    
    Args:
        value: The float value to approximate.
        max_denominator: Maximum allowed denominator.
        
    Returns:
        Tuple of (numerator, denominator) that best approximates the value.
    """
    frac = Fraction(value).limit_denominator(max_denominator)
    return (frac.numerator, frac.denominator)


def golden_ratio(n: int) -> float:
    """
    Calculate the nth power of the golden ratio (phi).
    
    Uses the closed-form formula: phi^n = F_n * phi + F_{n-1}
    where F_n is the nth Fibonacci number.
    
    Args:
        n: The power to raise phi to.
        
    Returns:
        phi raised to the power n.
    """
    phi = (1 + math.sqrt(5)) / 2
    
    if n >= 0:
        return phi ** n
    
    return (-phi) ** abs(n)


def silver_ratio(n: int = 1) -> float:
    """
    Calculate the nth silver ratio.
    
    The silver ratio (delta_S) is 1 + sqrt(2) ≈ 2.414.
    Higher order silver ratios follow the pattern.
    
    Args:
        n: The order of the silver ratio.
        
    Returns:
        The nth silver ratio value.
    """
    if n == 1:
        return 1 + math.sqrt(2)
    else:
        return 2 + (1 + math.sqrt(2)) ** n


def generate_partitions(total: float, num_parts: int, mode: str = "equal") -> List[float]:
    """
    Partition a total into parts based on a ratio.
    
    Args:
        total: The total value to partition.
        num_parts: Number of parts to create.
        mode: Partition mode - "equal", "random", or "golden".
        
    Returns:
        List of partition values that sum to the total.
        
    Raises:
        ValueError: If mode is invalid.
    """
    if num_parts < 1:
        raise ValueError(f"num_parts must be >= 1, got {num_parts}")
    
    if mode == "equal":
        part_size = total / num_parts
        return [part_size] * num_parts
    
    elif mode == "random":
        parts = [random.random() for _ in range(num_parts)]
        total_random = sum(parts)
        return [p * total / total_random for p in parts]
    
    elif mode == "golden":
        if num_parts == 1:
            return [total]
        
        phi = (1 + math.sqrt(5)) / 2
        parts = [phi ** i for i in range(num_parts)]
        total_parts = sum(parts)
        return [p * total / total_parts for p in parts]
    
    else:
        raise ValueError(f"Unknown mode: {mode}")


def ratio_equality(a: Tuple[float, float], b: Tuple[float, float], tolerance: float = 1e-9) -> bool:
    """
    Check if two ratios are approximately equal within a tolerance.
    
    Args:
        a: First ratio as (numerator, denominator).
        b: Second ratio as (numerator, denominator).
        tolerance: Maximum allowed difference between normalized values.
        
    Returns:
        True if the ratios are approximately equal.
    """
    val_a = a[0] / a[1] if a[1] != 0 else float('inf')
    val_b = b[0] / b[1] if b[1] != 0 else float('inf')
    
    return abs(val_a - val_b) < tolerance


def harmonic_mean(values: List[float]) -> float:
    """
    Calculate the harmonic mean of a list of values.
    
    The harmonic mean is the reciprocal of the arithmetic mean of reciprocals.
    Useful for averaging rates and ratios.
    
    Args:
        values: List of positive numeric values.
        
    Returns:
        The harmonic mean.
        
    Raises:
        ValueError: If any value is zero or negative.
    """
    if not values:
        raise ValueError("Cannot calculate harmonic mean of empty list")
    
    if any(v <= 0 for v in values):
        raise ValueError("All values must be positive for harmonic mean")
    
    reciprocal_sum = sum(1 / v for v in values)
    return len(values) / reciprocal_sum


def geometric_mean(values: List[float]) -> float:
    """
    Calculate the geometric mean of a list of values.
    
    The geometric mean is the nth root of the product of all values.
    Useful for growth rates and multiplicative processes.
    
    Args:
        values: List of positive numeric values.
        
    Returns:
        The geometric mean.
    """
    if not values:
        raise ValueError("Cannot calculate geometric mean of empty list")
    
    if any(v <= 0 for v in values):
        raise ValueError("All values must be positive for geometric mean")
    
    product = math.prod(values)
    return product ** (1 / len(values))


def cross_multiply(a: float, b: float, c: float, d: float) -> bool:
    """
    Check if two fractions are equal using cross multiplication.
    
    a/b == c/d  iff  a*d == b*c
    
    Args:
        a, b, c, d: The four values representing fractions a/b and c/d.
        
    Returns:
        True if the fractions are equal.
    """
    return a * d == b * c


def ratio_comparison(a: float, b: float, c: float, d: float) -> int:
    """
    Compare two fractions using cross multiplication.
    
    Args:
        a, b, c, d: The four values representing fractions a/b and c/d.
        
    Returns:
        -1 if a/b < c/d, 0 if equal, 1 if a/b > c/d.
    """
    diff = a * d - b * c
    
    if diff < 0:
        return -1
    elif diff > 0:
        return 1
    else:
        return 0
