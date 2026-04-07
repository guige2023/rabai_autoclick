"""
Fractions Action Module

Provides fractions operations including creation, arithmetic, comparison,
and conversion for precise fractional arithmetic in the automation framework.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import fractions
import math
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

# Type variables
N = TypeVar("N", int, float)


class FractionsAction:
    """
    Main fractions action handler providing fraction operations.
    
    This class wraps Python's fractions module with additional utilities
    for precise fractional arithmetic and automation tasks.
    
    Attributes:
        None (all methods are static or class methods)
    """
    
    @staticmethod
    def create_fraction(
        numerator: Union[int, float, str],
        denominator: Optional[Union[int, float]] = None,
    ) -> fractions.Fraction:
        """
        Create a new Fraction object.
        
        Args:
            numerator: Numerator value (int, float, string, or Fraction)
            denominator: Optional denominator (if numerator is int/float)
        
        Returns:
            New Fraction
        
        Example:
            >>> FractionsAction.create_fraction(1, 4)
            Fraction(1, 4)
            >>> FractionsAction.create_fraction("3/8")
            Fraction(3, 8)
        """
        if denominator is None:
            return fractions.Fraction(numerator)
        return fractions.Fraction(numerator, denominator)
    
    @staticmethod
    def create_from_float(
        value: float,
        max_denominator: Optional[int] = None,
    ) -> fractions.Fraction:
        """
        Create a Fraction from a float with optional precision limit.
        
        Args:
            value: Float value to convert
            max_denominator: Maximum denominator to use for approximation
        
        Returns:
            Fraction approximating the float
        
        Example:
            >>> FractionsAction.create_from_float(0.5)
            Fraction(1, 2)
            >>> FractionsAction.create_from_float(3.14159, max_denominator=100)
            Fraction(1571, 500)
        """
        frac = fractions.Fraction(value)
        if max_denominator is not None:
            return fractions.Fraction(frac.numerator, frac.denominator).limit_denominator(max_denominator)
        return frac
    
    @staticmethod
    def add(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
        """
        Add two fractions: a + b
        
        Args:
            a: First fraction
            b: Second fraction
        
        Returns:
            Sum of fractions
        
        Example:
            >>> FractionsAction.add(Fraction(1, 4), Fraction(1, 4))
            Fraction(1, 2)
        """
        return a + b
    
    @staticmethod
    def subtract(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
        """
        Subtract two fractions: a - b
        
        Args:
            a: First fraction
            b: Second fraction
        
        Returns:
            Difference of fractions
        
        Example:
            >>> FractionsAction.subtract(Fraction(3, 4), Fraction(1, 4))
            Fraction(1, 2)
        """
        return a - b
    
    @staticmethod
    def multiply(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
        """
        Multiply two fractions: a * b
        
        Args:
            a: First fraction
            b: Second fraction
        
        Returns:
            Product of fractions
        
        Example:
            >>> FractionsAction.multiply(Fraction(1, 2), Fraction(2, 3))
            Fraction(1, 3)
        """
        return a * b
    
    @staticmethod
    def divide(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
        """
        Divide two fractions: a / b
        
        Args:
            a: First fraction (dividend)
            b: Second fraction (divisor)
        
        Returns:
            Quotient of fractions
        
        Raises:
            ZeroDivisionError: If b is zero
        
        Example:
            >>> FractionsAction.divide(Fraction(1, 2), Fraction(2, 3))
            Fraction(3, 4)
        """
        return a / b
    
    @staticmethod
    def power(frac: fractions.Fraction, exponent: int) -> fractions.Fraction:
        """
        Raise a fraction to a power: frac ** exponent
        
        Args:
            frac: Fraction to raise
            exponent: Exponent (integer)
        
        Returns:
            Fraction raised to power
        
        Example:
            >>> FractionsAction.power(Fraction(1, 2), 3)
            Fraction(1, 8)
        """
        return frac ** exponent
    
    @staticmethod
    def reciprocal(frac: fractions.Fraction) -> fractions.Fraction:
        """
        Get the reciprocal of a fraction.
        
        Args:
            frac: Fraction
        
        Returns:
            Reciprocal fraction
        
        Example:
            >>> FractionsAction.reciprocal(Fraction(3, 4))
            Fraction(4, 3)
        """
        return fractions.Fraction(frac.denominator, frac.numerator)
    
    @staticmethod
    def numerator(frac: fractions.Fraction) -> int:
        """
        Get the numerator of a fraction.
        
        Args:
            frac: Fraction
        
        Returns:
            Numerator
        
        Example:
            >>> FractionsAction.numerator(Fraction(3, 4))
            3
        """
        return frac.numerator
    
    @staticmethod
    def denominator(frac: fractions.Fraction) -> int:
        """
        Get the denominator of a fraction.
        
        Args:
            frac: Fraction
        
        Returns:
            Denominator
        
        Example:
            >>> FractionsAction.denominator(Fraction(3, 4))
            4
        """
        return frac.denominator
    
    @staticmethod
    def to_float(frac: fractions.Fraction) -> float:
        """
        Convert a fraction to a float.
        
        Args:
            frac: Fraction to convert
        
        Returns:
            Float value
        
        Example:
            >>> FractionsAction.to_float(Fraction(1, 4))
            0.25
        """
        return float(frac)
    
    @staticmethod
    def to_int(frac: fractions.Fraction) -> int:
        """
        Convert a fraction to an integer (truncated).
        
        Args:
            frac: Fraction to convert
        
        Returns:
            Integer value (floor)
        
        Example:
            >>> FractionsAction.to_int(Fraction(7, 4))
            1
        """
        return int(frac)
    
    @staticmethod
    def to_decimal(frac: fractions.Fraction) -> fractions.Decimal:
        """
        Convert a fraction to a Decimal.
        
        Args:
            frac: Fraction to convert
        
        Returns:
            Decimal value
        """
        return fractions.Decimal(frac.numerator) / fractions.Decimal(frac.denominator)
    
    @staticmethod
    def to_tuple(frac: fractions.Fraction) -> Tuple[int, int, int]:
        """
        Convert a fraction to a mixed number tuple.
        
        Args:
            frac: Fraction to convert
        
        Returns:
            Tuple of (whole, numerator, denominator)
        
        Example:
            >>> FractionsAction.to_tuple(Fraction(7, 4))
            (1, 3, 4)
        """
        whole = frac.numerator // frac.denominator
        remainder = frac - whole
        return (whole, remainder.numerator, remainder.denominator)
    
    @staticmethod
    def from_tuple(mixed: Tuple[int, int, int]) -> fractions.Fraction:
        """
        Create a fraction from a mixed number tuple.
        
        Args:
            mixed: Tuple of (whole, numerator, denominator)
        
        Returns:
            Fraction
        
        Example:
            >>> FractionsAction.from_tuple((1, 3, 4))
            Fraction(7, 4)
        """
        whole, num, denom = mixed
        return fractions.Fraction(whole * denom + num, denom)
    
    @staticmethod
    def simplify(frac: fractions.Fraction) -> fractions.Fraction:
        """
        Simplify a fraction to lowest terms.
        
        Args:
            frac: Fraction to simplify
        
        Returns:
            Simplified fraction
        
        Example:
            >>> FractionsAction.simplify(Fraction(6, 8))
            Fraction(3, 4)
        """
        return fractions.Fraction(frac.numerator, frac.denominator)
    
    @staticmethod
    def is_integer(frac: fractions.Fraction) -> bool:
        """
        Check if a fraction is equivalent to an integer.
        
        Args:
            frac: Fraction to check
        
        Returns:
            True if denominator is 1
        
        Example:
            >>> FractionsAction.is_integer(Fraction(6, 3))
            True
        """
        return frac.denominator == 1
    
    @staticmethod
    def is_proper(frac: fractions.Fraction) -> bool:
        """
        Check if a fraction is proper (numerator < denominator).
        
        Args:
            frac: Fraction to check
        
        Returns:
            True if proper fraction
        
        Example:
            >>> FractionsAction.is_proper(Fraction(3, 4))
            True
            >>> FractionsAction.is_proper(Fraction(5, 4))
            False
        """
        return abs(frac.numerator) < abs(frac.denominator)
    
    @staticmethod
    def is_negative(frac: fractions.Fraction) -> bool:
        """
        Check if a fraction is negative.
        
        Args:
            frac: Fraction to check
        
        Returns:
            True if negative
        """
        return frac.numerator < 0
    
    @staticmethod
    def absolute(frac: fractions.Fraction) -> fractions.Fraction:
        """
        Get the absolute value of a fraction.
        
        Args:
            frac: Fraction
        
        Returns:
            Absolute value fraction
        """
        return abs(frac)
    
    @staticmethod
    def negate(frac: fractions.Fraction) -> fractions.Fraction:
        """
        Negate a fraction.
        
        Args:
            frac: Fraction to negate
        
        Returns:
            Negated fraction
        """
        return -frac
    
    @staticmethod
    def compare(a: fractions.Fraction, b: fractions.Fraction) -> int:
        """
        Compare two fractions.
        
        Args:
            a: First fraction
            b: Second fraction
        
        Returns:
            -1 if a < b, 0 if a == b, 1 if a > b
        
        Example:
            >>> FractionsAction.compare(Fraction(1, 4), Fraction(1, 3))
            -1
        """
        if a < b:
            return -1
        elif a > b:
            return 1
        return 0
    
    @staticmethod
    def min_fraction(*fractions_list: fractions.Fraction) -> fractions.Fraction:
        """
        Get the minimum fraction from arguments.
        
        Args:
            *fractions_list: Fractions to compare
        
        Returns:
            Minimum fraction
        """
        return min(*fractions_list)
    
    @staticmethod
    def max_fraction(*fractions_list: fractions.Fraction) -> fractions.Fraction:
        """
        Get the maximum fraction from arguments.
        
        Args:
            *fractions_list: Fractions to compare
        
        Returns:
            Maximum fraction
        """
        return max(*fractions_list)
    
    @staticmethod
    def floor(frac: fractions.Fraction) -> int:
        """
        Get the floor of a fraction.
        
        Args:
            frac: Fraction
        
        Returns:
            Floor value
        
        Example:
            >>> FractionsAction.floor(Fraction(7, 4))
            1
        """
        return math.floor(frac)
    
    @staticmethod
    def ceiling(frac: fractions.Fraction) -> int:
        """
        Get the ceiling of a fraction.
        
        Args:
            frac: Fraction
        
        Returns:
            Ceiling value
        
        Example:
            >>> FractionsAction.ceiling(Fraction(7, 4))
            2
        """
        return math.ceil(frac)
    
    @staticmethod
    def round_frac(frac: fractions.Fraction, ndigits: int = 0) -> fractions.Fraction:
        """
        Round a fraction to ndigits decimal places.
        
        Args:
            frac: Fraction to round
            ndigits: Number of decimal places
        
        Returns:
            Rounded fraction
        
        Example:
            >>> FractionsAction.round_frac(Fraction(1, 3), ndigits=2)
            Fraction(33, 100)
        """
        float_val = round(float(frac), ndigits)
        return fractions.Fraction(float_val).limit_denominator(10 ** ndigits if ndigits else 1)
    
    @staticmethod
    def sum_fractions(fractions_list: List[fractions.Fraction]) -> fractions.Fraction:
        """
        Sum a list of fractions.
        
        Args:
            fractions_list: List of fractions to sum
        
        Returns:
            Sum of fractions
        
        Example:
            >>> FractionsAction.sum_fractions([Fraction(1, 4), Fraction(1, 4), Fraction(1, 4)])
            Fraction(3, 4)
        """
        result = fractions.Fraction(0, 1)
        for frac in fractions_list:
            result += frac
        return result
    
    @staticmethod
    def average_fractions(fractions_list: List[fractions.Fraction]) -> fractions.Fraction:
        """
        Calculate the average of a list of fractions.
        
        Args:
            fractions_list: List of fractions
        
        Returns:
            Average fraction
        
        Example:
            >>> FractionsAction.average_fractions([Fraction(1, 2), Fraction(1, 4)])
            Fraction(3, 8)
        """
        if not fractions_list:
            raise ValueError("Cannot average empty list")
        total = FractionsAction.sum_fractions(fractions_list)
        return total / len(fractions_list)
    
    @staticmethod
    def lcm(a: int, b: int) -> int:
        """
        Calculate the least common multiple of two integers.
        
        Args:
            a: First integer
            b: Second integer
        
        Returns:
            LCM of a and b
        
        Example:
            >>> FractionsAction.lcm(4, 6)
            12
        """
        return abs(a * b) // math.gcd(a, b)
    
    @staticmethod
    def common_denominator(
        fractions_list: List[fractions.Fraction],
    ) -> Tuple[int, List[fractions.Fraction]]:
        """
        Convert fractions to have a common denominator.
        
        Args:
            fractions_list: List of fractions
        
        Returns:
            Tuple of (common_denominator, list of converted fractions)
        
        Example:
            >>> FractionsAction.common_denominator([Fraction(1, 3), Fraction(1, 4)])
            (12, [Fraction(4, 12), Fraction(3, 12)])
        """
        if not fractions_list:
            raise ValueError("Cannot process empty list")
        
        # Calculate LCM of all denominators
        common_denom = fractions_list[0].denominator
        for frac in fractions_list[1:]:
            common_denom = FractionsAction.lcm(common_denom, frac.denominator)
        
        # Convert each fraction
        converted = []
        for frac in fractions_list:
            multiplier = common_denom // frac.denominator
            converted.append(fractions.Fraction(frac.numerator * multiplier, common_denom))
        
        return (common_denom, converted)
    
    @staticmethod
    def from_percent(value: Union[int, float]) -> fractions.Fraction:
        """
        Create a fraction from a percentage.
        
        Args:
            value: Percentage value (e.g., 50 for 50%)
        
        Returns:
            Fraction
        
        Example:
            >>> FractionsAction.from_percent(50)
            Fraction(1, 2)
        """
        return fractions.Fraction(value, 100)
    
    @staticmethod
    def to_percent(frac: fractions.Fraction, decimals: int = 2) -> float:
        """
        Convert a fraction to a percentage.
        
        Args:
            frac: Fraction to convert
            decimals: Number of decimal places
        
        Returns:
            Percentage value
        
        Example:
            >>> FractionsAction.to_percent(Fraction(1, 4))
            25.0
        """
        return round(float(frac) * 100, decimals)
    
    @staticmethod
    def continued_fraction(frac: fractions.Fraction, depth: int = 10) -> List[int]:
        """
        Convert a fraction to its continued fraction representation.
        
        Args:
            frac: Fraction to convert
            depth: Maximum depth
        
        Returns:
            List of continued fraction terms
        
        Example:
            >>> FractionsAction.continued_fraction(Fraction(22, 7))
            [3, 7]
        """
        terms = []
        num, denom = frac.numerator, frac.denominator
        
        for _ in range(depth):
            if denom == 0:
                break
            terms.append(num // denom)
            num, denom = denom, num % denom
            if denom == 0:
                break
        
        return terms
    
    @staticmethod
    def from_continued_fraction(terms: List[int]) -> fractions.Fraction:
        """
        Create a fraction from continued fraction terms.
        
        Args:
            terms: List of continued fraction terms
        
        Returns:
            Fraction
        
        Example:
            >>> FractionsAction.from_continued_fraction([3, 7])
            Fraction(22, 7)
        """
        if not terms:
            return fractions.Fraction(0, 1)
        
        frac = fractions.Fraction(terms[-1], 1)
        for term in reversed(terms[:-1]):
            frac = fractions.Fraction(term, 1) + fractions.Fraction(1, frac)
        
        return frac
    
    @staticmethod
    def format_fraction(
        frac: fractions.Fraction,
        as_mixed: bool = False,
        as_decimal: bool = False,
        decimal_places: int = 6,
    ) -> str:
        """
        Format a fraction as a string.
        
        Args:
            frac: Fraction to format
            as_mixed: Format as mixed number if True
            as_decimal: Format as decimal if True
            decimal_places: Number of decimal places
        
        Returns:
            Formatted string
        
        Example:
            >>> FractionsAction.format_fraction(Fraction(7, 4), as_mixed=True)
            '1 3/4'
        """
        if as_decimal:
            return f"{float(frac):.{decimal_places}f}"
        
        if as_mixed and abs(frac.numerator) >= abs(frac.denominator):
            whole = frac.numerator // frac.denominator
            remainder = frac - whole
            if remainder.numerator == 0:
                return str(whole)
            return f"{whole} {abs(remainder.numerator)}/{abs(remainder.denominator)}"
        
        if frac.numerator < 0:
            return f"-{-frac.numerator}/{frac.denominator}"
        
        return f"{frac.numerator}/{frac.denominator}"


# Module-level convenience functions
def create_fraction(numerator: Any, denominator: Any = None) -> fractions.Fraction:
    """Create a new Fraction object."""
    return FractionsAction.create_fraction(numerator, denominator)


def add_fractions(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
    """Add two fractions."""
    return FractionsAction.add(a, b)


def multiply_fractions(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
    """Multiply two fractions."""
    return FractionsAction.multiply(a, b)


def divide_fractions(a: fractions.Fraction, b: fractions.Fraction) -> fractions.Fraction:
    """Divide two fractions."""
    return FractionsAction.divide(a, b)


def to_float(frac: fractions.Fraction) -> float:
    """Convert fraction to float."""
    return FractionsAction.to_float(frac)


def simplify(frac: fractions.Fraction) -> fractions.Fraction:
    """Simplify a fraction."""
    return FractionsAction.simplify(frac)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "FractionsAction",
    "create_fraction",
    "add_fractions",
    "multiply_fractions",
    "divide_fractions",
    "to_float",
    "simplify",
]
