"""Tests for math utilities."""

import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.math_utils import (
    clamp,
    lerp,
    inverse_lerp,
    map_range,
    mean,
    median,
    mode,
    variance,
    standard_deviation,
    percentile,
    min_max_normalize,
    z_score,
    round_to_decimal,
    is_close,
    gcd,
    lcm,
    is_prime,
    fibonacci,
    factorial,
    combinations,
    permutations,
    format_number,
    format_bytes,
    format_duration,
    distance_2d,
    distance_3d,
    manhattan_distance,
    chebyshev_distance,
)


class TestClamp:
    """Tests for clamp function."""

    def test_clamp_in_range(self) -> None:
        """Test clamping value in range."""
        assert clamp(5, 0, 10) == 5

    def test_clamp_below_min(self) -> None:
        """Test clamping value below minimum."""
        assert clamp(-5, 0, 10) == 0

    def test_clamp_above_max(self) -> None:
        """Test clamping value above maximum."""
        assert clamp(15, 0, 10) == 10


class TestLerp:
    """Tests for lerp function."""

    def test_lerp_midpoint(self) -> None:
        """Test linear interpolation at midpoint."""
        assert lerp(0, 10, 0.5) == 5

    def test_lerp_start(self) -> None:
        """Test linear interpolation at start."""
        assert lerp(0, 10, 0) == 0

    def test_lerp_end(self) -> None:
        """Test linear interpolation at end."""
        assert lerp(0, 10, 1) == 10


class TestInverseLerp:
    """Tests for inverse_lerp function."""

    def test_inverse_lerp_midpoint(self) -> None:
        """Test inverse lerp at midpoint."""
        assert inverse_lerp(0, 10, 5) == 0.5

    def test_inverse_lerp_start(self) -> None:
        """Test inverse lerp at start."""
        assert inverse_lerp(0, 10, 0) == 0

    def test_inverse_lerp_end(self) -> None:
        """Test inverse lerp at end."""
        assert inverse_lerp(0, 10, 10) == 1


class TestMapRange:
    """Tests for map_range function."""

    def test_map_range_identity(self) -> None:
        """Test mapping with identical ranges."""
        assert map_range(5, 0, 10, 0, 10) == 5

    def test_map_range_different_ranges(self) -> None:
        """Test mapping with different ranges."""
        assert map_range(5, 0, 10, 0, 100) == 50


class TestMean:
    """Tests for mean function."""

    def test_mean_basic(self) -> None:
        """Test mean of values."""
        assert mean([1, 2, 3, 4, 5]) == 3

    def test_mean_empty(self) -> None:
        """Test mean of empty list."""
        assert mean([]) == 0.0


class TestMedian:
    """Tests for median function."""

    def test_median_odd(self) -> None:
        """Test median of odd number of values."""
        assert median([1, 2, 3, 4, 5]) == 3

    def test_median_even(self) -> None:
        """Test median of even number of values."""
        assert median([1, 2, 3, 4]) == 2.5

    def test_median_empty(self) -> None:
        """Test median of empty list."""
        assert median([]) == 0.0


class TestMode:
    """Tests for mode function."""

    def test_mode_basic(self) -> None:
        """Test mode of values."""
        assert mode([1, 2, 2, 3, 3, 3]) == 3

    def test_mode_empty(self) -> None:
        """Test mode of empty list."""
        assert mode([]) is None


class TestVariance:
    """Tests for variance function."""

    def test_variance_basic(self) -> None:
        """Test variance of values."""
        result = variance([2, 4, 4, 4, 5, 5, 7, 9])
        assert is_close(result, 4.0)


class TestStandardDeviation:
    """Tests for standard_deviation function."""

    def test_std_dev_basic(self) -> None:
        """Test standard deviation."""
        result = standard_deviation([2, 4, 4, 4, 5, 5, 7, 9])
        assert is_close(result, 2.0)


class TestPercentile:
    """Tests for percentile function."""

    def test_percentile_median(self) -> None:
        """Test 50th percentile."""
        assert percentile([1, 2, 3, 4, 5], 50) == 3

    def test_percentile_empty(self) -> None:
        """Test percentile of empty list."""
        assert percentile([], 50) == 0.0


class TestMinMaxNormalize:
    """Tests for min_max_normalize function."""

    def test_normalize_basic(self) -> None:
        """Test normalization."""
        result = min_max_normalize([0, 5, 10])
        assert result == [0.0, 0.5, 1.0]


class TestZScore:
    """Tests for z_score function."""

    def test_z_score_basic(self) -> None:
        """Test z-score calculation."""
        assert z_score(5, 10, 2) == -2.5


class TestRoundToDecimal:
    """Tests for round_to_decimal function."""

    def test_round_to_decimal_basic(self) -> None:
        """Test rounding."""
        assert round_to_decimal(3.14159, 2) == 3.14


class TestIsClose:
    """Tests for is_close function."""

    def test_is_close_true(self) -> None:
        """Test close values."""
        assert is_close(1.0, 1.0001, rel_tol=1e-3)

    def test_is_close_false(self) -> None:
        """Test non-close values."""
        assert not is_close(1.0, 2.0)


class TestGCD:
    """Tests for gcd function."""

    def test_gcd_basic(self) -> None:
        """Test GCD calculation."""
        assert gcd(12, 18) == 6

    def test_gcd_coprime(self) -> None:
        """Test GCD of coprime numbers."""
        assert gcd(7, 11) == 1


class TestLCM:
    """Tests for lcm function."""

    def test_lcm_basic(self) -> None:
        """Test LCM calculation."""
        assert lcm(4, 6) == 12


class TestIsPrime:
    """Tests for is_prime function."""

    def test_is_prime_true(self) -> None:
        """Test prime number."""
        assert is_prime(7)

    def test_is_prime_false(self) -> None:
        """Test non-prime number."""
        assert not is_prime(8)


class TestFibonacci:
    """Tests for fibonacci function."""

    def test_fibonacci_basic(self) -> None:
        """Test Fibonacci number."""
        assert fibonacci(10) == 55


class TestFactorial:
    """Tests for factorial function."""

    def test_factorial_basic(self) -> None:
        """Test factorial."""
        assert factorial(5) == 120

    def test_factorial_zero(self) -> None:
        """Test factorial of zero."""
        assert factorial(0) == 1


class TestCombinations:
    """Tests for combinations function."""

    def test_combinations_basic(self) -> None:
        """Test combinations."""
        assert combinations(5, 2) == 10


class TestPermutations:
    """Tests for permutations function."""

    def test_permutations_basic(self) -> None:
        """Test permutations."""
        assert permutations(5, 2) == 20


class TestFormatNumber:
    """Tests for format_number function."""

    def test_format_number_basic(self) -> None:
        """Test number formatting."""
        assert format_number(1234.567, 2) == "1,234.57"


class TestFormatBytes:
    """Tests for format_bytes function."""

    def test_format_bytes_kb(self) -> None:
        """Test KB formatting."""
        assert format_bytes(1024) == "1.0 KB"

    def test_format_bytes_mb(self) -> None:
        """Test MB formatting."""
        assert format_bytes(1024 * 1024) == "1.0 MB"


class TestFormatDuration:
    """Tests for format_duration function."""

    def test_format_duration_seconds(self) -> None:
        """Test seconds formatting."""
        result = format_duration(30.5)
        assert "s" in result

    def test_format_duration_minutes(self) -> None:
        """Test minutes formatting."""
        result = format_duration(90)
        assert "m" in result


class TestDistance2D:
    """Tests for distance_2d function."""

    def test_distance_2d_basic(self) -> None:
        """Test 2D distance."""
        assert is_close(distance_2d(0, 0, 3, 4), 5.0)


class TestDistance3D:
    """Tests for distance_3d function."""

    def test_distance_3d_basic(self) -> None:
        """Test 3D distance."""
        d = distance_3d(0, 0, 0, 1, 2, 2)
        assert is_close(d, 3.0)


class TestManhattanDistance:
    """Tests for manhattan_distance function."""

    def test_manhattan_basic(self) -> None:
        """Test Manhattan distance."""
        assert manhattan_distance(0, 0, 3, 4) == 7


class TestChebyshevDistance:
    """Tests for chebyshev_distance function."""

    def test_chebyshev_basic(self) -> None:
        """Test Chebyshev distance."""
        assert chebyshev_distance(0, 0, 3, 4) == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
