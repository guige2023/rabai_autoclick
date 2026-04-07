"""Math utilities for RabAI AutoClick.

Provides:
- Mathematical helper functions
- Statistical operations
- Number formatting
"""

import math
import random
from typing import Any, Callable, List, Optional, TypeVar


T = TypeVar("T")


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max.

    Args:
        value: Value to clamp.
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Clamped value.
    """
    return max(min_val, min(max_val, value))


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation between a and b.

    Args:
        a: Start value.
        b: End value.
        t: Interpolation factor (0 to 1).

    Returns:
        Interpolated value.
    """
    return a + (b - a) * t


def inverse_lerp(a: float, b: float, value: float) -> float:
    """Inverse linear interpolation.

    Args:
        a: Start value.
        b: End value.
        value: Value between a and b.

    Returns:
        Interpolation factor (0 to 1).
    """
    if b == a:
        return 0.0
    return (value - a) / (b - a)


def map_range(
    value: float,
    in_min: float,
    in_max: float,
    out_min: float,
    out_max: float,
) -> float:
    """Map value from input range to output range.

    Args:
        value: Value to map.
        in_min: Input minimum.
        in_max: Input maximum.
        out_min: Output minimum.
        out_max: Output maximum.

    Returns:
        Mapped value.
    """
    t = inverse_lerp(in_min, in_max, value)
    return lerp(out_min, out_max, t)


def mean(values: List[float]) -> float:
    """Calculate mean of values.

    Args:
        values: List of values.

    Returns:
        Mean value.
    """
    if not values:
        return 0.0
    return sum(values) / len(values)


def median(values: List[float]) -> float:
    """Calculate median of values.

    Args:
        values: List of values.

    Returns:
        Median value.
    """
    if not values:
        return 0.0
    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2
    return sorted_values[mid]


def mode(values: List[float]) -> Optional[float]:
    """Calculate mode of values.

    Args:
        values: List of values.

    Returns:
        Most common value or None.
    """
    if not values:
        return None
    counts: dict = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    max_count = max(counts.values())
    for v, count in counts.items():
        if count == max_count:
            return v
    return None


def variance(values: List[float]) -> float:
    """Calculate variance of values.

    Args:
        values: List of values.

    Returns:
        Variance.
    """
    if len(values) < 2:
        return 0.0
    m = mean(values)
    return sum((x - m) ** 2 for x in values) / len(values)


def standard_deviation(values: List[float]) -> float:
    """Calculate standard deviation.

    Args:
        values: List of values.

    Returns:
        Standard deviation.
    """
    return math.sqrt(variance(values))


def percentile(values: List[float], p: float) -> float:
    """Calculate percentile of values.

    Args:
        values: List of values.
        p: Percentile (0 to 100).

    Returns:
        Percentile value.
    """
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * p / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return lerp(sorted_values[int(f)], sorted_values[int(c)], k - f)


def min_max_normalize(values: List[float]) -> List[float]:
    """Normalize values to 0-1 range.

    Args:
        values: List of values.

    Returns:
        Normalized values.
    """
    if not values:
        return []
    min_val = min(values)
    max_val = max(values)
    if max_val == min_val:
        return [0.5] * len(values)
    return [(v - min_val) / (max_val - min_val) for v in values]


def z_score(value: float, mean_val: float, std_dev: float) -> float:
    """Calculate z-score.

    Args:
        value: Value to score.
        mean_val: Mean.
        std_dev: Standard deviation.

    Returns:
        Z-score.
    """
    if std_dev == 0:
        return 0.0
    return (value - mean_val) / std_dev


def round_to_decimal(value: float, decimals: int) -> float:
    """Round to specified decimal places.

    Args:
        value: Value to round.
        decimals: Number of decimal places.

    Returns:
        Rounded value.
    """
    multiplier = 10 ** decimals
    return round(value * multiplier) / multiplier


def is_close(a: float, b: float, rel_tol: float = 1e-9, abs_tol: float = 0.0) -> bool:
    """Check if values are close.

    Args:
        a: First value.
        b: Second value.
        rel_tol: Relative tolerance.
        abs_tol: Absolute tolerance.

    Returns:
        True if values are close.
    """
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def gcd(a: int, b: int) -> int:
    """Calculate greatest common divisor.

    Args:
        a: First integer.
        b: Second integer.

    Returns:
        GCD of a and b.
    """
    while b:
        a, b = b, a % b
    return abs(a)


def lcm(a: int, b: int) -> int:
    """Calculate least common multiple.

    Args:
        a: First integer.
        b: Second integer.

    Returns:
        LCM of a and b.
    """
    if a == 0 or b == 0:
        return 0
    return abs(a * b) // gcd(a, b)


def is_prime(n: int) -> bool:
    """Check if number is prime.

    Args:
        n: Number to check.

    Returns:
        True if prime.
    """
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True


def fibonacci(n: int) -> int:
    """Calculate nth Fibonacci number.

    Args:
        n: Index (0-based).

    Returns:
        Fibonacci number.
    """
    if n <= 0:
        return 0
    if n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def factorial(n: int) -> int:
    """Calculate factorial.

    Args:
        n: Non-negative integer.

    Returns:
        Factorial of n.
    """
    if n < 0:
        raise ValueError("Factorial requires non-negative integer")
    if n <= 1:
        return 1
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


def combinations(n: int, k: int) -> int:
    """Calculate combinations.

    Args:
        n: Total items.
        k: Items to choose.

    Returns:
        Number of combinations.
    """
    if k > n or k < 0:
        return 0
    return factorial(n) // (factorial(k) * factorial(n - k))


def permutations(n: int, k: int) -> int:
    """Calculate permutations.

    Args:
        n: Total items.
        k: Items to arrange.

    Returns:
        Number of permutations.
    """
    if k > n or k < 0:
        return 0
    return factorial(n) // factorial(n - k)


def random_float(min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Generate random float.

    Args:
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Random float.
    """
    return random.uniform(min_val, max_val)


def random_int(min_val: int, max_val: int) -> int:
    """Generate random integer.

    Args:
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Random integer.
    """
    return random.randint(min_val, max_val)


def random_choice(choices: List[T]) -> T:
    """Random choice from list.

    Args:
        choices: List of choices.

    Returns:
        Random element.
    """
    if not choices:
        raise ValueError("Cannot choose from empty list")
    return random.choice(choices)


def random_sample(population: List[T], k: int) -> List[T]:
    """Random sample from population.

    Args:
        population: Population to sample from.
        k: Number of samples.

    Returns:
        List of samples.
    """
    return random.sample(population, k)


def format_number(n: float, decimals: int = 2) -> str:
    """Format number with thousands separator.

    Args:
        n: Number to format.
        decimals: Decimal places.

    Returns:
        Formatted string.
    """
    return f"{n:,.{decimals}f}"


def format_bytes(bytes_val: int) -> str:
    """Format bytes as human-readable string.

    Args:
        bytes_val: Number of bytes.

    Returns:
        Formatted string (e.g., "1.5 MB").
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(bytes_val) < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration as human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "1h 30m").
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    seconds = seconds % 60
    if minutes < 60:
        return f"{minutes}m {seconds:.0f}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"


def deg_to_rad(degrees: float) -> float:
    """Convert degrees to radians.

    Args:
        degrees: Degrees.

    Returns:
        Radians.
    """
    return degrees * math.pi / 180.0


def rad_to_deg(radians: float) -> float:
    """Convert radians to degrees.

    Args:
        radians: Radians.

    Returns:
        Degrees.
    """
    return radians * 180.0 / math.pi


def distance_2d(x1: float, y1: float, x2: float, y2: float) -> float:
    """Calculate 2D Euclidean distance.

    Args:
        x1: First point x.
        y1: First point y.
        x2: Second point x.
        y2: Second point y.

    Returns:
        Distance.
    """
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


def distance_3d(x1: float, y1: float, z1: float, x2: float, y2: float, z2: float) -> float:
    """Calculate 3D Euclidean distance.

    Args:
        x1: First point x.
        y1: First point y.
        z1: First point z.
        x2: Second point x.
        y2: Second point y.
        z2: Second point z.

    Returns:
        Distance.
    """
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2 + (z2 - z1) ** 2)


def manhattan_distance(x1: float, y1: float, x2: float, y2: float) -> float:
    """Calculate Manhattan distance.

    Args:
        x1: First point x.
        y1: First point y.
        x2: Second point x.
        y2: Second point y.

    Returns:
        Manhattan distance.
    """
    return abs(x2 - x1) + abs(y2 - y1)


def chebyshev_distance(x1: float, y1: float, x2: float, y2: float) -> float:
    """Calculate Chebyshev distance.

    Args:
        x1: First point x.
        y1: First point y.
        x2: Second point x.
        y2: Second point y.

    Returns:
        Chebyshev distance.
    """
    return max(abs(x2 - x1), abs(y2 - y1))
