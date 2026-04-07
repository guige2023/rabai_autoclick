"""
Algorithmic complexity analysis utilities.

Provides Big-O notation analysis, time/space complexity estimators,
amortized analysis, and complexity class detection.
"""

from __future__ import annotations

import math
from typing import Callable


def big_o_notation(n: int, factor: float = 1.0) -> dict[str, float]:
    """
    Compute operation counts for common complexity classes.

    Args:
        n: Input size
        factor: Scaling factor

    Returns:
        Dictionary of complexity class names to operation counts.
    """
    return {
        "O(1)": factor,
        "O(log n)": factor * math.log2(max(1, n)),
        "O(n)": factor * n,
        "O(n log n)": factor * n * math.log2(max(1, n)),
        "O(n^2)": factor * n * n,
        "O(n^3)": factor * n * n * n,
        "O(2^n)": factor * (2 ** n if n < 100 else float("inf")),
        "O(n!)": factor * math.prod(range(min(n, 20))),
    }


def complexity_class(measured_times: list[tuple[int, float]]) -> str:
    """
    Detect complexity class from empirical measurements.

    Args:
        measured_times: List of (n, time) measurements

    Returns:
        Estimated Big-O class string.
    """
    if len(measured_times) < 3:
        return "Unknown"

    ratios: dict[str, list[float]] = {
        "O(1)": [], "O(log n)": [], "O(n)": [],
        "O(n log n)": [], "O(n^2)": [], "O(n^3)": [],
    }

    for i in range(len(measured_times) - 1):
        n1, t1 = measured_times[i]
        n2, t2 = measured_times[i + 1]
        if t1 < 1e-9 or n1 == n2:
            continue

        ratio_n = n2 / n1
        ratio_t = t2 / t1

        if abs(ratio_t - 1.0) < 0.1:
            ratios["O(1)"].append(ratio_t)
        elif abs(ratio_t - math.log2(ratio_n)) < 0.5:
            ratios["O(log n)"].append(ratio_t / ratio_n * math.log2(ratio_n))
        elif abs(ratio_t - ratio_n) < ratio_n * 0.5:
            ratios["O(n)"].append(ratio_t / ratio_n)
        elif abs(ratio_t - ratio_n * math.log2(ratio_n)) < ratio_n * math.log2(ratio_n) * 0.5:
            ratios["O(n log n)"].append(ratio_t / (ratio_n * math.log2(ratio_n)))
        elif abs(ratio_t - ratio_n ** 2) < ratio_n ** 2 * 0.5:
            ratios["O(n^2)"].append(ratio_t / ratio_n ** 2)
        elif abs(ratio_t - ratio_n ** 3) < ratio_n ** 3 * 0.5:
            ratios["O(n^3)"].append(ratio_t / ratio_n ** 3)

    best_class = "Unknown"
    best_score = -1.0
    for cls, scores in ratios.items():
        if scores:
            avg = sum(scores) / len(scores)
            consistency = len(scores) / (len(measured_times) - 1)
            score = consistency * (1.0 / abs(math.log2(avg) + 0.01) if avg > 0 else 0)
            if score > best_score:
                best_score = score
                best_class = cls

    return best_class


def amortized_analysis(
    costs: list[int],
    method: str = "aggregate",
) -> float:
    """
    Amortized cost analysis.

    Args:
        costs: List of operation costs over time
        method: 'aggregate', 'accounting', or 'potential'

    Returns:
        Amortized cost per operation.
    """
    if method == "aggregate":
        return sum(costs) / len(costs) if costs else 0.0
    elif method == "accounting":
        # Simple accounting method: credit = sum of (cheap - actual)
        # Amortized = cheap + credit/operations
        avg = sum(costs) / len(costs)
        return avg
    else:  # potential
        return sum(costs) / len(costs) if costs else 0.0


def master_theorem(
    a: float,
    b: float,
    f_n: Callable[[int], float],
    n: int = 1024,
) -> str:
    """
    Apply the Master Theorem to estimate complexity of divide-and-conquer.

    Recurrence: T(n) = a * T(n/b) + f(n)

    Args:
        a: Number of subproblems
        b: Factor by which problem size shrinks
        f_n: Cost of combining subproblems
        n: Input size

    Returns:
        Complexity class string.
    """
    if a <= 0 or b <= 1:
        return "Invalid recurrence"
    n_log_b_a = n ** (math.log(a) / math.log(b))
    f_val = f_n(n)

    epsilon = 0.01
    if f_val < n_log_b_a * (1 - epsilon):
        return f"O(n^{math.log(a) / math.log(b):.2f})"
    elif abs(f_val - n_log_b_a) < n_log_b_a * epsilon:
        return f"O(n^{math.log(a) / math.log(b):.2f} log n)"
    elif f_val > n_log_b_a * (1 + epsilon):
        return f"O(f(n))"
    return f"n^{math.log(a) / math.log(b):.2f}"


def space_complexity(
    memory_bytes: list[tuple[int, int]],
) -> str:
    """
    Estimate space complexity from memory measurements.

    Args:
        memory_bytes: List of (n, bytes) measurements

    Returns:
        Space complexity class.
    """
    if len(memory_bytes) < 2:
        return "Unknown"

    n1, m1 = memory_bytes[0]
    n2, m2 = memory_bytes[1]
    if n1 == 0 or n2 == 0 or m1 == 0:
        return "Unknown"

    ratio_n = n2 / n1
    ratio_m = m2 / m1

    if abs(ratio_m - 1.0) < 0.2:
        return "O(1)"
    elif abs(ratio_m - ratio_n) < ratio_n * 0.3:
        return "O(n)"
    elif abs(ratio_m - ratio_n ** 2) < ratio_n ** 2 * 0.3:
        return "O(n²)"
    elif abs(ratio_m - math.log2(ratio_n)) < 0.5:
        return "O(log n)"
    return "Unknown"


def estimate_growth_rate(n: int, ops: float) -> str:
    """
    Classify algorithm growth from operation count.

    Args:
        n: Input size
        ops: Measured operations

    Returns:
        Growth classification.
    """
    if n <= 0:
        return "Edge case"
    ratio = ops / n
    if ops < n * 0.1:
        return "Sublinear"
    elif abs(ops - n) < n * 0.5:
        return "Linear O(n)"
    elif abs(ops - n * math.log2(n)) < n * math.log2(n) * 0.5:
        return "Linearithmic O(n log n)"
    elif abs(ops - n ** 2) < n ** 2 * 0.5:
        return "Quadratic O(n²)"
    elif ops > 2 ** n:
        return "Exponential O(2^n)"
    return "Unknown"
