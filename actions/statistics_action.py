"""
Statistics utilities - distributions, hypothesis testing, correlation, regression.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import math
import random
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _mean(data: List[float]) -> float:
    return sum(data) / len(data) if data else 0.0


def _median(data: List[float]) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    n = len(sorted_data)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_data[mid - 1] + sorted_data[mid]) / 2.0
    return sorted_data[mid]


def _variance(data: List[float], ddof: int = 1) -> float:
    if len(data) < 2:
        return 0.0
    m = _mean(data)
    return sum((x - m) ** 2 for x in data) / (len(data) - ddof)


def _stddev(data: List[float], ddof: int = 1) -> float:
    return math.sqrt(_variance(data, ddof))


def _z_score(value: float, mean: float, std: float) -> float:
    return (value - mean) / std if std > 0 else 0.0


def _correlation(xs: List[float], ys: List[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    n = len(xs)
    mx, my = _mean(xs), _mean(ys)
    sx, sy = _stddev(xs), _stddev(ys)
    if sx == 0 or sy == 0:
        return 0.0
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n * sx * sy)


def _linear_regression(xs: List[float], ys: List[float]) -> Dict[str, float]:
    if len(xs) != len(ys) or len(xs) < 2:
        return {"slope": 0.0, "intercept": 0.0, "r_squared": 0.0}
    mx, my = _mean(xs), _mean(ys)
    ssxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    ssxx = sum((x - mx) ** 2 for x in xs)
    if ssxx == 0:
        return {"slope": 0.0, "intercept": my, "r_squared": 0.0}
    slope = ssxy / ssxx
    intercept = my - slope * mx
    ssres = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    sstot = sum((y - my) ** 2 for y in ys)
    r_squared = 1 - (ssres / sstot) if sstot > 0 else 0.0
    return {"slope": slope, "intercept": intercept, "r_squared": r_squared}


def _normal_pdf(x: float, mean: float, std: float) -> float:
    coeff = 1.0 / (std * math.sqrt(2 * math.pi))
    exponent = -0.5 * ((x - mean) / std) ** 2
    return coeff * math.exp(exponent)


def _combinations(n: int, r: int) -> float:
    if r > n or r < 0:
        return 0.0
    return math.factorial(n) / (math.factorial(r) * math.factorial(n - r))


def _permutations(n: int, r: int) -> float:
    if r > n or r < 0:
        return 0.0
    return math.factorial(n) / math.factorial(n - r)


def _factorial(n: int) -> float:
    return float(math.factorial(max(0, n)))


class StatisticsAction(BaseAction):
    """Statistical analysis and hypothesis testing operations.

    Provides descriptive statistics, correlation, regression, distributions, and tests.
    Pure Python implementation - no external dependencies required.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "stats")
        data = params.get("data", [])
        alpha = params.get("alpha", 0.05)

        try:
            if operation == "stats":
                numeric_data = [float(x) for x in data if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(".", "").replace("-", "").isdigit())]
                if not numeric_data:
                    return {"success": False, "error": "No numeric data found"}
                return {
                    "success": True,
                    "count": len(numeric_data),
                    "mean": _mean(numeric_data),
                    "median": _median(numeric_data),
                    "variance": _variance(numeric_data),
                    "stddev": _stddev(numeric_data),
                    "min": min(numeric_data),
                    "max": max(numeric_data),
                    "range": max(numeric_data) - min(numeric_data),
                }

            elif operation == "zscore":
                value = float(params.get("value", 0))
                mean = float(params.get("mean", _mean(data) if data else 0))
                std = float(params.get("std", _stddev(data) if len(data) > 1 else 1))
                return {"success": True, "z_score": _z_score(value, mean, std)}

            elif operation == "zscore_all":
                numeric_data = [float(x) for x in data if isinstance(x, (int, float))]
                if len(numeric_data) < 2:
                    return {"success": False, "error": "Need at least 2 data points"}
                m, s = _mean(numeric_data), _stddev(numeric_data)
                z_scores = [{"value": v, "z_score": _z_score(v, m, s)} for v in numeric_data]
                return {"success": True, "z_scores": z_scores}

            elif operation == "correlation":
                ys = [float(x) for x in params.get("y", [])]
                xs = [float(x) for x in data]
                return {"success": True, "correlation": _correlation(xs, ys)}

            elif operation == "regression":
                ys = [float(x) for x in params.get("y", [])]
                xs = [float(x) for x in data]
                result = _linear_regression(xs, ys)
                return {"success": True, **result}

            elif operation == "normal_pdf":
                x = float(params.get("x", 0))
                mean = float(params.get("mean", 0))
                std = float(params.get("std", 1))
                return {"success": True, "pdf": _normal_pdf(x, mean, std)}

            elif operation == "combinations":
                n = int(params.get("n", 0))
                r = int(params.get("r", 0))
                return {"success": True, "combinations": _combinations(n, r)}

            elif operation == "permutations":
                n = int(params.get("n", 0))
                r = int(params.get("r", 0))
                return {"success": True, "permutations": _permutations(n, r)}

            elif operation == "factorial":
                n = int(params.get("n", 0))
                return {"success": True, "factorial": _factorial(n)}

            elif operation == "percentile":
                numeric_data = sorted([float(x) for x in data if isinstance(x, (int, float))])
                p = float(params.get("percentile", 50))
                if not numeric_data:
                    return {"success": False, "error": "No numeric data"}
                idx = (p / 100.0) * (len(numeric_data) - 1)
                lower = int(math.floor(idx))
                upper = int(math.ceil(idx))
                if lower == upper:
                    return {"success": True, "percentile": numeric_data[lower]}
                frac = idx - lower
                result = numeric_data[lower] * (1 - frac) + numeric_data[upper] * frac
                return {"success": True, "percentile": result, "p": p}

            elif operation == "outliers":
                numeric_data = [float(x) for x in data if isinstance(x, (int, float))]
                if len(numeric_data) < 3:
                    return {"success": False, "error": "Need at least 3 data points"}
                m, s = _mean(numeric_data), _stddev(numeric_data)
                threshold = float(params.get("threshold", 2.0))
                outliers = [v for v in numeric_data if abs(_z_score(v, m, s)) > threshold]
                return {"success": True, "outliers": outliers, "count": len(outliers)}

            elif operation == "sample":
                size = int(params.get("size", 10))
                replace = params.get("replace", False)
                if replace:
                    return {"success": True, "sample": random.choices(data, k=size)}
                return {"success": True, "sample": random.sample(data, k=min(size, len(data)))}

            elif operation == "confidence_interval":
                numeric_data = [float(x) for x in data if isinstance(x, (int, float))]
                if len(numeric_data) < 2:
                    return {"success": False, "error": "Need at least 2 data points"}
                m, s, n = _mean(numeric_data), _stddev(numeric_data), len(numeric_data)
                se = s / math.sqrt(n)
                z_crit = {0.01: 2.576, 0.05: 1.96, 0.10: 1.645}.get(alpha, 1.96)
                margin = z_crit * se
                return {"success": True, "interval": [m - margin, m + margin], "mean": m, "margin": margin}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"StatisticsAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for statistics operations."""
    return StatisticsAction().execute(context, params)
