"""Data Monte Carlo Action.

Monte Carlo simulation for risk analysis, probability estimation,
and stochastic process modeling.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
import random
import math


@dataclass
class SimulationResult:
    mean: float
    std: float
    median: float
    percentile_5: float
    percentile_95: float
    min: float
    max: float
    confidence_95: Tuple[float, float]
    iterations: int

    def as_dict(self) -> Dict[str, float]:
        return {
            "mean": round(self.mean, 4),
            "std": round(self.std, 4),
            "median": round(self.median, 4),
            "p5": round(self.percentile_5, 4),
            "p95": round(self.percentile_95, 4),
            "min": round(self.min, 4),
            "max": round(self.max, 4),
            "ci95_lower": round(self.confidence_95[0], 4),
            "ci95_upper": round(self.confidence_95[1], 4),
            "iterations": self.iterations,
        }


@dataclass
class ProbabilityEstimate:
    probability: float
    confidence: Tuple[float, float]
    successes: int
    trials: int


class DataMonteCarloAction:
    """Monte Carlo simulation engine."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)

    def simulate(
        self,
        payoff_fn: Callable[[random.Random], float],
        iterations: int = 10000,
        seed: Optional[int] = None,
    ) -> SimulationResult:
        rng = random.Random(seed)
        results: List[float] = []
        for _ in range(iterations):
            results.append(payoff_fn(rng))
        results.sort()
        n = len(results)
        mean = sum(results) / n
        variance = sum((r - mean) ** 2 for r in results) / n
        std = math.sqrt(variance)
        median = results[n // 2]
        p5_idx = int(n * 0.05)
        p95_idx = int(n * 0.95)
        p5 = results[p5_idx]
        p95 = results[p95_idx]
        ci_lower = mean - 1.96 * std / math.sqrt(n)
        ci_upper = mean + 1.96 * std / math.sqrt(n)
        return SimulationResult(
            mean=mean,
            std=std,
            median=median,
            percentile_5=p5,
            percentile_95=p95,
            min=results[0],
            max=results[-1],
            confidence_95=(ci_lower, ci_upper),
            iterations=iterations,
        )

    def estimate_probability(
        self,
        condition_fn: Callable[[random.Random], bool],
        trials: int = 100000,
        confidence_level: float = 0.95,
        seed: Optional[int] = None,
    ) -> ProbabilityEstimate:
        rng = random.Random(seed)
        successes = sum(1 for _ in range(trials) if condition_fn(rng))
        p = successes / trials
        # Wilson score interval
        z = 1.96
        denom = 1 + z * z / trials
        center = p + z * z / (2 * trials)
        margin = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials))
        ci_lower = max(0.0, (center - margin) / denom)
        ci_upper = min(1.0, (center + margin) / denom)
        return ProbabilityEstimate(
            probability=p,
            confidence=(ci_lower, ci_upper),
            successes=successes,
            trials=trials,
        )

    def portfolio_simulation(
        self,
        returns: List[float],
        weights: List[float],
        initial_value: float = 10000.0,
        years: int = 30,
        trials: int = 10000,
    ) -> SimulationResult:
        if len(returns) != len(weights):
            raise ValueError("Returns and weights must have same length")
        mean_return = sum(r * w for r, w in zip(returns, weights))
        variance = sum((r - mean_return) ** 2 * w for r, w in zip(returns, weights))
        std_dev = math.sqrt(variance)
        def payoff_fn(rng: random.Random) -> float:
            value = initial_value
            for _ in range(years):
                annual_return = rng.gauss(mean_return, std_dev)
                value *= 1 + annual_return
            return value
        return self.simulate(payoff_fn, iterations=trials)
