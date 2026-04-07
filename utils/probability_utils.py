"""
Probability distributions and statistical utilities.

Provides common probability distributions, random sampling,
entropy, and combinatorics.
"""

from __future__ import annotations

import math
import random
from typing import Callable


def factorial(n: int) -> int:
    """Compute n! for non-negative integers."""
    if n < 0:
        raise ValueError("Factorial undefined for negative integers")
    if n <= 1:
        return 1
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


def binomial_coefficient(n: int, k: int) -> int:
    """Compute C(n, k) = n! / (k! * (n-k)!)."""
    if k < 0 or k > n:
        return 0
    if k == 0 or k == n:
        return 1
    k = min(k, n - k)
    return math.prod(n - i for i in range(k)) // math.prod(i + 1 for i in range(k))


def combinations(n: int, k: int) -> int:
    """Alias for binomial_coefficient."""
    return binomial_coefficient(n, k)


def permutations(n: int, k: int) -> int:
    """Compute P(n, k) = n! / (n-k)!."""
    if k < 0 or k > n:
        return 0
    return math.prod(n - i for i in range(k))


def bernoulli_trial(p: float) -> int:
    """Single Bernoulli trial."""
    return 1 if random.random() < p else 0


def binomial_sample(n: int, p: float) -> int:
    """Binomial distribution sample."""
    return sum(bernoulli_trial(p) for _ in range(n))


def geometric_sample(p: float) -> int:
    """Geometric distribution sample (number of trials until first success)."""
    if p <= 0 or p > 1:
        raise ValueError("p must be in (0, 1]")
    count = 1
    while random.random() >= p:
        count += 1
    return count


def poisson_sample(lam: float) -> int:
    """Poisson distribution sample using inverse transform."""
    if lam <= 0:
        raise ValueError("lambda must be positive")
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def normal_sample(mu: float = 0.0, sigma: float = 1.0) -> float:
    """Normal (Gaussian) distribution sample using Box-Muller transform."""
    u1 = random.random()
    u2 = random.random()
    z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    return mu + sigma * z


def exponential_sample(lam: float) -> float:
    """Exponential distribution sample."""
    if lam <= 0:
        raise ValueError("lambda must be positive")
    return -math.log(random.random()) / lam


def beta_sample(alpha: float, beta: float) -> float:
    """Beta distribution sample using Jöhnk's algorithm."""
    if alpha <= 0 or beta <= 0:
        raise ValueError("alpha and beta must be positive")
    while True:
        u = random.random()
        v = random.random()
        w = u * v
        x = u ** (1.0 / alpha)
        y = v ** (1.0 / beta)
        if x + y <= 1:
            z = w / (x + y)
            if z <= 1:
                return z
            if math.log(z) >= (alpha - 1) * math.log(x) + (beta - 1) * math.log(y):
                return z


def gamma_sample(shape: float, scale: float = 1.0) -> float:
    """Gamma distribution sample (shape > 0, scale > 0)."""
    if shape <= 0 or scale <= 0:
        raise ValueError("shape and scale must be positive")
    if shape < 1:
        return gamma_sample(shape + 1, scale) * random.random() ** (1.0 / shape)
    d = shape - 1.0 / 3.0
    c = 1.0 / math.sqrt(9.0 * d)
    while True:
        x = normal_sample()
        v = 1.0 + c * x
        while v <= 0:
            x = normal_sample()
            v = 1.0 + c * x
        v = v ** 3
        u = random.random()
        if u < 1 - 0.0331 * (x * x) ** 2:
            return d * v * scale
        if math.log(u) < 0.5 * x * x + d * (1 - v + math.log(v)):
            return d * v * scale


def entropy(probs: list[float], base: float = 2.0) -> float:
    """
    Shannon entropy H = -sum(p * log(p)).

    Args:
        probs: Probability distribution (sum should be 1)
        base: Logarithm base (default 2 for bits)

    Returns:
        Entropy value.
    """
    h = 0.0
    for p in probs:
        if p > 0:
            h -= p * math.log(p) / math.log(base)
    return h


def kl_divergence(p: list[float], q: list[float], base: float = 2.0) -> float:
    """
    KL divergence D(P||Q) = sum(p * log(p/q)).
    """
    d = 0.0
    for pi, qi in zip(p, q):
        if pi > 0:
            if qi <= 0:
                return float("inf")
            d += pi * math.log(pi / qi) / math.log(base)
    return d


def normal_pdf(x: float, mu: float = 0.0, sigma: float = 1.0) -> float:
    """Probability density function of normal distribution."""
    if sigma <= 0:
        raise ValueError("sigma must be positive")
    z = (x - mu) / sigma
    return math.exp(-0.5 * z * z) / (sigma * math.sqrt(2.0 * math.pi))


def normal_cdf(x: float, mu: float = 0.0, sigma: float = 1.0) -> float:
    """Cumulative distribution function of standard normal (approximation)."""
    z = (x - mu) / sigma
    t = 1.0 / (1.0 + 0.2316419 * abs(z))
    p = (
        0.319381530
        + t * (-0.356563782)
        + t * t * (1.781477937)
        + t * t * t * (-1.821255978)
        + t * t * t * t * (1.330274429)
    )
    p = p * math.exp(-z * z / 2.0) / math.sqrt(2.0 * math.pi)
    if z >= 0:
        return 1.0 - t * p
    else:
        return t * p


def uniform_pdf(x: float, a: float = 0.0, b: float = 1.0) -> float:
    """PDF of uniform distribution on [a, b]."""
    return 1.0 / (b - a) if a <= x <= b else 0.0


def weighted_sample(items: list[T], weights: list[float]) -> T:
    """
    Random sample from list with given weights.

    Type: T can be any type.
    """
    if len(items) != len(weights):
        raise ValueError("items and weights must have same length")
    if not items:
        raise ValueError("items cannot be empty")
    total = sum(weights)
    if total <= 0:
        raise ValueError("weights must sum to positive value")
    r = random.random() * total
    cumsum = 0.0
    for item, w in zip(items, weights):
        cumsum += w
        if r <= cumsum:
            return item
    return items[-1]


def monte_carlo_integration(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 10000,
) -> float:
    """
    Monte Carlo integration estimate of ∫f(x)dx from a to b.

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        n: Number of samples

    Returns:
        Approximate integral value.
    """
    if n < 1:
        return 0.0
    samples = [f(a + (b - a) * random.random()) for _ in range(n)]
    return (b - a) * sum(samples) / n


from typing import TypeVar
T = TypeVar("T")
