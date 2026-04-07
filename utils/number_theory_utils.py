"""
Number theory utilities for primality testing, factorization, and arithmetic.

Provides efficient implementations of fundamental number theory algorithms
including sieve of Eratosthenes, GCD, LCM, modular arithmetic, and more.
"""

from __future__ import annotations

import math
from typing import Iterator, NamedTuple


class FactorizationResult(NamedTuple):
    """Result of integer factorization."""
    n: int
    factors: list[tuple[int, int]]


def is_prime(n: int) -> bool:
    """
    Check if a number is prime using trial division.

    Args:
        n: Integer to test (n >= 0)

    Returns:
        True if n is prime, False otherwise

    Example:
        >>> is_prime(17)
        True
        >>> is_prime(1)
        False
        >>> is_prime(97)
        True
    """
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    if n < 9:
        return True
    if n % 3 == 0:
        return False
    limit = int(math.isqrt(n))
    i = 5
    while i <= limit:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def sieve_of_eratosthenes(limit: int) -> list[int]:
    """
    Generate all primes up to limit using the Sieve of Eratosthenes.

    Args:
        limit: Upper bound (inclusive)

    Returns:
        List of all primes <= limit

    Example:
        >>> sieve_of_eratosthenes(30)
        [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    """
    if limit < 2:
        return []
    is_prime_arr = [True] * (limit + 1)
    is_prime_arr[0] = is_prime_arr[1] = False

    for i in range(2, int(limit ** 0.5) + 1):
        if is_prime_arr[i]:
            for j in range(i * i, limit + 1, i):
                is_prime_arr[j] = False

    return [i for i, val in enumerate(is_prime_arr) if val]


def gcd(a: int, b: int) -> int:
    """
    Compute the greatest common divisor using Euclidean algorithm.

    Args:
        a: First integer
        b: Second integer

    Returns:
        GCD(a, b)

    Example:
        >>> gcd(48, 18)
        6
        >>> gcd(17, 13)
        1
    """
    a, b = abs(a), abs(b)
    while b:
        a, b = b, a % b
    return a


def gcd_extended(a: int, b: int) -> tuple[int, int, int]:
    """
    Extended Euclidean algorithm: returns gcd and coefficients.

    Finds integers x, y such that ax + by = gcd(a, b).

    Args:
        a: First integer
        b: Second integer

    Returns:
        Tuple of (gcd, x, y)

    Example:
        >>> g, x, y = gcd_extended(35, 15)
        >>> g == 35*x + 15*y
        True
    """
    if b == 0:
        return (abs(a), 1 if a > 0 else -1, 0)
    x0, x1, y0, y1 = 1, 0, 0, 1
    while b != 0:
        q, a, b = a // b, b, a % b
        x0, x1 = x1, x0 - q * x1
        y0, y1 = y1, y0 - q * y1
    return (a, x0, y0)


def lcm(a: int, b: int) -> int:
    """
    Compute the least common multiple.

    Args:
        a: First integer
        b: Second integer

    Returns:
        LCM(a, b), or 0 if either is 0

    Example:
        >>> lcm(12, 18)
        36
    """
    if a == 0 or b == 0:
        return 0
    return abs(a // gcd(a, b) * b)


def prime_factors(n: int) -> FactorizationResult:
    """
    Compute prime factorization of n.

    Args:
        n: Integer to factorize (n >= 2)

    Returns:
        FactorizationResult with n and list of (prime, exponent) pairs

    Example:
        >>> result = prime_factors(360)
        >>> result.factors
        [(2, 3), (3, 2), (5, 1)]
    """
    if n < 2:
        return FactorizationResult(n, [])
    factors: list[tuple[int, int]] = []
    d = 2
    while d * d <= n:
        count = 0
        while n % d == 0:
            n //= d
            count += 1
        if count > 0:
            factors.append((d, count))
        d += 1 if d == 2 else 2
    if n > 1:
        factors.append((n, 1))
    return FactorizationResult(n, factors)


def euler_totient(n: int) -> int:
    """
    Compute Euler's totient function φ(n).

    Counts integers from 1 to n that are coprime with n.

    Args:
        n: Positive integer

    Returns:
        φ(n), count of numbers coprime to n

    Example:
        >>> euler_totient(12)
        4
        >>> euler_totient(7)
        6
    """
    if n <= 0:
        return 0
    result = n
    p = 2
    while p * p <= n:
        if n % p == 0:
            while n % p == 0:
                n //= p
            result -= result // p
        p += 1 if p == 2 else 2
    if n > 1:
        result -= result // n
    return result


def modular_inverse(a: int, m: int) -> int | None:
    """
    Compute modular multiplicative inverse of a modulo m.

    Args:
        a: Number to invert
        m: Modulus

    Returns:
        x such that (a*x) % m == 1, or None if inverse doesn't exist

    Example:
        >>> modular_inverse(3, 11)
        4
        >>> (3 * 4) % 11
        1
    """
    g, x, _ = gcd_extended(a, m)
    if g != 1:
        return None
    return x % m


def fast_pow(base: int, exp: int, mod: int = 0) -> int:
    """
    Compute base^exponent efficiently using binary exponentiation.

    Args:
        base: Base number
        exp: Exponent (non-negative)
        mod: Optional modulus

    Returns:
        base^exp or base^exp % mod if mod > 0

    Example:
        >>> fast_pow(2, 10)
        1024
        >>> fast_pow(2, 10, 1000)
        24
    """
    if exp < 0:
        raise ValueError("Exponent must be non-negative")
    result = 1
    base %= mod if mod else base
    while exp:
        if exp & 1:
            result = (result * base) % mod if mod else result * base
        exp >>= 1
        base = (base * base) % mod if mod else base * base
    return result


def binomial_coefficient(n: int, k: int) -> int:
    """
    Compute binomial coefficient C(n, k) = n! / (k! * (n-k)!).

    Args:
        n: Total items
        k: Items to choose

    Returns:
        C(n, k)

    Example:
        >>> binomial_coefficient(10, 3)
        120
    """
    if k < 0 or k > n:
        return 0
    if k == 0 or k == n:
        return 1
    k = min(k, n - k)
    result = 1
    for i in range(k):
        result = result * (n - i) // (i + 1)
    return result


def catalan_number(n: int) -> int:
    """
    Compute the nth Catalan number.

    Catalan numbers: C(0)=1, C(1)=1, C(2)=2, C(3)=5, ...

    Args:
        n: Index (non-negative)

    Returns:
        nth Catalan number

    Example:
        >>> catalan_number(5)
        42
    """
    return binomial_coefficient(2 * n, n) // (n + 1)
