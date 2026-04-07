"""
Number theory and arithmetic utilities for automation actions.

Provides primality testing, factorization, GCD/LCM, number conversion,
and arithmetic sequence utilities.
"""

from __future__ import annotations

import math
import random
from typing import Callable, Iterator, NamedTuple


class Fraction(NamedTuple):
    """Immutable fraction represented as numerator/denominator."""
    numerator: int
    denominator: int

    def __new__(cls, numerator: int, denominator: int) -> "Fraction":
        if denominator == 0:
            raise ZeroDivisionError("Denominator cannot be zero")
        if denominator < 0:
            numerator = -numerator
            denominator = -denominator
        g = math.gcd(numerator, denominator)
        return super().__new__(cls, numerator // g, denominator // g)

    @property
    def value(self) -> float:
        return self.numerator / self.denominator

    def __str__(self) -> str:
        return f"{self.numerator}/{self.denominator}"

    def __repr__(self) -> str:
        return f"Fraction({self.numerator}, {self.denominator})"


def is_prime(n: int) -> bool:
    """Test if n is prime."""
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
    """Generate all primes up to limit using Sieve of Eratosthenes."""
    if limit < 2:
        return []
    is_prime = [True] * (limit + 1)
    is_prime[0] = is_prime[1] = False
    for i in range(2, int(limit**0.5) + 1):
        if is_prime[i]:
            for j in range(i * i, limit + 1, i):
                is_prime[j] = False
    return [i for i, p in enumerate(is_prime) if p]


def factorize(n: int) -> dict[int, int]:
    """Return prime factorization as {prime: exponent}."""
    if n == 0:
        raise ValueError("Cannot factorize zero")
    if n == 1:
        return {}
    factors: dict[int, int] = {}
    n_abs = abs(n)
    d = 2
    while d * d <= n_abs:
        while n_abs % d == 0:
            factors[d] = factors.get(d, 0) + 1
            n_abs //= d
        d += 1
    if n_abs > 1:
        factors[n_abs] = factors.get(n_abs, 0) + 1
    return factors


def gcd(a: int, b: int) -> int:
    """Compute greatest common divisor using Euclidean algorithm."""
    a = abs(a)
    b = abs(b)
    while b:
        a, b = b, a % b
    return a


def lcm(a: int, b: int) -> int:
    """Compute least common multiple."""
    if a == 0 or b == 0:
        return 0
    return abs(a * b) // gcd(a, b)


def extended_gcd(a: int, b: int) -> tuple[int, int, int]:
    """Extended Euclidean algorithm. Returns (g, x, y) where ax + by = g."""
    if a == 0:
        return b, 0, 1
    g, x1, y1 = extended_gcd(b % a, a)
    return g, y1 - (b // a) * x1, x1


def chinese_remainder(remainders: list[int], moduli: list[int]) -> int | None:
    """Solve system of congruences using Chinese Remainder Theorem."""
    if len(remainders) != len(moduli):
        raise ValueError("remainders and moduli must have same length")
    for m in moduli:
        if m <= 0:
            raise ValueError("All moduli must be positive")
    for i, m_i in enumerate(moduli):
        for j, m_j in enumerate(moduli):
            if i != j and gcd(m_i, m_j) != 1:
                return None
    result = 0
    M = 1
    for m in moduli:
        M *= m
    for r, m in zip(remainders, moduli):
        M_i = M // m
        _, inv, _ = extended_gcd(M_i, m)
        result += r * M_i * inv
    return result % M


def modular_inverse(a: int, m: int) -> int | None:
    """Compute modular multiplicative inverse of a modulo m. Returns None if doesn't exist."""
    g, x, _ = extended_gcd(a % m, m)
    if g != 1:
        return None
    return x % m


def is_power_of(n: int, base: int) -> bool:
    """Test if n is a power of base."""
    if n <= 0 or base <= 0:
        return False
    if n == 1:
        return True
    if base == 1:
        return n == 1
    exp = 0
    temp = n
    while temp % base == 0:
        temp //= base
        exp += 1
    return temp == 1


def next_power_of(n: int, base: int) -> int:
    """Find the smallest power of base >= n."""
    if n <= 1:
        return 1
    if base <= 1:
        raise ValueError("base must be > 1")
    power = 1
    while power < n:
        power *= base
    return power


def int_sqrt(n: int) -> int:
    """Integer square root (floor of sqrt(n))."""
    if n < 0:
        raise ValueError("Cannot compute square root of negative number")
    return int(math.isqrt(n))


def is_square(n: int) -> bool:
    """Test if n is a perfect square."""
    if n < 0:
        return False
    r = int_sqrt(n)
    return r * r == n


def is_cube(n: int) -> bool:
    """Test if n is a perfect cube."""
    c = round(n ** (1 / 3))
    return c * c * c == n


def fibonacci(n: int) -> int:
    """Return nth Fibonacci number (0-indexed)."""
    if n < 0:
        raise ValueError("Fibonacci index cannot be negative")
    if n == 0:
        return 0
    if n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def fibonacci_sequence(limit: int) -> Iterator[int]:
    """Generate Fibonacci numbers up to limit."""
    a, b = 0, 1
    while a <= limit:
        yield a
        a, b = b, a + b


def collatz_sequence(n: int) -> list[int]:
    """Generate Collatz sequence starting from n."""
    if n <= 0:
        raise ValueError("Collatz sequence requires positive integer")
    seq = [n]
    while n != 1:
        if n % 2 == 0:
            n //= 2
        else:
            n = 3 * n + 1
        seq.append(n)
    return seq


def collatz_length(n: int) -> int:
    """Return length of Collatz sequence starting from n."""
    length = 1
    while n != 1:
        if n % 2 == 0:
            n //= 2
        else:
            n = 3 * n + 1
        length += 1
    return length


def sum_of_digits(n: int) -> int:
    """Return sum of decimal digits."""
    return sum(int(c) for c in str(abs(n)))


def product_of_digits(n: int) -> int:
    """Return product of decimal digits."""
    n = abs(n)
    result = 1
    for c in str(n):
        result *= int(c)
    return result


def is_palindrome(n: int, base: int = 10) -> bool:
    """Test if n is palindromic in given base."""
    if n == 0:
        return True
    negative = n < 0
    n = abs(n)
    digits: list[int] = []
    while n:
        digits.append(n % base)
        n //= base
    return digits == digits[::-1]


def format_number(n: int, width: int = 0, fill_char: str = "0") -> str:
    """Format number as string with zero-padding."""
    s = str(n)
    if width > 0 and len(s) < width:
        s = fill_char * (width - len(s)) + s
    return s


def clamp(value: int, min_val: int, max_val: int) -> int:
    """Clamp value to range [min_val, max_val]."""
    return max(min_val, min(max_val, value))


def lerp(a: int, b: int, t: float) -> float:
    """Linear interpolation between a and b."""
    return a + (b - a) * t


def moving_average(values: list[int | float], window: int) -> list[float]:
    """Compute moving average with given window size."""
    if window <= 0:
        raise ValueError("Window size must be positive")
    if len(values) < window:
        return []
    result: list[float] = []
    window_sum = sum(values[:window])
    result.append(window_sum / window)
    for i in range(window, len(values)):
        window_sum += values[i] - values[i - window]
        result.append(window_sum / window)
    return result


def prime_sieve_segmented(limit: int, segment_size: int = 32768) -> Iterator[int]:
    """Segmented sieve for memory efficiency on large limits."""
    if limit < 2:
        return
    yield 2
    is_prime = [True] * segment_size
    sieve_size = int_sqrt(limit) + 1
    base_primes = sieve_of_eratosthenes(sieve_size)
    for low in range(3, limit + 1, segment_size):
        high = min(low + segment_size - 1, limit)
        is_prime = [True] * (high - low + 1)
        for p in base_primes:
            start = ((low + p - 1) // p) * p
            if start == p:
                start = p * p
            for j in range(start, high + 1, p):
                if j >= low:
                    is_prime[j - low] = False
        for i in range(low, high + 1, 2):
            if is_prime[i - low]:
                yield i
