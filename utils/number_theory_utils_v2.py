"""
Number theory utilities v2 — advanced algorithms.

Companion to number_theory_utils.py. Adds continued fractions,
quadratic residues, and Diophantine equation solvers.
"""

from __future__ import annotations

import math


def continued_fraction_sqrt(n: int) -> tuple[list[int], list[int]]:
    """
    Compute continued fraction expansion of sqrt(n).

    Args:
        n: Non-square integer

    Returns:
        Tuple of (periodic part, non-repeating part)

    Example:
        >>> cf, pre = continued_fraction_sqrt(23)
        >>> len(cf) > 0
        True
    """
    if math.isqrt(n) ** 2 == n:
        return [], [math.isqrt(n)]
    m, d, a0 = 0, 1, math.isqrt(n)
    a = a0
    periodic = []
    seen = {}
    i = 0
    while True:
        key = (m, d)
        if key in seen:
            break
        seen[key] = i
        m = d * a - m
        d = (n - m * m) // d
        a = (a0 + m) // d
        periodic.append(a)
        i += 1
    return periodic, [a0]


def is_quadratic_residue(a: int, p: int) -> bool:
    """
    Check if a is a quadratic residue modulo p (Legendre symbol).

    Args:
        a: Number to test
        p: Prime modulus

    Returns:
        True if a is a quadratic residue mod p
    """
    if a % p == 0:
        return True
    a = a % p
    exp = (p - 1) // 2
    result = pow(a, exp, p)
    return result == 1


def tonelli_shanks(a: int, p: int) -> int | None:
    """
    Find square root of a modulo p using Tonelli-Shanks algorithm.

    Args:
        a: Number whose square root to find
        p: Prime modulus

    Returns:
        One of the square roots, or None if no root exists
    """
    if a == 0:
        return 0
    if p == 2:
        return a % 2
    if not is_quadratic_residue(a, p):
        return None
    if p % 4 == 3:
        return pow(a, (p + 1) // 4, p)
    q, s = p - 1, 0
    while q % 2 == 0:
        q //= 2
        s += 1
    z = 2
    while is_quadratic_residue(z, p):
        z += 1
    c = pow(z, q, p)
    x = pow(a, (q + 1) // 2, p)
    t = pow(a, q, p)
    m = s
    while t != 1:
        i = 1
        t2 = pow(t, 2, p)
        while i < m:
            if t2 == 1:
                break
            t2 = pow(t2, 2, p)
            i += 1
        b = pow(c, 1 << (m - i - 1), p)
        x = x * b % p
        t = t * b * b % p
        c = b * b % p
        m = i
    return x


def chinese_remainder(remainders: list[int], moduli: list[int]) -> int:
    """
    Solve system of congruences using Chinese Remainder Theorem.

    Args:
        remainders: List of remainders (r_i)
        moduli: List of moduli (n_i), pairwise coprime

    Returns:
        Smallest non-negative solution x

    Example:
        >>> chinese_remainder([2, 3, 1], [3, 5, 7])
        52
    """
    n = 1
    for mod in moduli:
        n *= mod
    result = 0
    for r_i, n_i in zip(remainders, moduli):
        n_i_bar = n // n_i
        s = pow(n_i_bar, -1, n_i)
        result += r_i * n_i_bar * s
    return result % n


def diophantine_linear(a: int, b: int, c: int) -> tuple[int, int] | None:
    """
    Solve ax + by = c for integers x, y.

    Args:
        a: Coefficient of x
        b: Coefficient of y
        c: Target value

    Returns:
        Tuple of (x, y) solution, or None if no solution
    """
    from utils.number_theory_utils import gcd_extended
    g, x0, y0 = gcd_extended(a, b)
    if c % g != 0:
        return None
    scale = c // g
    return x0 * scale, y0 * scale


def mobius_function(n: int) -> int:
    """
    Compute the Mobius function mu(n).

    Args:
        n: Positive integer

    Returns:
        mu(n): 0 if n has squared prime factor, 1 if n is square-free with even prime count, -1 if odd
    """
    if n == 1:
        return 1
    from utils.number_theory_utils import prime_factors
    factors = prime_factors(n).factors
    for p, e in factors:
        if e > 1:
            return 0
    return -1 if len(factors) % 2 == 1 else 1


def mertens_function(n: int) -> int:
    """
    Compute Mertens function M(n) = sum_{k=1}^{n} mu(k).

    Args:
        n: Upper bound

    Returns:
        Mertens value
    """
    return sum(mobius_function(k) for k in range(1, n + 1))


def is_carmichael(n: int) -> bool:
    """
    Check if n is a Carmichael number (pseudoprime for all bases).

    Args:
        n: Number to test

    Returns:
        True if n is Carmichael
    """
    if n < 2:
        return False
    if n % 2 == 0:
        return False
    from utils.number_theory_utils import is_prime
    d = 3
    while d * d <= n:
        if n % d == 0:
            if (n - 1) % (d - 1) != 0:
                return False
            while n % d == 0:
                n //= d
        d += 2
    if n > 1 and (n - 1) % (n - 1) != 0:
        return False
    return True


def primitive_root(p: int) -> int | None:
    """
    Find a primitive root modulo p (p must be prime).

    Args:
        p: Prime number

    Returns:
        A primitive root, or None
    """
    from utils.number_theory_utils import is_prime
    if not is_prime(p):
        return None
    if p == 2:
        return 1
    from utils.number_theory_utils import euler_totient
    phi = p - 1
    factors = list(set(pf[0] for pf in [(2, 0)] + [(i, 0) for i in range(2, int(math.sqrt(phi)) + 1)]))
    for g in range(2, p):
        ok = True
        for q in [2, 3, 5, 7, 11, 13, 17, 19, 23]:
            if q * q > phi:
                break
            if phi % q == 0:
                if pow(g, phi // q, p) == 1:
                    ok = False
                    break
        if ok:
            return g
    return None


def discrete_log(a: int, b: int, p: int) -> int | None:
    """
    Solve a^x ≡ b (mod p) for x using baby-step giant-step.

    Args:
        a: Base
        b: Target value
        p: Prime modulus

    Returns:
        x such that a^x ≡ b (mod p), or None
    """
    n = int(math.isqrt(p)) + 1
    table = {pow(a, j, p): j for j in range(n)}
    an = pow(a, -n, p)
    for i in range(n):
        val = (b * pow(an, i, p)) % p
        if val in table:
            return i * n + table[val]
    return None
