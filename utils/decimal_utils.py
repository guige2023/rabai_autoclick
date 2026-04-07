"""
Decimal utilities for precise arithmetic.

Provides arbitrary-precision decimal operations
and financial calculation utilities.
"""

from __future__ import annotations

import decimal
import math


decimal.getcontext().prec = 50


def to_decimal(value: int | float | str) -> decimal.Decimal:
    """Convert value to Decimal."""
    return decimal.Decimal(str(value))


def add(a, b) -> decimal.Decimal:
    return to_decimal(a) + to_decimal(b)


def sub(a, b) -> decimal.Decimal:
    return to_decimal(a) - to_decimal(b)


def mul(a, b) -> decimal.Decimal:
    return to_decimal(a) * to_decimal(b)


def div(a, b) -> decimal.Decimal:
    return to_decimal(a) / to_decimal(b)


def sqrt(x) -> decimal.Decimal:
    """Compute square root with high precision."""
    d = to_decimal(x)
    if d < 0:
        raise ValueError("Cannot compute square root of negative number")
    if d == 0:
        return decimal.Decimal(0)
    x = d
    y = (d + 1) / 2
    while y < x:
        x = y
        y = (x + d / x) / 2
    return x


def compound_interest(
    principal: float,
    rate: float,
    n: int,
    t: float,
) -> decimal.Decimal:
    """
    Compound interest: A = P(1 + r/n)^(nt).

    Args:
        principal: Initial amount
        rate: Annual interest rate (e.g., 0.05 for 5%)
        n: Compounding frequency per year
        t: Time in years
    """
    p, r, nt = to_decimal(principal), to_decimal(rate), to_decimal(n * t)
    base = to_decimal(1) + r / to_decimal(n)
    return p * base ** nt


def present_value(
    future_value: float,
    rate: float,
    n: int,
    t: float,
) -> decimal.Decimal:
    """PV = FV / (1 + r/n)^(nt)."""
    return div(future_value, (1 + rate / n) ** (n * t))


def irr(cash_flows: list[float], guess: float = 0.1) -> decimal.Decimal | None:
    """
    Internal Rate of Return using Newton-Raphson.

    Args:
        cash_flows: List of cash flows (first is initial investment, negative)
        guess: Initial guess for IRR

    Returns:
        IRR as decimal, or None if no solution
    """
    def npv(rate: decimal.Decimal) -> decimal.Decimal:
        return sum(to_decimal(cf) / (1 + rate) ** to_decimal(i) for i, cf in enumerate(cash_flows))
    r = to_decimal(guess)
    for _ in range(100):
        f = npv(r)
        df = sum(-i * to_decimal(cf) / (1 + r) ** (i + 1) for i, cf in enumerate(cash_flows))
        if abs(df) < 1e-30:
            break
        r_new = r - f / df
        if abs(r_new - r) < 1e-12:
            return r_new
        r = r_new
    return None


def annuity_future_value(
    payment: float,
    rate: float,
    periods: int,
) -> decimal.Decimal:
    """
    Future value of annuity: FV = PMT * ((1+r)^n - 1) / r.
    """
    r = to_decimal(rate)
    pmt = to_decimal(payment)
    return pmt * ((1 + r) ** to_decimal(periods) - 1) / r


def loan_payment(
    principal: float,
    annual_rate: float,
    years: int,
    periods_per_year: int = 12,
) -> decimal.Decimal:
    """
    Calculate periodic loan payment.

    Args:
        principal: Loan amount
        annual_rate: Annual interest rate
        years: Loan term in years
        periods_per_year: Number of payments per year (default 12 for monthly)
    """
    r = to_decimal(annual_rate) / to_decimal(periods_per_year)
    n = to_decimal(years * periods_per_year)
    p = to_decimal(principal)
    if r == 0:
        return p / n
    return p * r * (1 + r) ** n / ((1 + r) ** n - 1)


def round_decimal(value, decimals: int = 2) -> decimal.Decimal:
    """Round Decimal to specified decimal places."""
    return to_decimal(value).quantize(to_decimal(f"0.{'0' * decimals}"), rounding=decimal.ROUND_HALF_UP)
