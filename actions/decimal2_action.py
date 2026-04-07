"""Decimal action v2 - financial and scientific extensions.

Extended decimal utilities including currency formatting,
statistical functions, and engineering calculations.
"""

from __future__ import annotations

from decimal import (
    Context,
    Decimal,
    getcontext,
    localcontext,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
)
from typing import Sequence, Union

__all__ = [
    "currency_format",
    "accounting_format",
    "percentage_format",
    "scientific_format",
    "engineering_format",
    "significant_figures",
    "decimal_moving_average",
    "decimal_exponential_moving_average",
    "decimal_variance",
    "decimal_correlation",
    "decimal_covariance",
    "decimal_percentile",
    "decimal_zscore",
    "decimal_normalize",
    "decimal_range",
    "decimal_linreg",
    "decimal_poly_fit",
    "compound_interest_advanced",
    "effective_rate",
    "nominal_to_effective",
    "effective_to_nominal",
    "amortization_schedule",
    "loan_balance",
    "discount_factor",
    "annuity_factor",
    "perpetuity_value",
    "DecimalVector",
    "DecimalMatrix",
    "DecimalStats2",
]


def currency_format(value: Decimal | int | float, currency: str = "USD", symbol: str = "$", decimals: int = 2) -> str:
    """Format decimal as currency.

    Args:
        value: Value to format.
        currency: Currency code.
        symbol: Currency symbol.
        decimals: Decimal places.

    Returns:
        Formatted currency string.
    """
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    sign, digits, exp = d.as_tuple()
    int_part = "".join(str(d) for d in digits[:exp])
    dec_part = "".join(str(d) for d in digits[exp:exp + decimals]) if exp < 0 else ""
    while len(dec_part) < decimals:
        dec_part += "0"
    result = f"{symbol}{int_part}.{dec_part}"
    if sign:
        result = f"({result})"
    return result


def accounting_format(value: Decimal | int | float, symbol: str = "$", decimals: int = 2) -> str:
    """Format in accounting style (parentheses for negatives).

    Args:
        value: Value to format.
        symbol: Currency symbol.
        decimals: Decimal places.

    Returns:
        Accounting formatted string.
    """
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    sign, digits, exp = d.as_tuple()
    int_part = "".join(str(d) for d in digits[:exp]) if exp > 0 else "0"
    dec_part = "".join(str(d) for d in digits[exp:exp + decimals]) if exp < 0 else ""
    while len(dec_part) < decimals:
        dec_part += "0"
    result = f"{symbol}{int_part}.{dec_part}"
    if sign:
        result = f"({result})"
    return result


def percentage_format(value: Decimal | int | float, decimals: int = 2) -> str:
    """Format as percentage.

    Args:
        value: Value (e.g., 0.05 for 5%).
        decimals: Decimal places.

    Returns:
        Formatted percentage string.
    """
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    return f"{float(d) * 100:.{decimals}f}%"


def scientific_format(value: Decimal | int | float, sigfigs: int = 6) -> str:
    """Format in scientific notation.

    Args:
        value: Value to format.
        sigfigs: Significant figures.

    Returns:
        Scientific notation string.
    """
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    f = float(d)
    return f"{f:.{sigfigs}e}"


def engineering_format(value: Decimal | int | float, decimals: int = 3) -> str:
    """Format in engineering notation.

    Args:
        value: Value to format.
        decimals: Decimal places.

    Returns:
        Engineering notation string.
    """
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    f = float(d)
    if f == 0:
        return "0"
    import math
    exp = int(math.floor(math.log10(abs(f))))
    eng_exp = exp - (exp % 3)
    mantissa = f / (10 ** eng_exp)
    return f"{mantissa:.{decimals}f}e{eng_exp}"


def significant_figures(value: Decimal | int | float, sigfigs: int) -> Decimal:
    """Round to significant figures.

    Args:
        value: Value to round.
        sigfigs: Number of significant figures.

    Returns:
        Rounded Decimal.
    """
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    if d == 0:
        return d
    sign, digits, exp = d.as_tuple()
    n = len(digits)
    shift = n - sigfigs
    if shift > 0:
        new_digits = digits[:sigfigs] + (0,) * shift
    else:
        new_digits = digits + (0,) * (-shift)
    return Decimal((sign, new_digits[:sigfigs], exp + shift))


def decimal_moving_average(values: Sequence[Decimal | int | float], window: int) -> list[Decimal]:
    """Calculate simple moving average.

    Args:
        values: Sequence of values.
        window: Window size.

    Returns:
        List of moving averages.
    """
    vals = [Decimal(str(v)) for v in values]
    if window < 1:
        raise ValueError("Window must be >= 1")
    result = []
    for i in range(len(vals) - window + 1):
        window_vals = vals[i:i + window]
        avg = sum(window_vals) / window
        result.append(avg)
    return result


def decimal_exponential_moving_average(values: Sequence[Decimal | int | float], alpha: float = 0.3) -> list[Decimal]:
    """Calculate exponential moving average.

    Args:
        values: Sequence of values.
        alpha: Smoothing factor (0 < alpha <= 1).

    Returns:
        List of EMAs.
    """
    if not (0 < alpha <= 1):
        raise ValueError("Alpha must be in (0, 1]")
    vals = [Decimal(str(v)) for v in values]
    result = [vals[0]]
    for v in vals[1:]:
        ema = alpha * v + (1 - alpha) * result[-1]
        result.append(ema)
    return result


def decimal_variance(values: Sequence[Decimal | int | float], population: bool = True) -> Decimal:
    """Calculate variance.

    Args:
        values: Sequence of values.
        population: If True, population variance; else sample.

    Returns:
        Variance.
    """
    vals = [Decimal(str(v)) for v in values]
    n = len(vals)
    mean = sum(vals) / n
    squared_diffs = [(v - mean) ** 2 for v in vals]
    divisor = n if population else n - 1
    if divisor == 0:
        raise ValueError("Need at least 1 value for population or 2 for sample")
    return sum(squared_diffs) / divisor


def decimal_stddev(values: Sequence[Decimal | int | float], population: bool = True) -> Decimal:
    """Calculate standard deviation."""
    from math import sqrt
    var = decimal_variance(values, population)
    return Decimal(str(sqrt(float(var))))


def decimal_correlation(x_vals: Sequence[Decimal | int | float], y_vals: Sequence[Decimal | int | float]) -> Decimal:
    """Calculate Pearson correlation coefficient.

    Args:
        x_vals: X values.
        y_vals: Y values.

    Returns:
        Correlation coefficient (-1 to 1).
    """
    if len(x_vals) != len(y_vals):
        raise ValueError("Sequences must have same length")
    x = [Decimal(str(v)) for v in x_vals]
    y = [Decimal(str(v)) for v in y_vals]
    n = len(x)
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    numerator = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    x_var = sum((xi - x_mean) ** 2 for xi in x)
    y_var = sum((yi - y_mean) ** 2 for yi in y)
    denom = (x_var * y_var) ** Decimal("0.5")
    if denom == 0:
        raise ValueError("Variance is zero")
    return numerator / denom


def decimal_covariance(x_vals: Sequence[Decimal | int | float], y_vals: Sequence[Decimal | int | float], population: bool = True) -> Decimal:
    """Calculate covariance."""
    if len(x_vals) != len(y_vals):
        raise ValueError("Sequences must have same length")
    x = [Decimal(str(v)) for v in x_vals]
    y = [Decimal(str(v)) for v in y_vals]
    n = len(x)
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    cov = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    divisor = n if population else n - 1
    return cov / divisor


def decimal_percentile(values: Sequence[Decimal | int | float], percentile: float) -> Decimal:
    """Calculate percentile value."""
    vals = sorted(Decimal(str(v)) for v in values)
    if not (0 <= percentile <= 100):
        raise ValueError("Percentile must be 0-100")
    n = len(vals)
    k = (n - 1) * percentile / 100
    f = int(k)
    c = f + 1 if f + 1 < n else f
    return vals[f] + (vals[c] - vals[f]) * Decimal(str(k - f))


def decimal_zscore(value: Decimal | int | float, values: Sequence[Decimal | int | float]) -> Decimal:
    """Calculate z-score.

    Args:
        value: Value to score.
        values: Population values.

    Returns:
        Z-score.
    """
    v = Decimal(str(value))
    vals = [Decimal(str(x)) for x in values]
    mean = sum(vals) / len(vals)
    std = decimal_stddev(vals, population=True)
    if std == 0:
        raise ValueError("Standard deviation is zero")
    return (v - mean) / std


def decimal_normalize(value: Decimal | int | float, min_val: Decimal | int | float, max_val: Decimal | int | float) -> Decimal:
    """Normalize value to 0-1 range.

    Args:
        value: Value to normalize.
        min_val: Minimum of range.
        max_val: Maximum of range.

    Returns:
        Normalized value.
    """
    v = Decimal(str(value))
    lo = Decimal(str(min_val))
    hi = Decimal(str(max_val))
    if hi == lo:
        raise ValueError("min and max must be different")
    return (v - lo) / (hi - lo)


def decimal_range(start: Decimal | int | float, stop: Decimal | int | float, step: Decimal | int | float) -> list[Decimal]:
    """Generate range of decimal values.

    Args:
        start: Start value.
        stop: Stop value.
        step: Step size.

    Returns:
        List of Decimals.
    """
    result = []
    current = Decimal(str(start))
    stop_d = Decimal(str(stop))
    step_d = Decimal(str(step))
    if step_d == 0:
        raise ValueError("Step cannot be 0")
    if step_d > 0:
        while current < stop_d:
            result.append(current)
            current += step_d
    else:
        while current > stop_d:
            result.append(current)
            current += step_d
    return result


def decimal_linreg(x_vals: Sequence[Decimal | int | float], y_vals: Sequence[Decimal | int | float]) -> tuple[Decimal, Decimal]:
    """Linear regression y = mx + b.

    Args:
        x_vals: X values.
        y_vals: Y values.

    Returns:
        Tuple of (slope, intercept).
    """
    if len(x_vals) != len(y_vals):
        raise ValueError("Sequences must have same length")
    n = len(x_vals)
    x = [Decimal(str(v)) for v in x_vals]
    y = [Decimal(str(v)) for v in y_vals]
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi ** 2 for xi in x)
    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        raise ValueError("Cannot fit line")
    m = (n * sum_xy - sum_x * sum_y) / denom
    b = (sum_y - m * sum_x) / n
    return (m, b)


def decimal_poly_fit(x_vals: Sequence[Decimal | int | float], y_vals: Sequence[Decimal | int | float], degree: int = 2) -> list[Decimal]:
    """Polynomial fit using least squares.

    Args:
        x_vals: X values.
        y_vals: Y values.
        degree: Polynomial degree.

    Returns:
        List of coefficients.
    """
    from numpy import dot
    if len(x_vals) != len(y_vals):
        raise ValueError("Sequences must have same length")
    x = [Decimal(str(v)) for v in x_vals]
    y = [Decimal(str(v)) for v in y_vals]
    n = len(x)
    powers = [[xi ** p for xi in x] for p in range(degree + 1)]
    matrix = [[sum(powers[i][k] * powers[j][k] for k in range(n)) for j in range(degree + 1)] for i in range(degree + 1)]
    rhs = [sum(y[k] * powers[i][k] for k in range(n)) for i in range(degree + 1)]
    return rhs


def effective_rate(nominal_rate: Decimal | int | float, periods: int) -> Decimal:
    """Calculate effective interest rate from nominal.

    Args:
        nominal_rate: Nominal annual rate (decimal).
        periods: Compounding periods per year.

    Returns:
        Effective annual rate.
    """
    r = Decimal(str(nominal_rate))
    n = Decimal(str(periods))
    return (1 + r / n) ** n - 1


def nominal_to_effective(effective_rate: Decimal | int | float, periods: int) -> Decimal:
    """Convert effective rate to nominal rate.

    Args:
        effective_rate: Effective annual rate.
        periods: Compounding periods per year.

    Returns:
        Nominal annual rate.
    """
    ear = Decimal(str(effective_rate))
    n = Decimal(str(periods))
    return n * ((1 + ear) ** (Decimal(1) / n) - 1)


def effective_to_nominal(effective_rate: Decimal | int | float, periods: int) -> Decimal:
    """Alias for nominal_to_effective."""
    return nominal_to_effective(effective_rate, periods)


def amortization_schedule(principal: Decimal | int | float, annual_rate: Decimal | int | float, years: int, payments_per_year: int = 12) -> list[dict]:
    """Generate amortization schedule.

    Args:
        principal: Loan amount.
        annual_rate: Annual interest rate.
        years: Loan term in years.
        payments_per_year: Payments per year.

    Returns:
        List of payment records.
    """
    p = Decimal(str(principal))
    r = Decimal(str(annual_rate)) / payments_per_year
    n = years * payments_per_year
    if r == 0:
        payment = p / n
    else:
        payment = (p * r * (1 + r) ** n) / ((1 + r) ** n - 1)
    schedule = []
    balance = p
    for i in range(1, n + 1):
        interest = balance * r
        principal_paid = payment - interest
        balance -= principal_paid
        schedule.append({
            "payment": i,
            "payment_amount": payment,
            "principal": principal_paid,
            "interest": interest,
            "balance": max(balance, Decimal(0)),
        })
    return schedule


def loan_balance(principal: Decimal | int | float, annual_rate: Decimal | int | float, payments_made: int, total_payments: int) -> Decimal:
    """Calculate remaining loan balance.

    Args:
        principal: Original loan amount.
        annual_rate: Annual interest rate.
        payments_made: Number of payments made.
        total_payments: Total number of payments.

    Returns:
        Remaining balance.
    """
    p = Decimal(str(principal))
    r = Decimal(str(annual_rate)) / 12
    n = total_payments
    k = payments_made
    if r == 0:
        return p * (n - k) / n
    return p * ((1 + r) ** n - (1 + r) ** k) / ((1 + r) ** n - 1)


def discount_factor(rate: Decimal | int | float, periods: int) -> Decimal:
    """Calculate present value discount factor.

    Args:
        rate: Discount rate per period.
        periods: Number of periods.

    Returns:
        Discount factor.
    """
    r = Decimal(str(rate))
    if r == 0:
        return Decimal(1)
    return Decimal(1) / (1 + r) ** periods


def annuity_factor(rate: Decimal | int | float, periods: int) -> Decimal:
    """Calculate present value annuity factor.

    Args:
        rate: Rate per period.
        periods: Number of periods.

    Returns:
        Annuity factor.
    """
    r = Decimal(str(rate))
    n = Decimal(str(periods))
    if r == 0:
        return n
    return (1 - (1 + r) ** (-n)) / r


def perpetuity_value(payment: Decimal | int | float, rate: Decimal | int | float) -> Decimal:
    """Calculate present value of perpetuity.

    Args:
        payment: Payment per period.
        rate: Discount rate per period.

    Returns:
        Present value.
    """
    p = Decimal(str(payment))
    r = Decimal(str(rate))
    if r == 0:
        raise ValueError("Rate cannot be 0 for perpetuity")
    return p / r


class DecimalVector:
    """1D vector of decimals with operations."""

    def __init__(self, values: Sequence[Decimal | int | float]) -> None:
        self._data = [Decimal(str(v)) for v in values]

    def dot(self, other: DecimalVector) -> Decimal:
        """Dot product."""
        return sum(a * b for a, b in zip(self._data, other._data))

    def magnitude(self) -> Decimal:
        """Vector magnitude."""
        from math import sqrt
        return Decimal(str(sqrt(float(sum(x ** 2 for x in self._data)))))


class DecimalMatrix:
    """2D matrix of decimals."""

    def __init__(self, data: Sequence[Sequence[Decimal | int | float]]) -> None:
        self._data = [[Decimal(str(v)) for v in row] for row in data]
        self._rows = len(data)
        self._cols = len(data[0]) if data else 0

    def transpose(self) -> DecimalMatrix:
        """Transpose matrix."""
        result = [[self._data[r][c] for r in range(self._rows)] for c in range(self._cols)]
        return DecimalMatrix(result)


class DecimalStats2:
    """Extended decimal statistics."""

    def __init__(self) -> None:
        self._values: list[Decimal] = []

    def add(self, value: Decimal | int | float | str) -> None:
        """Add a value."""
        d = Decimal(str(value)) if not isinstance(value, Decimal) else value
        self._values.append(d)

    def mean(self) -> Decimal:
        if not self._values:
            raise ValueError("No values")
        return sum(self._values) / len(self._values)

    def median(self) -> Decimal:
        return decimal_percentile(self._values, 50)

    def mode(self) -> Decimal:
        from collections import Counter
        counts = Counter(self._values)
        return max(counts, key=counts.get)

    def variance(self) -> Decimal:
        return decimal_variance(self._values, population=True)

    def stddev(self) -> Decimal:
        return decimal_stddev(self._values, population=True)

    def skewness(self) -> Decimal:
        if len(self._values) < 3:
            raise ValueError("Need at least 3 values")
        mean = self.mean()
        std = self.stddev()
        if std == 0:
            raise ValueError("Stddev is zero")
        n = len(self._values)
        skew = sum(((v - mean) / std) ** 3 for v in self._values) / n
        return skew

    def kurtosis(self) -> Decimal:
        if len(self._values) < 4:
            raise ValueError("Need at least 4 values")
        mean = self.mean()
        std = self.stddev()
        if std == 0:
            raise ValueError("Stddev is zero")
        n = len(self._values)
        kurt = sum(((v - mean) / std) ** 4 for v in self._values) / n - 3
        return kurt

    def count(self) -> int:
        return len(self._values)

    def min(self) -> Decimal:
        return min(self._values)

    def max(self) -> Decimal:
        return max(self._values)

    def summary(self) -> dict:
        return {
            "count": self.count(),
            "mean": self.mean(),
            "median": self.median(),
            "stddev": self.stddev(),
            "min": self.min(),
            "max": self.max(),
            "skewness": self.skewness(),
            "kurtosis": self.kurtosis(),
        }
