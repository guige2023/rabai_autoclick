"""decimal_action.py - precise decimal arithmetic operations.

Provides high-precision decimal arithmetic for financial calculations,
currency operations, and any scenario where floating-point errors
are unacceptable.
"""

from __future__ import annotations

import decimal
import math
from typing import (
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

__all__ = [
    # Core construction
    "decimal",
    "from_float",
    "from_int",
    "from_str",
    "from_currency",
    "to_float",
    "to_int",
    "to_str",
    "to_currency",
    # Arithmetic
    "add",
    "sub",
    "mul",
    "div",
    "floordiv",
    "mod",
    "pow_",
    "abs_",
    "neg",
    # Comparison
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "compare",
    "is_zero",
    "is_positive",
    "is_negative",
    # Formatting
    "quantize",
    "round_",
    "floor_",
    "ceil_",
    "trunc",
    "format_currency",
    "format_percent",
    "format_scientific",
    # Operations
    "sqrt",
    "ln",
    "log10",
    "exp",
    "min_",
    "max_",
    "sum_",
    "avg",
    "median",
    # Utilities
    "get_precision",
    "set_precision",
    "normalize",
    "split",
    "fractional_part",
    "integer_part",
]

# Module-level precision context
_PRECISION = 28
_CONTEXT = decimal.Context(precision=_PRECISION)


def _ctx() -> decimal.Context:
    """Get current decimal context."""
    return decimal.Context(precision=_PRECISION)


def _d(value: Union[int, float, str, decimal.Decimal, None]) -> decimal.Decimal:
    """Convert value to Decimal safely."""
    if value is None:
        raise ValueError("Cannot convert None to Decimal")
    if isinstance(value, decimal.Decimal):
        return value
    try:
        return decimal.Decimal(str(value), context=_ctx())
    except Exception as e:
        raise ValueError(f"Cannot convert {value!r} to Decimal: {e}")


# --- Core construction ---


def decimal(value: Union[int, float, str, decimal.Decimal]) -> decimal.Decimal:
    """Create a Decimal from int, float, string, or Decimal.

    Args:
        value: Input value to convert.

    Returns:
        Decimal representation.

    Raises:
        ValueError: If conversion fails.

    Example:
        >>> decimal("3.14")
        Decimal('3.14')
        >>> decimal(100)
        Decimal('100')
    """
    return _d(value)


def from_float(value: float, precision: Optional[int] = None) -> decimal.Decimal:
    """Convert float to Decimal with optional precision.

    Args:
        value: Float value.
        precision: Optional decimal places.

    Returns:
        Decimal representation.
    """
    result = _d(value)
    if precision is not None:
        quantize_str = "0." + "0" * precision
        result = result.quantize(
            decimal.Decimal(quantize_str), rounding=decimal.ROUND_HALF_UP
        )
    return result


def from_int(value: int) -> decimal.Decimal:
    """Convert integer to Decimal.

    Args:
        value: Integer value.

    Returns:
        Decimal representation.
    """
    return decimal.Decimal(value)


def from_str(value: str) -> decimal.Decimal:
    """Convert string to Decimal.

    Args:
        value: String representation.

    Returns:
        Decimal representation.

    Raises:
        ValueError: If string is not a valid decimal.
    """
    try:
        return decimal.Decimal(value)
    except Exception as e:
        raise ValueError(f"Invalid decimal string '{value}': {e}")


def from_currency(value: Union[int, float, str], currency_code: str = "USD") -> decimal.Decimal:
    """Convert value to Decimal suitable for currency.

    Automatically detects and removes currency symbols.

    Args:
        value: Monetary value.
        currency_code: Currency code (USD, EUR, etc.).

    Returns:
        Decimal value without currency symbols.
    """
    if isinstance(value, str):
        # Remove common currency symbols
        for sym in ["$", "€", "£", "¥", "₹", ",", " "]:
            value = value.replace(sym, "")
    return _d(value)


def to_float(value: Union[decimal.Decimal, str]) -> float:
    """Convert Decimal to float.

    Args:
        value: Decimal value.

    Returns:
        Float representation.
    """
    return float(_d(value))


def to_int(value: Union[decimal.Decimal, str], rounding: str = "ROUND_HALF_UP") -> int:
    """Convert Decimal to integer with optional rounding.

    Args:
        value: Decimal value.
        rounding: Rounding mode (decimal.ROUND_*).

    Returns:
        Integer representation.
    """
    d = _d(value)
    rounding_mode = getattr(decimal, rounding, decimal.ROUND_HALF_UP)
    return int(d.to_integral_value(rounding=rounding_mode))


def to_str(value: Union[decimal.Decimal, str], strip_trailing: bool = True) -> str:
    """Convert Decimal to string.

    Args:
        value: Decimal value.
        strip_trailing: Remove trailing zeros after decimal point.

    Returns:
        String representation.
    """
    d = _d(value)
    s = str(d)
    if strip_trailing and "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def to_currency(
    value: Union[decimal.Decimal, str],
    currency_code: str = "USD",
    symbol: str = "$",
    include_symbol: bool = True,
) -> str:
    """Format Decimal as currency string.

    Args:
        value: Decimal value.
        currency_code: Currency code.
        symbol: Currency symbol.
        include_symbol: Whether to include symbol.

    Returns:
        Formatted currency string.
    """
    d = _d(value)
    result = f"{d:.2f}"
    if include_symbol:
        result = f"{symbol}{result}"
    return result


# --- Arithmetic ---


def add(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Add two values with Decimal precision.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Sum a + b.
    """
    return _d(a) + _d(b)


def sub(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Subtract two values with Decimal precision.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Difference a - b.
    """
    return _d(a) - _d(b)


def mul(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Multiply two values with Decimal precision.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Product a * b.
    """
    return _d(a) * _d(b)


def div(
    a: Union[decimal.Decimal, int, float, str],
    b: Union[decimal.Decimal, int, float, str],
    precision: Optional[int] = None,
) -> decimal.Decimal:
    """Divide two values with Decimal precision.

    Args:
        a: Dividend.
        b: Divisor.
        precision: Optional decimal places for result.

    Returns:
        Quotient a / b.

    Raises:
        ZeroDivisionError: If b is zero.
    """
    divisor = _d(b)
    if divisor == 0:
        raise ZeroDivisionError("Division by zero")
    result = _d(a) / divisor
    if precision is not None:
        quantize_str = "0." + "0" * precision
        result = result.quantize(
            decimal.Decimal(quantize_str), rounding=decimal.ROUND_HALF_UP
        )
    return result


def floordiv(
    a: Union[decimal.Decimal, int, float, str],
    b: Union[decimal.Decimal, int, float, str],
) -> decimal.Decimal:
    """Floor divide two values.

    Args:
        a: Dividend.
        b: Divisor.

    Returns:
        Floor of a / b.
    """
    divisor = _d(b)
    if divisor == 0:
        raise ZeroDivisionError("Division by zero")
    return _d(a).to_integral_value(rounding=decimal.ROUND_FLOOR) / divisor


def mod(
    a: Union[decimal.Decimal, int, float, str],
    b: Union[decimal.Decimal, int, float, str],
) -> decimal.Decimal:
    """Modulo operation.

    Args:
        a: Dividend.
        b: Divisor.

    Returns:
        Remainder of a / b.
    """
    return _d(a) % _d(b)


def pow_(
    base: Union[decimal.Decimal, int, float, str],
    exp: Union[decimal.Decimal, int, float, str],
) -> decimal.Decimal:
    """Power operation.

    Args:
        base: Base value.
        exp: Exponent.

    Returns:
        base ** exp.
    """
    return _d(base) ** _d(exp)


def abs_(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Absolute value.

    Args:
        value: Input value.

    Returns:
        Absolute value.
    """
    d = _d(value)
    return d.copy_abs()


def neg(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Negate value.

    Args:
        value: Input value.

    Returns:
        Negated value.
    """
    return -_d(value)


# --- Comparison ---


def eq(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check equality.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if a == b.
    """
    return _d(a) == _d(b)


def ne(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check inequality.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if a != b.
    """
    return _d(a) != _d(b)


def lt(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check less than.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if a < b.
    """
    return _d(a) < _d(b)


def le(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check less than or equal.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if a <= b.
    """
    return _d(a) <= _d(b)


def gt(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check greater than.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if a > b.
    """
    return _d(a) > _d(b)


def ge(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check greater than or equal.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if a >= b.
    """
    return _d(a) >= _d(b)


def compare(a: Union[decimal.Decimal, int, float, str], b: Union[decimal.Decimal, int, float, str]) -> int:
    """Compare two values.

    Args:
        a: First value.
        b: Second value.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b.
    """
    return _d(a).compare(_d(b))


def is_zero(value: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check if value is zero.

    Args:
        value: Value to check.

    Returns:
        True if value == 0.
    """
    return _d(value) == 0


def is_positive(value: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check if value is positive.

    Args:
        value: Value to check.

    Returns:
        True if value > 0.
    """
    return _d(value) > 0


def is_negative(value: Union[decimal.Decimal, int, float, str]) -> bool:
    """Check if value is negative.

    Args:
        value: Value to check.

    Returns:
        True if value < 0.
    """
    return _d(value) < 0


# --- Formatting ---


def quantize(
    value: Union[decimal.Decimal, int, float, str],
    exp: Union[decimal.Decimal, str],
    rounding: str = "ROUND_HALF_UP",
) -> decimal.Decimal:
    """Quantize value to specific exponent.

    Args:
        value: Value to quantize.
        exp: Exponent or string like "0.01".
        rounding: Rounding mode.

    Returns:
        Quantized value.
    """
    d = _d(value)
    e = _d(exp) if isinstance(exp, str) else exp
    rounding_mode = getattr(decimal, rounding, decimal.ROUND_HALF_UP)
    return d.quantize(e, rounding=rounding_mode)


def round_(
    value: Union[decimal.Decimal, int, float, str],
    ndigits: int = 0,
) -> decimal.Decimal:
    """Round to specified decimal places.

    Args:
        value: Value to round.
        ndigits: Number of decimal places.

    Returns:
        Rounded value.
    """
    d = _d(value)
    if ndigits == 0:
        return d.to_integral_value(rounding=decimal.ROUND_HALF_UP)
    factor = decimal.Decimal("0.1") ** ndigits
    return (d / factor).to_integral_value(rounding=decimal.ROUND_HALF_UP) * factor


def floor_(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Floor value.

    Args:
        value: Value to floor.

    Returns:
        Floor of value.
    """
    return _d(value).to_integral_value(rounding=decimal.ROUND_FLOOR)


def ceil(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:  # noqa: A002
    """Ceiling value.

    Args:
        value: Value to ceiling.

    Returns:
        Ceiling of value.
    """
    return _d(value).to_integral_value(rounding=decimal.ROUND_CEILING)


def trunc(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Truncate value.

    Args:
        value: Value to truncate.

    Returns:
        Truncated value.
    """
    return _d(value).to_integral_value(rounding=decimal.ROUND_DOWN)


def format_currency(
    value: Union[decimal.Decimal, int, float, str],
    currency_code: str = "USD",
    symbol: str = "$",
    include_symbol: bool = True,
    include_code: bool = False,
    decimal_places: int = 2,
) -> str:
    """Format as currency string.

    Args:
        value: Value to format.
        currency_code: ISO currency code.
        symbol: Currency symbol.
        include_symbol: Include symbol.
        include_code: Include currency code.
        decimal_places: Number of decimal places.

    Returns:
        Formatted currency string.
    """
    d = _d(value)
    fmt = f"{{:{symbol}.,{decimal_places}f}}"
    result = fmt.format(d)
    if include_code:
        result += f" {currency_code}"
    return result


def format_percent(
    value: Union[decimal.Decimal, int, float, str],
    decimal_places: int = 2,
    include_symbol: bool = True,
) -> str:
    """Format as percentage string.

    Args:
        value: Value (e.g., 0.15 for 15%).
        decimal_places: Number of decimal places.
        include_symbol: Include % symbol.

    Returns:
        Formatted percentage string.
    """
    d = _d(value)
    result = f"{d * 100:.{decimal_places}f}"
    if include_symbol:
        result += "%"
    return result


def format_scientific(value: Union[decimal.Decimal, int, float, str], precision: int = 6) -> str:
    """Format in scientific notation.

    Args:
        value: Value to format.
        precision: Number of significant digits.

    Returns:
        Scientific notation string.
    """
    d = _d(value)
    return format(d, f".{precision}e")


# --- Math functions ---


def sqrt(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Square root.

    Args:
        value: Value to sqrt.

    Returns:
        Square root.

    Raises:
        ValueError: If value is negative.
    """
    d = _d(value)
    if d < 0:
        raise ValueError("Cannot compute sqrt of negative number")
    return d.sqrt()


def ln(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Natural logarithm.

    Args:
        value: Value.

    Returns:
        Natural log.
    """
    return _d(value).ln()


def log10(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Base 10 logarithm.

    Args:
        value: Value.

    Returns:
        Log base 10.
    """
    return _d(value).log10()


def exp(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Exponential (e^x).

    Args:
        value: Exponent.

    Returns:
        e ** value.
    """
    return _d(value).exp()


# --- Aggregates ---


def min_(*values: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Minimum of values.

    Args:
        *values: Values to compare.

    Returns:
        Minimum value.
    """
    if not values:
        raise ValueError("min requires at least one argument")
    decimals = [_d(v) for v in values]
    return min(decimals)


def max_(*values: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Maximum of values.

    Args:
        *values: Values to compare.

    Returns:
        Maximum value.
    """
    if not values:
        raise ValueError("max requires at least one argument")
    decimals = [_d(v) for v in values]
    return max(decimals)


def sum_(values: Iterable[Union[decimal.Decimal, int, float, str]]) -> decimal.Decimal:
    """Sum of values.

    Args:
        values: Values to sum.

    Returns:
        Sum of all values.
    """
    total = decimal.Decimal(0)
    for v in values:
        total += _d(v)
    return total


def avg(values: Iterable[Union[decimal.Decimal, int, float, str]]) -> decimal.Decimal:
    """Average of values.

    Args:
        values: Values to average.

    Returns:
        Average value.
    """
    vals = list(values)
    if not vals:
        raise ValueError("avg requires at least one value")
    return sum_(vals) / len(vals)


def median(values: Iterable[Union[decimal.Decimal, int, float, str]]) -> decimal.Decimal:
    """Median of values.

    Args:
        values: Values to compute median.

    Returns:
        Median value.
    """
    vals = sorted([_d(v) for v in values])
    n = len(vals)
    if n == 0:
        raise ValueError("median requires at least one value")
    if n % 2 == 1:
        return vals[n // 2]
    return (vals[n // 2 - 1] + vals[n // 2]) / 2


# --- Utilities ---


def get_precision() -> int:
    """Get current decimal precision.

    Returns:
        Current precision setting.
    """
    return _PRECISION


def set_precision(precision: int) -> None:
    """Set decimal precision globally.

    Args:
        precision: New precision value (must be positive).
    """
    global _PRECISION  # noqa: PLW0603
    if precision < 1:
        raise ValueError("Precision must be positive")
    _PRECISION = precision


def normalize(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Normalize decimal (remove trailing zeros).

    Args:
        value: Value to normalize.

    Returns:
        Normalized decimal.
    """
    return _d(value).normalize()


def split(value: Union[decimal.Decimal, str]) -> Tuple[decimal.Decimal, decimal.Decimal]:
    """Split into integer and fractional parts.

    Args:
        value: Decimal value.

    Returns:
        Tuple of (integer_part, fractional_part).
    """
    d = _d(value)
    int_part = d.to_integral_value(rounding=decimal.ROUND_DOWN)
    frac_part = d - int_part
    return int_part, frac_part


def fractional_part(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Get fractional part.

    Args:
        value: Decimal value.

    Returns:
        Fractional part.
    """
    d = _d(value)
    int_part = d.to_integral_value(rounding=decimal.ROUND_DOWN)
    return d - int_part


def integer_part(value: Union[decimal.Decimal, int, float, str]) -> decimal.Decimal:
    """Get integer part.

    Args:
        value: Decimal value.

    Returns:
        Integer part (truncated toward zero).
    """
    return _d(value).to_integral_value(rounding=decimal.ROUND_DOWN)
