"""complex_action.py - complex number operations.

Provides comprehensive complex number arithmetic, polar conversions,
trigonometric functions, and complex analysis utilities.
"""

from __future__ import annotations

import math
from typing import (
    Callable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

__all__ = [
    # Construction
    "complex",
    "from_polar",
    "from_rect",
    "from_str",
    "to_tuple",
    "to_polar",
    # Arithmetic
    "add",
    "sub",
    "mul",
    "div",
    "neg",
    "abs_",
    "conj",
    "inv",
    "pow_",
    # Comparison
    "eq",
    "is_real",
    "is_imaginary",
    "is_zero",
    # Trigonometric
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "sinh",
    "cosh",
    "tanh",
    "asinh",
    "acosh",
    "atanh",
    # Utility
    "sqrt",
    "ln",
    "log",
    "exp",
    "abs_squared",
    "arg",
    "phase",
    "real",
    "imag",
    "min_",
    "max_",
    "sum_",
]

T = TypeVar("T", bound=Union[int, float])


class Complex:
    """Immutable complex number.

    Attributes:
        re: Real part.
        im: Imaginary part.
    """

    __slots__ = ("re", "im")

    def __init__(self, re: float = 0.0, im: float = 0.0) -> None:
        self.re = float(re)
        self.im = float(im)

    def __repr__(self) -> str:
        if self.im >= 0:
            return f"({self.re}+{self.im}j)"
        return f"({self.re}{self.im}j)"

    def __str__(self) -> str:
        return self.__repr__()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Complex):
            return NotImplemented
        return abs(self.re - other.re) < 1e-12 and abs(self.im - other.im) < 1e-12

    def __hash__(self) -> int:
        return hash((self.re, self.im))

    def __add__(self, other: Union[Complex, int, float]) -> Complex:
        if isinstance(other, Complex):
            return Complex(self.re + other.re, self.im + other.im)
        if isinstance(other, (int, float)):
            return Complex(self.re + other, self.im)
        return NotImplemented

    def __radd__(self, other: Union[int, float]) -> Complex:
        return Complex(self.re + other, self.im)

    def __sub__(self, other: Union[Complex, int, float]) -> Complex:
        if isinstance(other, Complex):
            return Complex(self.re - other.re, self.im - other.im)
        if isinstance(other, (int, float)):
            return Complex(self.re - other, self.im)
        return NotImplemented

    def __rsub__(self, other: Union[int, float]) -> Complex:
        return Complex(other - self.re, -self.im)

    def __mul__(self, other: Union[Complex, int, float]) -> Complex:
        if isinstance(other, Complex):
            re = self.re * other.re - self.im * other.im
            im = self.re * other.im + self.im * other.re
            return Complex(re, im)
        if isinstance(other, (int, float)):
            return Complex(self.re * other, self.im * other)
        return NotImplemented

    def __rmul__(self, other: Union[int, float]) -> Complex:
        return Complex(self.re * other, self.im * other)

    def __truediv__(self, other: Union[Complex, int, float]) -> Complex:
        if isinstance(other, Complex):
            denom = other.re ** 2 + other.im ** 2
            if denom == 0:
                raise ZeroDivisionError("Division by zero in complex division")
            re = (self.re * other.re + self.im * other.im) / denom
            im = (self.im * other.re - self.re * other.im) / denom
            return Complex(re, im)
        if isinstance(other, (int, float)):
            if other == 0:
                raise ZeroDivisionError("Division by zero")
            return Complex(self.re / other, self.im / other)
        return NotImplemented

    def __neg__(self) -> Complex:
        return Complex(-self.re, -self.im)

    def __abs__(self) -> float:
        return math.hypot(self.re, self.im)

    def __pow__(self, exponent: Union[int, float, Complex]) -> Complex:
        if isinstance(exponent, (int, float)):
            if exponent == 0:
                return Complex(1, 0)
            if isinstance(exponent, int):
                result = Complex(1, 0)
                for _ in range(abs(exponent)):
                    result = result * self
                if exponent < 0:
                    result = Complex(1, 0) / result
                return result
            return exp(float(exponent) * ln(self))
        if isinstance(exponent, Complex):
            return exp(exponent * ln(self))
        return NotImplemented

    @property
    def real(self) -> float:
        """Real part."""
        return self.re

    @property
    def imag(self) -> float:
        """Imaginary part."""
        return self.im

    def conjugate(self) -> Complex:
        """Return complex conjugate."""
        return Complex(self.re, -self.im)

    def abs_squared(self) -> float:
        """Return |z|^2."""
        return self.re ** 2 + self.im ** 2


# --- Module-level functions ---


def complex(re: float = 0.0, im: float = 0.0) -> Complex:
    """Create a Complex number.

    Args:
        re: Real part.
        im: Imaginary part.

    Returns:
        Complex number.
    """
    return Complex(re, im)


def from_polar(r: float, theta: float) -> Complex:
    """Create complex from polar form.

    Args:
        r: Magnitude.
        theta: Angle in radians.

    Returns:
        Complex number.
    """
    return Complex(r * math.cos(theta), r * math.sin(theta))


def from_rect(re: float, im: float) -> Complex:
    """Create complex from rectangular form.

    Args:
        re: Real part.
        im: Imaginary part.

    Returns:
        Complex number.
    """
    return Complex(re, im)


def from_str(s: str) -> Complex:
    """Parse string to Complex.

    Args:
        s: String like "3+4j" or "3+4i".

    Returns:
        Complex number.

    Raises:
        ValueError: If string is invalid.
    """
    s = s.replace("i", "j").strip()
    try:
        return Complex(**complex.__kwdefaults__)  # fallback
    except Exception:
        pass
    # Manual parse
    s = s.strip("() ")
    if "+" in s or "-" in s:
        # Find the sign before j
        idx = s.rfind("+") if s.rfind("+") > s.rfind("-") else s.rfind("-")
        if idx == -1:
            idx = len(s)
        try:
            re = float(s[:idx]) if s[:idx] else 0.0
        except ValueError:
            re = 0.0
        try:
            im_str = s[idx:].replace("j", "").strip()
            im = float(im_str) if im_str else 1.0
        except ValueError:
            im = 1.0
        return Complex(re, im)
    try:
        return Complex(float(s), 0)
    except ValueError as e:
        raise ValueError(f"Cannot parse '{s}' as complex: {e}")


def to_tuple(c: Complex) -> Tuple[float, float]:
    """Convert to (real, imag) tuple.

    Args:
        c: Complex number.

    Returns:
        Tuple of (real, imag).
    """
    return c.re, c.im


def to_polar(c: Complex) -> Tuple[float, float]:
    """Convert to polar (r, theta).

    Args:
        c: Complex number.

    Returns:
        Tuple of (magnitude, angle_radians).
    """
    return abs(c), math.atan2(c.im, c.re)


def add(a: Union[Complex, int, float], b: Union[Complex, int, float]) -> Complex:
    """Add two complex numbers.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Sum.
    """
    if not isinstance(a, Complex):
        a = Complex(a, 0)
    if not isinstance(b, Complex):
        b = Complex(b, 0)
    return a + b


def sub(a: Union[Complex, int, float], b: Union[Complex, int, float]) -> Complex:
    """Subtract complex numbers.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Difference.
    """
    if not isinstance(a, Complex):
        a = Complex(a, 0)
    if not isinstance(b, Complex):
        b = Complex(b, 0)
    return a - b


def mul(a: Union[Complex, int, float], b: Union[Complex, int, float]) -> Complex:
    """Multiply complex numbers.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Product.
    """
    if not isinstance(a, Complex):
        a = Complex(a, 0)
    if not isinstance(b, Complex):
        b = Complex(b, 0)
    return a * b


def div(a: Union[Complex, int, float], b: Union[Complex, int, float]) -> Complex:
    """Divide complex numbers.

    Args:
        a: Dividend.
        b: Divisor.

    Returns:
        Quotient.
    """
    if not isinstance(a, Complex):
        a = Complex(a, 0)
    if not isinstance(b, Complex):
        b = Complex(b, 0)
    return a / b


def neg(c: Union[Complex, int, float]) -> Complex:
    """Negate complex number.

    Args:
        c: Complex number.

    Returns:
        Negated value.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return -c


def abs_(c: Union[Complex, int, float]) -> float:
    """Absolute value (magnitude).

    Args:
        c: Complex number.

    Returns:
        |c|.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return abs(c)


def conj(c: Union[Complex, int, float]) -> Complex:
    """Complex conjugate.

    Args:
        c: Complex number.

    Returns:
        Conjugate.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return c.conjugate()


def inv(c: Union[Complex, int, float]) -> Complex:
    """Multiplicative inverse.

    Args:
        c: Complex number.

    Returns:
        1/c.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return Complex(1, 0) / c


def pow_(c: Union[Complex, int, float], exp: Union[int, float, Complex]) -> Complex:
    """Power operation.

    Args:
        c: Base.
        exp: Exponent.

    Returns:
        c ** exp.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return c ** exp  # type: ignore


def eq(a: Union[Complex, int, float], b: Union[Complex, int, float]) -> bool:
    """Check equality.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if equal.
    """
    if not isinstance(a, Complex):
        a = Complex(a, 0)
    if not isinstance(b, Complex):
        b = Complex(b, 0)
    return a == b


def is_real(c: Union[Complex, int, float], tol: float = 1e-12) -> bool:
    """Check if complex is real (imaginary part near zero).

    Args:
        c: Complex number.
        tol: Tolerance.

    Returns:
        True if imaginary part is near zero.
    """
    if not isinstance(c, Complex):
        return True
    return abs(c.im) < tol


def is_imaginary(c: Union[Complex, int, float], tol: float = 1e-12) -> bool:
    """Check if complex is purely imaginary.

    Args:
        c: Complex number.
        tol: Tolerance.

    Returns:
        True if real part is near zero.
    """
    if not isinstance(c, Complex):
        return abs(c) < tol
    return abs(c.re) < tol


def is_zero(c: Union[Complex, int, float], tol: float = 1e-12) -> bool:
    """Check if complex is zero.

    Args:
        c: Complex number.
        tol: Tolerance.

    Returns:
        True if magnitude is near zero.
    """
    return abs_(c) < tol


def sin(c: Union[Complex, int, float]) -> Complex:
    """Complex sine.

    Args:
        c: Angle in radians.

    Returns:
        sin(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    # sin(z) = (e^(iz) - e^(-iz)) / 2i
    iz = Complex(-c.im, c.re)
    neg_iz = Complex(c.im, -c.re)
    return (exp(iz) - exp(neg_iz)) * Complex(0, 0.5)


def cos(c: Union[Complex, int, float]) -> Complex:
    """Complex cosine.

    Args:
        c: Angle in radians.

    Returns:
        cos(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    # cos(z) = (e^(iz) + e^(-iz)) / 2
    iz = Complex(-c.im, c.re)
    neg_iz = Complex(c.im, -c.re)
    return (exp(iz) + exp(neg_iz)) * 0.5


def tan(c: Union[Complex, int, float]) -> Complex:
    """Complex tangent.

    Args:
        c: Angle in radians.

    Returns:
        tan(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return sin(c) / cos(c)


def asin(c: Union[Complex, int, float]) -> Complex:
    """Complex arcsine.

    Args:
        c: Value.

    Returns:
        arcsin(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    # asin(z) = -i * ln(i*z + sqrt(1 - z^2))
    return Complex(0, -1) * ln(Complex(-c.im, c.re) + sqrt(Complex(1, 0) - c * c))


def acos(c: Union[Complex, int, float]) -> Complex:
    """Complex arccosine.

    Args:
        c: Value.

    Returns:
        arccos(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    # acos(z) = -i * ln(z + i*sqrt(1 - z^2))
    return Complex(0, -1) * ln(c + Complex(-1, 0) * sqrt(Complex(1, 0) - c * c))


def atan(c: Union[Complex, int, float]) -> Complex:
    """Complex arctangent.

    Args:
        c: Value.

    Returns:
        arctan(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    # atan(z) = (i/2) * ln((i+z)/(i-z))
    half_i = Complex(0, 0.5)
    return half_i * ln(Complex(-c.im, 1 + c.re) / Complex(c.im, 1 - c.re))


def sinh(c: Union[Complex, int, float]) -> Complex:
    """Hyperbolic sine.

    Args:
        c: Value.

    Returns:
        sinh(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return (exp(c) - exp(-c)) * 0.5


def cosh(c: Union[Complex, int, float]) -> Complex:
    """Hyperbolic cosine.

    Args:
        c: Value.

    Returns:
        cosh(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return (exp(c) + exp(-c)) * 0.5


def tanh(c: Union[Complex, int, float]) -> Complex:
    """Hyperbolic tangent.

    Args:
        c: Value.

    Returns:
        tanh(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return sinh(c) / cosh(c)


def asinh(c: Union[Complex, int, float]) -> Complex:
    """Inverse hyperbolic sine.

    Args:
        c: Value.

    Returns:
        asinh(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return ln(c + sqrt(c * c + Complex(1, 0)))


def acosh(c: Union[Complex, int, float]) -> Complex:
    """Inverse hyperbolic cosine.

    Args:
        c: Value.

    Returns:
        acosh(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return ln(c + sqrt(c * c - Complex(1, 0)))


def atanh(c: Union[Complex, int, float]) -> Complex:
    """Inverse hyperbolic tangent.

    Args:
        c: Value.

    Returns:
        atanh(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return 0.5 * ln((Complex(1, 0) + c) / (Complex(1, 0) - c))


def sqrt(c: Union[Complex, int, float]) -> Complex:
    """Complex square root.

    Args:
        c: Value.

    Returns:
        sqrt(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    r, theta = to_polar(c)
    return from_polar(math.sqrt(r), theta / 2)


def ln(c: Union[Complex, int, float]) -> Complex:
    """Complex natural logarithm.

    Args:
        c: Value.

    Returns:
        ln(c).
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    r, theta = to_polar(c)
    if r == 0:
        raise ValueError("Logarithm of zero is undefined")
    return Complex(math.log(r), theta)


def log(c: Union[Complex, int, float], base: Union[float, Complex] = math.e) -> Complex:
    """Complex logarithm base.

    Args:
        c: Value.
        base: Base of logarithm.

    Returns:
        log_base(c).
    """
    if not isinstance(base, Complex):
        base = Complex(base, 0)
    return ln(c) / ln(base)


def exp(c: Union[Complex, int, float]) -> Complex:
    """Complex exponential.

    Args:
        c: Exponent.

    Returns:
        e^c.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    r = math.exp(c.re)
    return Complex(r * math.cos(c.im), r * math.sin(c.im))


def abs_squared(c: Union[Complex, int, float]) -> float:
    """Magnitude squared.

    Args:
        c: Complex number.

    Returns:
        |c|^2.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return c.abs_squared()


def arg(c: Union[Complex, int, float]) -> float:
    """Argument (angle).

    Args:
        c: Complex number.

    Returns:
        Angle in radians.
    """
    if not isinstance(c, Complex):
        c = Complex(c, 0)
    return math.atan2(c.im, c.re)


def phase(c: Union[Complex, int, float]) -> float:
    """Phase (alias for arg).

    Args:
        c: Complex number.

    Returns:
        Phase in radians.
    """
    return arg(c)


def real(c: Union[Complex, int, float]) -> float:
    """Real part.

    Args:
        c: Complex number.

    Returns:
        Real part.
    """
    if isinstance(c, Complex):
        return c.re
    return float(c)


def imag(c: Union[Complex, int, float]) -> float:
    """Imaginary part.

    Args:
        c: Complex number.

    Returns:
        Imaginary part.
    """
    if isinstance(c, Complex):
        return c.im
    return 0.0


def min_(*values: Union[Complex, int, float]) -> Complex:
    """Minimum by magnitude.

    Args:
        *values: Values to compare.

    Returns:
        Value with minimum magnitude.
    """
    if not values:
        raise ValueError("min requires at least one argument")
    return min(values, key=abs_)


def max_(*values: Union[Complex, int, float]) -> Complex:
    """Maximum by magnitude.

    Args:
        *values: Values to compare.

    Returns:
        Value with maximum magnitude.
    """
    if not values:
        raise ValueError("max requires at least one argument")
    return max(values, key=abs_)


def sum_(values: Iterable[Union[Complex, int, float]]) -> Complex:
    """Sum of complex numbers.

    Args:
        values: Values to sum.

    Returns:
        Sum.
    """
    result = Complex(0, 0)
    for v in values:
        if not isinstance(v, Complex):
            v = Complex(v, 0)
        result = result + v
    return result
