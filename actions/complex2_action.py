"""Complex action v2 - advanced complex operations.

Extended complex number utilities including polynomials,
 roots, transformations, and signal processing.
"""

from __future__ import annotations

from cmath import acos, asin, atan, cos, exp, log, sin, sqrt, tan
from complex import abs as cabs, complex, polar, rect
from typing import Sequence, Union

__all__ = [
    "complex_polynomial_eval",
    "complex_polynomial_roots",
    "complex_roots_of_unity",
    "complex_derivative",
    "complex_integral",
    "complex_gradient",
    "complex_laplace",
    "complex_fourier",
    "complex_fft",
    "complex_ifft",
    "complex_rotate",
    "complex_scale",
    "complex_translate",
    "complex_mirror",
    "complex_reflect",
    "complex_to_polar_tuple",
    "polar_tuple_to_complex",
    "complex_zoom",
    "complex_mandelbrot",
    "complex_julia",
    "complex_moebius",
    "complex_cross_ratio",
    "is_on_unit_circle",
    "is_on_real_axis",
    "complex_bilinear",
    "complex_inverse",
    "complex_conj_product",
    "complex_real_part",
    "complex_imag_part",
    "complex_argument",
    "complex_magnitude_sq",
    "ComplexPolynomial",
    "ComplexVector",
    "ComplexMatrix",
    "ComplexTransformer",
]


def complex_polynomial_eval(coeffs: Sequence[complex | int | float], z: complex) -> complex:
    """Evaluate polynomial at complex point.

    Args:
        coeffs: Coefficients [a0, a1, a2, ...] for a0 + a1*z + a2*z^2 + ...
        z: Complex point.

    Returns:
        Polynomial value.
    """
    result = complex(0)
    z_power = complex(1)
    for c in coeffs:
        result += complex(c) * z_power
        z_power *= z
    return result


def complex_polynomial_roots(coeffs: Sequence[complex | int | float]) -> list[complex]:
    """Find roots of polynomial using numpy.roots.

    Args:
        coeffs: Coefficients of polynomial.

    Returns:
        List of complex roots.
    """
    try:
        import numpy as np
        return [complex(r) for r in np.roots(coeffs)]
    except ImportError:
        raise ImportError("numpy required for polynomial roots")


def complex_roots_of_unity(n: int, k: int = 0) -> complex:
    """Get k-th n-th root of unity.

    Args:
        n: Root number.
        k: Which root (0 to n-1).

    Returns:
        e^(2*pi*i*k/n).
    """
    if n <= 0:
        raise ValueError(f"n must be positive, got {n}")
    return rect(1, 2 * 3.141592653589793 * k / n)


def complex_derivative(coeffs: Sequence[complex | int | float]) -> list[complex]:
    """Compute derivative coefficients.

    Args:
        coeffs: Polynomial coefficients.

    Returns:
        Derivative polynomial coefficients.
    """
    return [complex(i * c) for i, c in enumerate(coeffs[1:], start=1)]


def complex_integral(constant: complex, coeffs: Sequence[complex | int | float]) -> list[complex]:
    """Compute integral coefficients.

    Args:
        constant: Integration constant.
        coeffs: Polynomial coefficients to integrate.

    Returns:
        Integrated polynomial coefficients.
    """
    result = [complex(constant)]
    for i, c in enumerate(coeffs):
        result.append(complex(c) / complex(i + 1))
    return result


def complex_gradient(f: complex, h: float = 1e-8) -> complex:
    """Numerical gradient of complex function.

    Args:
        f: Complex function (returns complex).
        h: Step size.

    Returns:
        Numerical gradient.
    """
    return (f(1 + h) - f(1 - h)) / (2 * h)


def complex_laplace(f: complex, t: float) -> complex:
    """Placeholder for Laplace transform (requires integral)."""
    raise NotImplementedError("Laplace transform not yet implemented")


def complex_fourier(signal: Sequence[complex]) -> list[complex]:
    """Compute Fourier transform (O(n^2) naive implementation).

    Args:
        signal: Complex signal.

    Returns:
        Frequency domain representation.
    """
    n = len(signal)
    result = []
    for k in range(n):
        sum_val = complex(0)
        for j, xj in enumerate(signal):
            angle = -2 * 3.141592653589793 * k * j / n
            sum_val += xj * rect(1, angle)
        result.append(sum_val)
    return result


def complex_fft(signal: Sequence[complex]) -> list[complex]:
    """Fast Fourier Transform.

    Args:
        signal: Complex signal (length must be power of 2).

    Returns:
        FFT of signal.
    """
    import math
    n = len(signal)
    if n == 1:
        return list(signal)
    if n & (n - 1):
        raise ValueError("Signal length must be power of 2")
    even = complex_fft(signal[::2])
    odd = complex_fft(signal[1::2])
    result = [complex(0)] * n
    for k in range(n // 2):
        t = even[k] + rect(1, -2 * math.pi * k / n) * odd[k]
        result[k] = t
        result[k + n // 2] = even[k] - rect(1, -2 * math.pi * k / n) * odd[k]
    return result


def complex_ifft(signal: Sequence[complex]) -> list[complex]:
    """Inverse FFT.

    Args:
        signal: Frequency domain signal.

    Returns:
        Time domain signal.
    """
    n = len(signal)
    conj = [c.conjugate() for c in signal]
    forward = complex_fft(conj)
    return [c.conjugate() / n for c in forward]


def complex_rotate(z: complex, angle: float, center: complex = 0) -> complex:
    """Rotate complex number around center.

    Args:
        z: Complex point.
        angle: Rotation angle in radians.
        center: Center of rotation.

    Returns:
        Rotated complex.
    """
    z_shifted = z - center
    rotated = z_shifted * rect(1, angle)
    return rotated + center


def complex_scale(z: complex, factor: float, center: complex = 0) -> complex:
    """Scale complex number relative to center.

    Args:
        z: Complex point.
        factor: Scale factor.
        center: Center of scaling.

    Returns:
        Scaled complex.
    """
    z_shifted = z - center
    return z_shifted * factor + center


def complex_translate(z: complex, w: complex) -> complex:
    """Translate (shift) complex number.

    Args:
        z: Complex point.
        w: Translation vector.

    Returns:
        Translated complex.
    """
    return z + w


def complex_mirror(z: complex, axis: str = "real") -> complex:
    """Mirror complex number across axis.

    Args:
        z: Complex point.
        axis: 'real' or 'imag'.

    Returns:
        Mirrored complex.
    """
    if axis == "real":
        return complex(z.real, -z.imag)
    elif axis == "imag":
        return complex(-z.real, z.imag)
    raise ValueError(f"Unknown axis: {axis}")


def complex_reflect(z: complex, a: complex, b: complex) -> complex:
    """Reflect z across line defined by points a and b.

    Args:
        z: Point to reflect.
        a: First point on line.
        b: Second point on line.

    Returns:
        Reflected point.
    """
    ab = b - a
    t = ((z - a) * ab.conjugate()).real / (ab * ab.conjugate()).real
    proj = a + ab * complex(t)
    return proj * 2 - z


def complex_to_polar_tuple(c: complex) -> tuple[float, float]:
    """Convert complex to (r, theta) tuple."""
    return (abs(c), c.phase)


def polar_tuple_to_complex(r: float, theta: float) -> complex:
    """Convert (r, theta) to complex."""
    return rect(r, theta)


def complex_zoom(z: complex, factor: float, center: complex = 0) -> complex:
    """Zoom (scale) around center."""
    return complex_scale(z, factor, center)


def complex_mandelbrot(c: complex, max_iter: int = 100) -> int:
    """Mandelbrot iteration count.

    Args:
        c: Complex point.
        max_iter: Maximum iterations.

    Returns:
        Iteration count at escape.
    """
    z = complex(0)
    for i in range(max_iter):
        if abs(z) > 2:
            return i
        z = z * z + c
    return max_iter


def complex_julia(z: complex, c: complex, max_iter: int = 100) -> int:
    """Julia set iteration count.

    Args:
        z: Starting point.
        c: Julia constant.
        max_iter: Maximum iterations.

    Returns:
        Iteration count.
    """
    for i in range(max_iter):
        if abs(z) > 2:
            return i
        z = z * z + c
    return max_iter


def complex_moebius(z: complex, a: complex, b: complex, c: complex, d: complex) -> complex:
    """Moebius transformation (z) = (az + b) / (cz + d).

    Args:
        z: Input complex.
        a, b, c, d: Transformation parameters.

    Returns:
        Transformed complex.
    """
    numerator = a * z + b
    denominator = c * z + d
    if denominator == 0:
        raise ValueError("Denominator cannot be 0")
    return numerator / denominator


def complex_cross_ratio(z1: complex, z2: complex, z3: complex, z4: complex) -> complex:
    """Cross ratio (z1, z2; z3, z4).

    Args:
        z1, z2, z3, z4: Four complex points.

    Returns:
        Cross ratio.
    """
    return ((z1 - z3) * (z2 - z4)) / ((z1 - z4) * (z2 - z3))


def is_on_unit_circle(z: complex, tolerance: float = 1e-10) -> bool:
    """Check if complex number is on unit circle."""
    return abs(abs(z) - 1) < tolerance


def is_on_real_axis(z: complex, tolerance: float = 1e-10) -> bool:
    """Check if complex number is on real axis."""
    return abs(z.imag) < tolerance


def complex_bilinear(z: complex, ax: complex, by: complex, c: complex) -> complex:
    """Bilinear (Mobius) transformation: (az + b) / (cz + d).

    Args:
        z: Input.
        ax: a parameter.
        by: b parameter.
        c: c parameter (d=1).

    Returns:
        Transformed value.
    """
    return (ax * z + by) / (c * z + 1)


def complex_inverse(z: complex) -> complex:
    """Multiplicative inverse 1/z."""
    if z == 0:
        raise ZeroDivisionError("Cannot invert 0")
    return z.conjugate() / (z.real * z.real + z.imag * z.imag)


def complex_conj_product(a: complex, b: complex) -> complex:
    """Compute a * conjugate(b)."""
    return a * b.conjugate()


def complex_real_part(z: complex) -> float:
    """Get real part."""
    return z.real


def complex_imag_part(z: complex) -> float:
    """Get imaginary part."""
    return z.imag


def complex_argument(z: complex) -> float:
    """Get argument (phase)."""
    return z.phase


def complex_magnitude_sq(z: complex) -> float:
    """Get squared magnitude."""
    return z.real * z.real + z.imag * z.imag


class ComplexPolynomial:
    """Complex polynomial."""

    def __init__(self, coeffs: Sequence[complex | int | float]) -> None:
        self._coeffs = [complex(c) for c in coeffs]

    def eval(self, z: complex) -> complex:
        """Evaluate at z."""
        return complex_polynomial_eval(self._coeffs, z)

    def derivative(self) -> ComplexPolynomial:
        """Get derivative polynomial."""
        return ComplexPolynomial(complex_derivative(self._coeffs))

    def roots(self) -> list[complex]:
        """Find roots."""
        return complex_polynomial_roots(self._coeffs)


class ComplexVector:
    """Vector of complex numbers."""

    def __init__(self, values: Sequence[complex]) -> None:
        self._data = list(values)

    def dot(self, other: ComplexVector) -> complex:
        """Dot product with conjugate."""
        return sum(a * b.conjugate() for a, b in zip(self._data, other._data))

    def magnitude(self) -> float:
        """L2 norm."""
        return sum(abs(x) ** 2 for x in self._data) ** 0.5

    def normalize(self) -> ComplexVector:
        """Return normalized vector."""
        mag = self.magnitude()
        if mag == 0:
            raise ValueError("Cannot normalize zero vector")
        return ComplexVector([x / mag for x in self._data])


class ComplexMatrix:
    """Matrix of complex numbers."""

    def __init__(self, data: Sequence[Sequence[complex]]) -> None:
        self._data = [[complex(c) for c in row] for row in data]
        self._rows = len(data)
        self._cols = len(data[0]) if data else 0

    def transpose(self) -> ComplexMatrix:
        """Transpose."""
        result = [[self._data[r][c] for r in range(self._rows)] for c in range(self._cols)]
        return ComplexMatrix(result)

    def conjugate(self) -> ComplexMatrix:
        """Conjugate."""
        return ComplexMatrix([[c.conjugate() for c in row] for row in self._data])

    def adjoint(self) -> ComplexMatrix:
        """Conjugate transpose (Hermitian adjoint)."""
        return self.transpose().conjugate()


class ComplexTransformer:
    """2D complex plane transformations."""

    @staticmethod
    def rotate_around(z: complex, center: complex, angle: float) -> complex:
        """Rotate around center."""
        return complex_rotate(z, angle, center)

    @staticmethod
    def scale_around(z: complex, center: complex, factor: float) -> complex:
        """Scale around center."""
        return complex_scale(z, factor, center)

    @staticmethod
    def translate(dx: float, dy: float) -> complex:
        """Translation vector."""
        return complex(dx, dy)

    @staticmethod
    def reflect_over_line(z: complex, a: complex, b: complex) -> complex:
        """Reflect over line ab."""
        return complex_reflect(z, a, b)
