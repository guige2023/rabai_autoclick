"""
Signal transformation utilities.

Provides Fourier transform, Laplace transform, Z-transform,
discrete cosine transform, and Walsh-Hadamard transform.
"""

from __future__ import annotations

import math
from typing import Callable, Sequence


def next_power_of_2(n: int) -> int:
    if n <= 0:
        return 1
    return 1 << (n - 1).bit_length()


def fft(signal: list[complex]) -> list[complex]:
    """Cooley-Tukey iterative FFT."""
    n = len(signal)
    if n == 1:
        return list(signal)
    if n & (n - 1):
        padded = signal + [0.0j] * (next_power_of_2(n) - n)
        result = fft(padded)
        return result[:n]
    n2 = n // 2
    even = fft([signal[i] for i in range(0, n, 2)])
    odd = fft([signal[i] for i in range(1, n, 2)])
    result = [0.0j] * n
    for k in range(n2):
        twiddle = math.e ** (-2j * math.pi * k / n) * odd[k]
        result[k] = even[k] + twiddle
        result[k + n2] = even[k] - twiddle
    return result


def ifft(spectrum: list[complex]) -> list[complex]:
    """Inverse FFT."""
    n = len(spectrum)
    conjugated = [c.conjugate() for c in spectrum]
    time_domain = fft(conjugated)
    return [c.conjugate() / n for c in time_domain]


def dft(signal: list[complex]) -> list[complex]:
    """
    Naive O(n^2) Discrete Fourier Transform.

    For production use, prefer fft().
    """
    n = len(signal)
    result: list[complex] = []
    for k in range(n):
        sum_val = 0.0j
        for n_i, x in enumerate(signal):
            angle = -2j * math.pi * k * n_i / n
            sum_val += x * math.e ** angle
        result.append(sum_val)
    return result


def idft(spectrum: list[complex]) -> list[complex]:
    """Naive inverse DFT."""
    n = len(spectrum)
    result: list[complex] = []
    for k in range(n):
        sum_val = 0.0j
        for n_i, X in enumerate(spectrum):
            angle = 2j * math.pi * k * n_i / n
            sum_val += X * math.e ** angle
        result.append(sum_val / n)
    return result


def discrete_cosine_transform(signal: list[float]) -> list[float]:
    """
    DCT Type-II (used in JPEG, MP3).

    Args:
        signal: Input signal

    Returns:
        DCT coefficients.
    """
    n = len(signal)
    result: list[float] = []
    for k in range(n):
        ck = 0.0
        for i in range(n):
            ck += signal[i] * math.cos(math.pi * k * (2 * i + 1) / (2 * n))
        result.append(ck * (2 / n if k != 0 else 1 / n))
    return result


def inverse_dct(dct_coeffs: list[float]) -> list[float]:
    """Inverse DCT Type-II."""
    n = len(dct_coeffs)
    result: list[float] = []
    for i in range(n):
        x_i = dct_coeffs[0] / 2
        for k in range(1, n):
            x_i += dct_coeffs[k] * math.cos(math.pi * k * (2 * i + 1) / (2 * n))
        result.append(x_i)
    return result


def walsh_hadamard_transform(signal: list[float]) -> list[float]:
    """
    Walsh-Hadamard Transform (Hadamard ordered).

    Useful for signal processing and quantum computing simulation.
    """
    n = len(signal)
    if n & (n - 1):
        padded = signal + [0.0] * (next_power_of_2(n) - n)
        result = walsh_hadamard_transform(padded)
        return result[:n]

    # In-place Hadamard transform
    h = list(signal)
    step = 1
    while step < n:
        for i in range(0, n, step * 2):
            for j in range(step):
                u = h[i + j]
                v = h[i + j + step]
                h[i + j] = u + v
                h[i + j + step] = u - v
        step *= 2
    return [x / n for x in h]


def laplace_transform(
    f: Callable[[float], float],
    s: float,
    method: str = "gauss_legendre",
    n_points: int = 32,
) -> float:
    """
    Numerical Laplace transform F(s) = ∫_0^∞ f(t) * e^{-st} dt.

    Args:
        f: Time-domain function
        s: Laplace variable (complex allowed)
        method: Integration method
        n_points: Number of quadrature points

    Returns:
        Approximate F(s).
    """
    if method == "gauss_legendre":
        # Gauss-Legendre quadrature on [0, 1]
        # Use precomputed nodes and weights (order 8)
        nodes = [0.019855070, 0.10166676, 0.23723379, 0.40828268, 0.59462447, 0.75884936, 0.8822128, 0.98255826]
        weights = [0.05061427, 0.11119051, 0.15685332, 0.18134189, 0.15685332, 0.11119051, 0.05061427, 0.02783447]
        # Scale to [0, infinity] using t = -ln(u) transformation
        result = 0.0
        for u, w in zip(nodes, weights):
            t = -math.log(u + 1e-15)
            result += w * f(t) * math.e ** (-s * t) / u
        return result
    return 0.0


def z_transform(
    signal: list[float],
    z: complex,
) -> complex:
    """
    Z-transform: X(z) = Σ x[n] * z^{-n}

    Args:
        signal: Discrete-time signal
        z: Z-domain value (complex)

    Returns:
        X(z) value.
    """
    result = 0.0j
    for n, x in enumerate(signal):
        result += x * (z ** -n)
    return result


def short_time_fourier_transform(
    signal: list[float],
    window_size: int = 256,
    hop_size: int = 128,
) -> list[list[complex]]:
    """
    Short-Time Fourier Transform (STFT).

    Args:
        signal: Input signal
        window_size: Analysis window size
        hop_size: Hop between windows

    Returns:
        2D spectrogram (time frames x frequency bins).
    """
    n = len(signal)
    frames: list[list[complex]] = []
    for start in range(0, n - window_size, hop_size):
        frame = signal[start:start + window_size]
        # Apply Hann window
        windowed = [frame[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (window_size - 1))) for i in range(window_size)]
        spectrum = fft([complex(x, 0) for x in windowed])
        frames.append(spectrum[:window_size // 2])
    return frames


def spectrogram(
    signal: list[float],
    sample_rate: float = 1.0,
    window_size: int = 256,
    hop_size: int = 128,
) -> tuple[list[float], list[float], list[list[float]]]:
    """
    Compute spectrogram.

    Returns:
        Tuple of (frequencies, time_bins, magnitude_spectrogram).
    """
    stft_result = short_time_fourier_transform(signal, window_size, hop_size)
    freqs = [i * sample_rate / window_size for i in range(window_size // 2)]
    times = [i * hop_size / sample_rate for i in range(len(stft_result))]
    magnitudes = [[abs(v) for v in frame] for frame in stft_result]
    return freqs, times, magnitudes


def convolution_theorem(
    signal1: list[float],
    signal2: list[float],
) -> list[float]:
    """
    Fast convolution using FFT (O(n log n)).

    Args:
        signal1: First signal
        signal2: Second signal

    Returns:
        Convolution result.
    """
    n = len(signal1) + len(signal2) - 1
    n_fft = next_power_of_2(n)
    f1 = fft([complex(x, 0) for x in signal1] + [0.0j] * (n_fft - len(signal1)))
    f2 = fft([complex(x, 0) for x in signal2] + [0.0j] * (n_fft - len(signal2)))
    product = [a * b for a, b in zip(f1, f2)]
    result = ifft(product)
    # Take real part (imaginary part should be near zero)
    return [c.real for c in result[:n]]


def chirp_z_transform(
    signal: list[float],
    omega_start: float,
    omega_end: float,
    num_points: int,
) -> list[complex]:
    """
    Chirp Z-transform for arbitrary spiral contour evaluation.

    Args:
        signal: Input signal
        omega_start: Start frequency (radians/sample)
        omega_end: End frequency (radians/sample)
        num_points: Number of output points

    Returns:
        Z-transform values along spiral.
    """
    n = len(signal)
    result: list[complex] = []
    for k in range(num_points):
        theta = omega_start + (omega_end - omega_start) * k / (num_points - 1)
        z = math.e ** (1j * theta)
        val = z_transform(signal, z)
        result.append(val)
    return result


def goertzel_algorithm(
    signal: list[float],
    target_freq: float,
    sample_rate: float,
) -> float:
    """
    Goertzel algorithm for single DFT bin (efficient for few frequencies).

    Args:
        signal: Input signal
        target_freq: Frequency to detect (Hz)
        sample_rate: Sample rate (Hz)

    Returns:
        Magnitude at target frequency.
    """
    k = int(0.5 + (len(signal) * target_freq) / sample_rate)
    omega = 2 * math.pi * k / len(signal)
    coeff = 2 * math.cos(omega)
    s0 = 0.0
    s1 = 0.0
    s2 = 0.0
    for sample in signal:
        s0 = sample + coeff * s1 - s2
        s2 = s1
        s1 = s0
    power = s1 * s1 + s2 * s2 - coeff * s1 * s2
    return math.sqrt(power)
