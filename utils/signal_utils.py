"""
Signal processing utilities.

Provides FFT, convolution, autocorrelation, cross-correlation,
spectral analysis, and windowing functions.
"""

from __future__ import annotations

import math
from typing import Callable


def next_power_of_2(n: int) -> int:
    """Find the next power of 2 >= n."""
    if n <= 0:
        return 1
    return 1 << (n - 1).bit_length()


def fft(signal: list[complex]) -> list[complex]:
    """
    Cooley-Tukey iterative FFT.

    Args:
        signal: Input complex signal (length must be power of 2)

    Returns:
        Frequency-domain representation.
    """
    n = len(signal)
    if n == 1:
        return list(signal)
    if n & (n - 1):
        # Pad to next power of 2
        padded = signal + [0.0j] * (next_power_of_2(n) - n)
        return fft(padded)[:n]
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
    """
    Inverse FFT.

    Args:
        spectrum: Frequency-domain signal

    Returns:
        Time-domain representation.
    """
    n = len(spectrum)
    conjugated = [c.conjugate() for c in spectrum]
    time_domain = fft(conjugated)
    return [c.conjugate() / n for c in time_domain]


def fft_magnitude(signal: list[float]) -> list[float]:
    """Compute FFT magnitude spectrum."""
    complex_signal = [complex(x, 0.0) for x in signal]
    spectrum = fft(complex_signal)
    n = len(spectrum)
    return [abs(spectrum[i]) for i in range(n // 2)]


def fft_phase(signal: list[float]) -> list[float]:
    """Compute FFT phase spectrum."""
    complex_signal = [complex(x, 0.0) for x in signal]
    spectrum = fft(complex_signal)
    n = len(spectrum)
    return [math.atan2(spectrum[i].imag, spectrum[i].real) for i in range(n // 2)]


def convolve(a: list[float], b: list[float]) -> list[float]:
    """
    Linear convolution of two signals.

    Args:
        a: First signal
        b: Second signal

    Returns:
        Convolution result of length len(a) + len(b) - 1.
    """
    n, m = len(a), len(b)
    result = [0.0] * (n + m - 1)
    for i in range(n):
        for j in range(m):
            result[i + j] += a[i] * b[j]
    return result


def cross_correlate(a: list[float], b: list[float], mode: str = "full") -> list[float]:
    """
    Cross-correlation of two signals.

    Args:
        a: First signal
        b: Second signal
        mode: 'full', 'same', or 'valid'

    Returns:
        Cross-correlation result.
    """
    n, m = len(a), len(b)
    if mode == "full":
        result = [0.0] * (n + m - 1)
        for i in range(n):
            for j in range(m):
                result[i + j] += a[i] * b[j]
    elif mode == "same":
        result = [0.0] * max(n, m)
        offset = abs(n - m) // 2
        for i in range(min(n, m)):
            for j in range(min(n, m)):
                if i + offset < len(result) and j < len(a) and i < len(b):
                    result[i + offset] += a[j] * b[i]
    else:  # valid
        min_len = max(0, max(n, m) - min(n, m) + 1)
        result = [0.0] * min_len
        offset = min(n, m) - 1
        for i in range(min_len):
            result[i] = sum(a[i + k] * b[k] for k in range(min(n, m)) if i + k < n and k < m)
    return result


def autocorrelation(signal: list[float], mode: str = "full") -> list[float]:
    """Autocorrelation of a signal."""
    return cross_correlate(signal, signal, mode)


def low_pass_filter(
    signal: list[float],
    cutoff: float,
    sample_rate: float,
    order: int = 5,
) -> list[float]:
    """
    Simple FIR low-pass filter using windowed sinc.

    Args:
        signal: Input signal
        cutoff: Cutoff frequency (Hz)
        sample_rate: Sample rate (Hz)
        order: Filter order (must be odd)

    Returns:
        Filtered signal.
    """
    if order % 2 == 0:
        order += 1
    n = order
    fc = cutoff / sample_rate
    kernel: list[float] = []
    for i in range(n):
        m = i - n // 2
        if m == 0:
            kernel.append(2.0 * math.pi * fc)
        else:
            kernel.append(math.sin(2.0 * math.pi * fc * m) / m)
    # Hamming window
    for i in range(n):
        window = 0.54 - 0.46 * math.cos(2.0 * math.pi * i / (n - 1))
        kernel[i] *= window
    # Normalize
    s = sum(kernel)
    kernel = [k / s for k in kernel]
    return convolve(signal, kernel)


def high_pass_filter(
    signal: list[float],
    cutoff: float,
    sample_rate: float,
    order: int = 5,
) -> list[float]:
    """
    FIR high-pass filter.

    Args:
        signal: Input signal
        cutoff: Cutoff frequency (Hz)
        sample_rate: Sample rate (Hz)
        order: Filter order (must be odd)

    Returns:
        Filtered signal.
    """
    lp = low_pass_filter(signal, cutoff, sample_rate, order)
    # Subtract from original (high-pass = original - low-pass)
    return [a - b for a, b in zip(signal, lp)]


def band_pass_filter(
    signal: list[float],
    low_cutoff: float,
    high_cutoff: float,
    sample_rate: float,
    order: int = 5,
) -> list[float]:
    """Band-pass filter."""
    lp_low = low_pass_filter(signal, low_cutoff, sample_rate, order)
    return low_pass_filter(lp_low, high_cutoff, sample_rate, order)


def moving_average_smooth(signal: list[float], window: int) -> list[float]:
    """Simple moving average smoothing."""
    if window < 1:
        return list(signal)
    result: list[float] = []
    half = window // 2
    for i in range(len(signal)):
        start = max(0, i - half)
        end = min(len(signal), i + half + 1)
        result.append(sum(signal[start:end]) / (end - start))
    return result


def envelope(signal: list[float], sample_rate: float) -> list[float]:
    """
    Extract signal envelope using Hilbert transform approximation.

    Args:
        signal: Input signal
        sample_rate: Sample rate

    Returns:
        Envelope (instantaneous amplitude).
    """
    n = len(signal)
    analytic = [complex(s, 0) for s in signal]
    # Simple Hilbert: shift by 90 degrees for positive frequencies
    fft_vals = fft(analytic)
    for i in range(1, n):
        if i < n // 2:
            fft_vals[i] *= 2.0
        elif i > n // 2:
            fft_vals[i] = 0.0
    analytic = ifft(fft_vals)
    return [abs(c) for c in analytic]


def spectral_centroid(signal: list[float], sample_rate: float) -> float:
    """
    Compute spectral centroid (center of mass of spectrum).

    Returns:
        Spectral centroid in Hz.
    """
    n = len(signal)
    freqs = [i * sample_rate / n for i in range(n // 2)]
    mag = fft_magnitude(signal)
    total_mag = sum(mag)
    if total_mag < 1e-12:
        return 0.0
    return sum(f * m for f, m in zip(freqs, mag)) / total_mag


def zero_crossing_rate(signal: list[float]) -> list[float]:
    """Compute zero crossing rate over windows."""
    if not signal:
        return []
    zcr: list[float] = []
    window = 256
    step = window // 2
    for i in range(0, len(signal) - window, step):
        window_signal = signal[i:i + window]
        zc = sum(
            1 for j in range(len(window_signal) - 1)
            if window_signal[j] * window_signal[j + 1] < 0
        )
        zcr.append(zc / (len(window_signal) - 1))
    return zcr
