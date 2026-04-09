"""Fourier transform utilities for RabAI AutoClick.

Provides:
- FFT for 1D signal analysis
- Frequency spectrum analysis
- Windowing functions
- Convolution operations
"""

from typing import Callable, List, Optional, Tuple
import math


def fft(signal: List[complex]) -> List[complex]:
    """Compute Fast Fourier Transform using Cooley-Tukey.

    Args:
        signal: Input signal (length must be power of 2).

    Returns:
        Frequency domain representation.
    """
    n = len(signal)
    if n == 1:
        return signal[:]
    if n % 2 != 0:
        # Pad to next power of 2
        padded = signal[:] + [complex(0)] * (next_power_of_2(n) - n)
        n = len(padded)
    else:
        padded = signal

    half = n // 2
    even = fft(padded[0::2])
    odd = fft(padded[1::2])

    result: List[complex] = [complex(0)] * n
    for k in range(half):
        exp = complex(math.cos(2 * math.pi * k / n), -math.sin(2 * math.pi * k / n))
        result[k] = even[k] + exp * odd[k]
        result[k + half] = even[k] - exp * odd[k]
    return result


def ifft(spectrum: List[complex]) -> List[complex]:
    """Compute inverse FFT.

    Args:
        spectrum: Frequency domain input.

    Returns:
        Time domain signal.
    """
    n = len(spectrum)
    conjugated = [c.conjugate() for c in spectrum]
    forward = fft(conjugated)
    return [c.conjugate() / n for c in forward]


def next_power_of_2(n: int) -> int:
    """Get next power of 2 >= n."""
    p = 1
    while p < n:
        p *= 2
    return p


def fft_magnitude(signal: List[float]) -> List[float]:
    """Compute magnitude spectrum of signal.

    Args:
        signal: Input time-domain signal.

    Returns:
        Magnitude values (first half only, DC to Nyquist).
    """
    complex_signal = [complex(x, 0) for x in signal]
    spectrum = fft(complex_signal)
    n = len(spectrum)
    half = n // 2
    return [abs(spectrum[i]) for i in range(half)]


def fft_phase(signal: List[float]) -> List[float]:
    """Compute phase spectrum of signal.

    Args:
        signal: Input time-domain signal.

    Returns:
        Phase values in radians.
    """
    complex_signal = [complex(x, 0) for x in signal]
    spectrum = fft(complex_signal)
    n = len(spectrum)
    half = n // 2
    return [math.atan2(spectrum[i].imag, spectrum[i].real) for i in range(half)]


def spectral_energy(magnitude: List[float]) -> float:
    """Compute total spectral energy.

    Args:
        magnitude: FFT magnitude values.

    Returns:
        Total energy.
    """
    return sum(m ** 2 for m in magnitude)


def dominant_frequency(
    magnitude: List[float],
    sample_rate: float,
) -> Tuple[float, float]:
    """Find dominant frequency and its magnitude.

    Args:
        magnitude: FFT magnitude values.
        sample_rate: Samples per second.

    Returns:
        (frequency, magnitude) of strongest component.
    """
    if not magnitude:
        return (0.0, 0.0)
    max_idx = max(range(len(magnitude)), key=lambda i: magnitude[i])
    freq = max_idx * sample_rate / (2 * len(magnitude))
    return (freq, magnitude[max_idx])


def apply_window(signal: List[float], window_type: str = "hann") -> List[float]:
    """Apply windowing function to signal.

    Args:
        signal: Input signal.
        window_type: 'hann', 'hamming', 'blackman', 'bartlett', 'flat'.

    Returns:
        Windowed signal.
    """
    n = len(signal)
    w: List[float]

    if window_type == "hann":
        w = [0.5 * (1 - math.cos(2 * math.pi * i / (n - 1))) for i in range(n)]
    elif window_type == "hamming":
        w = [0.54 - 0.46 * math.cos(2 * math.pi * i / (n - 1)) for i in range(n)]
    elif window_type == "blackman":
        a0 = 0.42
        a1 = 0.5
        a2 = 0.08
        w = [a0 - a1 * math.cos(2 * math.pi * i / (n - 1)) +
             a2 * math.cos(4 * math.pi * i / (n - 1)) for i in range(n)]
    elif window_type == "bartlett":
        w = [1 - abs(2 * i / (n - 1) - 1) for i in range(n)]
    elif window_type == "flat":
        # Flat top window approximation
        a0, a1, a2, a3, a4 = 0.2156, 0.4160, 0.2781, 0.0836, 0.0066
        w = [a0 - a1 * math.cos(2 * math.pi * i / (n - 1)) +
             a2 * math.cos(4 * math.pi * i / (n - 1)) -
             a3 * math.cos(6 * math.pi * i / (n - 1)) +
             a4 * math.cos(8 * math.pi * i / (n - 1)) for i in range(n)]
    else:
        w = [1.0] * n

    return [signal[i] * w[i] for i in range(n)]


def bandpass_filter(
    signal: List[float],
    low_freq: float,
    high_freq: float,
    sample_rate: float,
) -> List[float]:
    """Apply simple bandpass filter in frequency domain.

    Args:
        signal: Input time-domain signal.
        low_freq: Low cutoff frequency (Hz).
        high_freq: High cutoff frequency (Hz).
        sample_rate: Samples per second.

    Returns:
        Filtered signal.
    """
    n = len(signal)
    padded = signal + [0.0] * n
    spectrum = fft([complex(x, 0) for x in padded])

    freq_resolution = sample_rate / (2 * n)
    low_bin = max(1, int(low_freq / freq_resolution))
    high_bin = min(n - 1, int(high_freq / freq_resolution))

    filtered: List[complex] = [complex(0)] * (2 * n)
    for i in range(low_bin, high_bin + 1):
        filtered[i] = spectrum[i]
        filtered[2 * n - i] = spectrum[2 * n - i]

    result = ifft(filtered)
    return [result[i].real for i in range(n)]


def convolution(
    signal: List[float],
    kernel: List[float],
    mode: str = "full",
) -> List[float]:
    """Compute convolution of signal and kernel.

    Args:
        signal: Input signal.
        kernel: Convolution kernel.
        mode: 'full', 'same', or 'valid'.

    Returns:
        Convolved signal.
    """
    n, m = len(signal), len(kernel)
    result_len = n + m - 1
    result: List[float] = [0.0] * result_len

    for i in range(n):
        for j in range(m):
            result[i + j] += signal[i] * kernel[j]

    if mode == "same":
        start = (m - 1) // 2
        return result[start:start + n]
    elif mode == "valid":
        start = m - 1
        return result[start:start + max(n - m + 1, 0)]
    return result


def autocorr(signal: List[float], max_lag: Optional[int] = None) -> List[float]:
    """Compute autocorrelation function.

    Args:
        signal: Input signal.
        max_lag: Maximum lag to compute (default: len(signal) - 1).

    Returns:
        Autocorrelation values.
    """
    n = len(signal)
    if max_lag is None:
        max_lag = n - 1
    max_lag = min(max_lag, n - 1)

    result: List[float] = []
    for lag in range(max_lag + 1):
        ac = sum(signal[i] * signal[i + lag] for i in range(n - lag))
        result.append(ac)
    return result


def crosscorr(signal1: List[float], signal2: List[float], max_lag: int = 50) -> List[float]:
    """Compute cross-correlation of two signals.

    Args:
        signal1: First signal.
        signal2: Second signal.
        max_lag: Maximum lag to compute.

    Returns:
        Cross-correlation values for lags 0..max_lag.
    """
    n1, n2 = len(signal1), len(signal2)
    result: List[float] = []

    for lag in range(max_lag + 1):
        if lag < n2:
            cc = sum(signal1[i] * signal2[i + lag] for i in range(n1 - lag))
        else:
            cc = 0.0
        result.append(cc)
    return result
