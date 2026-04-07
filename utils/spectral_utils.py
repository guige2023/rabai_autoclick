"""
Spectral analysis utilities.

Provides power spectral density, periodogram, Welch's method,
spectral moments, and frequency domain features.
"""

from __future__ import annotations

import math
from typing import Callable


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


def periodogram(
    signal: list[float],
    sample_rate: float = 1.0,
    window: str = "hann",
) -> tuple[list[float], list[float]]:
    """
    Compute periodogram power spectral density.

    Args:
        signal: Input signal
        sample_rate: Sample rate in Hz
        window: Window type ('hann', 'hamming', 'blackman', 'rect')

    Returns:
        Tuple of (frequencies, power_spectrum).
    """
    n = len(signal)
    n_fft = next_power_of_2(n)

    # Apply window
    if window == "hann":
        w = [0.5 * (1 - math.cos(2 * math.pi * i / (n - 1))) for i in range(n)]
    elif window == "hamming":
        w = [0.54 - 0.46 * math.cos(2 * math.pi * i / (n - 1)) for i in range(n)]
    elif window == "blackman":
        w = [0.42 - 0.5 * math.cos(2 * math.pi * i / (n - 1)) + 0.08 * math.cos(4 * math.pi * i / (n - 1)) for i in range(n)]
    else:
        w = [1.0] * n

    windowed = [signal[i] * w[i] for i in range(n)]
    windowed += [0.0j] * (n_fft - n)
    spectrum = fft(windowed)

    # Power spectrum (one-sided)
    psd: list[float] = []
    freqs: list[float] = []
    for i in range(n_fft // 2):
        power = (spectrum[i].real ** 2 + spectrum[i].imag ** 2) / n_fft
        freq = i * sample_rate / n_fft
        if i > 0:
            power *= 2
        psd.append(power)
        freqs.append(freq)
    return freqs, psd


def welch_psd(
    signal: list[float],
    nperseg: int = 256,
    noverlap: int | None = None,
    sample_rate: float = 1.0,
) -> tuple[list[float], list[float]]:
    """
    Welch's method for PSD estimation (averaged periodograms).

    Args:
        signal: Input signal
        nperseg: Segment length
        noverlap: Overlap between segments
        sample_rate: Sample rate in Hz

    Returns:
        Tuple of (frequencies, psd).
    """
    if noverlap is None:
        noverlap = nperseg // 2
    n = len(signal)
    freqs_list: list[float] = []
    psd_sum: list[float] = []
    count = 0
    for start in range(0, n - nperseg, nperseg - noverlap):
        segment = signal[start:start + nperseg]
        freqs, psd = periodogram(segment, sample_rate, window="hann")
        if count == 0:
            psd_sum = [0.0] * len(psd)
        for i in range(len(psd)):
            psd_sum[i] += psd[i]
        count += 1
        freqs_list = freqs
    if count > 0:
        psd_sum = [p / count for p in psd_sum]
    return freqs_list, psd_sum


def spectral_moments(psd: list[float], freqs: list[float]) -> dict[str, float]:
    """
    Compute spectral moments from PSD.

    Returns:
        Dictionary with m0 (spectral power), m1 (spectral centroid/frequency),
        m2 (spectral spread), m-1 (spectral flatness-related).
    """
    m0 = sum(psd)
    if m0 < 1e-12:
        return {"m0": 0.0, "m1": 0.0, "m2": 0.0, "m-1": 0.0}
    m1 = sum(psd[i] * freqs[i] for i in range(len(freqs))) / m0
    m2 = sum(psd[i] * (freqs[i] - m1) ** 2 for i in range(len(freqs))) / m0
    # Geometric mean for m-1 (spectral flatness)
    import math
    log_sum = sum(math.log(max(p, 1e-12)) for p in psd if p > 0)
    m_minus1 = math.exp(log_sum / len(psd)) if psd else 0.0
    return {"m0": m0, "m1": m1, "m2": m2, "m-1": m_minus1}


def spectral_entropy(psd: list[float]) -> float:
    """
    Spectral entropy (Shannon entropy of normalized PSD).

    Args:
        psd: Power spectral density

    Returns:
        Spectral entropy value.
    """
    total = sum(psd)
    if total < 1e-12:
        return 0.0
    probs = [p / total for p in psd if p > 0]
    if not probs:
        return 0.0
    return -sum(p * math.log(p) for p in probs if p > 0) / math.log(len(probs))


def spectral_flatness(psd: list[float]) -> float:
    """
    Spectral flatness: ratio of geometric mean to arithmetic mean.

    Returns:
        Value between 0 (tonal) and 1 (noisy).
    """
    total = sum(psd)
    n = len(psd)
    if total < 1e-12:
        return 0.0
    log_sum = sum(math.log(max(p, 1e-12)) for p in psd if p > 0)
    geom_mean = math.exp(log_sum / n)
    arith_mean = total / n
    if arith_mean < 1e-12:
        return 0.0
    return geom_mean / arith_mean


def spectral_rolloff(psd: list[float], freqs: list[float], threshold: float = 0.85) -> float:
    """
    Find frequency below which threshold% of total power exists.

    Args:
        psd: Power spectral density
        freqs: Frequency bins
        threshold: Roll-off percentage (default 85%)

    Returns:
        Roll-off frequency in Hz.
    """
    total = sum(psd)
    if total < 1e-12:
        return 0.0
    cumulative = 0.0
    for p, f in zip(psd, freqs):
        cumulative += p
        if cumulative >= threshold * total:
            return f
    return freqs[-1] if freqs else 0.0


def spectral_band_energy(
    psd: list[float],
    freqs: list[float],
    low_freq: float,
    high_freq: float,
) -> float:
    """Compute energy in a frequency band."""
    return sum(
        psd[i] for i in range(len(freqs))
        if low_freq <= freqs[i] <= high_freq
    )


def band_power_ratio(
    psd: list[float],
    freqs: list[float],
    low_band: tuple[float, float],
    high_band: tuple[float, float],
) -> float:
    """
    Ratio of power in two frequency bands.

    Useful for EEG analysis, audio classification.
    """
    power_low = spectral_band_energy(psd, freqs, *low_band)
    power_high = spectral_band_energy(psd, freqs, *high_band)
    if power_high < 1e-12:
        return 0.0
    return power_low / power_high


def peak_frequency(psd: list[float], freqs: list[float]) -> float:
    """Find dominant frequency."""
    if not psd:
        return 0.0
    max_idx = max(range(len(psd)), key=lambda i: psd[i])
    return freqs[max_idx] if max_idx < len(freqs) else 0.0


def coherence_function(
    x: list[float],
    y: list[float],
    sample_rate: float = 1.0,
    nperseg: int = 256,
) -> tuple[list[float], list[float]]:
    """
    Coherence function between two signals.

    Args:
        x: First signal
        y: Second signal
        sample_rate: Sample rate
        nperseg: FFT segment length

    Returns:
        Tuple of (frequencies, coherence).
    """
    _, psd_x = welch_psd(x, nperseg=nperseg, sample_rate=sample_rate)
    _, psd_y = welch_psd(y, nperseg=nperseg, sample_rate=sample_rate)

    n = len(x)
    n_fft = next_power_of_2(nperseg)
    noverlap = nperseg // 2

    # Cross-spectral density
    csd_sum = None
    count = 0
    for start in range(0, n - nperseg, nperseg - noverlap):
        x_seg = x[start:start + nperseg]
        y_seg = y[start:start + nperseg]
        w = [0.5 * (1 - math.cos(2 * math.pi * i / (nperseg - 1))) for i in range(nperseg)]
        x_win = [x_seg[i] * w[i] for i in range(nperseg)]
        y_win = [y_seg[i] * w[i] for i in range(nperseg)]
        X = fft(x_win + [0.0j] * (n_fft - nperseg))
        Y = fft(y_win + [0.0j] * (n_fft - nperseg))
        csd = [X[i] * Y[i].conjugate() for i in range(n_fft // 2)]
        if csd_sum is None:
            csd_sum = [abs(c) ** 2 for c in csd]
        else:
            csd_sum = [csd_sum[i] + abs(c) ** 2 for i, c in enumerate(csd)]
        count += 1

    if csd_sum is None or count == 0:
        return [], []

    coherence = []
    freqs_out = []
    for i in range(n_fft // 2):
        freq = i * sample_rate / n_fft
        Pxy = csd_sum[i] / count
        Pxx = psd_x[i] if i < len(psd_x) else 0.0
        Pyy = psd_y[i] if i < len(psd_y) else 0.0
        if Pxx > 0 and Pyy > 0:
            coh = Pxy / (Pxx * Pyy) ** 0.5
            coherence.append(min(1.0, max(0.0, coh)))
            freqs_out.append(freq)
    return freqs_out, coherence
