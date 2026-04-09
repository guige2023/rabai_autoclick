"""Waveform generation and analysis utilities for RabAI AutoClick.

Provides:
- Waveform generation (sine, square, sawtooth, etc.)
- Waveform analysis and features
- Envelope extraction
- Frequency estimation
"""

from typing import List, Optional, Tuple, Callable
import math


def sine_wave(freq: float, amp: float, phase: float, t: float) -> float:
    """Generate sine wave value at time t."""
    return amp * math.sin(2 * math.pi * freq * t + phase)


def square_wave(freq: float, amp: float, phase: float, t: float, duty: float = 0.5) -> float:
    """Generate square wave value."""
    cycle = (freq * t + phase / (2 * math.pi)) % 1.0
    return amp if cycle < duty else -amp


def sawtooth_wave(freq: float, amp: float, phase: float, t: float) -> float:
    """Generate sawtooth wave value."""
    cycle = (freq * t + phase / (2 * math.pi)) % 1.0
    return amp * (2 * cycle - 1)


def triangle_wave(freq: float, amp: float, phase: float, t: float) -> float:
    """Generate triangle wave value."""
    cycle = (freq * t + phase / (2 * math.pi)) % 1.0
    if cycle < 0.5:
        return amp * (4 * cycle - 1)
    else:
        return amp * (3 - 4 * cycle)


def white_noise(amp: float = 1.0) -> float:
    """Generate white noise sample."""
    import random
    return amp * (2 * random.random() - 1)


def generate_waveform(
    func: Callable[[float], float],
    freq: float,
    duration: float,
    sample_rate: float,
    amp: float = 1.0,
    phase: float = 0.0,
) -> List[float]:
    """Generate waveform samples.

    Args:
        func: Wave function (t -> value).
        freq: Frequency in Hz.
        duration: Duration in seconds.
        sample_rate: Samples per second.
        amp: Amplitude.
        phase: Phase offset.

    Returns:
        List of samples.
    """
    n = int(duration * sample_rate)
    return [func(freq, amp, phase, i / sample_rate) for i in range(n)]


def waveform_rms(waveform: List[float]) -> float:
    """Compute RMS level of waveform."""
    if not waveform:
        return 0.0
    return math.sqrt(sum(w * w for w in waveform) / len(waveform))


def waveform_peak(waveform: List[float]) -> float:
    """Compute peak amplitude."""
    if not waveform:
        return 0.0
    return max(abs(w) for w in waveform)


def waveform_zero_crossings(waveform: List[float]) -> List[int]:
    """Find zero-crossing indices."""
    crossings = []
    for i in range(len(waveform) - 1):
        if waveform[i] >= 0 and waveform[i + 1] < 0:
            crossings.append(i)
        elif waveform[i] < 0 and waveform[i + 1] >= 0:
            crossings.append(i)
    return crossings


def estimate_frequency(
    waveform: List[float],
    sample_rate: float,
) -> float:
    """Estimate fundamental frequency from zero crossings.

    Args:
        waveform: Audio samples.
        sample_rate: Samples per second.

    Returns:
        Estimated frequency in Hz.
    """
    crossings = waveform_zero_crossings(waveform)
    if len(crossings) < 2:
        return 0.0
    # Average period from zero crossings
    periods = [crossings[i + 1] - crossings[i] for i in range(len(crossings) - 1)]
    avg_period = sum(periods) / len(periods)
    return sample_rate / avg_period if avg_period > 0 else 0.0


def waveform_envelope(
    waveform: List[float],
    window_size: int = 512,
) -> List[float]:
    """Extract amplitude envelope using peak detection.

    Args:
        waveform: Input samples.
        window_size: Analysis window size.

    Returns:
        Envelope values.
    """
    if not waveform:
        return []
    n = len(waveform)
    envelope: List[float] = []
    for i in range(0, n, window_size // 2):
        window = waveform[i:i + window_size]
        envelope.append(max(abs(w) for w in window) if window else 0.0)
    return envelope


def hann_window(n: int) -> List[float]:
    """Generate Hann window."""
    return [0.5 * (1 - math.cos(2 * math.pi * i / (n - 1))) for i in range(n)]


def hamming_window(n: int) -> List[float]:
    """Generate Hamming window."""
    return [0.54 - 0.46 * math.cos(2 * math.pi * i / (n - 1)) for i in range(n)]


def apply_window_to_waveform(
    waveform: List[float],
    window_func: Callable[[int], List[float]],
) -> List[float]:
    """Apply window function to waveform.

    Args:
        waveform: Input samples.
        window_func: Window function (n -> window).

    Returns:
        Windowed waveform.
    """
    w = window_func(len(waveform))
    return [waveform[i] * w[i] for i in range(len(waveform))]


def synthesize_chord(
    freqs: List[float],
    duration: float,
    sample_rate: float,
    amp: float = 0.3,
) -> List[float]:
    """Synthesize a chord from multiple frequencies.

    Args:
        freqs: List of component frequencies.
        duration: Duration in seconds.
        sample_rate: Samples per second.
        amp: Per-component amplitude.

    Returns:
        Mixed waveform.
    """
    n = int(duration * sample_rate)
    result = [0.0] * n
    for freq in freqs:
        for i in range(n):
            t = i / sample_rate
            result[i] += amp * math.sin(2 * math.pi * freq * t)
    return result


def pluck_envelope(
    waveform: List[float],
    decay: float = 0.995,
) -> List[float]:
    """Apply pluck (decaying amplitude) envelope to waveform.

    Args:
        waveform: Input samples.
        decay: Per-sample decay factor (< 1.0).

    Returns:
        Decayed waveform.
    """
    result = []
    gain = 1.0
    for w in waveform:
        result.append(w * gain)
        gain *= decay
    return result


def beat_frequency(f1: float, f2: float) -> float:
    """Compute beat frequency between two tones."""
    return abs(f1 - f2)


def waveform_fft_magnitudes(
    waveform: List[float],
) -> List[float]:
    """Compute FFT magnitude spectrum.

    Args:
        waveform: Input samples (will pad to power of 2).

    Returns:
        Magnitude spectrum.
    """
    from utils.fourier_utils import fft, next_power_of_2
    n = len(waveform)
    padded_n = next_power_of_2(n)
    padded = waveform[:] + [0.0] * (padded_n - n)
    spectrum = fft([complex(x, 0) for x in padded])
    half = padded_n // 2
    return [abs(spectrum[i]) for i in range(half)]
