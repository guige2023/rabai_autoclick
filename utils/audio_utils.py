"""Audio signal processing utilities for RabAI AutoClick.

Provides:
- Audio buffer utilities
- RMS and peak level detection
- Simple tone generation
- Audio event detection
"""

from typing import List, Optional, Tuple
import math
import struct


def rms_level(samples: List[float]) -> float:
    """Compute RMS (Root Mean Square) level of audio samples.

    Args:
        samples: Audio sample values (-1.0 to 1.0).

    Returns:
        RMS level (0.0 to 1.0).
    """
    if not samples:
        return 0.0
    return math.sqrt(sum(s * s for s in samples) / len(samples))


def peak_level(samples: List[float]) -> float:
    """Compute peak level of audio samples.

    Args:
        samples: Audio sample values.

    Returns:
        Peak amplitude (0.0 to 1.0).
    """
    if not samples:
        return 0.0
    return max(abs(s) for s in samples)


def dbfs(samples: List[float]) -> float:
    """Compute dBFS (decibels relative to full scale) level.

    Args:
        samples: Audio samples.

    Returns:
        dBFS value (-inf to 0).
    """
    rms = rms_level(samples)
    if rms < 1e-10:
        return float("-inf")
    return 20 * math.log10(rms)


def generate_sine_wave(
    frequency: float,
    duration: float,
    sample_rate: float = 44100.0,
    amplitude: float = 0.5,
) -> List[float]:
    """Generate a sine wave audio signal.

    Args:
        frequency: Frequency in Hz.
        duration: Duration in seconds.
        sample_rate: Samples per second.
        amplitude: Amplitude (0.0 to 1.0).

    Returns:
        List of audio samples.
    """
    n_samples = int(duration * sample_rate)
    return [
        amplitude * math.sin(2 * math.pi * frequency * i / sample_rate)
        for i in range(n_samples)
    ]


def generate_square_wave(
    frequency: float,
    duration: float,
    sample_rate: float = 44100.0,
    amplitude: float = 0.5,
    duty_cycle: float = 0.5,
) -> List[float]:
    """Generate a square wave.

    Args:
        frequency: Frequency in Hz.
        duration: Duration in seconds.
        sample_rate: Samples per second.
        amplitude: Amplitude (0.0 to 1.0).
        duty_cycle: Fraction of cycle that is high.

    Returns:
        List of audio samples.
    """
    n_samples = int(duration * sample_rate)
    period = sample_rate / frequency
    return [
        amplitude if (i % period) < (period * duty_cycle) else -amplitude
        for i in range(n_samples)
    ]


def mix_signals(signals: List[List[float]], gains: Optional[List[float]] = None) -> List[float]:
    """Mix multiple audio signals together.

    Args:
        signals: List of audio sample lists.
        gains: Per-signal gain (default equal).

    Returns:
        Mixed audio samples.
    """
    if not signals:
        return []
    max_len = max(len(s) for s in signals)
    if gains is None:
        gains = [1.0 / len(signals)] * len(signals)

    result: List[float] = [0.0] * max_len
    for sig, g in zip(signals, gains):
        for i, s in enumerate(sig):
            result[i] += s * g
    return result


def normalize(samples: List[float], target_peak: float = 0.95) -> List[float]:
    """Normalize audio to target peak level.

    Args:
        samples: Input audio samples.
        target_peak: Desired peak amplitude.

    Returns:
        Normalized samples.
    """
    peak = peak_level(samples)
    if peak < 1e-10:
        return samples[:]
    scale = target_peak / peak
    return [s * scale for s in samples]


def fade_in(samples: List[float], duration_samples: int) -> List[float]:
    """Apply fade-in to audio.

    Args:
        samples: Input audio.
        duration_samples: Fade length in samples.

    Returns:
        Faded audio.
    """
    n = min(duration_samples, len(samples))
    return [
        samples[i] * i / n if i < n else samples[i]
        for i in range(len(samples))
    ]


def fade_out(samples: List[float], duration_samples: int) -> List[float]:
    """Apply fade-out to audio.

    Args:
        samples: Input audio.
        duration_samples: Fade length in samples.

    Returns:
        Faded audio.
    """
    n = min(duration_samples, len(samples))
    start = len(samples) - n
    return [
        samples[i] * (len(samples) - i) / n if i >= start else samples[i]
        for i in range(len(samples))
    ]


def detect_silence(
    samples: List[float],
    threshold_db: float = -40.0,
    min_duration: float = 0.1,
    sample_rate: float = 44100.0,
) -> List[Tuple[int, int]]:
    """Detect silent regions in audio.

    Args:
        samples: Audio samples.
        threshold_db: Silence threshold in dBFS.
        min_duration: Minimum silence duration in seconds.
        sample_rate: Samples per second.

    Returns:
        List of (start_sample, end_sample) tuples.
    """
    threshold_linear = 10 ** (threshold_db / 20)
    min_samples = int(min_duration * sample_rate)
    regions: List[Tuple[int, int]] = []

    in_silence = False
    silence_start = 0

    for i, s in enumerate(samples):
        is_silent = abs(s) < threshold_linear
        if is_silent and not in_silence:
            in_silence = True
            silence_start = i
        elif not is_silent and in_silence:
            in_silence = False
            if i - silence_start >= min_samples:
                regions.append((silence_start, i))

    if in_silence and len(samples) - silence_start >= min_samples:
        regions.append((silence_start, len(samples)))

    return regions


def envelope_follower(
    samples: List[float],
    attack: float = 0.001,
    release: float = 0.1,
) -> List[float]:
    """Extract amplitude envelope from audio.

    Args:
        samples: Audio samples.
        attack: Attack time constant (seconds).
        release: Release time constant (seconds).

    Returns:
        Envelope values (0.0 to 1.0).
    """
    if not samples:
        return []
    a_coef = math.exp(-1.0 / (attack * 44100)) if attack > 0 else 0.0
    r_coef = math.exp(-1.0 / (release * 44100)) if release > 0 else 0.0

    envelope: List[float] = [0.0] * len(samples)
    env = 0.0
    for i, s in enumerate(samples):
        level = abs(s)
        if level > env:
            env = a_coef * env + (1 - a_coef) * level
        else:
            env = r_coef * env + (1 - r_coef) * level
        envelope[i] = env
    return envelope


def stereo_to_mono(left: List[float], right: List[float]) -> List[float]:
    """Convert stereo to mono by averaging channels.

    Args:
        left: Left channel samples.
        right: Right channel samples.

    Returns:
        Mono samples.
    """
    n = min(len(left), len(right))
    return [(left[i] + right[i]) / 2.0 for i in range(n)]


def pan_signal(
    samples: List[float],
    pan: float,
) -> Tuple[List[float], List[float]]:
    """Pan a mono signal to stereo.

    Args:
        samples: Mono audio samples.
        pan: Pan value (-1.0 left to 1.0 right).

    Returns:
        (left, right) stereo channels.
    """
    left_gain = math.sqrt((1.0 - pan) / 2.0)
    right_gain = math.sqrt((1.0 + pan) / 2.0)
    return ([s * left_gain for s in samples], [s * right_gain for s in samples])


def add_reverb(
    samples: List[float],
    delay_ms: float = 50.0,
    decay: float = 0.3,
    num_echoes: int = 4,
    sample_rate: float = 44100.0,
) -> List[float]:
    """Add simple reverb using multiple delay lines.

    Args:
        samples: Input audio.
        delay_ms: Base delay in milliseconds.
        decay: Decay factor per echo.
        num_echoes: Number of echoes.
        sample_rate: Samples per second.

    Returns:
        Audio with reverb.
    """
    delay_samples = int(delay_ms * sample_rate / 1000.0)
    result = samples[:]
    for n in range(1, num_echoes + 1):
        d = delay_samples * n
        gain = decay ** n
        for i in range(len(samples)):
            idx = i + d
            if idx < len(result):
                result[idx] += samples[i] * gain
    return result
