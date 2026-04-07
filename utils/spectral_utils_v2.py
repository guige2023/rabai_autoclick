"""
Advanced spectral analysis utilities v2.

Extends spectral_utils.py with pitch detection, onset detection,
chroma features, and audio fingerprinting.
"""

from __future__ import annotations

import math


def detect_pitch_autocorr(signal: list[float], sample_rate: float) -> float:
    """
    Pitch detection using autocorrelation.

    Args:
        signal: Audio signal
        sample_rate: Sample rate in Hz

    Returns:
        Detected pitch in Hz.
    """
    n = len(signal)
    min_period = int(sample_rate / 1000)  # Max freq 1000 Hz
    max_period = int(sample_rate / 50)   # Min freq 50 Hz

    # Compute autocorrelation
    best_corr = -1.0
    best_lag = min_period
    for lag in range(min_period, min(max_period, n // 2)):
        corr = 0.0
        for i in range(n - lag):
            corr += signal[i] * signal[i + lag]
        if corr > best_corr:
            best_corr = corr
            best_lag = lag

    if best_lag > 0:
        return sample_rate / best_lag
    return 0.0


def detect_pitch_fft(signal: list[float], sample_rate: float) -> float:
    """
    Pitch detection using FFT peak picking.

    Args:
        signal: Audio signal
        sample_rate: Sample rate in Hz

    Returns:
        Detected pitch in Hz.
    """
    # Apply window
    n = len(signal)
    windowed = [signal[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (n - 1))) for i in range(n)]

    # Find next power of 2
    n_fft = 1 << (n - 1).bit_length()
    from transform_utils import fft
    spectrum = fft([complex(x, 0) for x in windowed] + [0.0j] * (n_fft - n))

    # Find dominant frequency
    max_mag = 0.0
    max_idx = 0
    for i in range(n_fft // 2):
        mag = math.sqrt(spectrum[i].real ** 2 + spectrum[i].imag ** 2)
        if mag > max_mag:
            max_mag = mag
            max_idx = i

    freq = max_idx * sample_rate / n_fft
    return freq


def onset_detection(
    signal: list[float],
    sample_rate: float,
    frame_size: int = 1024,
    hop_size: int = 512,
) -> list[float]:
    """
    Onset detection using spectral flux.

    Args:
        signal: Audio signal
        sample_rate: Sample rate
        frame_size: FFT frame size
        hop_size: Hop between frames

    Returns:
        List of onset strength values.
    """
    from transform_utils import fft
    n = len(signal)
    onsets: list[float] = []
    prev_mags: list[float] | None = None

    for start in range(0, n - frame_size, hop_size):
        frame = signal[start:start + frame_size]
        windowed = [frame[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (frame_size - 1))) for i in range(frame_size)]
        spectrum = fft([complex(x, 0) for x in windowed])
        mags = [math.sqrt(spectrum[i].real ** 2 + spectrum[i].imag ** 2) for i in range(frame_size // 2)]

        if prev_mags is None:
            onsets.append(0.0)
        else:
            flux = sum(max(0.0, m - p) for m, p in zip(mags, prev_mags))
            onsets.append(flux)
        prev_mags = mags

    return onsets


def chroma_features(
    signal: list[float],
    sample_rate: float,
    n_bins: int = 12,
    frame_size: int = 2048,
) -> list[list[float]]:
    """
    Compute chromagram (pitch class profile).

    Args:
        signal: Audio signal
        sample_rate: Sample rate
        n_bins: Number of bins per octave (default 12 for equal temperament)
        frame_size: FFT frame size

    Returns:
        List of chroma vectors (one per frame).
    """
    from transform_utils import fft
    n = len(signal)
    chroma: list[list[float]] = []

    for start in range(0, n - frame_size, frame_size // 2):
        frame = signal[start:start + frame_size]
        windowed = [frame[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (frame_size - 1))) for i in range(frame_size)]
        spectrum = fft([complex(x, 0) for x in windowed])
        n_fft = len(spectrum) // 2

        # Compute spectral energy in each pitch class
        chroma_vec = [0.0] * n_bins
        bin_freq = sample_rate / frame_size
        for i in range(n_fft):
            freq = i * bin_freq
            if freq < 20:
                continue
            # Map frequency to pitch class
            pitch_class = round(12 * math.log2(freq / 440.0) + 69) % 12
            mag = math.sqrt(spectrum[i].real ** 2 + spectrum[i].imag ** 2)
            chroma_vec[pitch_class] += mag

        # Normalize
        total = sum(chroma_vec)
        if total > 0:
            chroma_vec = [c / total for c in chroma_vec]
        chroma.append(chroma_vec)

    return chroma


def spectral_flux(signal: list[float], frame_size: int = 1024) -> list[float]:
    """
    Compute spectral flux (rate of spectral change).

    Returns:
        List of spectral flux values per frame.
    """
    from transform_utils import fft
    n = len(signal)
    flux: list[float] = []
    prev_energy: dict[int, float] = {}

    for start in range(0, n - frame_size, frame_size // 2):
        frame = signal[start:start + frame_size]
        windowed = [frame[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (frame_size - 1))) for i in range(frame_size)]
        spectrum = fft([complex(x, 0) for x in windowed])

        frame_flux = 0.0
        for i in range(len(spectrum) // 2):
            mag = math.sqrt(spectrum[i].real ** 2 + spectrum[i].imag ** 2)
            if i in prev_energy:
                frame_flux += max(0.0, mag - prev_energy[i])
            prev_energy[i] = mag
        flux.append(frame_flux)

    return flux


def mfcc(
    signal: list[float],
    sample_rate: float,
    n_mfcc: int = 13,
    n_fft: int = 2048,
    hop_length: int = 512,
) -> list[list[float]]:
    """
    Mel-Frequency Cepstral Coefficients.

    Simplified implementation using filterbank approximation.

    Args:
        signal: Audio signal
        sample_rate: Sample rate
        n_mfcc: Number of MFCCs to return
        n_fft: FFT size
        hop_length: Hop between frames

    Returns:
        List of MFCC vectors.
    """
    from transform_utils import fft
    n = len(signal)
    mfcc_features: list[list[float]] = []

    # Precompute mel filterbank (simplified)
    n_mels = 40
    mel_min = 0
    mel_max = 2595 * math.log10(1 + sample_rate / 2 / 700)
    mel_points = [mel_min + i * (mel_max - mel_min) / (n_mels + 1) for i in range(n_mels + 2)]
    hz_points = [700 * (10 ** (m / 2595) - 1) for m in mel_points]
    bin_points = [int(hz / sample_rate * n_fft) for hz in hz_points]

    filterbank: list[list[float]] = []
    for i in range(n_mels):
        fb = [0.0] * (n_fft // 2 + 1)
        for j in range(bin_points[i], bin_points[i + 1]):
            fb[j] = (j - bin_points[i]) / (bin_points[i + 1] - bin_points[i])
        for j in range(bin_points[i + 1], bin_points[i + 2]):
            if bin_points[i + 2] != bin_points[i + 1]:
                fb[j] = (bin_points[i + 2] - j) / (bin_points[i + 2] - bin_points[i + 1])
        filterbank.append(fb)

    for start in range(0, n - n_fft, hop_length):
        frame = signal[start:start + n_fft]
        windowed = [frame[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (n_fft - 1))) for i in range(n_fft)]
        spectrum = fft([complex(x, 0) for x in windowed])
        mags = [math.sqrt(spectrum[i].real ** 2 + spectrum[i].imag ** 2) for i in range(n_fft // 2 + 1)]

        # Apply mel filterbank
        mel_energies = []
        for fb in filterbank:
            energy = sum(fb[i] * mags[i] ** 2 for i in range(len(fb)))
            mel_energies.append(math.log(max(energy, 1e-12)))

        # DCT to get MFCCs
        mfcc_vec = []
        for k in range(n_mfcc):
            s = 0.0
            for m, E in enumerate(mel_energies):
                s += E * math.cos(math.pi * k * (m + 0.5) / n_mels)
            mfcc_vec.append(s)
        mfcc_features.append(mfcc_vec)

    return mfcc_features


def audio_fingerprint(signal: list[float], sample_rate: float) -> list[int]:
    """
    Simple audio fingerprint using spectral peaks.

    Args:
        signal: Audio signal
        sample_rate: Sample rate

    Returns:
        List of fingerprint bits.
    """
    from transform_utils import fft
    n = len(signal)
    frame_size = 2048
    fingerprints: list[int] = []

    for start in range(0, n - frame_size, frame_size // 2):
        frame = signal[start:start + frame_size]
        windowed = [frame[i] * 0.5 * (1 - math.cos(2 * math.pi * i / (frame_size - 1))) for i in range(frame_size)]
        spectrum = fft([complex(x, 0) for x in windowed])
        mags = [math.sqrt(spectrum[i].real ** 2 + spectrum[i].imag ** 2) for i in range(frame_size // 2)]

        # Find peaks
        threshold = sum(mags) / len(mags)
        peaks = [i for i in range(1, len(mags) - 1) if mags[i] > threshold and mags[i] > mags[i-1] and mags[i] > mags[i+1]]

        # Create hash from peak pairs
        for i in range(len(peaks) - 1):
            for j in range(i + 1, min(i + 3, len(peaks))):
                f1 = peaks[i]
                f2 = peaks[j]
                t = start / sample_rate
                hash_val = int(f1 * 100 + f2 + t * 10) % (2 ** 16)
                fingerprints.append(hash_val)

    return fingerprints


def beat_tracking(
    onsets: list[float],
    bpm_range: tuple[float, float] = (60.0, 200.0),
) -> list[float]:
    """
    Simple beat tracking using onset peaks.

    Args:
        onsets: Onset strength envelope
        bpm_range: (min_bpm, max_bpm)

    Returns:
        List of beat times in seconds.
    """
    min_bpm, max_bpm = bpm_range
    # Autocorrelation of onset envelope
    n = len(onsets)
    best_lag = 0
    best_corr = -1.0
    min_lag = int(60 / max_bpm * n)
    max_lag = int(60 / min_bpm * n)
    for lag in range(min_lag, min(max_lag, n // 2)):
        corr = sum(onsets[i] * onsets[i + lag] for i in range(n - lag))
        if corr > best_corr:
            best_corr = corr
            best_lag = lag

    # Beat period in frames
    beat_period = best_lag
    beats = []
    for i in range(0, n, beat_period):
        beats.append(float(i))
    return beats
