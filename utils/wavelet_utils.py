"""Wavelet utilities for RabAI AutoClick.

Provides:
- Wavelet transformation helpers
- Signal processing utilities
- Noise reduction
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    List,
)


def simple_smooth(data: List[float], window: int = 3) -> List[float]:
    """Apply simple moving average smoothing.

    Args:
        data: Input signal.
        window: Smoothing window size.

    Returns:
        Smoothed signal.
    """
    if window < 1:
        return data[:]

    result: List[float] = []
    half = window // 2

    for i in range(len(data)):
        start = max(0, i - half)
        end = min(len(data), i + half + 1)
        result.append(sum(data[start:end]) / (end - start))

    return result


def haar_wavelet_transform(
    signal: List[float],
    levels: int = 1,
) -> List[float]:
    """Apply Haar wavelet transform.

    Args:
        signal: Input signal.
        levels: Number of transform levels.

    Returns:
        Transformed signal.
    """
    result = signal[:]

    for _ in range(levels):
        if len(result) < 2:
            break
        new_result: List[float] = []
        for i in range(0, len(result) - 1, 2):
            avg = (result[i] + result[i + 1]) / 2
            diff = (result[i] - result[i + 1]) / 2
            new_result.append(avg)
            new_result.append(diff)
        result = new_result

    return result


def threshold_denoise(
    signal: List[float],
    threshold: float,
) -> List[float]:
    """Apply hard thresholding for noise reduction.

    Args:
        signal: Input signal.
        threshold: Threshold value.

    Returns:
        Denoised signal.
    """
    return [0.0 if abs(x) < threshold else x for x in signal]


__all__ = [
    "simple_smooth",
    "haar_wavelet_transform",
    "threshold_denoise",
]
