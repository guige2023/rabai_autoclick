"""
Wavelet transform utilities.

Provides Haar, Daubechies, and Morlet wavelets, continuous wavelet transform,
and discrete wavelet transform with denoising.
"""

from __future__ import annotations

import math
from typing import Callable


def haar_wavelet_transform(signal: list[float]) -> tuple[list[float], list[float]]:
    """
    Haar wavelet transform (simplest wavelet).

    Args:
        signal: Input signal (length should be power of 2)

    Returns:
        Tuple of (approximation coefficients, detail coefficients).
    """
    n = len(signal)
    if n < 2:
        return list(signal), []
    if n & (n - 1):
        # Pad to power of 2
        padded = signal + [0.0] * (2 ** (n.bit_length()) - n)
        approx, detail = haar_wavelet_transform(padded)
        return approx[:n], detail[:n // 2]

    half = n // 2
    approx: list[float] = []
    detail: list[float] = []
    for i in range(half):
        a = (signal[2 * i] + signal[2 * i + 1]) / math.sqrt(2)
        d = (signal[2 * i] - signal[2 * i + 1]) / math.sqrt(2)
        approx.append(a)
        detail.append(d)
    return approx, detail


def inverse_haar_wavelet_transform(
    approx: list[float],
    detail: list[float],
) -> list[float]:
    """
    Inverse Haar wavelet transform.

    Args:
        approx: Approximation coefficients
        detail: Detail coefficients

    Returns:
        Reconstructed signal.
    """
    n = len(approx)
    if n == 0:
        return []
    if n == 1:
        s0 = (approx[0] + detail[0]) / math.sqrt(2)
        s1 = (approx[0] - detail[0]) / math.sqrt(2)
        return [s0, s1]
    result: list[float] = []
    for i in range(n):
        s0 = (approx[i] + detail[i]) / math.sqrt(2)
        s1 = (approx[i] - detail[i]) / math.sqrt(2)
        result.extend([s0, s1])
    return result


def daubechies4_wavelet_transform(signal: list[float]) -> tuple[list[float], list[float]]:
    """
    Daubechies-4 (D4) wavelet transform.

    Args:
        signal: Input signal

    Returns:
        Tuple of (approximation, detail).
    """
    n = len(signal)
    if n < 4:
        return list(signal), [0.0] * max(1, n // 2)

    # D4 coefficients
    c0 = (1 + math.sqrt(3)) / (4 * math.sqrt(2))
    c1 = (3 + math.sqrt(3)) / (4 * math.sqrt(2))
    c2 = (3 - math.sqrt(3)) / (4 * math.sqrt(2))
    c3 = (1 - math.sqrt(3)) / (4 * math.sqrt(2))
    d0 = -(1 - math.sqrt(3)) / (4 * math.sqrt(2))
    d1 = -(3 - math.sqrt(3)) / (4 * math.sqrt(2))
    d2 = (3 + math.sqrt(3)) / (4 * math.sqrt(2))
    d3 = -(1 + math.sqrt(3)) / (4 * math.sqrt(2))

    half = n // 2
    approx: list[float] = []
    detail: list[float] = []
    for i in range(half):
        s0 = signal[(2 * i) % n]
        s1 = signal[(2 * i + 1) % n]
        s2 = signal[(2 * i + 2) % n]
        s3 = signal[(2 * i + 3) % n]
        a = c0 * s0 + c1 * s1 + c2 * s2 + c3 * s3
        d = d0 * s0 + d1 * s1 + d2 * s2 + d3 * s3
        approx.append(a)
        detail.append(d)
    return approx, detail


def inverse_daubechies4_wavelet_transform(
    approx: list[float],
    detail: list[float],
) -> list[float]:
    """Inverse D4 wavelet transform."""
    n = len(approx)
    if n == 0:
        return []
    c0 = (1 + math.sqrt(3)) / (4 * math.sqrt(2))
    c1 = (3 + math.sqrt(3)) / (4 * math.sqrt(2))
    c2 = (3 - math.sqrt(3)) / (4 * math.sqrt(2))
    c3 = (1 - math.sqrt(3)) / (4 * math.sqrt(2))
    d0 = -(1 - math.sqrt(3)) / (4 * math.sqrt(2))
    d1 = -(3 - math.sqrt(3)) / (4 * math.sqrt(2))
    d2 = (3 + math.sqrt(3)) / (4 * math.sqrt(2))
    d3 = -(1 + math.sqrt(3)) / (4 * math.sqrt(2))

    result: list[float] = []
    for i in range(n):
        a = approx[i]
        d = detail[i] if i < len(detail) else 0.0
        s0 = c0 * a + d0 * d
        s1 = c1 * a + d1 * d
        s2 = c2 * a + d2 * d
        s3 = c3 * a + d3 * d
        result.extend([s0, s1, s2, s3])
    return result[:2 * n]


def mexican_hat_wavelet(t: float, scale: float = 1.0) -> float:
    """
    Mexican hat (Ricker) wavelet: (1 - t²) * exp(-t²/2)

    Args:
        t: Position
        scale: Scale factor

    Returns:
        Wavelet value.
    """
    t_scaled = t / scale
    return (1 - t_scaled ** 2) * math.exp(-t_scaled ** 2 / 2)


def morlet_wavelet(t: float, omega0: float = 5.0) -> complex:
    """
    Morlet wavelet: exp(i * omega0 * t) * exp(-t²/2)

    Args:
        t: Position
        omega0: Central frequency

    Returns:
        Complex wavelet value.
    """
    return complex(
        math.cos(omega0 * t) * math.exp(-t * t / 2),
        math.sin(omega0 * t) * math.exp(-t * t / 2),
    )


def continuous_wavelet_transform(
    signal: list[float],
    wavelet: Callable[[float], float],
    scales: list[float],
    dt: float = 1.0,
) -> list[list[float]]:
    """
    Continuous Wavelet Transform (CWT).

    Args:
        signal: Input signal
        wavelet: Wavelet function
        scales: List of scales (larger = more stretched)
        dt: Sample spacing

    Returns:
        2D matrix (scales x time) of coefficients.
    """
    n = len(signal)
    result: list[list[float]] = []
    for scale in scales:
        coeffs: list[float] = []
        for i in range(n):
            t_center = i * dt
            val = 0.0
            for j in range(n):
                t = (j - i) * dt / scale
                val += signal[j] * wavelet(t) / math.sqrt(scale)
            coeffs.append(val * dt)
        result.append(coeffs)
    return result


def wavelet_denoise(
    signal: list[float],
    threshold: float,
    wavelet: str = "haar",
) -> list[float]:
    """
    Wavelet denoising using soft thresholding.

    Args:
        signal: Noisy signal
        threshold: Threshold for coefficient suppression
        wavelet: 'haar' or 'db4'

    Returns:
        Denoised signal.
    """
    # Forward transform
    if wavelet == "haar":
        approx, detail = haar_wavelet_transform(signal)
    else:
        approx, detail = daubechies4_wavelet_transform(signal)

    # Soft thresholding on detail coefficients
    denoised_detail = [0.0] * len(detail)
    for i, d in enumerate(detail):
        if d > threshold:
            denoised_detail[i] = d - threshold
        elif d < -threshold:
            denoised_detail[i] = d + threshold

    # Inverse transform
    if wavelet == "haar":
        return inverse_haar_wavelet_transform(approx, denoised_detail)
    else:
        return inverse_daubechies4_wavelet_transform(approx, denoised_detail)


def multiresolution_analysis(
    signal: list[float],
    levels: int = 3,
    wavelet: str = "haar",
) -> list[tuple[list[float], list[float]]]:
    """
    Multiresolution analysis decomposition.

    Args:
        signal: Input signal
        levels: Number of decomposition levels
        wavelet: 'haar' or 'db4'

    Returns:
        List of (approximation, detail) pairs at each level.
    """
    result: list[tuple[list[float], list[float]]] = []
    current = list(signal)
    for _ in range(levels):
        if wavelet == "haar":
            approx, detail = haar_wavelet_transform(current)
        else:
            approx, detail = daubechies4_wavelet_transform(current)
        result.append((approx, detail))
        current = approx
    return result


def wavelet_packet_transform(
    signal: list[float],
    wavelet: str = "haar",
) -> dict[str, list[float]]:
    """
    Wavelet packet decomposition (full binary tree).

    Returns:
        Dictionary of path strings to coefficient arrays.
    """
    if wavelet == "haar":
        forward = haar_wavelet_transform
        inverse = inverse_haar_wavelet_transform
    else:
        forward = daubechies4_wavelet_transform
        inverse = inverse_daubechies4_wavelet_transform

    def decompose(arr: list[float], path: str) -> dict[str, list[float]]:
        if len(arr) < 2:
            return {path: arr}
        approx, detail = forward(arr)
        result = {f"{path}a": approx, f"{path}d": detail}
        if len(approx) >= 2:
            result.update(decompose(approx, f"{path}a"))
        if len(detail) >= 2:
            result.update(decompose(detail, f"{path}d"))
        return result

    return decompose(signal, "")


def reconstruction_from_packets(
    packets: dict[str, list[float]],
    wavelet: str = "haar",
) -> list[float]:
    """Reconstruct signal from wavelet packets."""
    if wavelet == "haar":
        inverse = inverse_haar_wavelet_transform
    else:
        inverse = inverse_daubechies4_wavelet_transform

    def recombine(path: str) -> list[float]:
        a_path = path
        d_path = ""
        # Navigate tree
        i = 0
        while i < len(path):
            if path[i] == "a":
                a_path = path[:i+1]
                d_path = path[:i] + "d" + path[i+1:]
                break
            i += 1
        if d_path and d_path in packets:
            return inverse(packets.get(a_path, []), packets.get(d_path, []))
        return packets.get(path, [])

    return recombine("a")
