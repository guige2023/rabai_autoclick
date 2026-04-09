"""Wavelet transform utilities for RabAI AutoClick.

Provides:
- Discrete Wavelet Transform (DWT)
- Wavelet packet decomposition
- Signal denoising
- Multi-resolution analysis
"""

from typing import List, Tuple, Callable, Optional
import math


def haar_low_pass(signal: List[float]) -> List[float]:
    """Haar low-pass (approximation) filter.

    Args:
        signal: Input signal (length must be even).

    Returns:
        Approximation coefficients.
    """
    n = len(signal) // 2
    result: List[float] = []
    for i in range(n):
        result.append((signal[2 * i] + signal[2 * i + 1]) / math.sqrt(2))
    return result


def haar_high_pass(signal: List[float]) -> List[float]:
    """Haar high-pass (detail) filter.

    Args:
        signal: Input signal (length must be even).

    Returns:
        Detail coefficients.
    """
    n = len(signal) // 2
    result: List[float] = []
    for i in range(n):
        result.append((signal[2 * i] - signal[2 * i + 1]) / math.sqrt(2))
    return result


def haar_inverse_low_pass(approx: List[float]) -> List[float]:
    """Inverse Haar low-pass (reconstruction from approx)."""
    n = len(approx)
    result: List[float] = [0.0] * (2 * n)
    for i in range(n):
        val = approx[i] * math.sqrt(2)
        result[2 * i] = val / 2
        result[2 * i + 1] = val / 2
    return result


def haar_inverse_high_pass(detail: List[float]) -> List[float]:
    """Inverse Haar high-pass (reconstruction from detail)."""
    n = len(detail)
    result: List[float] = [0.0] * (2 * n)
    for i in range(n):
        val = detail[i] * math.sqrt(2)
        result[2 * i] = val / 2
        result[2 * i + 1] = -val / 2
    return result


def dwt_haar(signal: List[float]) -> Tuple[List[float], List[float]]:
    """Discrete Wavelet Transform using Haar wavelet.

    Args:
        signal: Input signal (length must be power of 2).

    Returns:
        (approximation, detail) coefficients.
    """
    if len(signal) < 2:
        return (signal, [])
    if len(signal) % 2 != 0:
        signal = signal[:-1]
    return (haar_low_pass(signal), haar_high_pass(signal))


def idwt_haar(
    approx: List[float],
    detail: List[float],
) -> List[float]:
    """Inverse DWT using Haar wavelet.

    Args:
        approx: Approximation coefficients.
        detail: Detail coefficients.

    Returns:
        Reconstructed signal.
    """
    if not detail:
        return haar_inverse_low_pass(approx)

    approx_recon = haar_inverse_low_pass(approx)
    detail_recon = haar_inverse_high_pass(detail)
    n = len(approx_recon)

    return [approx_recon[i] + detail_recon[i] for i in range(n)]


def dwt_decompose(
    signal: List[float],
    levels: int,
) -> Tuple[List[List[float]], List[List[float]]]:
    """Multi-level DWT decomposition.

    Args:
        signal: Input signal.
        levels: Number of decomposition levels.

    Returns:
        (approximations, details) where each is a list of coefficient arrays.
    """
    approx = signal[:]
    approximations: List[List[float]] = []
    details: List[List[float]] = []

    for _ in range(levels):
        if len(approx) < 2:
            break
        a, d = dwt_haar(approx)
        approximations.append(a)
        details.append(d)
        approx = a

    return (approximations, details)


def wavelet_denoise(
    signal: List[float],
    threshold: float,
    level: int = 1,
) -> List[float]:
    """Simple wavelet denoising using hard thresholding.

    Args:
        signal: Input signal.
        threshold: Coefficient threshold.
        level: DWT level.

    Returns:
        Denoised signal.
    """
    approx, details = dwt_decompose(signal, level)

    # Apply threshold to details
    denoised_details: List[List[float]] = []
    for d in details:
        denoised_details.append([0.0 if abs(v) < threshold else v for v in d])

    # Reconstruct
    current = approx[-1] if approx else signal
    for i in range(len(denoised_details) - 1, -1, -1):
        detail = denoised_details[i]
        if len(detail) > 0:
            current = idwt_haar(current, detail)

    return current if current else signal


def db4_low_pass(signal: List[float]) -> Tuple[List[float], List[float]]:
    """Daubechies-4 low-pass decomposition (approximation + coefficients).

    Uses a simplified 4-coefficient approximation.
    """
    # DB4 coefficients (scaled)
    h0 = 0.4829629131445341
    h1 = 0.8365163037378079
    h2 = 0.2241438680420134
    h3 = -0.1294095225512604

    n = len(signal) - 3
    if n < 1:
        return ([signal[0]], [])

    approx: List[float] = []
    detail: List[float] = []

    for i in range(0, n, 2):
        a = h0 * signal[i] + h1 * signal[i + 1] + h2 * signal[i + 2] + h3 * signal[i + 3]
        d = h3 * signal[i] - h2 * signal[i + 1] + h1 * signal[i + 2] - h0 * signal[i + 3]
        approx.append(a)
        detail.append(d)

    return (approx, detail)


def moving_average_smooth(
    signal: List[float],
    window_size: int,
) -> List[float]:
    """Simple moving average smoothing.

    Args:
        signal: Input signal.
        window_size: Window size (odd).

    Returns:
        Smoothed signal.
    """
    if window_size < 1:
        return signal[:]
    half = window_size // 2
    n = len(signal)
    result: List[float] = []

    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        result.append(sum(signal[lo:hi]) / (hi - lo))

    return result


def wavelet_packet_decompose(
    signal: List[float],
    depth: int,
    wavelet: str = "haar",
) -> List[Tuple[str, List[float]]]:
    """Wavelet packet decomposition.

    Args:
        signal: Input signal.
        depth: Decomposition depth.
        wavelet: Wavelet type ('haar' or 'db4').

    Returns:
        List of (path, coefficients) tuples.
    """
    results: List[Tuple[str, List[float]]] = []

    def decompose(sig: List[float], level: int, path: str) -> None:
        results.append((path, sig))
        if level >= depth or len(sig) < 2:
            return

        if wavelet == "haar":
            a, d = dwt_haar(sig)
        else:
            a, d = db4_low_pass(sig)

        decompose(a, level + 1, path + "a")
        decompose(d, level + 1, path + "d")

    decompose(signal, 0, "")
    return results


def energy_at_level(coefficients: List[float]) -> float:
    """Compute energy (sum of squared coefficients) at a level."""
    return sum(c * c for c in coefficients)


def wavelet_reconstruct_level(
    packet: Tuple[str, List[float]],
    original_length: int,
    wavelet: str = "haar",
) -> List[float]:
    """Reconstruct a single wavelet packet level to original signal length.

    Args:
        packet: (path, coefficients) from wavelet_packet_decompose.
        original_length: Length of original signal.
        wavelet: Wavelet type.

    Returns:
        Reconstructed signal at original length.
    """
    path, coeffs = packet
    current = coeffs[:]

    for i, char in enumerate(reversed(path)):
        if char == "a":
            if wavelet == "haar":
                current = idwt_haar(current, [0.0] * len(current))
            else:
                # Simplified DB4 reconstruction
                zeros = [0.0] * (len(current[0]) if isinstance(current, list) and current else 0)
        else:  # 'd'
            if wavelet == "haar":
                current = idwt_haar([0.0] * len(current), current)

    # Pad or trim to original length
    if len(current) > original_length:
        return current[:original_length]
    elif len(current) < original_length:
        return current + [0.0] * (original_length - len(current))
    return current
