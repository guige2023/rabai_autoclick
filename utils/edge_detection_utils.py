"""
Edge detection utilities for UI element boundary identification.

Provides various edge detection algorithms commonly used in computer
vision for UI automation: Sobel, Canny, Laplacian, and Prewitt operators.

Example:
    >>> from edge_detection_utils import sobel_edges, canny_edges, laplacian_edges
    >>> edges = canny_edges(gray_image, low=50, high=150)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

import numpy as np


# =============================================================================
# Types
# =============================================================================


class EdgeDetector(Enum):
    """Supported edge detection algorithms."""
    SOBEL = "sobel"
    PREWITT = "prewitt"
    CANNY = "canny"
    LAPLACIAN = "laplacian"
    SCHARR = "scharr"


@dataclass
class EdgeResult:
    """Result of edge detection."""
    edges: np.ndarray
    detector: EdgeDetector
    angle_map: Optional[np.ndarray] = None
    strength_map: Optional[np.ndarray] = None


# =============================================================================
# Gradient Computation
# =============================================================================


def compute_gradients(
    image: np.ndarray,
    kernel_size: int = 3,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute image gradients using Sobel operator.

    Args:
        image: Input grayscale image.
        kernel_size: Size of Sobel kernel (3 or 5).

    Returns:
        Tuple of (gradient_x, gradient_y, gradient_magnitude).
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    # Normalize
    if image.dtype != float:
        image = image.astype(float) / 255.0

    # Sobel kernels
    if kernel_size == 3:
        gx_kernel = np.array([[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]], dtype=float)
        gy_kernel = np.array([[-1, -2, -1], [0, 0, 0], [1, 2, 1]], dtype=float)
    else:
        gx_kernel = np.array([
            [-1, -2, 0, 2, 1],
            [-4, -8, 0, 8, 4],
            [-6, -12, 0, 12, 6],
            [-4, -8, 0, 8, 4],
            [-1, -2, 0, 2, 1]
        ], dtype=float) / 48.0
        gy_kernel = np.array([
            [-1, -4, -6, -4, -1],
            [-2, -8, -12, -8, -2],
            [0, 0, 0, 0, 0],
            [2, 8, 12, 8, 2],
            [1, 4, 6, 4, 1]
        ], dtype=float) / 48.0

    gx = _convolve2d(image, gx_kernel)
    gy = _convolve2d(image, gy_kernel)

    magnitude = np.sqrt(gx ** 2 + gy ** 2)
    magnitude = (magnitude / magnitude.max() * 255).astype(np.uint8)

    return gx, gy, magnitude


def _convolve2d(image: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    """2D convolution with zero padding."""
    kh, kw = kernel.shape
    pad_h, pad_w = kh // 2, kw // 2
    padded = np.pad(image, ((pad_h, pad_h), (pad_w, pad_w)), mode="constant")
    h, w = image.shape
    result = np.zeros_like(image, dtype=float)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + kh, j:j + kw]
            result[i, j] = (region * kernel).sum()

    return result


# =============================================================================
# Sobel Edge Detection
# =============================================================================


def sobel_edges(
    image: np.ndarray,
    threshold: float = 50.0,
    kernel_size: int = 3,
) -> EdgeResult:
    """
    Detect edges using Sobel operator.

    Args:
        image: Input grayscale image.
        threshold: Edge threshold (0-255).
        kernel_size: Kernel size (3 or 5).

    Returns:
        EdgeResult with edges and angle map.
    """
    gx, gy, magnitude = compute_gradients(image, kernel_size)

    # Compute gradient angles
    angle = np.arctan2(gy, gx)
    angle_deg = ((angle * 180 / np.pi) + 180) % 360

    # Non-maximum suppression
    suppressed = _non_maximum_suppression(magnitude, angle_deg)

    # Thresholding
    edges = (suppressed > threshold).astype(np.uint8) * 255

    return EdgeResult(
        edges=edges,
        detector=EdgeDetector.SOBEL,
        angle_map=angle_deg,
        strength_map=magnitude,
    )


def _non_maximum_suppression(
    magnitude: np.ndarray,
    angles: np.ndarray,
    window_size: int = 3,
) -> np.ndarray:
    """
    Apply non-maximum suppression to thin edges.

    Args:
        magnitude: Gradient magnitudes.
        angles: Gradient angles in degrees.
        window_size: Suppression window size.

    Returns:
        Thinned edge map.
    """
    h, w = magnitude.shape
    suppressed = np.zeros_like(magnitude)
    pad = window_size // 2
    padded = np.pad(magnitude, pad, mode="constant")

    for i in range(h):
        for j in range(w):
            angle = angles[i, j]
            # Map angle to nearest 0, 45, 90, 135
            if 0 <= angle < 22.5 or 157.5 <= angle < 202.5 or 337.5 <= angle < 360:
                neighbors = [padded[i, j - 1], padded[i, j + 1]]
            elif 22.5 <= angle < 67.5 or 202.5 <= angle < 247.5:
                neighbors = [padded[i - 1, j + 1], padded[i + 1, j - 1]]
            elif 67.5 <= angle < 112.5 or 247.5 <= angle < 292.5:
                neighbors = [padded[i - 1, j], padded[i + 1, j]]
            else:
                neighbors = [padded[i - 1, j - 1], padded[i + 1, j + 1]]

            current = padded[i + pad, j + pad]
            if current >= max(neighbors):
                suppressed[i, j] = current

    return suppressed


# =============================================================================
# Prewitt Edge Detection
# =============================================================================


def prewitt_edges(
    image: np.ndarray,
    threshold: float = 50.0,
) -> EdgeResult:
    """
    Detect edges using Prewitt operator.

    Args:
        image: Input grayscale image.
        threshold: Edge threshold (0-255).

    Returns:
        EdgeResult with edges and angle map.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    if image.dtype != float:
        image = image.astype(float) / 255.0

    # Prewitt kernels
    gx_kernel = np.array([[-1, 0, 1], [-1, 0, 1], [-1, 0, 1]], dtype=float)
    gy_kernel = np.array([[-1, -1, -1], [0, 0, 0], [1, 1, 1]], dtype=float)

    gx = _convolve2d(image, gx_kernel)
    gy = _convolve2d(image, gy_kernel)

    magnitude = np.sqrt(gx ** 2 + gy ** 2)
    magnitude = (magnitude / magnitude.max() * 255).astype(np.uint8)
    angle = np.arctan2(gy, gx) * 180 / np.pi

    suppressed = _non_maximum_suppression(magnitude, (angle + 180) % 360)
    edges = (suppressed > threshold).astype(np.uint8) * 255

    return EdgeResult(
        edges=edges,
        detector=EdgeDetector.PREWIIT,
        angle_map=angle,
        strength_map=magnitude,
    )


# =============================================================================
# Laplacian Edge Detection
# =============================================================================


def laplacian_edges(
    image: np.ndarray,
    kernel_size: int = 3,
    threshold: float = 50.0,
) -> EdgeResult:
    """
    Detect edges using Laplacian operator.

    Args:
        image: Input grayscale image.
        kernel_size: Kernel size (3 or 5).
        threshold: Edge threshold.

    Returns:
        EdgeResult with edges.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    if image.dtype != float:
        image = image.astype(float) / 255.0

    if kernel_size == 3:
        kernel = np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]], dtype=float)
    else:
        kernel = np.array([
            [0, 0, 1, 0, 0],
            [0, 1, 2, 1, 0],
            [1, 2, -16, 2, 1],
            [0, 1, 2, 1, 0],
            [0, 0, 1, 0, 0]
        ], dtype=float)

    laplacian = _convolve2d(image, kernel)
    laplacian = np.abs(laplacian)
    laplacian = (laplacian / laplacian.max() * 255).astype(np.uint8)
    edges = (laplacian > threshold).astype(np.uint8) * 255

    return EdgeResult(
        edges=edges,
        detector=EdgeDetector.LAPLACIAN,
        strength_map=laplacian,
    )


# =============================================================================
# Canny Edge Detection
# =============================================================================


def canny_edges(
    image: np.ndarray,
    low_threshold: float = 50.0,
    high_threshold: float = 150.0,
    kernel_size: int = 3,
) -> EdgeResult:
    """
    Detect edges using Canny algorithm.

    Multi-stage algorithm: Gaussian blur, gradient computation,
    non-maximum suppression, and hysteresis thresholding.

    Args:
        image: Input grayscale image.
        low_threshold: Lower hysteresis threshold.
        high_threshold: Upper hysteresis threshold.
        kernel_size: Blur kernel size.

    Returns:
        EdgeResult with thin edges.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    # Stage 1: Gaussian blur
    blurred = _gaussian_blur(image, kernel_size)

    # Stage 2: Gradient computation
    gx, gy, magnitude = compute_gradients(blurred, kernel_size)
    angle = np.arctan2(gy, gx)
    angle_deg = ((angle * 180 / np.pi) + 180) % 360

    # Stage 3: Non-maximum suppression
    suppressed = _non_maximum_suppression(magnitude, angle_deg)

    # Stage 4: Hysteresis thresholding
    edges = _hysteresis_thresholding(suppressed, low_threshold, high_threshold)

    return EdgeResult(
        edges=edges,
        detector=EdgeDetector.CANNY,
        angle_map=angle_deg,
        strength_map=magnitude,
    )


def _gaussian_blur(image: np.ndarray, kernel_size: int = 5) -> np.ndarray:
    """Apply Gaussian blur."""
    if kernel_size % 2 == 0:
        kernel_size += 1

    sigma = kernel_size / 6.0
    x = np.arange(kernel_size) - kernel_size // 2
    kernel = np.exp(-(x ** 2) / (2 * sigma ** 2))
    kernel = kernel[:, None] * kernel[None, :]
    kernel /= kernel.sum()

    if image.dtype != float:
        image = image.astype(float) / 255.0

    blurred = _convolve2d(image, kernel)
    return blurred


def _hysteresis_thresholding(
    image: np.ndarray,
    low: float,
    high: float,
) -> np.ndarray:
    """
    Apply hysteresis thresholding.

    Strong edges are kept, weak edges are kept only if connected
    to strong edges.

    Args:
        image: Input suppressed edge map.
        low: Lower threshold.
        high: Upper threshold.

    Returns:
        Binary edge map.
    """
    h, w = image.shape
    result = np.zeros_like(image)
    pad = 1
    padded = np.pad(image, pad, mode="constant")

    # Strong edges
    strong = image > high
    # Weak edges
    weak = (image > low) & (image <= high)

    result[strong] = 255

    # Connect weak to strong
    for i in range(h):
        for j in range(w):
            if weak[i, j]:
                neighbors = [
                    padded[i, j - 1], padded[i, j + 1],
                    padded[i - 1, j], padded[i + 1, j],
                    padded[i - 1, j - 1], padded[i - 1, j + 1],
                    padded[i + 1, j - 1], padded[i + 1, j + 1],
                ]
                if any(n > high for n in neighbors):
                    result[i, j] = 255

    return result.astype(np.uint8)


# =============================================================================
# Scharr Edge Detection
# =============================================================================


def scharr_edges(
    image: np.ndarray,
    threshold: float = 50.0,
) -> EdgeResult:
    """
    Detect edges using Scharr operator (higher accuracy than Sobel).

    Args:
        image: Input grayscale image.
        threshold: Edge threshold.

    Returns:
        EdgeResult with edges.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    if image.dtype != float:
        image = image.astype(float) / 255.0

    # Scharr kernels (higher accuracy than Sobel for 3x3)
    gx_kernel = np.array([[-3, 0, 3], [-10, 0, 10], [-3, 0, 3]], dtype=float) / 6.0
    gy_kernel = np.array([[-3, -10, -3], [0, 0, 0], [3, 10, 3]], dtype=float) / 6.0

    gx = _convolve2d(image, gx_kernel)
    gy = _convolve2d(image, gy_kernel)

    magnitude = np.sqrt(gx ** 2 + gy ** 2)
    magnitude = (magnitude / magnitude.max() * 255).astype(np.uint8)
    angle = np.arctan2(gy, gx) * 180 / np.pi

    suppressed = _non_maximum_suppression(magnitude, (angle + 180) % 360)
    edges = (suppressed > threshold).astype(np.uint8) * 255

    return EdgeResult(
        edges=edges,
        detector=EdgeDetector.SCHARR,
        angle_map=angle,
        strength_map=magnitude,
    )


# =============================================================================
# Utility Functions
# =============================================================================


def rgb_to_gray(image: np.ndarray) -> np.ndarray:
    """Convert RGB to grayscale."""
    if len(image.shape) == 2:
        return image
    r, g, b = image[:, :, 0], image[:, :, 1], image[:, :, 2]
    return (0.299 * r + 0.587 * g + 0.114 * b).astype(np.uint8)


def find_contours(edges: np.ndarray) -> list:
    """
    Find contours from edge map using simple marching squares.

    Args:
        edges: Binary edge image.

    Returns:
        List of contours, each contour is a list of (y, x) points.
    """
    h, w = edges.shape
    visited = np.zeros_like(edges, dtype=bool)
    contours = []

    for i in range(1, h - 1):
        for j in range(1, w - 1):
            if edges[i, j] > 0 and not visited[i, j]:
                contour = []
                _trace_contour(edges, visited, i, j, contour)
                if contour:
                    contours.append(contour)

    return contours


def _trace_contour(
    edges: np.ndarray,
    visited: np.ndarray,
    start_i: int,
    start_j: int,
    contour: list,
) -> None:
    """Trace a single contour starting from the given point."""
    h, w = edges.shape
    stack = [(start_i, start_j)]

    while stack:
        i, j = stack.pop()
        if visited[i, j]:
            continue
        visited[i, j] = True
        contour.append((i, j))

        for di, dj in [(-1, 0), (1, 0), (0, -1), (0, 1), (-1, -1), (-1, 1), (1, -1), (1, 1)]:
            ni, nj = i + di, j + dj
            if 0 <= ni < h and 0 <= nj < w and edges[ni, nj] > 0 and not visited[ni, nj]:
                stack.append((ni, nj))
