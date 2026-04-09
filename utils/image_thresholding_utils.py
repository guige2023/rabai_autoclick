"""
Image thresholding utilities for UI element detection.

Provides various thresholding methods for converting grayscale or
color images into binary masks useful for element detection and
template matching.

Example:
    >>> from image_thresholding_utils import (
    ...     adaptive_threshold, otsu_threshold, spectral_threshold
    ... )
    >>> mask = adaptive_threshold(gray_img, block_size=11, c=2)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple, Union

import numpy as np


# =============================================================================
# Types and Enums
# =============================================================================


class ThresholdMethod(Enum):
    """Supported thresholding methods."""
    BINARY = "binary"
    BINARY_INV = "binary_inv"
    TRUNCATE = "truncate"
    TO_ZERO = "to_zero"
    TO_ZERO_INV = "to_zero_inv"
    OTSU = "otsu"
    ADAPTIVE_MEAN = "adaptive_mean"
    ADAPTIVE_GAUSSIAN = "adaptive_gaussian"
    SPECTRAL = "spectral"


@dataclass
class ThresholdResult:
    """Result of a thresholding operation."""
    mask: np.ndarray
    method: ThresholdMethod
    threshold_value: float
    metadata: dict


# =============================================================================
# Simple Thresholds
# =============================================================================


def binary_threshold(
    image: np.ndarray,
    threshold: float,
    max_value: float = 255.0,
) -> np.ndarray:
    """
    Apply binary threshold to an image.

    Pixels above threshold become max_value, others become 0.

    Args:
        image: Input image (grayscale).
        threshold: Threshold value.
        max_value: Value for pixels above threshold.

    Returns:
        Binary mask array.
    """
    return np.where(image > threshold, max_value, 0.0).astype(
        np.float64 if max_value <= 1.0 else np.uint8
    )


def binary_inv_threshold(
    image: np.ndarray,
    threshold: float,
    max_value: float = 255.0,
) -> np.ndarray:
    """
    Apply inverted binary threshold.

    Pixels below threshold become max_value, others become 0.

    Args:
        image: Input image (grayscale).
        threshold: Threshold value.
        max_value: Value for pixels below threshold.

    Returns:
        Inverted binary mask.
    """
    return np.where(image <= threshold, max_value, 0.0).astype(
        np.float64 if max_value <= 1.0 else np.uint8
    )


def truncate_threshold(image: np.ndarray, threshold: float) -> np.ndarray:
    """
    Apply truncate threshold.

    Pixels above threshold are clamped to threshold value.

    Args:
        image: Input image.
        threshold: Threshold value.

    Returns:
        Truncated image.
    """
    return np.minimum(image, threshold).astype(image.dtype)


def to_zero_threshold(image: np.ndarray, threshold: float) -> np.ndarray:
    """
    Apply to-zero threshold.

    Pixels below threshold become 0, above threshold stay unchanged.

    Args:
        image: Input image.
        threshold: Threshold value.

    Returns:
        Thresholded image.
    """
    return np.where(image > threshold, image, 0).astype(image.dtype)


def to_zero_inv_threshold(image: np.ndarray, threshold: float) -> np.ndarray:
    """
    Apply inverted to-zero threshold.

    Pixels above threshold become 0, below threshold stay unchanged.

    Args:
        image: Input image.
        threshold: Threshold value.

    Returns:
        Thresholded image.
    """
    return np.where(image <= threshold, image, 0).astype(image.dtype)


# =============================================================================
# Otsu's Threshold
# =============================================================================


def otsu_threshold(image: np.ndarray) -> Tuple[np.ndarray, float]:
    """
    Compute Otsu's automatic threshold.

    Finds the optimal threshold that maximizes between-class variance.

    Args:
        image: Input grayscale image.

    Returns:
        Tuple of (binary mask, threshold value).
    """
    hist, _ = np.histogram(image.flatten(), bins=256, range=(0, 256))
    hist = hist.astype(float)

    total = image.size
    sum_total = np.dot(np.arange(256), hist)

    sum_bg, weight_bg = 0.0, 0.0
    max_variance, threshold = 0.0, 0

    for t in range(256):
        weight_bg += hist[t]
        if weight_bg == 0:
            continue

        weight_fg = total - weight_bg
        if weight_fg == 0:
            break

        sum_bg += t * hist[t]
        mean_bg = sum_bg / weight_bg
        mean_fg = (sum_total - sum_bg) / weight_fg

        variance = weight_bg * weight_fg * (mean_bg - mean_fg) ** 2

        if variance > max_variance:
            max_variance = variance
            threshold = t

    return binary_threshold(image, threshold), float(threshold)


def multi_otsu_threshold(
    image: np.ndarray,
    levels: int = 3,
) -> Tuple[np.ndarray, list[float]]:
    """
    Compute multi-level Otsu threshold for multiple classes.

    Args:
        image: Input grayscale image.
        levels: Number of threshold levels (classes = levels + 1).

    Returns:
        Tuple of (labeled mask, list of threshold values).
    """
    if levels < 2:
        mask, thresh = otsu_threshold(image)
        return mask, [thresh]

    hist, _ = np.histogram(image.flatten(), bins=256, range=(0, 256))
    hist = hist.astype(float)
    total = image.size

    # Simplified greedy approach for multi-level
    thresholds = []
    remaining_levels = levels - 1

    for _ in range(remaining_levels):
        if not thresholds:
            prev_thresh = 0
        else:
            prev_thresh = int(thresholds[-1])

        segment = hist[prev_thresh:]
        if segment.sum() == 0:
            break

        sum_total = np.dot(np.arange(len(segment)), segment)
        sum_bg, weight_bg = 0.0, 0.0
        max_variance, best_t = 0.0, 0

        for t in range(len(segment)):
            weight_bg += segment[t]
            if weight_bg == 0:
                continue
            weight_fg = total - weight_bg
            if weight_fg == 0:
                break
            sum_bg += t * segment[t]
            mean_bg = sum_bg / weight_bg
            mean_fg = (sum_total - sum_bg) / weight_fg
            variance = weight_bg * weight_fg * (mean_bg - mean_fg) ** 2
            if variance > max_variance:
                max_variance = variance
                best_t = t

        thresholds.append(float(prev_thresh + best_t))

    # Create labeled mask
    thresholds = sorted(thresholds)
    mask = np.zeros_like(image, dtype=np.uint8)
    for i, t in enumerate(thresholds):
        mask[image > t] = i + 1

    return mask, thresholds


# =============================================================================
# Adaptive Threshold
# =============================================================================


def adaptive_threshold(
    image: np.ndarray,
    block_size: int = 11,
    c: float = 2.0,
    method: str = "mean",
) -> np.ndarray:
    """
    Apply adaptive threshold using local mean or gaussian-weighted mean.

    Computes a local threshold for each pixel based on its neighborhood.

    Args:
        image: Input grayscale image.
        block_size: Size of the pixel neighborhood (must be odd).
        c: Constant subtracted from weighted mean.
        method: 'mean' or 'gaussian'.

    Returns:
        Binary mask array.
    """
    if block_size % 2 == 0:
        block_size += 1

    h, w = image.shape
    pad = block_size // 2
    padded = np.pad(image, pad, mode="edge")
    result = np.zeros_like(image)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + block_size, j:j + block_size]
            if method == "gaussian":
                # Gaussian-weighted center
                center = block_size // 2
                sigma = block_size / 6.0
                x = np.arange(block_size) - center
                kernel = np.exp(-(x ** 2) / (2 * sigma ** 2))
                kernel = kernel[:, None] * kernel[None, :]
                kernel /= kernel.sum()
                local_mean = (region * kernel).sum()
            else:
                local_mean = region.mean()

            threshold = local_mean - c
            result[i, j] = 255 if image[i, j] > threshold else 0

    return result.astype(np.uint8)


def adaptive_gaussian_threshold(
    image: np.ndarray,
    block_size: int = 11,
    c: float = 2.0,
) -> np.ndarray:
    """
    Apply gaussian-weighted adaptive threshold.

    Args:
        image: Input grayscale image.
        block_size: Size of the pixel neighborhood.
        c: Constant subtracted from weighted mean.

    Returns:
        Binary mask.
    """
    return adaptive_threshold(image, block_size, c, method="gaussian")


# =============================================================================
# Spectral Threshold
# =============================================================================


def spectral_threshold(
    image: np.ndarray,
    max_iterations: int = 100,
    convergence_threshold: float = 0.001,
) -> Tuple[np.ndarray, float]:
    """
    Compute spectral threshold using iterative refinement.

    Uses a 2D histogram approach to find natural clusters in the image.

    Args:
        image: Input grayscale image.
        max_iterations: Maximum refinement iterations.
        convergence_threshold: Stop when threshold change < this value.

    Returns:
        Tuple of (binary mask, final threshold).
    """
    hist, _ = np.histogram(image.flatten(), bins=256, range=(0, 256))
    hist = hist.astype(float)
    total = hist.sum()

    # Initialize using Otsu
    _, t = otsu_threshold(image)
    threshold = int(t)

    for _ in range(max_iterations):
        # Compute 2D histogram points
        left = hist[:threshold + 1].sum()
        right = hist[threshold + 1:].sum()

        if left == 0 or right == 0:
            break

        # Weighted means
        indices = np.arange(256)
        mean_left = np.dot(indices[:threshold + 1], hist[:threshold + 1]) / left
        mean_right = np.dot(
            indices[threshold + 1:], hist[threshold + 1:]
        ) / right

        new_threshold = (mean_left + mean_right) / 2

        if abs(new_threshold - threshold) < convergence_threshold:
            break

        threshold = int(new_threshold)

    return binary_threshold(image, threshold), float(threshold)


# =============================================================================
# Color-based Threshold
# =============================================================================


def color_channel_threshold(
    image: np.ndarray,
    lower: Tuple[float, float, float],
    upper: Tuple[float, float, float],
) -> np.ndarray:
    """
    Apply color-based threshold using HSV or RGB bounds.

    Args:
        image: Input color image (RGB).
        lower: Lower bound (R, G, B) or (H, S, V) depending on space.
        upper: Upper bound.

    Returns:
        Binary mask where pixels within range are white.
    """
    lower = np.array(lower, dtype=np.float64)
    upper = np.array(upper, dtype=np.float64)

    # Create mask for each channel
    mask = np.all(image >= lower, axis=-1) & np.all(image <= upper, axis=-1)
    return mask.astype(np.uint8) * 255


def hsv_threshold(
    image: np.ndarray,
    lower: Tuple[float, float, float],
    upper: Tuple[float, float, float],
) -> np.ndarray:
    """
    Apply threshold in HSV color space.

    Args:
        image: Input RGB image.
        lower: Lower HSV bounds (H: 0-180, S: 0-255, V: 0-255).
        upper: Upper HSV bounds.

    Returns:
        Binary mask.
    """
    hsv = rgb_to_hsv(image)
    return color_channel_threshold(hsv, lower, upper)


def rgb_to_hsv(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB image to HSV.

    Args:
        image: Input RGB image (uint8).

    Returns:
        HSV image.
    """
    image = image.astype(float) / 255.0
    h, w, c = image.shape

    r, g, b = image[:, :, 0], image[:, :, 1], image[:, :, 2]

    maxc = np.maximum(np.maximum(r, g), b)
    minc = np.minimum(np.minimum(r, g), b)
    v = maxc

    s = np.where(maxc != 0, (maxc - minc) / maxc, 0)

    delta = maxc - minc
    delta = np.where(delta == 0, 1e-10, delta)

    h = np.zeros_like(v)
    mask_r = (maxc == r)
    mask_g = (maxc == g) & ~mask_r
    mask_b = (maxc == b) & ~(mask_r | mask_g)

    h[mask_r] = 60 * (((g[mask_r] - b[mask_r]) / delta[mask_r]) % 6)
    h[mask_g] = 60 * ((b[mask_g] - r[mask_g]) / delta[mask_g] + 2)
    h[mask_b] = 60 * ((r[mask_b] - g[mask_b]) / delta[mask_b] + 4)

    h = h / 2  # Scale to OpenCV's H range (0-180)

    return np.stack([h, s * 255, v * 255], axis=-1).astype(np.uint8)


# =============================================================================
# Combined Utilities
# =============================================================================


def auto_threshold(
    image: np.ndarray,
    method: ThresholdMethod = ThresholdMethod.OTSU,
    block_size: int = 11,
    c: float = 2.0,
) -> ThresholdResult:
    """
    Automatically select and apply appropriate thresholding.

    Args:
        image: Input grayscale image.
        method: Thresholding method to use.
        block_size: Block size for adaptive methods.
        c: Constant for adaptive methods.

    Returns:
        ThresholdResult with mask and metadata.
    """
    if len(image.shape) > 2:
        image = np.mean(image, axis=-1).astype(np.uint8)

    if method == ThresholdMethod.OTSU:
        mask, thresh = otsu_threshold(image)
        return ThresholdResult(
            mask=mask,
            method=method,
            threshold_value=thresh,
            metadata={"algorithm": "otsu"},
        )
    elif method == ThresholdMethod.ADAPTIVE_MEAN:
        mask = adaptive_threshold(image, block_size, c, "mean")
        return ThresholdResult(
            mask=mask,
            method=method,
            threshold_value=c,
            metadata={"block_size": block_size, "c": c},
        )
    elif method == ThresholdMethod.ADAPTIVE_GAUSSIAN:
        mask = adaptive_gaussian_threshold(image, block_size, c)
        return ThresholdResult(
            mask=mask,
            method=method,
            threshold_value=c,
            metadata={"block_size": block_size, "c": c},
        )
    elif method == ThresholdMethod.SPECTRAL:
        mask, thresh = spectral_threshold(image)
        return ThresholdResult(
            mask=mask,
            method=method,
            threshold_value=thresh,
            metadata={"algorithm": "spectral"},
        )
    else:
        raise ValueError(f"Method {method} requires explicit threshold value")
