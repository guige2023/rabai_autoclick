"""
OCR preprocessing utilities for improving text recognition accuracy.

Provides image preprocessing functions specifically designed to enhance
text readability before OCR processing.

Example:
    >>> from ocr_preprocessing_utils import (
    ...     binarize, deskew, remove_noise, enhance_contrast
    ... )
    >>> processed = enhance_contrast(image, clip_histogram_percent=2)
    >>> binarized = binarize(processed, method='sauvola')
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

import numpy as np


# =============================================================================
# Types
# =============================================================================


class BinarizeMethod(Enum):
    """Binarization methods."""
    SIMPLE = "simple"
    OTSU = "otsu"
    SAUVOLA = "sauvola"
    BERNSEN = "bernsen"
    NIBLACK = "niblack"


@dataclass
class PreprocessingResult:
    """Result of OCR preprocessing pipeline."""
    image: np.ndarray
    operations_applied: list[str]
    processing_time_ms: float


# =============================================================================
# Contrast Enhancement
# =============================================================================


def enhance_contrast(
    image: np.ndarray,
    clip_histogram_percent: float = 2.0,
) -> np.ndarray:
    """
    Enhance image contrast using histogram clipping and stretching.

    Args:
        image: Input grayscale image.
        clip_histogram_percent: Percentage of histogram to clip from each tail.

    Returns:
        Contrast-enhanced image.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    hist, bins = np.histogram(image.flatten(), bins=256, range=(0, 256))
    total = image.size
    clip_pixels = int(total * clip_histogram_percent / 100)

    # Find low and high clip points
    low, high = 0, 255
    count = 0
    for i in range(256):
        count += hist[i]
        if count >= clip_pixels:
            low = bins[i]
            break

    count = 0
    for i in range(255, -1, -1):
        count += hist[i]
        if count >= clip_pixels:
            high = bins[i]
            break

    # Stretch
    if high > low:
        stretched = (image - low) * 255 / (high - low)
        stretched = np.clip(stretched, 0, 255)
    else:
        stretched = image

    return stretched.astype(np.uint8)


def adaptive_histogram_equalization(
    image: np.ndarray,
    clip_limit: float = 2.0,
    tile_grid_size: Tuple[int, int] = (8, 8),
) -> np.ndarray:
    """
    Apply Contrast Limited Adaptive Histogram Equalization (CLAHE).

    Args:
        image: Input grayscale image.
        clip_limit: Contrast limiting parameter.
        tile_grid_size: Size of grid for histogram equalization.

    Returns:
        CLAHE-enhanced image.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    h, w = image.shape
    tile_h, tile_w = tile_grid_size

    # Pad image
    pad_h = (tile_h - h % tile_h) % tile_h
    pad_w = (tile_w - w % tile_w) % tile_w
    padded = np.pad(image, ((0, pad_h), (0, pad_w)), mode="edge")

    result = np.zeros_like(padded)

    # Process tiles
    for i in range(0, padded.shape[0], tile_h):
        for j in range(0, padded.shape[1], tile_w):
            tile = padded[i:i + tile_h, j:j + tile_w]
            hist, _ = np.histogram(tile.flatten(), bins=256, range=(0, 256))
            hist = np.minimum(hist, clip_limit * tile.size / 256)
            hist = hist.astype(float)
            cdf = hist.cumsum()
            cdf = (cdf - cdf[0]) * 255 / (cdf[-1] - cdf[0])
            cdf = np.clip(cdf, 0, 255).astype(np.uint8)
            result[i:i + tile_h, j:j + tile_w] = cdf[tile]

    return result[:h, :w]


# =============================================================================
# Binarization
# =============================================================================


def binarize(
    image: np.ndarray,
    method: str = "otsu",
    block_size: int = 15,
    k: float = 0.2,
) -> np.ndarray:
    """
    Binarize an image using the specified method.

    Args:
        image: Input grayscale image.
        method: Binarization method ('simple', 'otsu', 'sauvola', 'bernsen', 'niblack').
        block_size: Local window size for adaptive methods.
        k: Sensitivity parameter for Niblack/Sauvola.

    Returns:
        Binary image (0 or 255).
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    if method == "simple":
        threshold = np.mean(image)
        binary = np.where(image > threshold, 255, 0)
    elif method == "otsu":
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
        binary = np.where(image > threshold, 255, 0)
    elif method == "sauvola":
        binary = _sauvola_binarize(image, block_size, k)
    elif method == "bernsen":
        binary = _bernsen_binarize(image, block_size)
    elif method == "niblack":
        binary = _niblack_binarize(image, block_size, k)
    else:
        raise ValueError(f"Unknown binarization method: {method}")

    return binary.astype(np.uint8)


def _sauvola_binarize(
    image: np.ndarray,
    block_size: int,
    k: float,
) -> np.ndarray:
    """Sauvola local binarization."""
    h, w = image.shape
    pad = block_size // 2
    padded = np.pad(image, pad, mode="edge")
    result = np.zeros_like(image, dtype=float)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + block_size, j:j + block_size]
            mean = region.mean()
            std = region.std()
            threshold = mean * (1 + k * (std / 128 - 1))
            result[i, j] = 255 if image[i, j] > threshold else 0

    return result.astype(np.uint8)


def _bernsen_binarize(image: np.ndarray, block_size: int) -> np.ndarray:
    """Bernsen local binarization using local contrast."""
    h, w = image.shape
    pad = block_size // 2
    padded = np.pad(image, pad, mode="edge")
    result = np.zeros_like(image, dtype=np.uint8)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + block_size, j:j + block_size]
            min_val, max_val = region.min(), region.max()
            contrast = max_val - min_val
            mid = (min_val + max_val) / 2
            if contrast > 30:
                result[i, j] = 255 if image[i, j] >= mid else 0
            else:
                result[i, j] = 255 if mid > 128 else 0

    return result


def _niblack_binarize(
    image: np.ndarray,
    block_size: int,
    k: float,
) -> np.ndarray:
    """Niblack local binarization."""
    h, w = image.shape
    pad = block_size // 2
    padded = np.pad(image, pad, mode="edge")
    result = np.zeros_like(image, dtype=float)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + block_size, j:j + block_size]
            mean = region.mean()
            std = region.std()
            threshold = mean + k * std
            result[i, j] = 255 if image[i, j] > threshold else 0

    return result.astype(np.uint8)


# =============================================================================
# Deskewing
# =============================================================================


def deskew(image: np.ndarray) -> Tuple[np.ndarray, float]:
    """
    Detect and correct image skew angle.

    Uses the Hough transform or projection method to find the dominant
    text orientation and rotates the image to correct it.

    Args:
        image: Input binary image.

    Returns:
        Tuple of (deskewed image, detected angle in degrees).
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    # Simple projection-based skew detection
    binary = binarize(image, method="otsu") if image.dtype != np.uint8 or image.max() > 1:
        (image > 127).astype(np.uint8) * 255
    else:
        image

    # Compute vertical projection profile at different angles
    best_angle = 0.0
    best_variance = 0.0

    for angle in np.arange(-15, 16, 0.5):
        rotated = rotate_image(image, angle)
        proj = rotated.sum(axis=0)
        variance = proj.var()
        if variance > best_variance:
            best_variance = variance
            best_angle = angle

    if abs(best_angle) > 0.1:
        corrected = rotate_image(image, -best_angle)
    else:
        corrected = image

    return corrected, best_angle


def rotate_image(image: np.ndarray, angle: float) -> np.ndarray:
    """
    Rotate an image by the specified angle.

    Args:
        image: Input image.
        angle: Rotation angle in degrees (positive = counter-clockwise).

    Returns:
        Rotated image.
    """
    if len(image.shape) == 2:
        h, w = image.shape
        center = (w / 2, h / 2)
    else:
        h, w, _ = image.shape
        center = (w / 2, h / 2)

    angle_rad = np.radians(angle)
    cos_a = np.cos(angle_rad)
    sin_a = np.sin(angle_rad)

    # Compute output bounds
    corners = [
        (-w / 2, -h / 2),
        (w / 2, -h / 2),
        (w / 2, h / 2),
        (-w / 2, h / 2),
    ]
    rot_corners = [
        (x * cos_a - y * sin_a, x * sin_a + y * cos_a)
        for x, y in corners
    ]
    min_x = min(c[0] for c in rot_corners)
    max_x = max(c[0] for c in rot_corners)
    min_y = min(c[1] for c in rot_corners)
    max_y = max(c[1] for c in rot_corners)

    out_w = int(max_x - min_x)
    out_h = int(max_y - min_y)

    # Use affine transform (simplified implementation)
    result = np.zeros((out_h, out_w) if len(image.shape) == 2 else (out_h, out_w, image.shape[2]))

    if len(image.shape) == 2:
        for y in range(out_h):
            for x in range(out_w):
                src_x = (x - out_w / 2) * cos_a + (y - out_h / 2) * sin_a + center[0]
                src_y = -(x - out_w / 2) * sin_a + (y - out_h / 2) * cos_a + center[1]
                if 0 <= src_x < w and 0 <= src_y < h:
                    result[y, x] = image[int(src_y), int(src_x)]
    else:
        for y in range(out_h):
            for x in range(out_w):
                src_x = (x - out_w / 2) * cos_a + (y - out_h / 2) * sin_a + center[0]
                src_y = -(x - out_w / 2) * sin_a + (y - out_h / 2) * cos_a + center[1]
                if 0 <= src_x < w and 0 <= src_y < h:
                    result[y, x] = image[int(src_y), int(src_x)]

    return result.astype(image.dtype)


# =============================================================================
# Noise Removal
# =============================================================================


def remove_noise(
    image: np.ndarray,
    kernel_size: int = 3,
    iterations: int = 2,
) -> np.ndarray:
    """
    Remove noise using morphological opening then closing.

    Args:
        image: Input binary image.
        kernel_size: Size of morphological kernel.
        iterations: Number of iterations.

    Returns:
        Denoised binary image.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    binary = (image > 127).astype(np.uint8) if image.max() > 1 else image

    kernel = np.ones((kernel_size, kernel_size), dtype=np.uint8)

    # Opening: erosion followed by dilation (removes small objects)
    result = binary.copy()
    for _ in range(iterations):
        result = _erode(result, kernel)
        result = _dilate(result, kernel)

    # Closing: dilation followed by erosion (fills small holes)
    for _ in range(iterations):
        result = _dilate(result, kernel)
        result = _erode(result, kernel)

    return result * 255


def _erode(image: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    """Erosion operation."""
    h, w = image.shape
    kh, kw = kernel.shape
    pad_h, pad_w = kh // 2, kw // 2
    padded = np.pad(image, ((pad_h, pad_h), (pad_w, pad_w)), constant_values=0)
    result = np.zeros_like(image)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + kh, j:j + kw]
            result[i, j] = 1 if (region * kernel).sum() == kernel.sum() else 0

    return result.astype(np.uint8)


def _dilate(image: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    """Dilation operation."""
    h, w = image.shape
    kh, kw = kernel.shape
    pad_h, pad_w = kh // 2, kw // 2
    padded = np.pad(image, ((pad_h, pad_h), (pad_w, pad_w)), constant_values=0)
    result = np.zeros_like(image)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + kh, j:j + kw]
            result[i, j] = 1 if (region * kernel).sum() > 0 else 0

    return result.astype(np.uint8)


def median_filter(image: np.ndarray, size: int = 3) -> np.ndarray:
    """
    Apply median filter to reduce salt-and-pepper noise.

    Args:
        image: Input grayscale image.
        size: Filter size (must be odd).

    Returns:
        Filtered image.
    """
    if len(image.shape) > 2:
        image = rgb_to_gray(image)

    if size % 2 == 0:
        size += 1

    h, w = image.shape
    pad = size // 2
    padded = np.pad(image, pad, mode="edge")
    result = np.zeros_like(image)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + size, j:j + size]
            result[i, j] = np.median(region)

    return result.astype(np.uint8)


# =============================================================================
# Scaling and Resizing
# =============================================================================


def scale_to_dpi(
    image: np.ndarray,
    current_dpi: float = 72.0,
    target_dpi: float = 300.0,
) -> np.ndarray:
    """
    Scale image to simulate higher DPI for better OCR.

    Args:
        image: Input image.
        current_dpi: Current DPI of the image.
        target_dpi: Target DPI for OCR.

    Returns:
        Scaled image.
    """
    if current_dpi >= target_dpi:
        return image

    scale_factor = target_dpi / current_dpi
    new_h = int(image.shape[0] * scale_factor)
    new_w = int(image.shape[1] * scale_factor)

    h, w = image.shape[:2]
    result = np.zeros((new_h, new_w) if len(image.shape) == 2 else (new_h, new_w, image.shape[2]))

    for i in range(new_h):
        for j in range(new_w):
            src_i = min(int(i / scale_factor), h - 1)
            src_j = min(int(j / scale_factor), w - 1)
            if len(image.shape) == 2:
                result[i, j] = image[src_i, src_j]
            else:
                result[i, j] = image[src_i, src_j]

    return result.astype(image.dtype)


# =============================================================================
# Utilities
# =============================================================================


def rgb_to_gray(image: np.ndarray) -> np.ndarray:
    """Convert RGB image to grayscale."""
    if len(image.shape) == 2:
        return image
    r, g, b = image[:, :, 0], image[:, :, 1], image[:, :, 2]
    gray = 0.299 * r + 0.587 * g + 0.114 * b
    return gray.astype(np.uint8)


def denormalize_box(
    box: Tuple[float, float, float, float],
    width: int,
    height: int,
) -> Tuple[int, int, int, int]:
    """
    Convert normalized bounding box to pixel coordinates.

    Args:
        box: Normalized box (y_min, x_min, y_max, x_max) in [0, 1].
        width: Image width.
        height: Image height.

    Returns:
        Pixel coordinates (y1, x1, y2, x2).
    """
    y_min, x_min, y_max, x_max = box
    return (
        int(y_min * height),
        int(x_min * width),
        int(y_max * height),
        int(x_max * width),
    )
