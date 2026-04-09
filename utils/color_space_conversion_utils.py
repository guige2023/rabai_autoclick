"""
Color space conversion utilities for image processing.

Provides conversions between RGB, HSV, HSL, LAB, LUV, YUV, YCbCr, XYZ,
and other color spaces commonly used in image processing and UI analysis.

Example:
    >>> from color_space_conversion_utils import rgb_to_hsv, hsv_to_rgb, rgb_to_lab
    >>> hsv = rgb_to_hsv(image)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple

import numpy as np


# =============================================================================
# Color Spaces
# =============================================================================


class ColorSpace(Enum):
    """Supported color spaces."""
    RGB = "rgb"
    HSV = "hsv"
    HSL = "hsl"
    LAB = "lab"
    LUV = "luv"
    YUV = "yuv"
    YCBCR = "ycbcr"
    XYZ = "xyz"
    GRAY = "gray"


# =============================================================================
# RGB Conversions
# =============================================================================


def rgb_to_gray(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to grayscale using luminance formula.

    Args:
        image: RGB image (H x W x 3).

    Returns:
        Grayscale image (H x W).
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r = image[:, :, 0].astype(float)
    g = image[:, :, 1].astype(float)
    b = image[:, :, 2].astype(float)

    gray = 0.299 * r + 0.587 * g + 0.114 * b
    return gray.astype(np.uint8)


def rgb_to_hsv(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to HSV color space.

    Uses OpenCV convention: H in [0, 180], S and V in [0, 255].

    Args:
        image: RGB image (H x W x 3), values in [0, 255].

    Returns:
        HSV image (H x W x 3).
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r, g, b = image[:, :, 0] / 255.0, image[:, :, 1] / 255.0, image[:, :, 2] / 255.0

    maxc = np.maximum(np.maximum(r, g), b)
    minc = np.minimum(np.minimum(r, g), b)
    v = maxc

    delta = maxc - minc
    delta = np.where(delta == 0, 1e-10, delta)

    s = np.where(maxc != 0, delta / maxc, 0)

    h = np.zeros_like(v)

    mask_max_r = (maxc == r) & (delta != 0)
    mask_max_g = (maxc == g) & (delta != 0) & ~mask_max_r
    mask_max_b = (maxc == b) & (delta != 0) & ~mask_max_r & ~mask_max_g

    h[mask_max_r] = 60 * (((g[mask_max_r] - b[mask_max_r]) / delta[mask_max_r]) % 6)
    h[mask_max_g] = 60 * ((b[mask_max_g] - r[mask_max_g]) / delta[mask_max_g]) + 2
    h[mask_max_b] = 60 * ((r[mask_max_b] - g[mask_max_b]) / delta[mask_max_b]) + 4

    h = h / 2

    h = np.clip(h, 0, 180).astype(np.uint8)
    s = (s * 255).astype(np.uint8)
    v = (v * 255).astype(np.uint8)

    return np.stack([h, s, v], axis=-1)


def hsv_to_rgb(image: np.ndarray) -> np.ndarray:
    """
    Convert HSV to RGB color space.

    Args:
        image: HSV image (H x W x 3), H in [0, 180], S and V in [0, 255].

    Returns:
        RGB image (H x W x 3).
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 HSV image")

    h = image[:, :, 0].astype(float) * 2
    s = image[:, :, 1].astype(float) / 255.0
    v = image[:, :, 2].astype(float) / 255.0

    c = v * s
    x = c * (1 - np.abs((h / 60) % 2 - 1))
    m = v - c

    r = np.zeros_like(h)
    g = np.zeros_like(h)
    b = np.zeros_like(h)

    mask1 = (0 <= h) & (h < 60)
    mask2 = (60 <= h) & (h < 120)
    mask3 = (120 <= h) & (h < 180)
    mask4 = (180 <= h) & (h < 240)
    mask5 = (240 <= h) & (h < 300)
    mask6 = (300 <= h) & (h < 360)

    r[mask1], g[mask1], b[mask1] = c[mask1], x[mask1], 0
    r[mask2], g[mask2], b[mask2] = x[mask2], c[mask2], 0
    r[mask3], g[mask3], b[mask3] = 0, c[mask3], x[mask3]
    r[mask4], g[mask4], b[mask4] = 0, x[mask4], c[mask4]
    r[mask5], g[mask5], b[mask5] = x[mask5], 0, c[mask5]
    r[mask6], g[mask6], b[mask6] = c[mask6], 0, x[mask6]

    r, g, b = r + m, g + m, b + m
    r, g, b = (np.clip(r, 0, 1) * 255).astype(np.uint8), \
              (np.clip(g, 0, 1) * 255).astype(np.uint8), \
              (np.clip(b, 0, 1) * 255).astype(np.uint8)

    return np.stack([r, g, b], axis=-1)


def rgb_to_hsl(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to HSL color space.

    Args:
        image: RGB image (H x W x 3).

    Returns:
        HSL image (H x W x 3), H in [0, 360], S and L in [0, 100].
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r, g, b = image[:, :, 0] / 255.0, image[:, :, 1] / 255.0, image[:, :, 2] / 255.0

    maxc = np.maximum(np.maximum(r, g), b)
    minc = np.minimum(np.minimum(r, g), b)
    l = (maxc + minc) / 2

    delta = maxc - minc
    delta = np.where(delta == 0, 1e-10, delta)

    s = np.where(l > 0.5, delta / (2 - maxc - minc), delta / (maxc + minc))

    h = np.zeros_like(l)

    mask_max_r = (maxc == r) & (delta != 0)
    mask_max_g = (maxc == g) & (delta != 0) & ~mask_max_r
    mask_max_b = (maxc == b) & (delta != 0) & ~mask_max_r & ~mask_max_g

    h[mask_max_r] = 60 * (((g[mask_max_r] - b[mask_max_r]) / delta[mask_max_r]) % 6)
    h[mask_max_g] = 60 * ((b[mask_max_g] - r[mask_max_g]) / delta[mask_max_g]) + 2
    h[mask_max_b] = 60 * ((r[mask_max_b] - g[mask_max_b]) / delta[mask_max_b]) + 4

    h = np.clip(h, 0, 360).astype(np.uint16)
    s = (s * 100).astype(np.uint8)
    l = (l * 100).astype(np.uint8)

    return np.stack([h, s, l], axis=-1)


# =============================================================================
# LAB Color Space
# =============================================================================


def rgb_to_xyz(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to CIE XYZ color space.

    Args:
        image: RGB image (H x W x 3).

    Returns:
        XYZ image (H x W x 3).
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r = image[:, :, 0] / 255.0
    g = image[:, :, 1] / 255.0
    b = image[:, :, 2] / 255.0

    def f(t):
        return np.where(t > 0.008856, t ** (1/3), 7.787 * t + 16/116)

    r = np.where(r > 0.04045, ((r + 0.055) / 1.055) ** 2.4, r / 12.92)
    g = np.where(g > 0.04045, ((g + 0.055) / 1.055) ** 2.4, g / 12.92)
    b = np.where(b > 0.04045, ((b + 0.055) / 1.055) ** 2.4, b / 12.92)

    x = (r * 0.4124564 + g * 0.3575761 + b * 0.1804375) / 0.95047
    y = (r * 0.2126729 + g * 0.7151522 + b * 0.0721750)
    z = (r * 0.0193339 + g * 0.1191920 + b * 0.9503041) / 1.08883

    x = np.where(x > 0.008856, x ** (1/3), 7.787 * x + 16/116)
    y = np.where(y > 0.008856, y ** (1/3), 7.787 * y + 16/116)
    z = np.where(z > 0.008856, z ** (1/3), 7.787 * z + 16/116)

    l = 116 * y - 16
    a = 500 * (x - y)
    b_val = 200 * (y - z)

    return np.stack([l, a, b_val], axis=-1)


def rgb_to_lab(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to CIE LAB color space.

    Args:
        image: RGB image (H x W x 3).

    Returns:
        LAB image (H x W x 3), L in [0, 100], a and b in [-128, 127].
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r = image[:, :, 0] / 255.0
    g = image[:, :, 1] / 255.0
    b = image[:, :, 2] / 255.0

    # To linear RGB
    r = np.where(r > 0.04045, ((r + 0.055) / 1.055) ** 2.4, r / 12.92)
    g = np.where(g > 0.04045, ((g + 0.055) / 1.055) ** 2.4, g / 12.92)
    b = np.where(b > 0.04045, ((b + 0.055) / 1.055) ** 2.4, b / 12.92)

    # To XYZ
    x = r * 0.4124564 + g * 0.3575761 + b * 0.1804375
    y = r * 0.2126729 + g * 0.7151522 + b * 0.0721750
    z = r * 0.0193339 + g * 0.1191920 + b * 0.9503041

    # Reference white D65
    x = x / 0.95047
    z = z / 1.08883

    # To LAB
    def f(t):
        delta = 6/29
        return np.where(t > delta ** 3, t ** (1/3), t / (3 * delta ** 2) + 4/29)

    fx = f(x)
    fy = f(y)
    fz = f(z)

    l = 116 * fy - 16
    a = 500 * (fx - fy)
    b_val = 200 * (fy - fz)

    l = np.clip(l, 0, 100).astype(np.uint8)
    a = np.clip(a, -128, 127).astype(np.int8)
    b_val = np.clip(b_val, -128, 127).astype(np.int8)

    return np.stack([l, a, b_val], axis=-1)


# =============================================================================
# YUV and YCbCr
# =============================================================================


def rgb_to_yuv(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to YUV color space.

    Args:
        image: RGB image (H x W x 3).

    Returns:
        YUV image (H x W x 3).
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r = image[:, :, 0].astype(float)
    g = image[:, :, 1].astype(float)
    b = image[:, :, 2].astype(float)

    y = 0.299 * r + 0.587 * g + 0.114 * b
    u = -0.147 * r - 0.289 * g + 0.436 * b
    v = 0.615 * r - 0.515 * g - 0.100 * b

    y = np.clip(y, 0, 255).astype(np.uint8)
    u = np.clip(u + 128, 0, 255).astype(np.uint8)
    v = np.clip(v + 128, 0, 255).astype(np.uint8)

    return np.stack([y, u, v], axis=-1)


def rgb_to_ycbcr(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB to YCbCr color space (JPEG compression standard).

    Args:
        image: RGB image (H x W x 3).

    Returns:
        YCbCr image (H x W x 3).
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Input must be HxWx3 RGB image")

    r = image[:, :, 0].astype(float)
    g = image[:, :, 1].astype(float)
    b = image[:, :, 2].astype(float)

    y = 16 + (65.738 * r + 129.057 * g + 25.216 * b) / 256
    cb = 128 + (-37.945 * r - 74.494 * g + 112.439 * b) / 256
    cr = 128 + (112.439 * r - 94.154 * g - 18.285 * b) / 256

    y = np.clip(y, 0, 255).astype(np.uint8)
    cb = np.clip(cb, 0, 255).astype(np.uint8)
    cr = np.clip(cr, 0, 255).astype(np.uint8)

    return np.stack([y, cb, cr], axis=-1)


# =============================================================================
# Color Difference
# =============================================================================


def delta_e_cie76(lab1: np.ndarray, lab2: np.ndarray) -> float:
    """
    Compute CIE76 Delta-E color difference.

    Args:
        lab1: First LAB color (L, a, b).
        lab2: Second LAB color (L, a, b).

    Returns:
        Delta-E value (0 = identical, higher = more different).
    """
    if lab1.shape != lab2.shape:
        raise ValueError("Color arrays must have the same shape")

    dL = lab1[:, :, 0] - lab2[:, :, 0]
    da = lab1[:, :, 1] - lab2[:, :, 1]
    db = lab1[:, :, 2] - lab2[:, :, 2]

    return float(np.sqrt((dL ** 2 + da ** 2 + db ** 2).mean()))


def delta_e_cie94(
    lab1: np.ndarray,
    lab2: np.ndarray,
    kL: float = 1.0,
    kC: float = 1.0,
    kH: float = 1.0,
) -> float:
    """
    Compute CIE94 Delta-E color difference.

    More accurate than CIE76 for industrial applications.

    Args:
        lab1: First LAB color.
        lab2: Second LAB color.
        kL: Lightness weight.
        kC: Chroma weight.
        kH: Hue weight.

    Returns:
        Delta-E94 value.
    """
    dL = lab1[:, :, 0] - lab2[:, :, 0]
    L1, a1, b1 = lab1[:, :, 0], lab1[:, :, 1], lab1[:, :, 2]
    L2, a2, b2 = lab2[:, :, 0], lab2[:, :, 1], lab2[:, :, 2]

    C1 = np.sqrt(a1 ** 2 + b1 ** 2)
    C2 = np.sqrt(a2 ** 2 + b2 ** 2)
    dC = C1 - C2

    dA = a1 - a2
    db = b1 - b2
    dH_sq = np.maximum(0.0, dA ** 2 + db ** 2 - dC ** 2)
    dH = np.sqrt(dH_sq)

    SL = 1.0
    SC = 1.0 + 0.045 * C1
    SH = 1.0 + 0.015 * C1

    kL_sq, kC_sq, kH_sq = kL ** 2, kC ** 2, kH ** 2

    L_term = dL / (kL * SL)
    C_term = dC / (kC * SC)
    H_term = dH / (kH * SH)

    de94 = np.sqrt(L_term ** 2 + C_term ** 2 + H_term ** 2)

    return float(de94.mean())


# =============================================================================
# Gamma and Tone Adjustments
# =============================================================================


def adjust_gamma(image: np.ndarray, gamma: float = 1.0) -> np.ndarray:
    """
    Apply gamma correction to an image.

    Args:
        image: Input image.
        gamma: Gamma value (> 1 brightens, < 1 darkens).

    Returns:
        Gamma-corrected image.
    """
    if len(image.shape) == 3:
        r, g, b = image[:, :, 0] / 255.0, image[:, :, 1] / 255.0, image[:, :, 2] / 255.0
        r = np.power(np.clip(r, 0, 1), 1.0 / gamma)
        g = np.power(np.clip(g, 0, 1), 1.0 / gamma)
        b = np.power(np.clip(b, 0, 1), 1.0 / gamma)
        return np.stack([r * 255, g * 255, b * 255], axis=-1).astype(np.uint8)
    else:
        normalized = image / 255.0
        corrected = np.power(np.clip(normalized, 0, 1), 1.0 / gamma)
        return (corrected * 255).astype(np.uint8)


def adjust_brightness(image: np.ndarray, factor: float) -> np.ndarray:
    """
    Adjust image brightness.

    Args:
        image: Input image.
        factor: Brightness factor (1.0 = no change, >1 = brighter).

    Returns:
        Brightness-adjusted image.
    """
    result = image.astype(float) * factor
    return np.clip(result, 0, 255).astype(np.uint8)


def adjust_contrast(image: np.ndarray, factor: float) -> np.ndarray:
    """
    Adjust image contrast.

    Args:
        image: Input image.
        factor: Contrast factor (1.0 = no change, >1 = more contrast).

    Returns:
        Contrast-adjusted image.
    """
    mean = image.mean()
    result = (image.astype(float) - mean) * factor + mean
    return np.clip(result, 0, 255).astype(np.uint8)
