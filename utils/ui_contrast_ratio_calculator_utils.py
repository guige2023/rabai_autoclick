"""
UI Contrast Ratio Calculator Utilities

Calculate contrast ratios between UI colors to verify
accessibility compliance (WCAG 2.1 guidelines).

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass
class ContrastRatioResult:
    """Result of a contrast ratio calculation."""
    ratio: float  # e.g., 4.5 for 4.5:1
    meets_aa_normal: bool
    meet_aa_large: bool
    meets_aaa_normal: bool
    meets_aaa_large: bool
    recommendation: str


def srgb_to_linear(c: float) -> float:
    """Convert sRGB component to linear light."""
    if c <= 0.03928:
        return c / 12.92
    return ((c + 0.055) / 1.055) ** 2.4


def get_luminance(r: float, g: float, b: float) -> float:
    """
    Compute relative luminance of an RGB color (sRGB).

    Args:
        r, g, b: Color components in [0.0, 1.0] range.

    Returns:
        Luminance value in [0.0, 1.0] range.
    """
    r_linear = srgb_to_linear(r)
    g_linear = srgb_to_linear(g)
    b_linear = srgb_to_linear(b)
    return 0.2126 * r_linear + 0.7152 * g_linear + 0.0722 * b_linear


def compute_contrast_ratio(
    color1: Tuple[float, float, float],  # (r, g, b) in [0, 255]
    color2: Tuple[float, float, float],
) -> float:
    """
    Compute WCAG contrast ratio between two colors.

    Args:
        color1: First color as (r, g, b) with values in [0, 255].
        color2: Second color as (r, g, b) with values in [0, 255].

    Returns:
        Contrast ratio (e.g., 4.5 for 4.5:1).
    """
    r1, g1, b1 = [c / 255.0 for c in color1]
    r2, g2, b2 = [c / 255.0 for c in color2]

    l1 = get_luminance(r1, g1, b1)
    l2 = get_luminance(r2, g2, b2)

    lighter = max(l1, l2)
    darker = min(l1, l2)

    return (lighter + 0.05) / (darker + 0.05)


def analyze_contrast(
    foreground: Tuple[float, float, float],
    background: Tuple[float, float, float],
) -> ContrastRatioResult:
    """
    Analyze contrast between foreground and background colors.

    Returns WCAG compliance levels and recommendations.
    """
    ratio = compute_contrast_ratio(foreground, background)

    meets_aa_normal = ratio >= 4.5
    meets_aa_large = ratio >= 3.0
    meets_aaa_normal = ratio >= 7.0
    meets_aaa_large = ratio >= 4.5

    if meets_aaa_normal:
        recommendation = "Excellent contrast, meets AAA for all text"
    elif meets_aa_normal:
        recommendation = "Good contrast, meets AA for normal text"
    elif meets_aa_large:
        recommendation = "Acceptable for large text only, upgrade recommended"
    elif ratio >= 2.0:
        recommendation = "Poor contrast, does not meet WCAG AA"
    else:
        recommendation = "Very poor contrast, likely inaccessible"

    return ContrastRatioResult(
        ratio=ratio,
        meets_aa_normal=meets_aa_normal,
        meet_aa_large=meets_aa_large,
        meets_aaa_normal=meets_aaa_normal,
        meets_aaa_large=meets_aaa_large,
        recommendation=recommendation,
    )


def suggest_better_color(
    foreground: Tuple[float, float, float],
    background: Tuple[float, float, float],
    target_ratio: float = 4.5,
) -> Tuple[float, float, float]:
    """
    Suggest a darker/lighter version of foreground to meet target ratio.

    Returns the adjusted foreground color.
    """
    fg_lum = get_luminance(*[c / 255.0 for c in foreground])
    bg_lum = get_luminance(*[c / 255.0 for c in background])

    # If fg is lighter than bg, darken fg; otherwise lighten
    if fg_lum > bg_lum:
        # Darken: reduce all channels
        factor = 0.8
    else:
        # Lighten: increase all channels
        factor = 1.2

    new_color = tuple(min(255, int(c * factor)) for c in foreground)
    current_ratio = compute_contrast_ratio(new_color, background)

    # Iteratively adjust
    iterations = 0
    while current_ratio < target_ratio and iterations < 20:
        if fg_lum > bg_lum:
            factor *= 0.9
        else:
            factor *= 1.1
        new_color = tuple(min(255, max(0, int(c * factor)) for c in foreground))
        current_ratio = compute_contrast_ratio(new_color, background)
        iterations += 1

    return new_color
