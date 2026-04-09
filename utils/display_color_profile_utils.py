"""
Display Color Profile Utilities

Utilities for working with display color profiles, color space
conversions, and color management for screenshots and UI capture.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Optional


@dataclass
class ColorProfile:
    """A display color profile definition."""
    name: str
    color_space: str  # 'sRGB', 'Display P3', 'Adobe RGB', etc.
    gamma: float
    red_primary: Tuple[float, float]
    green_primary: Tuple[float, float]
    blue_primary: Tuple[float, float]
    white_point: Tuple[float, float]  # x, y


BUILTIN_PROFILES = {
    "srgb": ColorProfile(
        name="sRGB",
        color_space="sRGB",
        gamma=2.2,
        red_primary=(0.64, 0.33),
        green_primary=(0.30, 0.60),
        blue_primary=(0.15, 0.06),
        white_point=(0.3127, 0.3290),
    ),
    "display_p3": ColorProfile(
        name="Display P3",
        color_space="Display P3",
        gamma=2.4,
        red_primary=(0.680, 0.320),
        green_primary=(0.265, 0.690),
        blue_primary=(0.150, 0.060),
        white_point=(0.3127, 0.3290),
    ),
}


def convert_color_space(
    r: float, g: float, b: float,
    from_profile: ColorProfile,
    to_profile: ColorProfile,
) -> Tuple[float, float, float]:
    """
    Convert a color from one color profile to another.

    Args:
        r, g, b: Color components in [0.0, 1.0] range.
        from_profile: Source color profile.
        to_profile: Target color profile.

    Returns:
        Converted (r, g, b) tuple.
    """
    # Simple gamma adjustment approximation
    from_linear = _gamma_decode(r, from_profile.gamma)
    from_linear_g = _gamma_decode(g, from_profile.gamma)
    from_linear_b = _gamma_decode(b, from_profile.gamma)

    to_linear = _gamma_encode(from_linear, to_profile.gamma)
    to_linear_g = _gamma_encode(from_linear_g, to_profile.gamma)
    to_linear_b = _gamma_encode(from_linear_b, to_profile.gamma)

    return (
        max(0.0, min(1.0, to_linear)),
        max(0.0, min(1.0, to_linear_g)),
        max(0.0, min(1.0, to_linear_b)),
    )


def _gamma_decode(value: float, gamma: float) -> float:
    """Decode a gamma-encoded value to linear."""
    if value <= 0.04045:
        return value / 12.92
    return ((value + 0.055) / 1.055) ** gamma


def _gamma_encode(value: float, gamma: float) -> float:
    """Encode a linear value to gamma."""
    if value <= 0.0031308:
        return value * 12.92
    return 1.055 * (value ** (1.0 / gamma)) - 0.055


def get_profile_for_color_space(color_space: str) -> Optional[ColorProfile]:
    """Get a built-in color profile by color space name."""
    for profile in BUILTIN_PROFILES.values():
        if profile.color_space.lower() == color_space.lower():
            return profile
    return None
