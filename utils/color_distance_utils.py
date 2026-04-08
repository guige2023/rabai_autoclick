"""Color distance and comparison utilities.

Provides color distance metrics and similarity scoring
for UI element matching and color analysis.
"""

import math
from typing import Tuple


RGBTriple = Tuple[int, int, int]
LABTriple = Tuple[float, float, float]


def rgb_to_lab(rgb: RGBTriple) -> LABTriple:
    """Convert RGB to LAB color space.

    Args:
        rgb: (R, G, B) tuple with values 0-255.

    Returns:
        (L, a, b) LAB values.
    """
    r, g, b = [x / 255.0 for x in rgb]
    r = _pivot_rgb(r)
    g = _pivot_rgb(g)
    b = _pivot_rgb(b)

    x = (r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047
    y = (r * 0.2126 + g * 0.7152 + b * 0.0722) / 1.0
    z = (r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883

    x = _pivot_xyz(x)
    y = _pivot_xyz(y)
    z = _pivot_xyz(z)

    l = max(0, 116 * y - 16)
    a = 500 * (x - y)
    b_lab = 200 * (y - z)
    return (l, a, b_lab)


def _pivot_rgb(n: float) -> float:
    if n > 0.04045:
        return math.pow((n + 0.055) / 1.055, 2.4)
    return n / 12.92


def _pivot_xyz(n: float) -> float:
    if n > 0.008856:
        return math.pow(n, 1.0 / 3.0)
    return 7.787 * n + 16.0 / 116.0


def euclidean_distance(rgb1: RGBTriple, rgb2: RGBTriple) -> float:
    """Calculate Euclidean distance between two RGB colors.

    Args:
        rgb1: First RGB color.
        rgb2: Second RGB color.

    Returns:
        Euclidean distance (0-441.67).
    """
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(rgb1, rgb2)))


def manhattan_distance(rgb1: RGBTriple, rgb2: RGBTriple) -> float:
    """Calculate Manhattan distance between two RGB colors.

    Args:
        rgb1: First RGB color.
        rgb2: Second RGB color.

    Returns:
        Manhattan distance (0-765).
    """
    return sum(abs(a - b) for a, b in zip(rgb1, rgb2))


def delta_e(rgb1: RGBTriple, rgb2: RGBTriple) -> float:
    """Calculate CIEDE2000 color difference (Delta E).

    Args:
        rgb1: First RGB color.
        rgb2: Second RGB color.

    Returns:
        Delta E value (0-100+). Below 1 is imperceptible,
        below 3 is noticeable to trained eye.
    """
    lab1 = rgb_to_lab(rgb1)
    lab2 = rgb_to_lab(rgb2)
    return _ciede2000(lab1, lab2)


def _ciede2000(lab1: LABTriple, lab2: LABTriple) -> float:
    """Simplified CIEDE2000 calculation."""
    l1, a1, b1 = lab1
    l2, a2, b2 = lab2
    delta_l = l2 - l1
    avg_l = (l1 + l2) / 2
    c1 = math.sqrt(a1 * a1 + b1 * b1)
    c2 = math.sqrt(a2 * a2 + b2 * b2)
    avg_c = (c1 + c2) / 2
    delta_c = c2 - c1
    delta_h_sq = (a2 - a1) ** 2 + (b2 - b1) ** 2 - delta_c ** 2
    delta_h = math.sqrt(max(0, delta_h_sq))
    return math.sqrt(delta_l ** 2 + delta_c ** 2 + delta_h ** 2)


def rgb_similarity(rgb1: RGBTriple, rgb2: RGBTriple) -> float:
    """Calculate RGB similarity as a percentage.

    Args:
        rgb1: First RGB color.
        rgb2: Second RGB color.

    Returns:
        Similarity from 0.0 (completely different) to 1.0 (identical).
    """
    dist = euclidean_distance(rgb1, rgb2)
    return 1.0 - min(dist / 441.67, 1.0)


def hex_to_rgb(hex_color: str) -> RGBTriple:
    """Parse hex color string to RGB tuple.

    Args:
        hex_color: Hex color string (e.g., "#FF5733" or "FF5733").

    Returns:
        (R, G, B) tuple.
    """
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 3:
        hex_color = "".join(c * 2 for c in hex_color)
    return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))


def rgb_to_hex(rgb: RGBTriple) -> str:
    """Convert RGB to hex color string.

    Args:
        rgb: (R, G, B) tuple.

    Returns:
        Hex color string (e.g., "#FF5733").
    """
    return "#{:02X}{:02X}{:02X}".format(*rgb)
