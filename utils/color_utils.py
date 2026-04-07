"""Color utilities for RabAI AutoClick.

Provides:
- Color conversion (RGB, HEX, HSL, HSV)
- ANSI color code generation
- Color manipulation and blending
"""

import re
from typing import (
    NamedTuple,
    Optional,
    Tuple,
    Union,
)


class RGB(NamedTuple):
    """RGB color tuple."""
    r: int
    g: int
    b: int

    def __repr__(self) -> str:
        return f"RGB({self.r}, {self.g}, {self.b})"


class HSL(NamedTuple):
    """HSL color tuple."""
    h: float  # 0-360
    s: float  # 0-100
    l: float  # 0-100

    def __repr__(self) -> str:
        return f"HSL({self.h:.1f}, {self.s:.1f}, {self.l:.1f})"


class HSV(NamedTuple):
    """HSV color tuple."""
    h: float  # 0-360
    s: float  # 0-100
    v: float  # 0-100

    def __repr__(self) -> str:
        return f"HSV({self.h:.1f}, {self.s:.1f}, {self.v:.1f})"


def hex_to_rgb(hex_color: str) -> RGB:
    """Convert HEX color to RGB.

    Args:
        hex_color: Hex color string (#RGB, #RRGGBB, RGB, RRGGBB).

    Returns:
        RGB tuple.
    """
    hex_color = hex_color.lstrip("#")

    if len(hex_color) == 3:
        hex_color = "".join(c * 2 for c in hex_color)

    if len(hex_color) != 6:
        raise ValueError(f"Invalid hex color: {hex_color}")

    return RGB(
        r=int(hex_color[0:2], 16),
        g=int(hex_color[2:4], 16),
        b=int(hex_color[4:6], 16),
    )


def rgb_to_hex(rgb: Union[Tuple[int, int, int], RGB]) -> str:
    """Convert RGB to HEX color string.

    Args:
        rgb: RGB tuple (values 0-255).

    Returns:
        Hex color string (#RRGGBB).
    """
    r, g, b = rgb
    return f"#{r:02x}{g:02x}{b:02x}"


def rgb_to_hsl(rgb: Union[Tuple[int, int, int], RGB]) -> HSL:
    """Convert RGB to HSL.

    Args:
        rgb: RGB tuple (values 0-255).

    Returns:
        HSL tuple.
    """
    r, g, b = [x / 255.0 for x in rgb]
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    l = (max_c + min_c) / 2.0

    if max_c == min_c:
        h = 0.0
        s = 0.0
    else:
        d = max_c - min_c
        s = d / (2.0 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)

        if max_c == r:
            h = ((g - b) / d + (6 if g < b else 0)) / 6.0
        elif max_c == g:
            h = ((b - r) / d + 2) / 6.0
        else:
            h = ((r - g) / d + 4) / 6.0

    return HSL(h=h * 360, s=s * 100, l=l * 100)


def hsl_to_rgb(hsl: Union[Tuple[float, float, float], HSL]) -> RGB:
    """Convert HSL to RGB.

    Args:
        hsl: HSL tuple (h: 0-360, s: 0-100, l: 0-100).

    Returns:
        RGB tuple (values 0-255).
    """
    h, s, l = [x / 100.0 if i > 0 else x / 360.0 for i, x in enumerate(hsl)]

    if s == 0:
        gray = int(round(l * 255))
        return RGB(r=gray, g=gray, b=gray)

    def hue_to_rgb(p: float, q: float, t: float) -> float:
        if t < 0:
            t += 1
        if t > 1:
            t -= 1
        if t < 1 / 6:
            return p + (q - p) * 6 * t
        if t < 1 / 2:
            return q
        if t < 2 / 3:
            return p + (q - p) * (2 / 3 - t) * 6
        return p

    q = l * (1 + s) if l < 0.5 else l + s - l * s
    p = 2 * l - q

    return RGB(
        r=int(round(hue_to_rgb(p, q, h + 1 / 3) * 255)),
        g=int(round(hue_to_rgb(p, q, h) * 255)),
        b=int(round(hue_to_rgb(p, q, h - 1 / 3) * 255)),
    )


def rgb_to_hsv(rgb: Union[Tuple[int, int, int], RGB]) -> HSV:
    """Convert RGB to HSV.

    Args:
        rgb: RGB tuple (values 0-255).

    Returns:
        HSV tuple.
    """
    r, g, b = [x / 255.0 for x in rgb]
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    v = max_c

    if max_c == 0:
        return HSV(h=0, s=0, v=v * 100)

    s = (max_c - min_c) / max_c
    d = max_c - min_c

    if max_c == r:
        h = ((g - b) / d + (6 if g < b else 0)) / 6.0
    elif max_c == g:
        h = ((b - r) / d + 2) / 6.0
    else:
        h = ((r - g) / d + 4) / 6.0

    return HSV(h=h * 360, s=s * 100, v=v * 100)


def hsv_to_rgb(hsv: Union[Tuple[float, float, float], HSV]) -> RGB:
    """Convert HSV to RGB.

    Args:
        hsv: HSV tuple (h: 0-360, s: 0-100, v: 0-100).

    Returns:
        RGB tuple (values 0-255).
    """
    h, s, v = [x / 100.0 if i > 0 else x / 360.0 for i, x in enumerate(hsv)]
    s *= v

    if s == 0:
        gray = int(round(v * 255))
        return RGB(r=gray, g=gray, b=gray)

    h_i = int(h * 6)
    f = h * 6 - h_i
    p = v * (1 - s)
    q = v * (1 - f * s)
    t = v * (1 - (1 - f) * s)

    if h_i == 0:
        r, g, b = v, t, p
    elif h_i == 1:
        r, g, b = q, v, p
    elif h_i == 2:
        r, g, b = p, v, t
    elif h_i == 3:
        r, g, b = p, q, v
    elif h_i == 4:
        r, g, b = t, p, v
    else:
        r, g, b = v, p, q

    return RGB(
        r=int(round(r * 255)),
        g=int(round(g * 255)),
        b=int(round(b * 255)),
    )


def lighten(rgb: Union[Tuple[int, int, int], RGB], amount: float = 0.2) -> RGB:
    """Lighten a color.

    Args:
        rgb: RGB color.
        amount: Amount to lighten (0-1).

    Returns:
        Lightened RGB color.
    """
    hsl = rgb_to_hsl(rgb)
    new_l = min(100, hsl.l + amount * 100)
    return hsl_to_rgb(HSL(hsl.h, hsl.s, new_l))


def darken(rgb: Union[Tuple[int, int, int], RGB], amount: float = 0.2) -> RGB:
    """Darken a color.

    Args:
        rgb: RGB color.
        amount: Amount to darken (0-1).

    Returns:
        Darkened RGB color.
    """
    hsl = rgb_to_hsl(rgb)
    new_l = max(0, hsl.l - amount * 100)
    return hsl_to_rgb(HSL(hsl.h, hsl.s, new_l))


def saturate(rgb: Union[Tuple[int, int, int], RGB], amount: float = 0.2) -> RGB:
    """Saturate a color.

    Args:
        rgb: RGB color.
        amount: Amount to saturate (0-1).

    Returns:
        Saturated RGB color.
    """
    hsl = rgb_to_hsl(rgb)
    new_s = min(100, hsl.s + amount * 100)
    return hsl_to_rgb(HSL(hsl.h, new_s, hsl.l))


def desaturate(rgb: Union[Tuple[int, int, int], RGB], amount: float = 0.2) -> RGB:
    """Desaturate a color.

    Args:
        rgb: RGB color.
        amount: Amount to desaturate (0-1).

    Returns:
        Desaturated RGB color.
    """
    hsl = rgb_to_hsl(rgb)
    new_s = max(0, hsl.s - amount * 100)
    return hsl_to_rgb(HSL(hsl.h, new_s, hsl.l))


def blend(
    color1: Union[Tuple[int, int, int], RGB],
    color2: Union[Tuple[int, int, int], RGB],
    factor: float = 0.5,
) -> RGB:
    """Blend two colors.

    Args:
        color1: First RGB color.
        color2: Second RGB color.
        factor: Blend factor (0 = color1, 1 = color2).

    Returns:
        Blended RGB color.
    """
    r1, g1, b1 = color1
    r2, g2, b2 = color2
    return RGB(
        r=int(r1 + (r2 - r1) * factor),
        g=int(g1 + (g2 - g1) * factor),
        b=int(b1 + (b2 - b1) * factor),
    )


def get_contrast_color(rgb: Union[Tuple[int, int, int], RGB]) -> RGB:
    """Get a contrasting text color (black or white) for a background.

    Args:
        rgb: Background RGB color.

    Returns:
        RGB(0, 0, 0) or RGB(255, 255, 255).
    """
    r, g, b = rgb
    # Calculate relative luminance
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return RGB(0, 0, 0) if luminance > 0.5 else RGB(255, 255, 255)


def ansi_fg(rgb: Union[Tuple[int, int, int], RGB]) -> str:
    """Get ANSI foreground color code.

    Args:
        rgb: RGB color.

    Returns:
        ANSI escape sequence for foreground color.
    """
    r, g, b = rgb
    return f"\033[38;2;{r};{g};{b}m"


def ansi_bg(rgb: Union[Tuple[int, int, int], RGB]) -> str:
    """Get ANSI background color code.

    Args:
        rgb: RGB color.

    Returns:
        ANSI escape sequence for background color.
    """
    r, g, b = rgb
    return f"\033[48;2;{r};{g};{b}m"


ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_DIM = "\033[2m"
ANSI_ITALIC = "\033[3m"
ANSI_UNDERLINE = "\033[4m"


def colorize(
    text: str,
    fg: Optional[Union[Tuple[int, int, int], RGB]] = None,
    bg: Optional[Union[Tuple[int, int, int], RGB]] = None,
    bold: bool = False,
    dim: bool = False,
    italic: bool = False,
    underline: bool = False,
) -> str:
    """Colorize text with ANSI codes.

    Args:
        text: Text to colorize.
        fg: Foreground RGB color.
        bg: Background RGB color.
        bold: Apply bold.
        dim: Apply dim.
        italic: Apply italic.
        underline: Apply underline.

    Returns:
        ANSI-colored text string.
    """
    codes: list[str] = []

    if fg:
        codes.append(ansi_fg(fg))
    if bg:
        codes.append(ansi_bg(bg))
    if bold:
        codes.append(ANSI_BOLD)
    if dim:
        codes.append(ANSI_DIM)
    if italic:
        codes.append(ANSI_ITALIC)
    if underline:
        codes.append(ANSI_UNDERLINE)

    if not codes:
        return text

    return "".join(codes) + text + ANSI_RESET


def is_valid_hex(hex_color: str) -> bool:
    """Check if a string is a valid hex color.

    Args:
        hex_color: String to check.

    Returns:
        True if valid hex color.
    """
    pattern = r"^#?([A-Fa-f0-9]{3}|[A-Fa-f0-9]{6})$"
    return bool(re.match(pattern, hex_color))
