"""UI Theme utilities for theme detection and management.

This module provides utilities for detecting and working with UI themes,
including color extraction, theme switching, and style consistency.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import Counter
import colorsys


class ThemeMode(Enum):
    """UI theme modes."""
    LIGHT = "light"
    DARK = "dark"
    HIGH_CONTRAST = "high_contrast"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


class ColorFormat(Enum):
    """Color format representations."""
    HEX = "hex"
    RGB = "rgb"
    RGBA = "rgba"
    HSL = "hsl"
    HSV = "hsv"


@dataclass
class Color:
    """Represents a color in multiple formats."""
    r: float  # 0-255
    g: float  # 0-255
    b: float  # 0-255
    a: float = 1.0  # 0-1

    def to_hex(self) -> str:
        """Convert to hex format."""
        if self.a < 1.0:
            return f"#{int(self.r):02x}{int(self.g):02x}{int(self.b):02x}{int(self.a * 255):02x}"
        return f"#{int(self.r):02x}{int(self.g):02x}{int(self.b):02x}"

    def to_rgb(self) -> Tuple[int, int, int]:
        """Convert to RGB tuple."""
        return (int(self.r), int(self.g), int(self.b))

    def to_rgba(self) -> Tuple[int, int, int, float]:
        """Convert to RGBA tuple."""
        return (int(self.r), int(self.g), int(self.b), self.a)

    def to_hsl(self) -> Tuple[float, float, float]:
        """Convert to HSL tuple."""
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        return (h * 360, s * 100, l * 100)

    def to_hsv(self) -> Tuple[float, float, float]:
        """Convert to HSV tuple."""
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        h, s, v = colorsys.rgb_to_hsv(r, g, b)
        return (h * 360, s * 100, v * 100)

    def luminance(self) -> float:
        """Calculate relative luminance."""
        r = self.r / 255
        g = self.g / 255
        b = self.b / 255
        return 0.299 * r + 0.587 * g + 0.114 * b

    def is_dark(self, threshold: float = 0.5) -> bool:
        """Check if color is dark."""
        return self.luminance() < threshold

    def is_light(self, threshold: float = 0.5) -> bool:
        """Check if color is light."""
        return self.luminance() >= threshold

    def contrast_ratio(self, other: 'Color') -> float:
        """Calculate contrast ratio with another color."""
        l1 = self.luminance()
        l2 = other.luminance()
        lighter = max(l1, l2)
        darker = min(l1, l2)
        return (lighter + 0.05) / (darker + 0.05)

    def meets_contrast_requirement(self, other: 'Color',
                                   level_aa: float = 4.5,
                                   level_aaa: float = 7.0) -> Dict[str, bool]:
        """Check if contrast meets WCAG requirements."""
        ratio = self.contrast_ratio(other)
        return {
            "aa_normal": ratio >= level_aa,
            "aa_large": ratio >= 3.0,
            "aaa_normal": ratio >= level_aaa,
            "aaa_large": ratio >= 4.5,
        }

    @classmethod
    def from_hex(cls, hex_str: str) -> 'Color':
        """Create color from hex string."""
        hex_str = hex_str.lstrip('#')
        if len(hex_str) == 6:
            return cls(
                r=float(int(hex_str[0:2], 16)),
                g=float(int(hex_str[2:4], 16)),
                b=float(int(hex_str[4:6], 16))
            )
        elif len(hex_str) == 8:
            return cls(
                r=float(int(hex_str[0:2], 16)),
                g=float(int(hex_str[2:4], 16)),
                b=float(int(hex_str[4:6], 16)),
                a=float(int(hex_str[6:8], 16)) / 255
            )
        raise ValueError(f"Invalid hex color: {hex_str}")

    @classmethod
    def from_rgb(cls, r: int, g: int, b: int, a: float = 1.0) -> 'Color':
        """Create color from RGB values."""
        return cls(r=float(r), g=float(g), b=float(b), a=a)

    @classmethod
    def from_hsl(cls, h: float, s: float, l: float) -> 'Color':
        """Create color from HSL values."""
        h_norm = h / 360
        s_norm = s / 100
        l_norm = l / 100
        r, g, b = colorsys.hls_to_rgb(h_norm, l_norm, s_norm)
        return cls(r * 255, g * 255, b * 255)

    def lighten(self, amount: float) -> 'Color':
        """Lighten color by amount (0-1)."""
        h, s, l = self.to_hsl()
        l = min(100, l + amount * 100)
        return Color.from_hsl(h, s, l)

    def darken(self, amount: float) -> 'Color':
        """Darken color by amount (0-1)."""
        h, s, l = self.to_hsl()
        l = max(0, l - amount * 100)
        return Color.from_hsl(h, s, l)

    def saturate(self, amount: float) -> 'Color':
        """Saturate color by amount (0-1)."""
        h, s, l = self.to_hsl()
        s = min(100, s + amount * 100)
        return Color.from_hsl(h, s, l)

    def desaturate(self, amount: float) -> 'Color':
        """Desaturate color by amount (0-1)."""
        h, s, l = self.to_hsl()
        s = max(0, s - amount * 100)
        return Color.from_hsl(h, s, l)

    def blend_with(self, other: 'Color', ratio: float = 0.5) -> 'Color':
        """Blend with another color."""
        r = self.r * (1 - ratio) + other.r * ratio
        g = self.g * (1 - ratio) + other.g * ratio
        b = self.b * (1 - ratio) + other.b * ratio
        a = self.a * (1 - ratio) + other.a * ratio
        return Color(r, g, b, a)

    def __repr__(self) -> str:
        if self.a < 1.0:
            return f"Color({self.r:.0f}, {self.g:.0f}, {self.b:.0f}, {self.a:.2f})"
        return f"Color({self.r:.0f}, {self.g:.0f}, {self.b:.0f})"


@dataclass
class ThemeColors:
    """Collection of theme colors."""
    primary: Color
    secondary: Color
    background: Color
    foreground: Color
    accent: Color
    error: Color
    warning: Color
    success: Color
    border: Color
    disabled: Color
    text_primary: Color
    text_secondary: Color
    text_disabled: Color
    link: Color
    link_hover: Color


@dataclass
class Theme:
    """Represents a UI theme."""
    name: str
    mode: ThemeMode
    colors: ThemeColors
    font_family: str = "system-ui"
    font_size_base: float = 16.0
    spacing_base: float = 4.0
    border_radius: float = 4.0
    shadows_enabled: bool = True

    def get_color(self, name: str) -> Optional[Color]:
        """Get color by name."""
        color_map = {
            "primary": self.colors.primary,
            "secondary": self.colors.secondary,
            "background": self.colors.background,
            "foreground": self.colors.foreground,
            "accent": self.colors.accent,
            "error": self.colors.error,
            "warning": self.colors.warning,
            "success": self.colors.success,
            "border": self.colors.border,
            "disabled": self.colors.disabled,
            "text_primary": self.colors.text_primary,
            "text_secondary": self.colors.text_secondary,
            "text_disabled": self.colors.text_disabled,
            "link": self.colors.link,
            "link_hover": self.colors.link_hover,
        }
        return color_map.get(name)


class ThemeDetector:
    """Detects UI theme from system or application."""

    LIGHT_COLORS = [
        Color.from_hex("#FFFFFF"),
        Color.from_hex("#F5F5F5"),
        Color.from_hex("#FAFAFA"),
    ]

    DARK_COLORS = [
        Color.from_hex("#000000"),
        Color.from_hex("#1A1A1A"),
        Color.from_hex("#2D2D2D"),
        Color.from_hex("#3D3D3D"),
    ]

    def __init__(self):
        self.detected_theme: Optional[Theme] = None
        self.theme_history: List[ThemeMode] = []

    def detect_from_colors(self, colors: List[Color]) -> ThemeMode:
        """Detect theme mode from a list of colors."""
        if not colors:
            return ThemeMode.UNKNOWN

        dark_count = sum(1 for c in colors if c.is_dark())
        light_count = sum(1 for c in colors if c.is_light())

        dark_ratio = dark_count / len(colors)

        if dark_ratio > 0.7:
            mode = ThemeMode.DARK
        elif dark_ratio < 0.3:
            mode = ThemeMode.LIGHT
        else:
            mode = ThemeMode.HIGH_CONTRAST

        self.theme_history.append(mode)
        return mode

    def detect_from_background(self, bg_color: Color) -> ThemeMode:
        """Detect theme mode from background color."""
        if bg_color.is_dark(threshold=0.3):
            mode = ThemeMode.DARK
        elif bg_color.is_dark(threshold=0.7):
            mode = ThemeMode.LIGHT
        else:
            mode = ThemeMode.HIGH_CONTRAST

        self.theme_history.append(mode)
        return mode

    def get_dominant_colors(self, colors: List[Color],
                           exclude_light: bool = True,
                           exclude_dark: bool = True) -> List[Color]:
        """Get dominant colors from a list, excluding extremes."""
        if exclude_light:
            colors = [c for c in colors if not c.is_light(threshold=0.9)]
        if exclude_dark:
            colors = [c for c in colors if not c.is_dark(threshold=0.1)]

        if not colors:
            return []

        color_counts = Counter([c.to_hex()[:7] for c in colors])
        dominant_hex = [c for c, _ in color_counts.most_common(5)]

        return [Color.from_hex(h) for h in dominant_hex]

    def infer_foreground(self, background: Color) -> Color:
        """Infer foreground color for given background."""
        if background.is_dark():
            return Color.from_hex("#FFFFFF")
        else:
            return Color.from_hex("#000000")

    def create_theme_from_detection(self,
                                    name: str,
                                    background: Color,
                                    foreground: Optional[Color] = None,
                                    accent: Optional[Color] = None) -> Theme:
        """Create a theme from detected colors."""
        mode = self.detect_from_background(background)

        if foreground is None:
            foreground = self.infer_foreground(background)

        if accent is None:
            accent = Color.from_hex("#007AFF")

        if mode == ThemeMode.DARK:
            border = Color.from_hex("#3D3D3D")
            disabled = Color.from_hex("#555555")
            text_secondary = Color.from_hex("#999999")
        else:
            border = Color.from_hex("#E5E5E5")
            disabled = Color.from_hex("#CCCCCC")
            text_secondary = Color.from_hex("#666666")

        colors = ThemeColors(
            primary=accent,
            secondary=foreground.blend_with(background, 0.3),
            background=background,
            foreground=foreground,
            accent=accent,
            error=Color.from_hex("#FF3B30"),
            warning=Color.from_hex("#FF9500"),
            success=Color.from_hex("#34C759"),
            border=border,
            disabled=disabled,
            text_primary=foreground,
            text_secondary=text_secondary,
            text_disabled=disabled,
            link=accent,
            link_hover=accent.lighten(0.1),
        )

        self.detected_theme = Theme(name=name, mode=mode, colors=colors)
        return self.detected_theme


def extract_colors_from_screenshot(color_map: Dict[str, Any]) -> List[Color]:
    """Extract color palette from screenshot color data."""
    colors = []
    for key, value in color_map.items():
        if isinstance(value, str) and value.startswith('#'):
            try:
                colors.append(Color.from_hex(value))
            except ValueError:
                continue
    return colors


def get_readable_text_color(background: Color) -> Color:
    """Get best text color for given background (black or white)."""
    dark = Color.from_hex("#000000")
    light = Color.from_hex("#FFFFFF")

    contrast_dark = background.contrast_ratio(dark)
    contrast_light = background.contrast_ratio(light)

    if contrast_dark > contrast_light:
        return dark
    return light


def generate_color_palette(base_color: Color, count: int = 5) -> List[Color]:
    """Generate a color palette from a base color."""
    h, s, l = base_color.to_hsl()
    palette = []

    for i in range(count):
        ratio = i / (count - 1) if count > 1 else 0.5
        if ratio < 0.5:
            new_l = l * (1 - ratio)
            new_s = s
        else:
            new_l = l + (100 - l) * (ratio - 0.5) * 2
            new_s = s * (1 - (ratio - 0.5) * 0.5)

        new_l = max(0, min(100, new_l))
        new_s = max(0, min(100, new_s))

        palette.append(Color.from_hsl(h, new_s, new_l))

    return palette
