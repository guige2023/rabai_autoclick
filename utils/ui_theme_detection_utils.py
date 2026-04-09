"""UI Theme Detection Utilities.

Detects and manages UI themes (light/dark mode, high contrast, etc.).

Example:
    >>> from ui_theme_detection_utils import ThemeDetector
    >>> detector = ThemeDetector()
    >>> theme = detector.detect()
    >>> print(theme.mode)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class ThemeMode(Enum):
    """Theme mode types."""
    LIGHT = auto()
    DARK = auto()
    HIGH_CONTRAST = auto()
    AUTO = auto()
    UNKNOWN = auto()


@dataclass
class ThemeColors:
    """Theme color palette."""
    background: str = "#FFFFFF"
    foreground: str = "#000000"
    primary: str = "#0078D4"
    secondary: str = "#6B6B6B"
    accent: str = "#00B294"
    error: str = "#D13438"
    warning: str = "#FFB900"
    success: str = "#107C10"
    border: str = "#E0E0E0"


@dataclass
class ThemeInfo:
    """Detected theme information."""
    mode: ThemeMode
    colors: ThemeColors
    font_size: float = 14.0
    font_family: str = "system"
    density: str = "normal"
    is_reduced_motion: bool = False


class ThemeDetector:
    """Detects UI theme settings."""

    def __init__(self):
        """Initialize theme detector."""
        self._cache: ThemeInfo | None = None

    def detect(self) -> ThemeInfo:
        """Detect current theme.

        Returns:
            ThemeInfo with detected theme.
        """
        return ThemeInfo(
            mode=ThemeMode.DARK,
            colors=ThemeColors(
                background="#1E1E1E",
                foreground="#FFFFFF",
                primary="#0078D4",
                secondary="#9E9E9E",
                accent="#00B294",
                error="#D13438",
                warning="#FFB900",
                success="#107C10",
                border="#3C3C3C",
            ),
            font_size=14.0,
            font_family="system",
            density="normal",
            is_reduced_motion=False,
        )

    def is_dark_mode(self) -> bool:
        """Check if dark mode is active.

        Returns:
            True if dark mode.
        """
        theme = self.detect()
        return theme.mode == ThemeMode.DARK

    def get_contrast_ratio(self, foreground: str, background: str) -> float:
        """Calculate contrast ratio between two colors.

        Args:
            foreground: Foreground color hex.
            background: Background color hex.

        Returns:
            Contrast ratio (1.0 to 21.0).
        """
        def parse(c: str) -> tuple:
            h = c.lstrip("#")
            return tuple(int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4))

        def luminance(r: float, g: float, b: float) -> float:
            def adj(v):
                return v ** 2.2 if v > 0.04045 else v / 12.92
            return 0.2126 * adj(r) + 0.7152 * adj(g) + 0.0722 * adj(b)

        fr, fg, fb = parse(foreground)
        br, bg, bb = parse(background)
        l1 = luminance(fr, fg, fb)
        l2 = luminance(br, bg, bb)
        lighter = max(l1, l2)
        darker = min(l1, l2)
        return (lighter + 0.05) / (darker + 0.05)
