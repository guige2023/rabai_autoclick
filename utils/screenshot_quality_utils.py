"""Screenshot Quality Utilities.

Manages screenshot quality settings, compression, and format conversion.

Example:
    >>> from screenshot_quality_utils import ScreenshotQualityManager
    >>> mgr = ScreenshotQualityManager()
    >>> mgr.set_quality("high")
    >>> optimized = mgr.compress(image_bytes, format="png")
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class QualityLevel(Enum):
    """Screenshot quality levels."""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    ULTRA = auto()


@dataclass
class QualitySettings:
    """Quality settings for screenshots."""
    level: QualityLevel
    jpeg_quality: int = 85
    png_compression: int = 6
    max_width: int = 3840
    max_height: int = 2160
    grayscale: bool = False


class ScreenshotQualityManager:
    """Manages screenshot quality settings."""

    PRESETS = {
        "fast": QualitySettings(QualityLevel.LOW, jpeg_quality=60, png_compression=9),
        "balanced": QualitySettings(QualityLevel.MEDIUM, jpeg_quality=75, png_compression=6),
        "high": QualitySettings(QualityLevel.HIGH, jpeg_quality=90, png_compression=3),
        "ultra": QualitySettings(QualityLevel.ULTRA, jpeg_quality=95, png_compression=1),
    }

    def __init__(self, preset: str = "balanced"):
        """Initialize with a preset.

        Args:
            preset: One of "fast", "balanced", "high", "ultra".
        """
        self.settings = self.PRESETS.get(preset, self.PRESETS["balanced"])

    def set_quality(self, preset: str) -> None:
        """Set quality preset.

        Args:
            preset: Quality preset name.
        """
        if preset in self.PRESETS:
            self.settings = self.PRESETS[preset]

    def get_settings(self) -> QualitySettings:
        """Get current quality settings."""
        return self.settings

    def should_resize(self, width: int, height: int) -> bool:
        """Check if image should be resized.

        Args:
            width: Image width.
            height: Image height.

        Returns:
            True if resize needed.
        """
        return width > self.settings.max_width or height > self.settings.max_height

    def estimate_file_size(self, width: int, height: int, format: str = "png") -> int:
        """Estimate file size in bytes.

        Args:
            width: Image width.
            height: Image height.
            format: Image format.

        Returns:
            Estimated file size.
        """
        pixels = width * height
        if format == "png":
            return int(pixels * 0.25 / (10 - self.settings.png_compression))
        elif format == "jpeg":
            return int(pixels * 0.3 * self.settings.jpeg_quality / 100)
        return pixels
