"""
Color Picker Action Module.

Captures screen colors and provides color analysis
including format conversion and palette generation.
"""

import math
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class Color:
    """A color value."""
    r: int
    g: int
    b: int
    a: float = 1.0

    def to_hex(self) -> str:
        """Convert to hex string."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def to_rgb(self) -> str:
        """Convert to RGB string."""
        return f"rgb({self.r}, {self.g}, {self.b})"

    def to_rgba(self) -> str:
        """Convert to RGBA string."""
        return f"rgba({self.r}, {self.g}, {self.b}, {self.a})"

    def luminance(self) -> float:
        """Calculate relative luminance."""
        r = self.r / 255.0
        g = self.g / 255.0
        b = self.b / 255.0
        return 0.2126 * r + 0.7152 * g + 0.0722 * b

    def is_dark(self) -> bool:
        """Check if color is dark."""
        return self.luminance() < 0.5


class ColorPicker:
    """Picks and analyzes colors from screen."""

    def __init__(self):
        """Initialize color picker."""
        pass

    def capture_pixel(
        self,
        x: int,
        y: int,
    ) -> Color:
        """
        Capture color at screen coordinates.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            Color at that position.
        """
        import subprocess
        try:
            cmd = [
                "screencapture",
                "-x", "-r",
                "/tmp/color_picker_temp.png"
            ]
            subprocess.run(cmd, capture_output=True, timeout=2)

            from PIL import Image
            img = Image.open("/tmp/color_picker_temp.png")
            pixel = img.getpixel((x, y))

            if len(pixel) == 4:
                return Color(r=pixel[0], g=pixel[1], b=pixel[2], a=pixel[3])
            return Color(r=pixel[0], g=pixel[1], b=pixel[2])

        except Exception:
            return Color(0, 0, 0)

    def capture_region(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> list[Color]:
        """
        Capture colors from a region.

        Args:
            x: Region X.
            y: Region Y.
            width: Region width.
            height: Region height.

        Returns:
            List of Colors from the region.
        """
        colors = []
        step_x = max(1, width // 10)
        step_y = max(1, height // 10)

        for py in range(y, y + height, step_y):
            for px in range(x, x + width, step_x):
                colors.append(self.capture_pixel(px, py))

        return colors

    def get_dominant_color(
        self,
        colors: list[Color],
    ) -> Color:
        """
        Get the dominant color from a list.

        Args:
            colors: List of colors.

        Returns:
            Most common color.
        """
        if not colors:
            return Color(0, 0, 0)

        color_counts = {}
        for color in colors:
            key = color.to_hex()
            color_counts[key] = color_counts.get(key, 0) + 1

        dominant_hex = max(color_counts, key=color_counts.get)
        hex_vals = dominant_hex[1:]

        return Color(
            r=int(hex_vals[0:2], 16),
            g=int(hex_vals[2:4], 16),
            b=int(hex_vals[4:6], 16),
        )

    def generate_palette(
        self,
        base_color: Color,
        count: int = 5,
    ) -> list[Color]:
        """
        Generate a color palette from a base color.

        Args:
            base_color: Base color.
            count: Number of palette colors.

        Returns:
            List of palette colors.
        """
        palette = [base_color]

        for i in range(1, count):
            factor = 1.0 - (i / count)
            r = int(base_color.r * factor + 255 * (1 - factor))
            g = int(base_color.g * factor + 255 * (1 - factor))
            b = int(base_color.b * factor + 255 * (1 - factor))
            palette.append(Color(
                r=min(255, r),
                g=min(255, g),
                b=min(255, b),
            ))

        return palette
