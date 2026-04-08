"""Color matching utilities with tolerance for automation triggers.

This module provides utilities for matching colors with configurable
tolerance, useful for detecting UI states based on color.
"""

from __future__ import annotations

from typing import NamedTuple, Optional
import math


class Color(NamedTuple):
    """RGB color with comparison methods."""
    r: int
    g: int
    b: int
    
    def euclidean_distance(self, other: "Color") -> float:
        """Calculate Euclidean distance between two colors."""
        return math.sqrt(
            (self.r - other.r) ** 2 +
            (self.g - other.g) ** 2 +
            (self.b - other.b) ** 2
        )
    
    def manhattan_distance(self, other: "Color") -> int:
        """Calculate Manhattan distance between two colors."""
        return abs(self.r - other.r) + abs(self.g - other.g) + abs(self.b - other.b)
    
    def matches(
        self,
        other: "Color",
        tolerance: int = 10,
        method: str = "manhattan",
    ) -> bool:
        """Check if this color matches another within tolerance.
        
        Args:
            other: Color to compare with.
            tolerance: Tolerance threshold.
            method: 'euclidean' or 'manhattan' distance.
        
        Returns:
            True if colors match within tolerance.
        """
        if method == "euclidean":
            return self.euclidean_distance(other) <= tolerance * math.sqrt(3)
        return self.manhattan_distance(other) <= tolerance * 3
    
    def hue_distance(self, other: "Color") -> float:
        """Calculate hue distance (0-180 for hue difference)."""
        # Convert to HSV-like representation
        def hue(c: Color) -> float:
            max_c = max(c.r, c.g, c.b)
            min_c = min(c.r, c.g, c.b)
            if max_c == min_c:
                return 0.0
            delta = max_c - min_c
            if max_c == c.r:
                return 60 * (((c.g - c.b) / delta) % 6)
            if max_c == c.g:
                return 60 * (((c.b - c.r) / delta) + 2)
            return 60 * (((c.r - c.g) / delta) + 4)
        
        h1, h2 = hue(self), hue(other)
        diff = abs(h1 - h2)
        return min(diff, 360 - diff)


class ColorMatcher:
    """Matches colors with configurable tolerance and methods."""
    
    def __init__(
        self,
        tolerance: int = 10,
        method: str = "manhattan",
        ignore_alpha: bool = True,
    ):
        """Initialize the color matcher.
        
        Args:
            tolerance: Default tolerance for color matching.
            method: Distance method ('euclidean' or 'manhattan').
            ignore_alpha: Whether to ignore alpha channel.
        """
        self.tolerance = tolerance
        self.method = method
        self.ignore_alpha = ignore_alpha
    
    def match(
        self,
        color1: Color,
        color2: Color,
        tolerance: Optional[int] = None,
    ) -> bool:
        """Check if two colors match.
        
        Args:
            color1: First color.
            color2: Second color.
            tolerance: Override default tolerance.
        
        Returns:
            True if colors match.
        """
        return color1.matches(
            color2,
            tolerance=tolerance or self.tolerance,
            method=self.method,
        )
    
    def find_closest_color(
        self,
        target: Color,
        palette: list[Color],
    ) -> tuple[Color, float]:
        """Find the closest color in a palette to a target color.
        
        Args:
            target: Target color to match.
            palette: List of candidate colors.
        
        Returns:
            Tuple of (closest_color, distance).
        """
        if not palette:
            raise ValueError("Palette cannot be empty")
        
        best_match = palette[0]
        best_distance = target.euclidean_distance(palette[0])
        
        for color in palette[1:]:
            distance = target.euclidean_distance(color)
            if distance < best_distance:
                best_distance = distance
                best_match = color
        
        return best_match, best_distance


# Predefined color palettes for common UI colors
COMMON_UI_COLORS = {
    "success": Color(76, 175, 80),      # Green
    "error": Color(244, 67, 54),        # Red
    "warning": Color(255, 193, 7),       # Amber
    "info": Color(33, 150, 243),        # Blue
    "text": Color(33, 33, 33),          # Dark gray
    "text_light": Color(255, 255, 255), # White
    "border": Color(189, 189, 189),      # Gray
}


def parse_color(color_str: str) -> Color:
    """Parse a color string into a Color object.
    
    Args:
        color_str: Color string (hex like '#FF0000' or 'red', 'green', etc.)
    
    Returns:
        Color object.
    
    Raises:
        ValueError: If color string is invalid.
    """
    # Check for named colors
    named_colors = {
        "red": Color(255, 0, 0),
        "green": Color(0, 255, 0),
        "blue": Color(0, 0, 255),
        "white": Color(255, 255, 255),
        "black": Color(0, 0, 0),
        "yellow": Color(255, 255, 0),
        "cyan": Color(0, 255, 255),
        "magenta": Color(255, 0, 255),
    }
    
    color_str_lower = color_str.lower()
    if color_str_lower in named_colors:
        return named_colors[color_str_lower]
    
    # Check for hex
    if color_str.startswith("#"):
        hex_str = color_str[1:]
        if len(hex_str) == 6:
            return Color(
                r=int(hex_str[0:2], 16),
                g=int(hex_str[2:4], 16),
                b=int(hex_str[4:6], 16),
            )
    
    raise ValueError(f"Invalid color string: {color_str}")
