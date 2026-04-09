"""Color palette extraction utilities for UI theme analysis.

This module provides utilities for extracting dominant colors and
creating color palettes from screenshots, useful for UI theme
detection and color-based element identification.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Tuple
from enum import Enum, auto
import io


class PaletteType(Enum):
    """Type of color palette to extract."""
    DOMINANT = auto()      # Most dominant colors
    VIBRANT = auto()       # Most saturated colors
    MUTED = auto()         # Least saturated colors
    SORTED = auto()        # Colors sorted by frequency


@dataclass
class ColorInfo:
    """Information about a color."""
    rgb: Tuple[int, int, int]
    hex: str
    frequency: int
    percentage: float
    
    def __post_init__(self):
        if not self.hex and self.rgb:
            self.hex = f"#{self.rgb[0]:02x}{self.rgb[1]:02x}{self.rgb[2]:02x}"


@dataclass
class ColorPalette:
    """Collection of colors extracted from an image."""
    colors: List[ColorInfo]
    palette_type: PaletteType
    total_pixels: int
    
    @property
    def dominant_color(self) -> Optional[ColorInfo]:
        """Get the most dominant color."""
        if not self.colors:
            return None
        return max(self.colors, key=lambda c: c.frequency)
    
    def get_hex_codes(self) -> List[str]:
        """Get list of hex color codes."""
        return [c.hex for c in self.colors]
    
    def filter_by_color(
        self,
        target_rgb: Tuple[int, int, int],
        tolerance: int = 30,
    ) -> List[ColorInfo]:
        """Filter colors similar to target color within tolerance."""
        filtered = []
        for color in self.colors:
            if _color_distance(color.rgb, target_rgb) <= tolerance:
                filtered.append(color)
        return filtered


def extract_color_palette(
    image_data: bytes,
    num_colors: int = 8,
    palette_type: PaletteType = PaletteType.DOMINANT,
) -> ColorPalette:
    """Extract color palette from image.
    
    Args:
        image_data: Raw image bytes.
        num_colors: Number of colors to extract.
        palette_type: Type of palette to extract.
    
    Returns:
        ColorPalette with extracted colors.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        img_array = np.array(img)
        
        pixels = img_array.reshape(-1, 3)
        
        unique_colors, counts = np.unique(pixels, axis=0, return_counts=True)
        
        sorted_indices = np.argsort(counts)[::-1]
        unique_colors = unique_colors[sorted_indices]
        counts = counts[sorted_indices]
        
        total_pixels = len(pixels)
        
        colors = []
        for i in range(min(num_colors, len(unique_colors))):
            rgb = tuple(int(c) for c in unique_colors[i])
            freq = int(counts[i])
            colors.append(ColorInfo(
                rgb=rgb,
                hex=f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}",
                frequency=freq,
                percentage=freq / total_pixels * 100,
            ))
        
        return ColorPalette(
            colors=colors,
            palette_type=palette_type,
            total_pixels=total_pixels,
        )
    except ImportError:
        raise ImportError("numpy and PIL are required for color palette extraction")


def extract_dominant_colors(
    image_data: bytes,
    num_colors: int = 5,
) -> List[Tuple[int, int, int]]:
    """Extract the N most dominant colors from an image.
    
    Args:
        image_data: Raw image bytes.
        num_colors: Number of colors to extract.
    
    Returns:
        List of RGB color tuples.
    """
    palette = extract_color_palette(image_data, num_colors, PaletteType.DOMINANT)
    return [c.rgb for c in palette.colors]


def find_similar_colors(
    image_data: bytes,
    target_rgb: Tuple[int, int, int],
    num_colors: int = 8,
    tolerance: int = 30,
) -> List[ColorInfo]:
    """Find colors in image similar to target color.
    
    Args:
        image_data: Raw image bytes.
        target_rgb: Target RGB color to match.
        num_colors: Number of palette colors to consider.
        tolerance: Color distance tolerance.
    
    Returns:
        List of similar ColorInfo objects.
    """
    palette = extract_color_palette(image_data, num_colors)
    return palette.filter_by_color(target_rgb, tolerance)


def _color_distance(c1: Tuple[int, int, int], c2: Tuple[int, int, int]) -> float:
    """Calculate Euclidean distance between two RGB colors."""
    dr = c1[0] - c2[0]
    dg = c1[1] - c2[1]
    db = c1[2] - c2[2]
    return (dr * dr + dg * dg + db * db) ** 0.5


def rgb_to_hsv(r: int, g: int, b: int) -> Tuple[float, float, float]:
    """Convert RGB to HSV color space.
    
    Args:
        r: Red (0-255).
        g: Green (0-255).
        b: Blue (0-255).
    
    Returns:
        Tuple of (hue, saturation, value) where hue is 0-360
        and saturation/value are 0-1.
    """
    r, g, b = r / 255.0, g / 255.0, b / 255.0
    
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    delta = max_c - min_c
    
    if delta == 0:
        hue = 0
    elif max_c == r:
        hue = 60 * (((g - b) / delta) % 6)
    elif max_c == g:
        hue = 60 * ((b - r) / delta + 2)
    else:
        hue = 60 * ((r - g) / delta + 4)
    
    saturation = 0 if max_c == 0 else delta / max_c
    value = max_c
    
    return (hue, saturation, value)


def get_colorfulness(image_data: bytes) -> float:
    """Calculate average colorfulness (saturation) of an image.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Average saturation value (0.0 to 1.0).
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        img_array = np.array(img)
        
        r, g, b = img_array[:, :, 0], img_array[:, :, 1], img_array[:, :, 2]
        
        max_c = np.maximum(np.maximum(r, g), b)
        min_c = np.minimum(np.minimum(r, g), b)
        delta = max_c - min_c
        
        saturation = np.where(max_c > 0, delta / (max_c + 1e-10), 0)
        
        return float(np.mean(saturation))
    except ImportError:
        raise ImportError("PIL and numpy are required for colorfulness calculation")


def create_color_gradient(
    start_rgb: Tuple[int, int, int],
    end_rgb: Tuple[int, int, int],
    steps: int = 10,
) -> List[Tuple[int, int, int]]:
    """Create a gradient between two colors.
    
    Args:
        start_rgb: Starting RGB color.
        end_rgb: Ending RGB color.
        steps: Number of gradient steps.
    
    Returns:
        List of RGB tuples forming the gradient.
    """
    gradient = []
    for i in range(steps):
        t = i / (steps - 1) if steps > 1 else 0
        r = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * t)
        g = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * t)
        b = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * t)
        gradient.append((r, g, b))
    return gradient


def generate_complementary_color(rgb: Tuple[int, int, int]) -> Tuple[int, int, int]:
    """Generate complementary (opposite) color.
    
    Args:
        rgb: RGB color tuple.
    
    Returns:
        Complementary RGB color.
    """
    return (255 - rgb[0], 255 - rgb[1], 255 - rgb[2])


def quantize_colors(
    image_data: bytes,
    num_colors: int = 16,
) -> bytes:
    """Reduce image to limited number of colors (color quantization).
    
    Args:
        image_data: Raw image bytes.
        num_colors: Number of colors to reduce to.
    
    Returns:
        Quantized image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        quantized = img.quantize(colors=num_colors)
        
        output = io.BytesIO()
        quantized.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for color quantization")
