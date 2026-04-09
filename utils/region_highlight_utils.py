"""Region highlight and spotlight utilities for visual feedback.

This module provides utilities for drawing highlights, spotlights, and
visual indicators on screenshots or screen regions to provide visual
feedback during automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class HighlightStyle(Enum):
    """Style of highlight to draw."""
    RECTANGLE = auto()      # Simple rectangle border
    ROUNDED_RECT = auto()   # Rounded rectangle
    SHADOW = auto()         # Shadow effect
    GLOW = auto()           # Glowing border
    SPOTLIGHT = auto()      # Dimmed background with clear center
    CORNER_BRACKET = auto() # Corner brackets only


class HighlightColor(Enum):
    """Predefined highlight colors."""
    RED = (255, 0, 0)
    GREEN = (0, 255, 0)
    BLUE = (0, 0, 255)
    YELLOW = (255, 255, 0)
    MAGENTA = (255, 0, 255)
    CYAN = (0, 255, 255)
    WHITE = (255, 255, 255)
    ORANGE = (255, 165, 0)


@dataclass
class HighlightConfig:
    """Configuration for highlight drawing."""
    style: HighlightStyle = HighlightStyle.RECTANGLE
    color: Tuple[int, int, int] = (255, 0, 0)
    line_width: int = 3
    corner_radius: int = 10
    glow_radius: int = 15
    glow_intensity: float = 0.5
    opacity: float = 1.0
    shadow_offset: int = 5
    shadow_blur: int = 10


def draw_highlight_rectangle(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    config: Optional[HighlightConfig] = None,
) -> bytes:
    """Draw a rectangle highlight on an image.
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        config: Optional highlight configuration.
    
    Returns:
        Modified image bytes.
    """
    try:
        from PIL import Image, ImageDraw
        
        config = config or HighlightConfig()
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        if config.style == HighlightStyle.ROUNDED_RECT:
            draw.rounded_rectangle(
                [(x, y), (x + width, y + height)],
                radius=config.corner_radius,
                outline=config.color,
                width=config.line_width,
            )
        else:
            draw.rectangle(
                [(x, y), (x + width, y + height)],
                outline=config.color,
                width=config.line_width,
            )
        
        img = Image.alpha_composite(img, overlay)
        return _rgba_to_bytes(img)
    except ImportError:
        raise ImportError("PIL is required for highlight drawing")


def draw_spotlight(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    dim_opacity: float = 0.7,
) -> bytes:
    """Draw a spotlight effect (dimmed background with clear region).
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of spotlight region.
        y: Top edge of spotlight region.
        width: Width of spotlight region.
        height: Height of spotlight region.
        dim_opacity: Opacity of dimming (0.0 to 1.0).
    
    Returns:
        Modified image bytes with spotlight effect.
    """
    try:
        from PIL import Image, ImageDraw
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        alpha = int(255 * dim_opacity)
        draw.rectangle(
            [(0, 0), (img.width, img.height)],
            fill=(0, 0, 0, alpha),
        )
        
        draw.rectangle(
            [(x, y), (x + width, y + height)],
            fill=(0, 0, 0, 0),
        )
        
        img = Image.alpha_composite(img, overlay)
        return _rgba_to_bytes(img)
    except ImportError:
        raise ImportError("PIL is required for spotlight drawing")


def draw_corner_brackets(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    bracket_size: int = 20,
    color: Tuple[int, int, int] = (255, 0, 0),
    line_width: int = 3,
) -> bytes:
    """Draw corner brackets around a region.
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        bracket_size: Size of each bracket arm.
        color: RGB color tuple.
        line_width: Line thickness.
    
    Returns:
        Modified image bytes with corner brackets.
    """
    try:
        from PIL import Image, ImageDraw
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        r = x + width
        b = y + height
        
        draw.line(
            [(x, y), (x + bracket_size, y)],
            fill=color,
            width=line_width,
        )
        draw.line(
            [(x, y), (x, y + bracket_size)],
            fill=color,
            width=line_width,
        )
        
        draw.line(
            [(r - bracket_size, y), (r, y)],
            fill=color,
            width=line_width,
        )
        draw.line(
            [(r, y), (r, y + bracket_size)],
            fill=color,
            width=line_width,
        )
        
        draw.line(
            [(x, b - bracket_size), (x, b)],
            fill=color,
            width=line_width,
        )
        draw.line(
            [(x, b), (x + bracket_size, b)],
            fill=color,
            width=line_width,
        )
        
        draw.line(
            [(r, b - bracket_size), (r, b)],
            fill=color,
            width=line_width,
        )
        draw.line(
            [(r - bracket_size, b), (r, b)],
            fill=color,
            width=line_width,
        )
        
        img = Image.alpha_composite(img, overlay)
        return _rgba_to_bytes(img)
    except ImportError:
        raise ImportError("PIL is required for corner bracket drawing")


def draw_glow_border(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    glow_radius: int = 15,
    glow_color: Tuple[int, int, int] = (255, 0, 0),
    intensity: float = 0.5,
) -> bytes:
    """Draw a glowing border around a region.
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        glow_radius: Radius of the glow effect.
        glow_color: RGB color of the glow.
        intensity: Glow intensity (0.0 to 1.0).
    
    Returns:
        Modified image bytes with glow effect.
    """
    try:
        from PIL import Image, ImageDraw, ImageFilter
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        alpha = int(255 * intensity)
        glow_with_alpha = (*glow_color, alpha)
        
        for i in range(glow_radius, 0, -1):
            layer_alpha = int(alpha * (1 - i / glow_radius) * 0.3)
            layer_color = (*glow_color, layer_alpha)
            draw.rectangle(
                [(x - i, y - i), (x + width + i, y + height + i)],
                outline=layer_color,
                width=1,
            )
        
        draw.rectangle(
            [(x, y), (x + width, y + height)],
            outline=(*glow_color, alpha),
            width=2,
        )
        
        img = Image.alpha_composite(img, overlay)
        return _rgba_to_bytes(img)
    except ImportError:
        raise ImportError("PIL is required for glow border drawing")


def draw_shadow(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    offset: int = 5,
    blur: int = 10,
) -> bytes:
    """Draw a shadow effect behind a region.
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        offset: Shadow offset in pixels.
        blur: Shadow blur radius.
    
    Returns:
        Modified image bytes with shadow effect.
    """
    try:
        from PIL import Image, ImageDraw, ImageFilter
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        draw.rectangle(
            [(x + offset, y + offset), (x + width + offset, y + height + offset)],
            fill=(0, 0, 0, 100),
        )
        
        img = Image.alpha_composite(img, overlay)
        blurred = img.filter(ImageFilter.GaussianBlur(blur))
        return _rgba_to_bytes(blurred)
    except ImportError:
        raise ImportError("PIL is required for shadow drawing")


def highlight_multiple_regions(
    image_data: bytes,
    regions: list[Tuple[int, int, int, int]],
    config: Optional[HighlightConfig] = None,
) -> bytes:
    """Draw highlights around multiple regions.
    
    Args:
        image_data: Raw image bytes.
        regions: List of (x, y, width, height) tuples.
        config: Optional highlight configuration.
    
    Returns:
        Modified image bytes with all regions highlighted.
    """
    result = image_data
    for x, y, w, h in regions:
        result = draw_highlight_rectangle(result, x, y, w, h, config)
    return result


def _rgba_to_bytes(img) -> bytes:
    """Convert RGBA PIL Image to bytes."""
    if img.mode != "RGB":
        img = img.convert("RGB")
    output = io.BytesIO()
    img.save(output, format="PNG")
    return output.getvalue()
