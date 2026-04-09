"""Window shadow utilities for visual effect manipulation.

This module provides utilities for adding, removing, and manipulating
window shadows and borders in screenshots, useful for consistent
visual output and UI presentation in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class ShadowStyle(Enum):
    """Style of shadow to apply."""
    DROP = auto()       # Standard drop shadow
    HARD = auto()       # Hard edge shadow
    SOFT = auto()       # Soft blurred shadow
    INNER = auto()      # Inner shadow
    OUTER = auto()      # Outer shadow


@dataclass
class ShadowConfig:
    """Configuration for shadow effect."""
    style: ShadowStyle = ShadowStyle.DROP
    offset_x: int = 10
    offset_y: int = 10
    blur_radius: int = 20
    shadow_color: Tuple[int, int, int, int] = (0, 0, 0, 128)
    shadow_opacity: float = 0.5


@dataclass
class BorderConfig:
    """Configuration for border effect."""
    width: int = 2
    color: Tuple[int, int, int] = (0, 0, 0)
    style: str = "solid"  # solid, dashed, dotted


def add_window_shadow(
    image_data: bytes,
    config: Optional[ShadowConfig] = None,
) -> bytes:
    """Add shadow effect to window image.
    
    Args:
        image_data: Window image bytes.
        config: Shadow configuration.
    
    Returns:
        Image with shadow effect.
    """
    try:
        from PIL import Image, ImageFilter, ImageDraw
        import numpy as np
        import io
        
        config = config or ShadowConfig()
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        
        shadow_img = Image.new("RGBA", img.size, (0, 0, 0, 0))
        
        shadow_offset = (config.offset_x, config.offset_y)
        
        shadow_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
        shadow_pixels = shadow_layer.load()
        img_pixels = img.load()
        
        for py in range(img.height):
            for px in range(img.width):
                if img_pixels[px, py][3] > 0:
                    sx = px + config.offset_x
                    sy = py + config.offset_y
                    if 0 <= sx < shadow_layer.width and 0 <= sy < shadow_layer.height:
                        shadow_pixels[sx, sy] = config.shadow_color
        
        if config.blur_radius > 0:
            shadow_layer = shadow_layer.filter(
                ImageFilter.GaussianBlur(radius=config.blur_radius // 2)
            )
        
        result = Image.alpha_composite(shadow_layer, img)
        
        output = io.BytesIO()
        if result.mode == "RGBA":
            result.save(output, format="PNG")
        else:
            result = result.convert("RGB")
            result.save(output, format="PNG")
        
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for shadow effect")


def add_border(
    image_data: bytes,
    config: Optional[BorderConfig] = None,
) -> bytes:
    """Add border to image.
    
    Args:
        image_data: Image bytes.
        config: Border configuration.
    
    Returns:
        Image with border.
    """
    try:
        from PIL import Image
        import io
        
        config = config or BorderConfig()
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        from PIL import ImageDraw
        
        bordered = Image.new("RGB", (img.width + config.width * 2, img.height + config.width * 2), config.color)
        bordered.paste(img, (config.width, config.width))
        
        output = io.BytesIO()
        bordered.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for border effect")


def remove_shadow(image_data: bytes) -> bytes:
    """Remove shadow from window image.
    
    Args:
        image_data: Window image with shadow.
    
    Returns:
        Image without shadow.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        img_array = np.array(img)
        
        alpha = img_array[:, :, 3]
        
        brightness = np.mean(img_array[:, :, :3], axis=2)
        
        shadow_mask = (alpha < 50) & (brightness < 30)
        
        img_array[shadow_mask, 3] = 0
        
        result = Image.fromarray(img_array, mode="RGBA")
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for shadow removal")


def add_rounded_corners(
    image_data: bytes,
    radius: int = 20,
) -> bytes:
    """Add rounded corners to image.
    
    Args:
        image_data: Image bytes.
        radius: Corner radius in pixels.
    
    Returns:
        Image with rounded corners.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        
        mask = Image.new("L", img.size, 255)
        from PIL import ImageDraw
        draw = ImageDraw.Draw(mask)
        
        draw.rounded_rectangle(
            [(0, 0), (img.width - 1, img.height - 1)],
            radius=radius,
            fill=0,
        )
        
        img.putalpha(mask)
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for rounded corners")


def add_drop_shadow(
    image_data: bytes,
    offset: Tuple[int, int] = (10, 10),
    blur: int = 20,
    color: Tuple[int, int, int] = (0, 0, 0),
) -> bytes:
    """Add drop shadow to image.
    
    Args:
        image_data: Image bytes.
        offset: Shadow offset (x, y).
        blur: Blur radius.
        color: Shadow RGB color.
    
    Returns:
        Image with drop shadow.
    """
    try:
        from PIL import Image, ImageFilter
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        
        shadow = Image.new("RGBA", img.size, (0, 0, 0, 0))
        shadow.paste(img, offset)
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=blur // 2))
        
        result = Image.new("RGBA", img.size, (0, 0, 0, 0))
        result.paste(shadow, (0, 0))
        result.paste(img, (0, 0))
        
        output = io.BytesIO()
        if result.mode == "RGBA":
            result.save(output, format="PNG")
        else:
            result = result.convert("RGB")
            result.save(output, format="PNG")
        
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for drop shadow")
