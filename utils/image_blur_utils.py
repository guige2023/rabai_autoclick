"""Image blur utilities for visual effects and preprocessing.

This module provides utilities for applying various blur effects to images,
including Gaussian blur, motion blur, box blur, and selective blur,
useful for privacy masking, visual effects, and preprocessing in automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class BlurType(Enum):
    """Type of blur effect."""
    GAUSSIAN = auto()
    BOX = auto()
    MOTION = auto()
    RADIAL = auto()
    SMART = auto()  # Selective blur that preserves edges


@dataclass
class BlurConfig:
    """Configuration for blur effect."""
    blur_type: BlurType = BlurType.GAUSSIAN
    radius: int = 10
    angle: float = 0.0  # For motion blur
    preserve_edges: bool = False


def apply_blur(
    image_data: bytes,
    config: Optional[BlurConfig] = None,
) -> bytes:
    """Apply blur effect to image.
    
    Args:
        image_data: Image bytes.
        config: Blur configuration.
    
    Returns:
        Blurred image bytes.
    """
    try:
        from PIL import Image, ImageFilter
        import io
        
        config = config or BlurConfig()
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        if config.blur_type == BlurType.GAUSSIAN:
            result = img.filter(ImageFilter.GaussianBlur(radius=config.radius))
        elif config.blur_type == BlurType.BOX:
            result = img.filter(ImageFilter.BoxBlur(radius=config.radius))
        elif config.blur_type == BlurType.MOTION:
            result = _apply_motion_blur(img, config.radius, config.angle)
        elif config.blur_type == BlurType.RADIAL:
            result = _apply_radial_blur(img, config.radius)
        elif config.blur_type == BlurType.SMART:
            result = _apply_smart_blur(img, config.radius)
        else:
            result = img.filter(ImageFilter.GaussianBlur(radius=config.radius))
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for blur effects")


def _apply_motion_blur(img, radius: int, angle: float) -> "Image.Image":
    """Apply motion blur effect."""
    import numpy as np
    from PIL import Image
    
    img_array = np.array(img)
    h, w = img_array.shape[:2]
    
    if angle == 0:
        kernel = np.zeros((radius, radius))
        kernel[radius // 2, :] = 1
        kernel = kernel / radius
    elif angle == 90:
        kernel = np.zeros((radius, radius))
        kernel[:, radius // 2] = 1
        kernel = kernel / radius
    else:
        radians = np.radians(angle)
        kernel = np.zeros((radius, radius))
        center = radius // 2
        
        for i in range(radius):
            x = int(center + i * np.cos(radians))
            y = int(center + i * np.sin(radians))
            if 0 <= x < radius and 0 <= y < radius:
                kernel[y, x] = 1
        
        if kernel.sum() > 0:
            kernel = kernel / kernel.sum()
        else:
            kernel[radius // 2, radius // 2] = 1
    
    from scipy.ndimage import convolve
    blurred = np.zeros_like(img_array)
    for c in range(3):
        blurred[:, :, c] = convolve(img_array[:, :, c], kernel, mode="reflect")
    
    return Image.fromarray(blurred.astype(np.uint8))


def _apply_radial_blur(img, radius: int) -> "Image.Image":
    """Apply radial blur effect."""
    from PIL import Image
    
    center_x, center_y = img.width // 2, img.height // 2
    
    result = img.filter(ImageFilter.GaussianBlur(radius=radius // 2))
    
    return result


def _apply_smart_blur(img, radius: int) -> "Image.Image":
    """Apply smart blur that preserves edges."""
    from PIL import ImageFilter
    
    blurred = img.filter(ImageFilter.GaussianBlur(radius=radius))
    edges = img.filter(ImageFilter.FIND_EDGES)
    edges_inverted = Image.eval(edges, lambda x: 255 - x)
    
    return Image.composite(blurred, img, edges_inverted)


def blur_region(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    radius: int = 10,
) -> bytes:
    """Blur a specific region of an image.
    
    Args:
        image_data: Image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        radius: Blur radius.
    
    Returns:
        Image with blurred region.
    """
    try:
        from PIL import Image, ImageFilter
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        x2 = min(x + width, img.width)
        y2 = min(y + height, img.height)
        
        region = img.crop((x, y, x2, y2))
        blurred_region = region.filter(ImageFilter.GaussianBlur(radius=radius))
        
        img.paste(blurred_region, (x, y))
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for region blur")


def pixelate_region(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    pixel_size: int = 10,
) -> bytes:
    """Pixelate a specific region for privacy masking.
    
    Args:
        image_data: Image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        pixel_size: Size of pixels.
    
    Returns:
        Image with pixelated region.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        x2 = min(x + width, img.width)
        y2 = min(y + height, img.height)
        
        region = img.crop((x, y, x2, y2))
        
        small = region.resize(
            (max(1, region.width // pixel_size), max(1, region.height // pixel_size)),
            Image.Resampling.NEAREST,
        )
        
        pixelated = small.resize(region.size, Image.Resampling.NEAREST)
        
        img.paste(pixelated, (x, y))
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for pixelation")


def anonymize_region(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    method: str = "blur",
) -> bytes:
    """Anonymize a region (blur or pixelate).
    
    Args:
        image_data: Image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
        method: "blur" or "pixelate".
    
    Returns:
        Image with anonymized region.
    """
    if method == "blur":
        return blur_region(image_data, x, y, width, height)
    elif method == "pixelate":
        return pixelate_region(image_data, x, y, width, height)
    else:
        return blur_region(image_data, x, y, width, height)
