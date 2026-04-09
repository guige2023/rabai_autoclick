"""Image filter utilities for applying visual filters to images.

This module provides utilities for applying various image filters including
sharpening, smoothing, edge enhancement, and artistic effects, useful for
image preprocessing and visual effects in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import io


class FilterType(Enum):
    """Type of image filter."""
    SHARPEN = auto()
    BLUR = auto()
    SMOOTH = auto()
    EDGE_ENHANCE = auto()
    EDGE_DETECT = auto()
    EMBOSS = auto()
    POSTERIZE = auto()
    SOLARIZE = auto()
    VINTAGE = auto()
    COOL = auto()
    WARM = auto()


@dataclass
class FilterConfig:
    """Configuration for image filter."""
    filter_type: FilterType = FilterType.SHARPEN
    strength: float = 1.0
    radius: int = 5


def apply_filter(
    image_data: bytes,
    config: Optional[FilterConfig] = None,
) -> bytes:
    """Apply filter to image.
    
    Args:
        image_data: Image bytes.
        config: Filter configuration.
    
    Returns:
        Filtered image bytes.
    """
    try:
        from PIL import Image, ImageFilter, ImageEnhance
        import io
        
        config = config or FilterConfig()
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        if config.filter_type == FilterType.SHARPEN:
            result = img.filter(ImageFilter.UnsharpMask(
                radius=config.radius,
                percent=int(150 * config.strength),
            ))
        elif config.filter_type == FilterType.BLUR:
            result = img.filter(ImageFilter.GaussianBlur(radius=config.radius))
        elif config.filter_type == FilterType.SMOOTH:
            result = img.filter(ImageFilter.SMOOTH_MORE)
        elif config.filter_type == FilterType.EDGE_ENHANCE:
            result = img.filter(ImageFilter.EDGE_ENHANCE)
        elif config.filter_type == FilterType.EDGE_DETECT:
            result = img.filter(ImageFilter.FIND_EDGES)
        elif config.filter_type == FilterType.EMBOSS:
            result = img.filter(ImageFilter.EMBOSS)
        elif config.filter_type == FilterType.POSTERIZE:
            result = _posterize(img, int(4 + (1 - config.strength) * 4))
        elif config.filter_type == FilterType.SOLARIZE:
            result = _solarize(img, threshold=int(128 + 64 * config.strength))
        elif config.filter_type == FilterType.VINTAGE:
            result = _apply_vintage(img)
        elif config.filter_type == FilterType.COOL:
            result = _apply_cool(img, config.strength)
        elif config.filter_type == FilterType.WARM:
            result = _apply_warm(img, config.strength)
        else:
            result = img
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image filtering")


def _posterize(img, bits: int) -> "Image.Image":
    """Apply posterize filter."""
    from PIL import ImageOps
    return ImageOps.posterize(img, bits)


def _solarize(img, threshold: int) -> "Image.Image":
    """Apply solarize filter."""
    from PIL import ImageOps
    return ImageOps.solarize(img, threshold)


def _apply_vintage(img) -> "Image.Image":
    """Apply vintage/sepia effect."""
    from PIL import ImageEnhance
    
    enhancer = ImageEnhance.Color(img)
    img = enhancer.enhance(0.8)
    
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(0.9)
    
    enhancer = ImageEnhance.Brightness(img)
    img = enhancer.enhance(1.1)
    
    return img


def _apply_cool(img, strength: float) -> "Image.Image":
    """Apply cool (blue) color shift."""
    import numpy as np
    from PIL import Image
    
    img_array = np.array(img).astype(float)
    
    img_array[:, :, 0] = img_array[:, :, 0] * (1 - strength * 0.3)
    img_array[:, :, 2] = img_array[:, :, 2] * (1 + strength * 0.3)
    
    img_array = np.clip(img_array, 0, 255).astype(np.uint8)
    
    return Image.fromarray(img_array)


def _apply_warm(img, strength: float) -> "Image.Image":
    """Apply warm (orange) color shift."""
    import numpy as np
    from PIL import Image
    
    img_array = np.array(img).astype(float)
    
    img_array[:, :, 0] = img_array[:, :, 0] * (1 + strength * 0.3)
    img_array[:, :, 2] = img_array[:, :, 2] * (1 - strength * 0.3)
    
    img_array = np.clip(img_array, 0, 255).astype(np.uint8)
    
    return Image.fromarray(img_array)


def sharpen_image(
    image_data: bytes,
    strength: float = 1.0,
) -> bytes:
    """Sharpen an image.
    
    Args:
        image_data: Image bytes.
        strength: Sharpening strength (0.0 to 1.0).
    
    Returns:
        Sharpened image bytes.
    """
    config = FilterConfig(
        filter_type=FilterType.SHARPEN,
        strength=strength,
        radius=3,
    )
    return apply_filter(image_data, config)


def smooth_image(
    image_data: bytes,
    strength: float = 1.0,
) -> bytes:
    """Smooth an image.
    
    Args:
        image_data: Image bytes.
        strength: Smoothing strength.
    
    Returns:
        Smoothed image bytes.
    """
    config = FilterConfig(
        filter_type=FilterType.SMOOTH,
        strength=strength,
    )
    return apply_filter(image_data, config)


def enhance_edges(
    image_data: bytes,
    strength: float = 1.0,
) -> bytes:
    """Enhance edges in an image.
    
    Args:
        image_data: Image bytes.
        strength: Enhancement strength.
    
    Returns:
        Edge-enhanced image bytes.
    """
    config = FilterConfig(
        filter_type=FilterType.EDGE_ENHANCE,
        strength=strength,
    )
    return apply_filter(image_data, config)


def apply_vintage_filter(image_data: bytes) -> bytes:
    """Apply vintage color effect.
    
    Args:
        image_data: Image bytes.
    
    Returns:
        Vintage-filtered image bytes.
    """
    config = FilterConfig(filter_type=FilterType.VINTAGE)
    return apply_filter(image_data, config)


def apply_cool_filter(
    image_data: bytes,
    strength: float = 1.0,
) -> bytes:
    """Apply cool (blue) color filter.
    
    Args:
        image_data: Image bytes.
        strength: Effect strength.
    
    Returns:
        Cool-filtered image bytes.
    """
    config = FilterConfig(filter_type=FilterType.COOL, strength=strength)
    return apply_filter(image_data, config)


def apply_warm_filter(
    image_data: bytes,
    strength: float = 1.0,
) -> bytes:
    """Apply warm (orange) color filter.
    
    Args:
        image_data: Image bytes.
        strength: Effect strength.
    
    Returns:
        Warm-filtered image bytes.
    """
    config = FilterConfig(filter_type=FilterType.WARM, strength=strength)
    return apply_filter(image_data, config)
