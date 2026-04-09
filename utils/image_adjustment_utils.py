"""Image adjustment utilities for brightness, contrast, and color correction.

This module provides utilities for adjusting image properties like
brightness, contrast, saturation, and color balance, useful for
enhancing screenshots and normalizing visual input for automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class ColorBalance(Enum):
    """Color balance preset."""
    NEUTRAL = auto()
    WARM = auto()
    COOL = auto()
    SEPIA = auto()


@dataclass
class AdjustmentConfig:
    """Configuration for image adjustments."""
    brightness: float = 1.0      # 0.0 = black, 1.0 = original, 2.0 = white
    contrast: float = 1.0        # 0.0 = gray, 1.0 = original, 2.0 = high
    saturation: float = 1.0       # 0.0 = gray, 1.0 = original, 2.0 = vivid
    gamma: float = 1.0           # 1.0 = original
    color_balance: ColorBalance = ColorBalance.NEUTRAL


def adjust_brightness(
    image_data: bytes,
    factor: float,
) -> bytes:
    """Adjust image brightness.
    
    Args:
        image_data: Raw image bytes.
        factor: Brightness factor (0.0 = black, 1.0 = original, 2.0 = twice as bright).
    
    Returns:
        Adjusted image bytes.
    """
    try:
        from PIL import Image, ImageEnhance
        import io
        
        img = Image.open(io.BytesIO(image_data))
        enhancer = ImageEnhance.Brightness(img)
        adjusted = enhancer.enhance(factor)
        
        output = io.BytesIO()
        adjusted.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for brightness adjustment")


def adjust_contrast(
    image_data: bytes,
    factor: float,
) -> bytes:
    """Adjust image contrast.
    
    Args:
        image_data: Raw image bytes.
        factor: Contrast factor (0.0 = gray, 1.0 = original, 2.0 = high contrast).
    
    Returns:
        Adjusted image bytes.
    """
    try:
        from PIL import Image, ImageEnhance
        import io
        
        img = Image.open(io.BytesIO(image_data))
        enhancer = ImageEnhance.Contrast(img)
        adjusted = enhancer.enhance(factor)
        
        output = io.BytesIO()
        adjusted.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for contrast adjustment")


def adjust_saturation(
    image_data: bytes,
    factor: float,
) -> bytes:
    """Adjust image saturation.
    
    Args:
        image_data: Raw image bytes.
        factor: Saturation factor (0.0 = grayscale, 1.0 = original, 2.0 = vivid).
    
    Returns:
        Adjusted image bytes.
    """
    try:
        from PIL import Image, ImageEnhance
        import io
        
        img = Image.open(io.BytesIO(image_data))
        enhancer = ImageEnhance.Color(img)
        adjusted = enhancer.enhance(factor)
        
        output = io.BytesIO()
        adjusted.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for saturation adjustment")


def adjust_gamma(
    image_data: bytes,
    gamma: float,
) -> bytes:
    """Apply gamma correction to image.
    
    Args:
        image_data: Raw image bytes.
        gamma: Gamma value (1.0 = original, <1.0 = brighten, >1.0 = darken).
    
    Returns:
        Adjusted image bytes.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        img_array = np.array(img).astype(float)
        
        adjusted = np.clip(np.power(img_array / 255.0, 1.0 / gamma) * 255, 0, 255).astype(np.uint8)
        
        result = Image.fromarray(adjusted)
        output = io.BytesIO()
        result.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for gamma correction")


def apply_adjustments(
    image_data: bytes,
    config: AdjustmentConfig,
) -> bytes:
    """Apply multiple adjustments from configuration.
    
    Args:
        image_data: Raw image bytes.
        config: Adjustment configuration.
    
    Returns:
        Adjusted image bytes.
    """
    try:
        from PIL import Image, ImageEnhance, ImageColor
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        if config.brightness != 1.0:
            enhancer = ImageEnhance.Brightness(img)
            img = enhancer.enhance(config.brightness)
        
        if config.contrast != 1.0:
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(config.contrast)
        
        if config.saturation != 1.0:
            enhancer = ImageEnhance.Color(img)
            img = enhancer.enhance(config.saturation)
        
        if config.color_balance != ColorBalance.NEUTRAL:
            img = _apply_color_balance(img, config.color_balance)
        
        output = io.BytesIO()
        img.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image adjustments")


def auto_levels(image_data: bytes) -> bytes:
    """Apply auto-levels (stretch histogram to full range).
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Adjusted image bytes.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        img_array = np.array(img).astype(float)
        
        for channel in range(img_array.shape[2] if len(img_array.shape) > 2 else 1):
            if len(img_array.shape) > 2:
                channel_data = img_array[:, :, channel]
            else:
                channel_data = img_array
            
            cmin = channel_data.min()
            cmax = channel_data.max()
            
            if cmax > cmin:
                img_array[:, :, channel] = (channel_data - cmin) / (cmax - cmin) * 255
        
        result = Image.fromarray(img_array.astype(np.uint8))
        output = io.BytesIO()
        result.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for auto-levels")


def invert_colors(image_data: bytes) -> bytes:
    """Invert image colors.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Inverted image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        inverted = Image.eval(img, lambda x: 255 - x)
        
        output = io.BytesIO()
        inverted.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for color inversion")


def _apply_color_balance(img, balance: ColorBalance) -> "Image.Image":
    """Apply color balance adjustment to image."""
    from PIL import Image, ImageEnhance
    
    if balance == ColorBalance.SEPIA:
        img = img.convert("RGB")
        pixels = img.load()
        width, height = img.size
        
        for y in range(height):
            for x in range(width):
                r, g, b = pixels[x, y]
                tr = int(0.393 * r + 0.769 * g + 0.189 * b)
                tg = int(0.349 * r + 0.686 * g + 0.168 * b)
                tb = int(0.272 * r + 0.534 * g + 0.131 * b)
                pixels[x, y] = (min(255, tr), min(255, tg), min(255, tb))
        
        return img
    elif balance == ColorBalance.WARM:
        enhancer = ImageEnhance.Color(img)
        return enhancer.enhance(1.2)
    elif balance == ColorBalance.COOL:
        enhancer = ImageEnhance.Color(img)
        return enhancer.enhance(0.8)
    
    return img
