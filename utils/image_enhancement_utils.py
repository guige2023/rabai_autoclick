"""Image enhancement utilities for improving image quality.

This module provides utilities for enhancing images including
sharpening, denoising, contrast adjustment, and color correction,
useful for preprocessing screenshots for OCR and analysis.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import io


class EnhancementType(Enum):
    """Type of enhancement to apply."""
    SHARPEN = auto()
    DENOISE = auto()
    CONTRAST = auto()
    BRIGHTNESS = auto()
    COLOR_BALANCE = auto()
    UNSHARP_MASK = auto()
    CLAHE = auto()  # Contrast Limited Adaptive Histogram Equalization


@dataclass
class EnhancementConfig:
    """Configuration for image enhancement."""
    enhancement_type: EnhancementType = EnhancementType.SHARPEN
    strength: float = 1.0
    radius: int = 5
    threshold: int = 0


def enhance_image(
    image_data: bytes,
    config: Optional[EnhancementConfig] = None,
) -> bytes:
    """Apply enhancement to image.
    
    Args:
        image_data: Image bytes.
        config: Enhancement configuration.
    
    Returns:
        Enhanced image bytes.
    """
    try:
        from PIL import Image, ImageEnhance, ImageFilter
        import io
        
        config = config or EnhancementConfig()
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        if config.enhancement_type == EnhancementType.SHARPEN:
            enhancer = ImageEnhance.Sharpness(img)
            img = enhancer.enhance(config.strength)
        
        elif config.enhancement_type == EnhancementType.CONTRAST:
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(config.strength)
        
        elif config.enhancement_type == EnhancementType.BRIGHTNESS:
            enhancer = ImageEnhance.Brightness(img)
            img = enhancer.enhance(config.strength)
        
        elif config.enhancement_type == EnhancementType.COLOR_BALANCE:
            enhancer = ImageEnhance.Color(img)
            img = enhancer.enhance(config.strength)
        
        elif config.enhancement_type == EnhancementType.UNSHARP_MASK:
            img = img.filter(ImageFilter.UnsharpMask(
                radius=config.radius,
                percent=int(150 * config.strength),
                threshold=config.threshold,
            ))
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image enhancement")


def sharpen_image(
    image_data: bytes,
    strength: float = 1.5,
) -> bytes:
    """Sharpen an image.
    
    Args:
        image_data: Image bytes.
        strength: Sharpening strength.
    
    Returns:
        Sharpened image bytes.
    """
    config = EnhancementConfig(
        enhancement_type=EnhancementType.SHARPEN,
        strength=strength,
    )
    return enhance_image(image_data, config)


def denoise_image(
    image_data: bytes,
    strength: float = 1.0,
) -> bytes:
    """Denoise an image using smoothing.
    
    Args:
        image_data: Image bytes.
        strength: Denoising strength.
    
    Returns:
        Denoised image bytes.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        img_array = np.array(img).astype(float)
        
        kernel_size = int(3 + strength * 2)
        if kernel_size % 2 == 0:
            kernel_size += 1
        
        from scipy.ndimage import uniform_filter
        denoised = uniform_filter(img_array, size=kernel_size)
        
        result = Image.fromarray(denoised.astype(np.uint8))
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("numpy and scipy are required for denoising")


def auto_enhance(image_data: bytes) -> bytes:
    """Automatically enhance image based on content analysis.
    
    Args:
        image_data: Image bytes.
    
    Returns:
        Auto-enhanced image bytes.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        img_array = np.array(img).astype(float)
        
        gray = np.mean(img_array, axis=2)
        mean_brightness = gray.mean() / 255.0
        
        if mean_brightness < 0.3:
            brightness_factor = 1.5
        elif mean_brightness > 0.7:
            brightness_factor = 0.8
        else:
            brightness_factor = 1.0
        
        img_array = img_array * brightness_factor
        img_array = np.clip(img_array, 0, 255)
        
        contrast_factor = 1.2
        img_array = (img_array - 128) * contrast_factor + 128
        img_array = np.clip(img_array, 0, 255)
        
        result = Image.fromarray(img_array.astype(np.uint8))
        
        from PIL import ImageEnhance
        enhancer = ImageEnhance.Sharpness(result)
        result = enhancer.enhance(1.2)
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for auto-enhancement")


def enhance_for_ocr(image_data: bytes) -> bytes:
    """Enhance image specifically for OCR readability.
    
    Args:
        image_data: Image bytes.
    
    Returns:
        OCR-enhanced image bytes.
    """
    try:
        import numpy as np
        from PIL import Image, ImageFilter, ImageEnhance
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        gray = img.convert("L")
        
        enhancer = ImageEnhance.Contrast(gray)
        gray = enhancer.enhance(1.5)
        
        enhancer = ImageEnhance.Brightness(gray)
        gray = enhancer.enhance(1.1)
        
        gray = gray.filter(ImageFilter.SHARPEN)
        
        threshold = 128
        gray_array = np.array(gray)
        binary = ((gray_array > threshold) * 255).astype(np.uint8)
        result = Image.fromarray(binary, mode="L")
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for OCR enhancement")


def adjust_contrast_gamma(
    image_data: bytes,
    gamma: float = 1.0,
) -> bytes:
    """Adjust image contrast using gamma correction.
    
    Args:
        image_data: Image bytes.
        gamma: Gamma value (< 1 brightens, > 1 darkens).
    
    Returns:
        Gamma-corrected image bytes.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        img_array = np.array(img).astype(float) / 255.0
        
        adjusted = np.power(img_array, 1.0 / gamma)
        adjusted = (adjusted * 255).astype(np.uint8)
        
        result = Image.fromarray(adjusted)
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for gamma correction")
