"""Grayscale conversion utilities for image processing.

This module provides utilities for converting images to grayscale with
various algorithms and weighting schemes, useful for preprocessing
images for OCR, pattern matching, and analysis in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import io


class GrayscaleMethod(Enum):
    """Method for converting to grayscale."""
    AVERAGE = auto()      # Simple average of RGB
    LUMINOSITY = auto()   # Human perception weighted (0.299R + 0.587G + 0.114B)
    DECOMPOSITION_LIGHTNESS = auto()  # (max(R,G,B) + min(R,G,B)) / 2
    DECOMPOSITION_MAX = auto()  # max(R,G,B)
    SINGLE_CHANNEL = auto()  # Use single channel (specified separately)


@dataclass
class GrayscaleConfig:
    """Configuration for grayscale conversion."""
    method: GrayscaleMethod = GrayscaleMethod.LUMINOSITY
    preserve_alpha: bool = True
    normalize: bool = False


def convert_to_grayscale(
    image_data: bytes,
    method: GrayscaleMethod = GrayscaleMethod.LUMINOSITY,
) -> bytes:
    """Convert image to grayscale.
    
    Args:
        image_data: Raw image bytes.
        method: Grayscale conversion method.
    
    Returns:
        Grayscale image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        if method == GrayscaleMethod.AVERAGE:
            gray = Image.open(io.BytesIO(image_data)).convert("1")
            gray = gray.convert("L")
        elif method == GrayscaleMethod.LUMINOSITY:
            gray = img.convert("L")
        elif method == GrayscaleMethod.DECOMPOSITION_LIGHTNESS:
            gray = _decomposition_lightness(img)
        elif method == GrayscaleMethod.DECOMPOSITION_MAX:
            gray = _decomposition_max(img)
        else:
            gray = img.convert("L")
        
        output = io.BytesIO()
        gray.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for grayscale conversion")


def _decomposition_lightness(img) -> "Image.Image":
    """Convert using lightness decomposition method."""
    import numpy as np
    
    img_array = np.array(img.convert("RGB"))
    
    max_rgb = np.max(img_array, axis=2)
    min_rgb = np.min(img_array, axis=2)
    
    lightness = ((max_rgb + min_rgb) / 2).astype(np.uint8)
    
    from PIL import Image
    return Image.fromarray(lightness, mode="L")


def _decomposition_max(img) -> "Image.Image":
    """Convert using max decomposition method."""
    import numpy as np
    
    img_array = np.array(img.convert("RGB"))
    
    max_rgb = np.max(img_array, axis=2)
    
    from PIL import Image
    return Image.fromarray(max_rgb, mode="L")


def channel_to_grayscale(
    image_data: bytes,
    channel: str = "R",
) -> bytes:
    """Extract single channel as grayscale.
    
    Args:
        image_data: Raw image bytes.
        channel: Channel to extract ('R', 'G', 'B').
    
    Returns:
        Grayscale image using single channel values.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        r, g, b = img.split()
        
        channel_map = {"R": r, "G": g, "B": b}
        gray = channel_map.get(channel.upper(), r)
        
        output = io.BytesIO()
        gray.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for channel extraction")


def blend_grayscale(
    image_data: bytes,
    target_gray: int = 128,
    strength: float = 0.5,
) -> bytes:
    """Blend grayscale image toward target gray value.
    
    Args:
        image_data: Raw image bytes.
        target_gray: Target gray value (0-255).
        strength: Blend strength (0.0 = original, 1.0 = full target).
    
    Returns:
        Blended grayscale image.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("L")
        img_array = np.array(img)
        
        blended = (img_array * (1 - strength) + target_gray * strength).astype(np.uint8)
        
        result = Image.fromarray(blended, mode="L")
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for grayscale blending")


def grayscale_histogram_equalization(image_data: bytes) -> bytes:
    """Apply histogram equalization to grayscale image.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Equalized grayscale image.
    """
    try:
        from PIL import Image, ImageOps
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("L")
        equalized = ImageOps.equalize(img)
        
        output = io.BytesIO()
        equalized.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for histogram equalization")


def tint_grayscale(
    image_data: bytes,
    tint_color: tuple[int, int, int],
    intensity: float = 0.5,
) -> bytes:
    """Apply color tint to grayscale image.
    
    Args:
        image_data: Raw image bytes.
        tint_color: RGB tint color.
        intensity: Tint intensity (0.0 = grayscale, 1.0 = full tint).
    
    Returns:
        Tinted RGB image.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("L")
        img_array = np.array(img)
        
        r = (img_array * (tint_color[0] / 255.0)).astype(np.uint8)
        g = (img_array * (tint_color[1] / 255.0)).astype(np.uint8)
        b = (img_array * (tint_color[2] / 255.0)).astype(np.uint8)
        
        tinted = np.stack([r, g, b], axis=2)
        
        if intensity < 1.0:
            gray = np.repeat(img_array[:, :, np.newaxis], 3, axis=2)
            tinted = (gray * (1 - intensity) + tinted * intensity).astype(np.uint8)
        
        result = Image.fromarray(tinted, mode="RGB")
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for tinting")


def sepia(image_data: bytes) -> bytes:
    """Apply sepia tone effect to image.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Sepia-toned image.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        r, g, b = img.split()
        
        tr = lambda px: min(255, int(0.393 * px[0] + 0.769 * px[1] + 0.189 * px[2]))
        tg = lambda px: min(255, int(0.349 * px[0] + 0.686 * px[1] + 0.168 * px[2]))
        tb = lambda px: min(255, int(0.272 * px[0] + 0.534 * px[1] + 0.131 * px[2]))
        
        sepia_r = r.point(lambda x: min(255, int(0.393 * x + 0.769 * x + 0.189 * x)))
        sepia_g = g.point(lambda x: min(255, int(0.349 * x + 0.686 * x + 0.168 * x)))
        sepia_b = b.point(lambda x: min(255, int(0.272 * x + 0.534 * x + 0.131 * x)))
        
        sepia_img = Image.merge("RGB", (sepia_r, sepia_g, sepia_b))
        
        output = io.BytesIO()
        sepia_img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for sepia effect")
