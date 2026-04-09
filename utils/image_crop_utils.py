"""Image crop utilities for precise region extraction.

This module provides utilities for cropping images with various methods
including center crop, smart crop, aspect ratio crop, and content-aware
cropping, useful for preparing images for analysis and display.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class CropMode(Enum):
    """Mode for cropping operation."""
    EXACT = auto()
    CENTER = auto()
    ASPECT_RATIO = auto()
    SMART = auto()  # Content-aware
    THUMBNAIL = auto()


@dataclass
class CropConfig:
    """Configuration for cropping."""
    mode: CropMode = CropMode.EXACT
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0
    target_aspect: Optional[float] = None
    maintain_aspect: bool = True


def crop_image(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
) -> bytes:
    """Crop image to exact dimensions.
    
    Args:
        image_data: Image bytes.
        x: Left edge.
        y: Top edge.
        width: Target width.
        height: Target height.
    
    Returns:
        Cropped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        x2 = min(x + width, img.width)
        y2 = min(y + height, img.height)
        
        x = min(x, x2)
        y = min(y, y2)
        
        cropped = img.crop((x, y, x2, y2))
        
        output = io.BytesIO()
        cropped.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for cropping")


def center_crop(
    image_data: bytes,
    target_width: int,
    target_height: int,
) -> bytes:
    """Crop from center of image.
    
    Args:
        image_data: Image bytes.
        target_width: Target width.
        target_height: Target height.
    
    Returns:
        Center-cropped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        left = (img.width - target_width) // 2
        top = (img.height - target_height) // 2
        right = left + target_width
        bottom = top + target_height
        
        left = max(0, left)
        top = max(0, top)
        right = min(img.width, right)
        bottom = min(img.height, bottom)
        
        cropped = img.crop((left, top, right, bottom))
        
        output = io.BytesIO()
        cropped.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for center cropping")


def crop_to_aspect_ratio(
    image_data: bytes,
    target_aspect: float,
) -> bytes:
    """Crop image to target aspect ratio.
    
    Args:
        image_data: Image bytes.
        target_aspect: Target aspect ratio (width/height).
    
    Returns:
        Cropped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        current_aspect = img.width / img.height
        
        if current_aspect > target_aspect:
            new_width = int(img.height * target_aspect)
            left = (img.width - new_width) // 2
            cropped = img.crop((left, 0, left + new_width, img.height))
        else:
            new_height = int(img.width / target_aspect)
            top = (img.height - new_height) // 2
            cropped = img.crop((0, top, img.width, top + new_height))
        
        output = io.BytesIO()
        cropped.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for aspect ratio cropping")


def smart_crop(
    image_data: bytes,
    target_width: int,
    target_height: int,
) -> bytes:
    """Smart crop that preserves important content.
    
    Args:
        image_data: Image bytes.
        target_width: Target width.
        target_height: Target height.
    
    Returns:
        Smart-cropped image bytes.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        gray = np.array(img.convert("L"))
        
        threshold = np.mean(gray) * 0.8
        important = gray > threshold
        
        y_coords, x_coords = np.where(important)
        
        if len(x_coords) > 0 and len(y_coords) > 0:
            x_min, x_max = x_coords.min(), x_coords.max()
            y_min, y_max = y_coords.min(), y_coords.max()
            
            center_x = (x_min + x_max) // 2
            center_y = (y_min + y_max) // 2
        else:
            center_x = img.width // 2
            center_y = img.height // 2
        
        left = max(0, center_x - target_width // 2)
        top = max(0, center_y - target_height // 2)
        right = min(img.width, left + target_width)
        bottom = min(img.height, top + target_height)
        
        if right - left < target_width:
            left = max(0, right - target_width)
        if bottom - top < target_height:
            top = max(0, bottom - target_height)
        
        cropped = img.crop((left, top, right, bottom))
        
        output = io.BytesIO()
        cropped.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL and numpy are required for smart cropping")


def create_thumbnail(
    image_data: bytes,
    max_width: int,
    max_height: int,
) -> bytes:
    """Create thumbnail maintaining aspect ratio.
    
    Args:
        image_data: Image bytes.
        max_width: Maximum width.
        max_height: Maximum height.
    
    Returns:
        Thumbnail image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        img.thumbnail((max_width, max_height), Image.LANCZOS)
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for thumbnail creation")


def crop_with_padding(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    pad_color: Tuple[int, int, int] = (0, 0, 0),
) -> bytes:
    """Crop with padding if region exceeds image bounds.
    
    Args:
        image_data: Image bytes.
        x: Left edge.
        y: Top edge.
        width: Target width.
        height: Target height.
        pad_color: Padding color.
    
    Returns:
        Padded/cropped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        result = Image.new("RGB", (width, height), pad_color)
        
        src_x = max(0, x)
        src_y = max(0, y)
        dst_x = max(0, -x)
        dst_y = max(0, -y)
        
        src_width = min(width - dst_x, img.width - src_x)
        src_height = min(height - dst_y, img.height - src_y)
        
        if src_width > 0 and src_height > 0:
            region = img.crop((src_x, src_y, src_x + src_width, src_y + src_height))
            result.paste(region, (dst_x, dst_y))
        
        output = io.BytesIO()
        result.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for padded cropping")
