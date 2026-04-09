"""Image transform utilities for geometric transformations.

This module provides utilities for various geometric transformations
including flip, mirror, crop, pad, and perspective transforms,
useful for data augmentation and image preprocessing in automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class FlipDirection(Enum):
    """Direction to flip the image."""
    HORIZONTAL = auto()  # Left-right mirror
    VERTICAL = auto()    # Top-bottom mirror
    BOTH = auto()        # Both directions


class CropMode(Enum):
    """Mode for cropping."""
    EXACT = auto()       # Exact pixel dimensions
    RATIO = auto()       # Ratio-based cropping
    CENTER = auto()      # Center crop to dimensions


@dataclass
class CropConfig:
    """Configuration for image cropping."""
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0
    mode: CropMode = CropMode.EXACT


@dataclass
class PadConfig:
    """Configuration for padding."""
    top: int = 0
    bottom: int = 0
    left: int = 0
    right: int = 0
    color: Tuple[int, int, int] = (0, 0, 0)
    mode: str = "constant"


def flip_image(
    image_data: bytes,
    direction: FlipDirection = FlipDirection.HORIZONTAL,
) -> bytes:
    """Flip an image in the specified direction.
    
    Args:
        image_data: Raw image bytes.
        direction: Direction to flip.
    
    Returns:
        Flipped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        if direction == FlipDirection.HORIZONTAL:
            flipped = img.transpose(Image.FLIP_LEFT_RIGHT)
        elif direction == FlipDirection.VERTICAL:
            flipped = img.transpose(Image.FLIP_TOP_BOTTOM)
        else:
            flipped = img.transpose(Image.FLIP_LEFT_RIGHT)
            flipped = flipped.transpose(Image.FLIP_TOP_BOTTOM)
        
        output = io.BytesIO()
        flipped.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image flipping")


def mirror_image(image_data: bytes) -> bytes:
    """Mirror image horizontally (same as flip HORIZONTAL).
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Mirrored image bytes.
    """
    return flip_image(image_data, FlipDirection.HORIZONTAL)


def crop_image(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
) -> bytes:
    """Crop a region from an image.
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of crop region.
        y: Top edge of crop region.
        width: Width of crop region.
        height: Height of crop region.
    
    Returns:
        Cropped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        width = min(width, img.width - x)
        height = min(height, img.height - y)
        
        cropped = img.crop((x, y, x + width, y + height))
        
        output = io.BytesIO()
        cropped.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image cropping")


def center_crop(
    image_data: bytes,
    target_width: int,
    target_height: int,
) -> bytes:
    """Crop image from center to target dimensions.
    
    Args:
        image_data: Raw image bytes.
        target_width: Desired width.
        target_height: Desired height.
    
    Returns:
        Center-cropped image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        left = (img.width - target_width) // 2
        top = (img.height - target_height) // 2
        right = left + target_width
        bottom = top + target_height
        
        cropped = img.crop((left, top, right, bottom))
        
        output = io.BytesIO()
        cropped.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for center cropping")


def pad_image(
    image_data: bytes,
    padding: PadConfig,
) -> bytes:
    """Add padding around an image.
    
    Args:
        image_data: Raw image bytes.
        padding: Padding configuration.
    
    Returns:
        Padded image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        new_width = img.width + padding.left + padding.right
        new_height = img.height + padding.top + padding.bottom
        
        result = Image.new(
            img.mode,
            (new_width, new_height),
            padding.color,
        )
        
        result.paste(img, (padding.left, padding.top))
        
        output = io.BytesIO()
        result.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image padding")


def resize_image_proportional(
    image_data: bytes,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
) -> bytes:
    """Resize image proportionally to fit within max dimensions.
    
    Args:
        image_data: Raw image bytes.
        max_width: Maximum width (None to not constrain).
        max_height: Maximum height (None to not constrain).
    
    Returns:
        Resized image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        if max_width is None and max_height is None:
            return image_data
        
        width_ratio = img.width / max_width if max_width else float("inf")
        height_ratio = img.height / max_height if max_height else float("inf")
        
        ratio = max(width_ratio, height_ratio)
        
        if ratio > 1:
            new_width = int(img.width / ratio)
            new_height = int(img.height / ratio)
            img = img.resize((new_width, new_height), Image.LANCZOS)
        
        output = io.BytesIO()
        img.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image resizing")


def perspective_transform(
    image_data: bytes,
    coeffs: Tuple[float, ...],
) -> bytes:
    """Apply perspective transformation to image.
    
    Args:
        image_data: Raw image bytes.
        coeffs: Perspective transformation coefficients (8 values).
    
    Returns:
        Transformed image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        transformed = img.transform(
            img.size,
            Image.Transform.PERSPECTIVE,
            coeffs,
            Image.BICUBIC,
        )
        
        output = io.BytesIO()
        transformed.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for perspective transform")


def shear_image(
    image_data: bytes,
    shear_x: float = 0.0,
    shear_y: float = 0.0,
) -> bytes:
    """Apply shear transformation to image.
    
    Args:
        image_data: Raw image bytes.
        shear_x: Horizontal shear factor.
        shear_y: Vertical shear factor.
    
    Returns:
        Sheared image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        width, height = img.size
        new_width = int(width + abs(shear_x) * height)
        new_height = int(height + abs(shear_y) * width)
        
        result = Image.new(img.mode, (new_width, new_height), (0, 0, 0))
        
        shear_matrix = (1, shear_x, -shear_y * width / 2 if shear_x else 0,
                        shear_y, 1, -shear_x * height / 2 if shear_y else 0)
        
        transformed = img.transform(
            (new_width, new_height),
            Image.Transform.AFFINE,
            shear_matrix,
            Image.BICUBIC,
        )
        
        result.paste(transformed, (0, 0))
        
        output = io.BytesIO()
        result.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for shear transform")
