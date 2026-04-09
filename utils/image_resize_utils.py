"""Image resize utilities for scaling and dimension manipulation.

This module provides utilities for resizing images with various algorithms
and constraints, including proportional scaling, fixed dimensions, and
quality-preserving transformations.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class ResizeMode(Enum):
    """Mode for resizing operation."""
    EXACT = auto()
    PROPORTIONAL = auto()
    FIT = auto()       # Fit within bounds
    FILL = auto()      # Fill to exact size
    THUMBNAIL = auto()


class ResizeAlgorithm(Enum):
    """Algorithm for resizing."""
    NEAREST = auto()
    BILINEAR = auto()
    BICUBIC = auto()
    LANCZOS = auto()


@dataclass
class ResizeConfig:
    """Configuration for resizing."""
    mode: ResizeMode = ResizeMode.PROPORTIONAL
    algorithm: ResizeAlgorithm = ResizeAlgorithm.LANCZOS
    target_width: Optional[int] = None
    target_height: Optional[int] = None
    max_width: Optional[int] = None
    max_height: Optional[int] = None
    quality: int = 95


def resize_image(
    image_data: bytes,
    width: int,
    height: int,
    algorithm: ResizeAlgorithm = ResizeAlgorithm.LANCZOS,
) -> bytes:
    """Resize image to exact dimensions.
    
    Args:
        image_data: Image bytes.
        width: Target width.
        height: Target height.
        algorithm: Resize algorithm.
    
    Returns:
        Resized image bytes.
    """
    try:
        from PIL import Image
        import io
        
        resample_map = {
            ResizeAlgorithm.NEAREST: Image.NEAREST,
            ResizeAlgorithm.BILINEAR: Image.BILINEAR,
            ResizeAlgorithm.BICUBIC: Image.BICUBIC,
            ResizeAlgorithm.LANCZOS: Image.LANCZOS,
        }
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        resized = img.resize((width, height), resample_map[algorithm])
        
        output = io.BytesIO()
        resized.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for resizing")


def resize_proportional(
    image_data: bytes,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
) -> bytes:
    """Resize proportionally to fit within max dimensions.
    
    Args:
        image_data: Image bytes.
        max_width: Maximum width.
        max_height: Maximum height.
    
    Returns:
        Proportionally resized image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
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
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for proportional resizing")


def resize_to_fit(
    image_data: bytes,
    max_width: int,
    max_height: int,
) -> bytes:
    """Resize to fit within bounds (may not fill all space).
    
    Args:
        image_data: Image bytes.
        max_width: Maximum width.
        max_height: Maximum height.
    
    Returns:
        Fitted image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        img.thumbnail((max_width, max_height), Image.LANCZOS, reducing_gap=1.0)
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for resize-to-fit")


def resize_to_fill(
    image_data: bytes,
    width: int,
    height: int,
) -> bytes:
    """Resize to fill exact dimensions (may crop).
    
    Args:
        image_data: Image bytes.
        width: Target width.
        height: Target height.
    
    Returns:
        Filled image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        target_aspect = width / height
        current_aspect = img.width / img.height
        
        if current_aspect > target_aspect:
            new_width = int(img.height * target_aspect)
            left = (img.width - new_width) // 2
            img = img.crop((left, 0, left + new_width, img.height))
        else:
            new_height = int(img.width / target_aspect)
            top = (img.height - new_height) // 2
            img = img.crop((0, top, img.width, top + new_height))
        
        img = img.resize((width, height), Image.LANCZOS)
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for resize-to-fill")


def resize_with_config(
    image_data: bytes,
    config: ResizeConfig,
) -> bytes:
    """Resize using configuration object.
    
    Args:
        image_data: Image bytes.
        config: Resize configuration.
    
    Returns:
        Resized image bytes.
    """
    if config.mode == ResizeMode.EXACT and config.target_width and config.target_height:
        return resize_image(image_data, config.target_width, config.target_height, config.algorithm)
    elif config.mode == ResizeMode.PROPORTIONAL:
        return resize_proportional(image_data, config.max_width, config.max_height)
    elif config.mode == ResizeMode.FIT and config.max_width and config.max_height:
        return resize_to_fit(image_data, config.max_width, config.max_height)
    elif config.mode == ResizeMode.FILL and config.target_width and config.target_height:
        return resize_to_fill(image_data, config.target_width, config.target_height)
    else:
        return resize_proportional(image_data, config.max_width, config.max_height)


def scale_by_factor(
    image_data: bytes,
    factor: float,
) -> bytes:
    """Scale image by a factor.
    
    Args:
        image_data: Image bytes.
        factor: Scale factor (e.g., 0.5 = half size, 2.0 = double size).
    
    Returns:
        Scaled image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        new_width = int(img.width * factor)
        new_height = int(img.height * factor)
        
        scaled = img.resize((new_width, new_height), Image.LANCZOS)
        
        output = io.BytesIO()
        scaled.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for scaling")


def downscale_to_size(
    image_data: bytes,
    max_dimension: int,
) -> bytes:
    """Downscale image if larger than max dimension.
    
    Args:
        image_data: Image bytes.
        max_dimension: Maximum width or height.
    
    Returns:
        Downscaled image bytes (or original if smaller).
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        if img.width <= max_dimension and img.height <= max_dimension:
            return image_data
        
        if img.width > img.height:
            new_width = max_dimension
            new_height = int(img.height * (max_dimension / img.width))
        else:
            new_height = max_dimension
            new_width = int(img.width * (max_dimension / img.height))
        
        img = img.resize((new_width, new_height), Image.LANCZOS)
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for downscaling")
