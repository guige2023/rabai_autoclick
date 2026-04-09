"""Image compression utilities for optimizing screenshot storage.

This module provides utilities for compressing screenshots with various
algorithms, quality settings, and format conversions to reduce storage
size while maintaining acceptable quality.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
from pathlib import Path
import io


class CompressionType(Enum):
    """Type of compression to apply."""
    LOSSY = auto()      # JPEG, WebP lossy
    LOSSLESS = auto()  # PNG, WebP lossless
    ZIP = auto()       # Raw bytes compressed with zlib


class ImageQuality(Enum):
    """Predefined quality levels for lossy compression."""
    LOW = 30
    MEDIUM = 60
    HIGH = 80
    MAXIMUM = 95


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    original_size: int
    compressed_size: int
    compression_ratio: float
    format: str
    quality: int
    processing_time_ms: float


@dataclass
class CompressionConfig:
    """Configuration for image compression."""
    compression_type: CompressionType = CompressionType.LOSSY
    quality: int = 80
    max_width: Optional[int] = None
    max_height: Optional[int] = None
    preserve_alpha: bool = True
    strip_metadata: bool = False


def compress_image(
    image_data: bytes,
    output_format: str = "JPEG",
    quality: int = 80,
) -> bytes:
    """Compress an image to the specified format and quality.
    
    Args:
        image_data: Raw image bytes.
        output_format: Target format (JPEG, PNG, WebP).
        quality: Quality level 1-100 (for lossy formats).
    
    Returns:
        Compressed image bytes.
    """
    try:
        from PIL import Image
        
        img = Image.open(io.BytesIO(image_data))
        
        if output_format.upper() == "JPEG" and img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        
        output = io.BytesIO()
        img.save(output, format=output_format.upper(), quality=quality, optimize=True)
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image compression")


def compress_to_png(image_data: bytes) -> bytes:
    """Compress an image to PNG format.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        PNG compressed image bytes.
    """
    return compress_image(image_data, "PNG", quality=100)


def compress_to_jpeg(
    image_data: bytes,
    quality: int = 80,
) -> bytes:
    """Compress an image to JPEG format.
    
    Args:
        image_data: Raw image bytes.
        quality: Quality level 1-100.
    
    Returns:
        JPEG compressed image bytes.
    """
    return compress_image(image_data, "JPEG", quality=quality)


def compress_to_webp(
    image_data: bytes,
    quality: int = 80,
    lossless: bool = False,
) -> bytes:
    """Compress an image to WebP format.
    
    Args:
        image_data: Raw image bytes.
        quality: Quality level 1-100.
        lossless: Use lossless compression if True.
    
    Returns:
        WebP compressed image bytes.
    """
    try:
        from PIL import Image
        
        img = Image.open(io.BytesIO(image_data))
        
        output = io.BytesIO()
        img.save(
            output,
            format="WEBP",
            quality=quality,
            lossless=lossless,
            method=6,
        )
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for WebP compression")


def calculate_compression_stats(
    original_data: bytes,
    compressed_data: bytes,
) -> dict:
    """Calculate statistics about compression results.
    
    Args:
        original_data: Original image bytes.
        compressed_data: Compressed image bytes.
    
    Returns:
        Dictionary with compression statistics.
    """
    original_size = len(original_data)
    compressed_size = len(compressed_data)
    ratio = compressed_size / original_size if original_size > 0 else 0
    savings_percent = (1 - ratio) * 100
    
    return {
        "original_size": original_size,
        "compressed_size": compressed_size,
        "compression_ratio": ratio,
        "savings_percent": savings_percent,
        "space_saved": original_size - compressed_size,
    }


def compress_with_auto_quality(
    image_data: bytes,
    target_size_kb: int,
    format: str = "JPEG",
) -> tuple[bytes, int]:
    """Compress image to approach target file size.
    
    Args:
        image_data: Raw image bytes.
        target_size_kb: Target size in kilobytes.
        format: Output format.
    
    Returns:
        Tuple of (compressed_data, actual_quality_used).
    """
    target_bytes = target_size_kb * 1024
    quality = 85
    step = 10
    min_quality = 10
    
    compressed = compress_image(image_data, format, quality)
    
    while len(compressed) > target_bytes and quality > min_quality:
        quality = max(quality - step, min_quality)
        compressed = compress_image(image_data, format, quality)
    
    return compressed, quality


def resize_image(
    image_data: bytes,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
    maintain_aspect: bool = True,
) -> bytes:
    """Resize an image to fit within max dimensions.
    
    Args:
        image_data: Raw image bytes.
        max_width: Maximum width in pixels.
        max_height: Maximum height in pixels.
        maintain_aspect: Maintain aspect ratio if True.
    
    Returns:
        Resized image bytes.
    """
    try:
        from PIL import Image
        
        img = Image.open(io.BytesIO(image_data))
        original_width, original_height = img.size
        
        if not maintain_aspect:
            if max_width and max_height:
                img = img.resize((max_width, max_height), Image.LANCZOS)
            return img
        
        if max_width and original_width > max_width:
            ratio = max_width / original_width
            new_width = max_width
            new_height = int(original_height * ratio)
            if max_height and new_height > max_height:
                ratio = max_height / new_height
                new_height = max_height
                new_width = int(new_width * ratio)
            img = img.resize((new_width, new_height), Image.LANCZOS)
        
        elif max_height and original_height > max_height:
            ratio = max_height / original_height
            new_height = max_height
            new_width = int(original_width * ratio)
            img = img.resize((new_width, new_height), Image.LANCZOS)
        
        output = io.BytesIO()
        img.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image resizing")


def batch_compress_images(
    images: list[bytes],
    format: str = "JPEG",
    quality: int = 80,
) -> list[bytes]:
    """Compress multiple images in batch.
    
    Args:
        images: List of image bytes.
        format: Output format.
        quality: Quality level 1-100.
    
    Returns:
        List of compressed image bytes.
    """
    return [compress_image(img, format, quality) for img in images]


def get_recommended_format(
    image_data: bytes,
    prefers_lossy: bool = True,
) -> str:
    """Recommend the best format for an image based on its properties.
    
    Args:
        image_data: Raw image bytes.
        prefers_lossy: Prefer lossy formats if True.
    
    Returns:
        Recommended format string.
    """
    try:
        from PIL import Image
        
        img = Image.open(io.BytesIO(image_data))
        mode = img.mode
        has_transparency = mode in ("RGBA", "LA", "P")
        
        if has_transparency:
            return "PNG"
        elif prefers_lossy:
            return "JPEG"
        else:
            return "PNG"
    except Exception:
        return "JPEG"
