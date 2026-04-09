"""Color watermark utilities for image watermarking and detection.

This module provides utilities for adding visible watermarks to images
and detecting existing watermarks, useful for branding, copyright
protection, and authenticity verification in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io
import hashlib


class WatermarkPosition(Enum):
    """Position for watermark placement."""
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    CENTER_LEFT = auto()
    CENTER = auto()
    CENTER_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()
    TILED = auto()  # Repeated across image


@dataclass
class WatermarkConfig:
    """Configuration for watermark."""
    text: str = ""
    position: WatermarkPosition = WatermarkPosition.BOTTOM_RIGHT
    font_size: int = 24
    font_color: Tuple[int, int, int, int] = (128, 128, 128, 128)
    opacity: float = 0.3
    rotation: float = -30.0
    margin: int = 20


@dataclass
class WatermarkDetectionResult:
    """Result of watermark detection."""
    has_watermark: bool
    confidence: float
    watermark_text: Optional[str]
    position: Optional[Tuple[int, int, int, int]]


def add_text_watermark(
    image_data: bytes,
    text: str,
    config: Optional[WatermarkConfig] = None,
) -> bytes:
    """Add text watermark to image.
    
    Args:
        image_data: Image bytes.
        text: Watermark text.
        config: Watermark configuration.
    
    Returns:
        Image with watermark.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        config = config or WatermarkConfig()
        config.text = text
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        
        try:
            font = ImageFont.truetype(
                "/System/Library/Fonts/Helvetica.ttc",
                config.font_size,
            )
        except Exception:
            font = ImageFont.load_default()
        
        watermark_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(watermark_layer)
        
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        positions = _get_watermark_position(
            config.position,
            img.width,
            img.height,
            text_width,
            text_height,
            config.margin,
        )
        
        if config.position == WatermarkPosition.TILED:
            _draw_tiled_watermark(
                draw, text, font, config, img.width, img.height
            )
        else:
            for x, y in positions:
                draw.text(
                    (x, y),
                    text,
                    fill=config.font_color,
                    font=font,
                )
        
        if config.rotation != 0:
            watermark_layer = watermark_layer.rotate(
                config.rotation,
                expand=True,
                fillcolor=(0, 0, 0, 0),
            )
        
        watermarked = Image.alpha_composite(img, watermark_layer)
        
        output = io.BytesIO()
        if watermarked.mode == "RGBA":
            watermarked.save(output, format="PNG")
        else:
            watermarked = watermarked.convert("RGB")
            watermarked.save(output, format="PNG")
        
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for watermarking")


def add_image_watermark(
    image_data: bytes,
    watermark_data: bytes,
    position: WatermarkPosition = WatermarkPosition.BOTTOM_RIGHT,
    opacity: float = 0.5,
    margin: int = 20,
) -> bytes:
    """Add image watermark to another image.
    
    Args:
        image_data: Base image bytes.
        watermark_data: Watermark image bytes.
        position: Position for watermark.
        opacity: Watermark opacity.
        margin: Margin from edges.
    
    Returns:
        Image with watermark.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        watermark = Image.open(io.BytesIO(watermark_data)).convert("RGBA")
        
        watermark = watermark.resize(
            (img.width // 5, img.height // 5),
            Image.LANCZOS,
        )
        
        alpha = watermark.split()[3]
        alpha = alpha.point(lambda p: int(p * opacity))
        watermark.putalpha(alpha)
        
        x, y = _get_image_watermark_position(
            position, img.width, img.height, watermark.width, watermark.height, margin
        )
        
        result = Image.new("RGBA", img.size, (0, 0, 0, 0))
        result.paste(img, (0, 0))
        result.paste(watermark, (x, y), watermark)
        
        output = io.BytesIO()
        if result.mode == "RGBA":
            result.save(output, format="PNG")
        else:
            result = result.convert("RGB")
            result.save(output, format="PNG")
        
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image watermark")


def detect_watermark(image_data: bytes) -> WatermarkDetectionResult:
    """Detect if image has a watermark.
    
    Args:
        image_data: Image bytes.
    
    Returns:
        WatermarkDetectionResult.
    """
    try:
        from PIL import Image
        import numpy as np
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGBA")
        img_array = np.array(img)
        
        alpha_variance = np.var(img_array[:, :, 3])
        
        has_watermark = alpha_variance > 100
        
        return WatermarkDetectionResult(
            has_watermark=bool(has_watermark),
            confidence=float(min(alpha_variance / 1000, 1.0)),
            watermark_text=None,
            position=None,
        )
    except ImportError:
        raise ImportError("PIL and numpy are required for watermark detection")


def _get_watermark_position(
    position: WatermarkPosition,
    img_width: int,
    img_height: int,
    text_width: int,
    text_height: int,
    margin: int,
) -> List[Tuple[int, int]]:
    """Get position for watermark text."""
    positions = []
    
    if position == WatermarkPosition.TOP_LEFT:
        positions = [(margin, margin)]
    elif position == WatermarkPosition.TOP_CENTER:
        positions = [(img_width - text_width) // 2]
    elif position == WatermarkPosition.TOP_RIGHT:
        positions = [(img_width - text_width - margin, margin)]
    elif position == WatermarkPosition.CENTER_LEFT:
        positions = [(margin, (img_height - text_height) // 2)]
    elif position == WatermarkPosition.CENTER:
        positions = [((img_width - text_width) // 2, (img_height - text_height) // 2)]
    elif position == WatermarkPosition.CENTER_RIGHT:
        positions = [(img_width - text_width - margin, (img_height - text_height) // 2)]
    elif position == WatermarkPosition.BOTTOM_LEFT:
        positions = [(margin, img_height - text_height - margin)]
    elif position == WatermarkPosition.BOTTOM_CENTER:
        positions = [((img_width - text_width) // 2, img_height - text_height - margin)]
    elif position == WatermarkPosition.BOTTOM_RIGHT:
        positions = [(img_width - text_width - margin, img_height - text_height - margin)]
    else:
        positions = [(margin, margin)]
    
    return positions


def _draw_tiled_watermark(draw, text, font, config, img_width, img_height):
    """Draw tiled watermark across image."""
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    
    spacing = text_width + 50
    
    for y in range(0, img_height, text_height + 50):
        for x in range(0, img_width, spacing):
            draw.text((x, y), text, fill=config.font_color, font=font)


def _get_image_watermark_position(
    position: WatermarkPosition,
    img_width: int,
    img_height: int,
    wm_width: int,
    wm_height: int,
    margin: int,
) -> Tuple[int, int]:
    """Get position for image watermark."""
    if position == WatermarkPosition.TOP_LEFT:
        return (margin, margin)
    elif position == WatermarkPosition.TOP_RIGHT:
        return (img_width - wm_width - margin, margin)
    elif position == WatermarkPosition.BOTTOM_LEFT:
        return (margin, img_height - wm_height - margin)
    elif position == WatermarkPosition.BOTTOM_RIGHT:
        return (img_width - wm_width - margin, img_height - wm_height - margin)
    elif position == WatermarkPosition.CENTER:
        return ((img_width - wm_width) // 2, (img_height - wm_height) // 2)
    else:
        return (img_width - wm_width - margin, img_height - wm_height - margin)
