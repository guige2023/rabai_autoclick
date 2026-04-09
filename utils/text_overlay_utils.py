"""Text overlay utilities for adding text annotations to images.

This module provides utilities for adding text annotations, labels, and
watermarks to screenshots, useful for documentation, debugging, and
visual feedback in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io


class TextAlignment(Enum):
    """Text alignment within bounding box."""
    LEFT = auto()
    CENTER = auto()
    RIGHT = auto()


class TextPosition(Enum):
    """Predefined text positions."""
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    MIDDLE_LEFT = auto()
    MIDDLE_CENTER = auto()
    MIDDLE_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()


@dataclass
class TextStyle:
    """Style configuration for text overlay."""
    font_size: int = 16
    font_color: Tuple[int, int, int] = (255, 255, 255)
    background_color: Optional[Tuple[int, int, int]] = (0, 0, 0)
    font_thickness: int = 1
    font_face: int = 0  # OpenCV font face
    anti_alias: bool = True


@dataclass
class TextOverlay:
    """Text overlay configuration."""
    text: str
    x: int
    y: int
    style: TextStyle
    alignment: TextAlignment = TextAlignment.LEFT
    rotation: float = 0.0


def add_text(
    image_data: bytes,
    text: str,
    x: int,
    y: int,
    style: Optional[TextStyle] = None,
) -> bytes:
    """Add text overlay to an image.
    
    Args:
        image_data: Raw image bytes.
        text: Text to add.
        x: X coordinate of text position.
        y: Y coordinate of text position.
        style: Text style configuration.
    
    Returns:
        Image with text overlay.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        style = style or TextStyle()
        
        pil_img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", style.font_size)
        except Exception:
            font = ImageFont.load_default()
        
        draw = ImageDraw.Draw(pil_img)
        
        if style.background_color:
            bbox = draw.textbbox((x, y), text, font=font)
            padding = 4
            draw.rectangle(
                [(bbox[0] - padding, bbox[1] - padding), (bbox[2] + padding, bbox[3] + padding)],
                fill=style.background_color,
            )
        
        draw.text((x, y), text, fill=style.font_color, font=font)
        
        output = io.BytesIO()
        pil_img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for text overlay")


def add_multiline_text(
    image_data: bytes,
    text: str,
    x: int,
    y: int,
    style: Optional[TextStyle] = None,
    line_spacing: float = 1.2,
    max_width: Optional[int] = None,
) -> bytes:
    """Add multi-line text with wrapping.
    
    Args:
        image_data: Raw image bytes.
        text: Multi-line text to add.
        x: X coordinate.
        y: Y coordinate.
        style: Text style configuration.
        line_spacing: Line spacing multiplier.
        max_width: Maximum width for text wrapping.
    
    Returns:
        Image with text overlay.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        style = style or TextStyle()
        lines = text.split("\n")
        
        pil_img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", style.font_size)
        except Exception:
            font = ImageFont.load_default()
        
        draw = ImageDraw.Draw(pil_img)
        
        current_y = y
        for line in lines:
            if max_width:
                line = _wrap_text(line, font, max_width)
            
            if style.background_color:
                bbox = draw.textbbox((x, current_y), line, font=font)
                padding = 4
                draw.rectangle(
                    [(bbox[0] - padding, bbox[1] - padding), (bbox[2] + padding, bbox[3] + padding)],
                    fill=style.background_color,
                )
            
            draw.text((x, current_y), line, fill=style.font_color, font=font)
            
            bbox = draw.textbbox((x, current_y), line, font=font)
            line_height = (bbox[3] - bbox[1]) * line_spacing
            current_y += line_height
        
        output = io.BytesIO()
        pil_img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for text overlay")


def add_text_at_position(
    image_data: bytes,
    text: str,
    position: TextPosition,
    style: Optional[TextStyle] = None,
    margin: int = 10,
) -> bytes:
    """Add text at a predefined position.
    
    Args:
        image_data: Raw image bytes.
        text: Text to add.
        position: Predefined position.
        style: Text style configuration.
        margin: Margin from edges.
    
    Returns:
        Image with text overlay.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        style = style or TextStyle()
        
        pil_img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", style.font_size)
        except Exception:
            font = ImageFont.load_default()
        
        draw = ImageDraw.Draw(pil_img)
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        if position == TextPosition.TOP_LEFT:
            x, y = margin, margin
        elif position == TextPosition.TOP_CENTER:
            x, y = (pil_img.width - text_width) // 2, margin
        elif position == TextPosition.TOP_RIGHT:
            x, y = pil_img.width - text_width - margin, margin
        elif position == TextPosition.MIDDLE_LEFT:
            x, y = margin, (pil_img.height - text_height) // 2
        elif position == TextPosition.MIDDLE_CENTER:
            x, y = (pil_img.width - text_width) // 2, (pil_img.height - text_height) // 2
        elif position == TextPosition.MIDDLE_RIGHT:
            x, y = pil_img.width - text_width - margin, (pil_img.height - text_height) // 2
        elif position == TextPosition.BOTTOM_LEFT:
            x, y = margin, pil_img.height - text_height - margin
        elif position == TextPosition.BOTTOM_CENTER:
            x, y = (pil_img.width - text_width) // 2, pil_img.height - text_height - margin
        elif position == TextPosition.BOTTOM_RIGHT:
            x, y = pil_img.width - text_width - margin, pil_img.height - text_height - margin
        else:
            x, y = margin, margin
        
        return add_text(image_data, text, x, y, style)
    except ImportError:
        raise ImportError("PIL is required for text overlay")


def add_timestamp(
    image_data: bytes,
    format_str: str = "%Y-%m-%d %H:%M:%S",
    position: TextPosition = TextPosition.BOTTOM_RIGHT,
    style: Optional[TextStyle] = None,
) -> bytes:
    """Add timestamp to image.
    
    Args:
        image_data: Raw image bytes.
        format_str: strftime format string.
        position: Position for timestamp.
        style: Text style configuration.
    
    Returns:
        Image with timestamp overlay.
    """
    from datetime import datetime
    
    timestamp = datetime.now().strftime(format_str)
    return add_text_at_position(image_data, timestamp, position, style)


def add_label(
    image_data: bytes,
    label: str,
    x: int,
    y: int,
    padding: int = 5,
    bg_color: Tuple[int, int, int] = (0, 0, 0),
    text_color: Tuple[int, int, int] = (255, 255, 255),
    font_size: int = 14,
) -> bytes:
    """Add a labeled tag/badge to the image.
    
    Args:
        image_data: Raw image bytes.
        label: Label text.
        x: X coordinate of label center.
        y: Y coordinate of label center.
        padding: Padding around text.
        bg_color: Background color.
        text_color: Text color.
        font_size: Font size.
    
    Returns:
        Image with label overlay.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        style = TextStyle(
            font_size=font_size,
            font_color=text_color,
            background_color=bg_color,
        )
        
        pil_img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
        except Exception:
            font = ImageFont.load_default()
        
        draw = ImageDraw.Draw(pil_img)
        bbox = draw.textbbox((0, 0), label, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        label_x = x - text_width // 2 - padding
        label_y = y - text_height // 2 - padding
        
        draw.rectangle(
            [(label_x, label_y), (label_x + text_width + padding * 2, label_y + text_height + padding * 2)],
            fill=bg_color,
        )
        
        draw.text((label_x + padding, label_y + padding), label, fill=text_color, font=font)
        
        output = io.BytesIO()
        pil_img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for label overlay")


def _wrap_text(text: str, font, max_width: int) -> str:
    """Wrap text to fit within max width."""
    words = text.split()
    lines = []
    current_line = ""
    
    for word in words:
        test_line = current_line + " " + word if current_line else word
        bbox = font.getbbox(test_line)
        width = bbox[2] - bbox[0]
        
        if width <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    
    if current_line:
        lines.append(current_line)
    
    return "\n".join(lines)
