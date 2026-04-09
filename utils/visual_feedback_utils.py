"""Visual feedback utilities for providing user feedback.

This module provides utilities for creating visual feedback elements
like loading indicators, progress bars, status messages, and notifications,
useful for user-facing automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io


class FeedbackType(Enum):
    """Type of visual feedback."""
    LOADING = auto()
    PROGRESS = auto()
    SUCCESS = auto()
    ERROR = auto()
    WARNING = auto()
    INFO = auto()


@dataclass
class FeedbackConfig:
    """Configuration for visual feedback."""
    feedback_type: FeedbackType = FeedbackType.INFO
    message: str = ""
    progress: float = 0.0
    duration_ms: float = 0.0
    position: str = "center"  # center, top, bottom, top-right, etc.


@dataclass
class NotificationStyle:
    """Style configuration for notifications."""
    bg_color: Tuple[int, int, int] = (50, 50, 50)
    text_color: Tuple[int, int, int] = (255, 255, 255)
    border_color: Tuple[int, int, int] = (100, 100, 100)
    border_width: int = 2
    corner_radius: int = 10
    padding: int = 20
    font_size: int = 16


def create_loading_indicator(
    image_data: bytes,
    x: int,
    y: int,
    size: int = 50,
    color: Tuple[int, int, int] = (255, 255, 255),
) -> bytes:
    """Add loading spinner indicator to image.
    
    Args:
        image_data: Image bytes.
        x: Center X of indicator.
        y: Center Y of indicator.
        size: Size of indicator.
        color: RGB color.
    
    Returns:
        Image with loading indicator.
    """
    try:
        from PIL import Image, ImageDraw
        import io
        import math
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        radius = size // 2
        draw.ellipse(
            [(x - radius, y - radius), (x + radius, y + radius)],
            outline=color,
            width=3,
        )
        
        arc_start = 0
        arc_end = 270
        
        for i in range(12):
            angle = math.radians(arc_start + i * 30)
            inner_x = int(x + (radius - 10) * math.cos(angle))
            inner_y = int(y + (radius - 10) * math.sin(angle))
            
            draw.ellipse(
                [(inner_x - 3, inner_y - 3), (inner_x + 3, inner_y + 3)],
                fill=color,
            )
        
        img = Image.alpha_composite(
            img.convert("RGBA"),
            overlay,
        ).convert("RGB")
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for loading indicator")


def create_progress_bar(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
    progress: float,
    bg_color: Tuple[int, int, int] = (50, 50, 50),
    fill_color: Tuple[int, int, int] = (0, 200, 0),
) -> bytes:
    """Add progress bar to image.
    
    Args:
        image_data: Image bytes.
        x: Left edge of bar.
        y: Top edge of bar.
        width: Bar width.
        height: Bar height.
        progress: Progress value (0.0 to 1.0).
        bg_color: Background color.
        fill_color: Fill color.
    
    Returns:
        Image with progress bar.
    """
    try:
        from PIL import Image, ImageDraw
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        draw.rectangle(
            [(x, y), (x + width, y + height)],
            fill=bg_color,
        )
        
        fill_width = int(width * min(1.0, max(0.0, progress)))
        if fill_width > 0:
            draw.rectangle(
                [(x, y), (x + fill_width, y + height)],
                fill=fill_color,
            )
        
        img = Image.alpha_composite(
            img.convert("RGBA"),
            overlay,
        ).convert("RGB")
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for progress bar")


def create_notification_banner(
    image_data: bytes,
    message: str,
    feedback_type: FeedbackType = FeedbackType.INFO,
    position: str = "top",
    config: Optional[NotificationStyle] = None,
) -> bytes:
    """Add notification banner to image.
    
    Args:
        image_data: Image bytes.
        message: Notification message.
        feedback_type: Type of notification.
        position: Position (top, bottom, center).
        config: Notification style configuration.
    
    Returns:
        Image with notification banner.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        config = config or NotificationStyle()
        
        colors = {
            FeedbackType.SUCCESS: (0, 150, 0),
            FeedbackType.ERROR: (200, 0, 0),
            FeedbackType.WARNING: (200, 150, 0),
            FeedbackType.INFO: (50, 50, 50),
            FeedbackType.LOADING: (50, 50, 50),
            FeedbackType.PROGRESS: (50, 50, 50),
        }
        
        bg_color = colors.get(feedback_type, (50, 50, 50))
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        try:
            font = ImageFont.truetype(
                "/System/Library/Fonts/Helvetica.ttc",
                config.font_size,
            )
        except Exception:
            font = ImageFont.load_default()
        
        draw = ImageDraw.Draw(img)
        bbox = draw.textbbox((0, 0), message, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        banner_height = text_height + config.padding * 2
        banner_width = text_width + config.padding * 2
        
        if position == "top":
            bx = (img.width - banner_width) // 2
            by = 20
        elif position == "bottom":
            bx = (img.width - banner_width) // 2
            by = img.height - banner_height - 20
        else:
            bx = (img.width - banner_width) // 2
            by = (img.height - banner_height) // 2
        
        draw.rectangle(
            [(bx, by), (bx + banner_width, by + banner_height)],
            fill=bg_color,
        )
        
        text_x = bx + (banner_width - text_width) // 2
        text_y = by + config.padding
        
        draw.text(
            (text_x, text_y),
            message,
            fill=config.text_color,
            font=font,
        )
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for notification banner")


def create_cursor_highlight(
    image_data: bytes,
    x: int,
    y: int,
    highlight_color: Tuple[int, int, int] = (255, 255, 0),
    size: int = 30,
) -> bytes:
    """Highlight cursor position with circle.
    
    Args:
        image_data: Image bytes.
        x: Cursor X coordinate.
        y: Cursor Y coordinate.
        highlight_color: Highlight color.
        size: Size of highlight.
    
    Returns:
        Image with cursor highlight.
    """
    try:
        from PIL import Image, ImageDraw
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        draw.ellipse(
            [(x - size // 2, y - size // 2), (x + size // 2, y + size // 2)],
            outline=highlight_color,
            width=3,
        )
        
        img = Image.alpha_composite(
            img.convert("RGBA"),
            overlay,
        ).convert("RGB")
        
        output = io.BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for cursor highlight")
