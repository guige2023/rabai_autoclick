"""Image rotation utilities for correcting orientation and rotation.

This module provides utilities for rotating and transforming images,
useful for correcting screen orientation, adjusting UI captures, and
performing geometric transformations in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import io
import math


class RotationAngle(Enum):
    """Predefined rotation angles."""
    DEG_90_CW = 90      # 90 degrees clockwise
    DEG_90_CCW = 270    # 270 degrees clockwise (90 CCW)
    DEG_180 = 180       # 180 degrees


@dataclass
class RotationConfig:
    """Configuration for image rotation."""
    angle_degrees: float = 0.0
    expand_canvas: bool = True
    fill_color: tuple[int, int, int] = (0, 0, 0)
    resize: bool = False


def rotate_image(
    image_data: bytes,
    angle: float,
    expand: bool = True,
    fill_color: Optional[tuple[int, int, int]] = None,
) -> bytes:
    """Rotate an image by specified angle.
    
    Args:
        image_data: Raw image bytes.
        angle: Rotation angle in degrees (positive = clockwise).
        expand: Expand canvas to fit rotated image if True.
        fill_color: Background fill color for expanded areas.
    
    Returns:
        Rotated image bytes.
    """
    try:
        from PIL import Image
        import io
        
        fill_color = fill_color or (0, 0, 0)
        img = Image.open(io.BytesIO(image_data))
        
        rotated = img.rotate(
            angle,
            expand=expand,
            fillcolor=fill_color,
            resample=Image.BICUBIC,
        )
        
        output = io.BytesIO()
        rotated.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for image rotation")


def rotate_90_cw(image_data: bytes) -> bytes:
    """Rotate image 90 degrees clockwise.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Rotated image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        rotated = img.transpose(Image.Transpose.ROTATE_270)
        
        output = io.BytesIO()
        rotated.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for rotation")


def rotate_90_ccw(image_data: bytes) -> bytes:
    """Rotate image 90 degrees counter-clockwise.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Rotated image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        rotated = img.transpose(Image.Transpose.ROTATE_90)
        
        output = io.BytesIO()
        rotated.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for rotation")


def rotate_180(image_data: bytes) -> bytes:
    """Rotate image 180 degrees.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Rotated image bytes.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        rotated = img.transpose(Image.Transpose.ROTATE_180)
        
        output = io.BytesIO()
        rotated.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for rotation")


def auto_rotate_by_exif(image_data: bytes) -> bytes:
    """Auto-rotate image based on EXIF orientation data.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Rotated image bytes (or original if no EXIF data).
    """
    try:
        from PIL import Image
        import io
        from PIL.Image import Exif
        
        img = Image.open(io.BytesIO(image_data))
        
        exif = img.getexif()
        if exif is None:
            return image_data
        
        orientation = exif.get(274, 1)
        
        rotations = {
            2: Image.Transpose.FLIP_LEFT_RIGHT,
            3: Image.Transpose.ROTATE_180,
            4: Image.Transpose.FLIP_TOP_BOTTOM,
            5: Image.Transpose.TRANSPOSE,
            6: Image.Transpose.ROTATE_270,
            7: Image.Transpose.TRANSVERSE,
            8: Image.Transpose.ROTATE_90,
        }
        
        if orientation in rotations:
            img = img.transpose(rotations[orientation])
        
        output = io.BytesIO()
        img.save(output, format=img.format or "PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for EXIF rotation")


def rotate_to_angle(
    image_data: bytes,
    target_angle: float,
    current_angle: float = 0.0,
) -> bytes:
    """Rotate image from current angle to target angle.
    
    Args:
        image_data: Raw image bytes.
        target_angle: Desired angle in degrees.
        current_angle: Current angle of the image.
    
    Returns:
        Rotated image bytes.
    """
    delta = (target_angle - current_angle) % 360
    if delta > 180:
        delta -= 360
    return rotate_image(image_data, delta)


def calculate_rotated_dimensions(
    width: int,
    height: int,
    angle: float,
) -> tuple[int, int]:
    """Calculate dimensions of rotated rectangle.
    
    Args:
        width: Original width.
        height: Original height.
        angle: Rotation angle in degrees.
    
    Returns:
        Tuple of (new_width, new_height).
    """
    angle_rad = math.radians(angle)
    
    cos_a = abs(math.cos(angle_rad))
    sin_a = abs(math.sin(angle_rad))
    
    new_width = int(width * cos_a + height * sin_a)
    new_height = int(width * sin_a + height * cos_a)
    
    return (new_width, new_height)


def straighten_image(
    image_data: bytes,
    angle: float,
) -> bytes:
    """Straighten (deskew) an image by rotating it.
    
    Args:
        image_data: Raw image bytes.
        angle: Skew angle to correct.
    
    Returns:
        Straightened image bytes.
    """
    return rotate_image(image_data, -angle, expand=True)
