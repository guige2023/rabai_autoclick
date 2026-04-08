"""
Image filtering and processing utilities.

Provides utilities for applying filters and transformations
to images including blur, sharpen, brightness, contrast, etc.
"""

from __future__ import annotations

from typing import Tuple, Optional, List, Callable
from dataclasses import dataclass


@dataclass
class RGB:
    """RGB color tuple."""
    r: int
    g: int
    b: int
    
    def __add__(self, other: "RGB") -> "RGB":
        return RGB(self.r + other.r, self.g + other.g, self.b + other.b)
    
    def __mul__(self, scalar: float) -> "RGB":
        return RGB(int(self.r * scalar), int(self.g * scalar), int(self.b * scalar))
    
    def clamp(self) -> "RGB":
        """Clamp values to 0-255."""
        return RGB(
            max(0, min(255, self.r)),
            max(0, min(255, self.g)),
            max(0, min(255, self.b))
        )


def hex_to_rgb(hex_color: str) -> RGB:
    """Convert hex color to RGB."""
    hex_color = hex_color.lstrip('#')
    return RGB(int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16))


def rgb_to_hex(rgb: RGB) -> str:
    """Convert RGB to hex color."""
    return f"#{rgb.r:02X}{rgb.g:02X}{rgb.b:02X}"


def adjust_brightness(image_data: bytes, width: int, height: int, factor: float) -> bytes:
    """Adjust image brightness.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        factor: Brightness factor (1.0 = original, >1 = brighter, <1 = darker)
        
    Returns:
        Modified image data
    """
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        output[i] = int(min(255, image_data[i] * factor))
        output[i + 1] = int(min(255, image_data[i + 1] * factor))
        output[i + 2] = int(min(255, image_data[i + 2] * factor))
        output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def adjust_contrast(image_data: bytes, width: int, height: int, factor: float) -> bytes:
    """Adjust image contrast.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        factor: Contrast factor (1.0 = original, >1 = more contrast, <1 = less)
        
    Returns:
        Modified image data
    """
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        for c in range(3):
            val = image_data[i + c]
            new_val = int(((val / 255 - 0.5) * factor + 0.5) * 255)
            output[i + c] = max(0, min(255, new_val))
        output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def adjust_saturation(image_data: bytes, width: int, height: int, factor: float) -> bytes:
    """Adjust image saturation.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        factor: Saturation factor (1.0 = original, 0 = grayscale, >1 = more saturated)
        
    Returns:
        Modified image data
    """
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        r = image_data[i]
        g = image_data[i + 1]
        b = image_data[i + 2]
        a = image_data[i + 3]
        
        # Convert to grayscale
        gray = 0.299 * r + 0.587 * g + 0.114 * b
        
        output[i] = int(max(0, min(255, gray + (r - gray) * factor)))
        output[i + 1] = int(max(0, min(255, gray + (g - gray) * factor)))
        output[i + 2] = int(max(0, min(255, gray + (b - gray) * factor)))
        output[i + 3] = a
    
    return bytes(output)


def grayscale(image_data: bytes, width: int, height: int) -> bytes:
    """Convert image to grayscale.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        
    Returns:
        Grayscale image data
    """
    return adjust_saturation(image_data, width, height, 0.0)


def invert_colors(image_data: bytes, width: int, height: int) -> bytes:
    """Invert image colors.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        
    Returns:
        Inverted image data
    """
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        output[i] = 255 - image_data[i]
        output[i + 1] = 255 - image_data[i + 1]
        output[i + 2] = 255 - image_data[i + 2]
        output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def apply_sepia(image_data: bytes, width: int, height: int) -> bytes:
    """Apply sepia tone effect.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        
    Returns:
        Sepia-toned image data
    """
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        r = image_data[i]
        g = image_data[i + 1]
        b = image_data[i + 2]
        a = image_data[i + 3]
        
        # Sepia matrix
        new_r = int(0.393 * r + 0.769 * g + 0.189 * b)
        new_g = int(0.349 * r + 0.686 * g + 0.168 * b)
        new_b = int(0.272 * r + 0.534 * g + 0.131 * b)
        
        output[i] = max(0, min(255, new_r))
        output[i + 1] = max(0, min(255, new_g))
        output[i + 2] = max(0, min(255, new_b))
        output[i + 3] = a
    
    return bytes(output)


def apply_posterize(
    image_data: bytes,
    width: int,
    height: int,
    levels: int = 6
) -> bytes:
    """Posterize image (reduce number of color levels).
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        levels: Number of levels per channel
        
    Returns:
        Posterized image data
    """
    if levels < 2:
        levels = 2
    if levels > 256:
        levels = 256
    
    step = 256 // levels
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        for c in range(3):
            val = image_data[i + c]
            new_val = (val // step) * step
            output[i + c] = new_val
        output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def apply_threshold(
    image_data: bytes,
    width: int,
    height: int,
    threshold: int = 128
) -> bytes:
    """Apply threshold to image (convert to black and white).
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        threshold: Threshold value (0-255)
        
    Returns:
        Thresholded image data
    """
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        # Use luminance
        gray = int(0.299 * image_data[i] + 0.587 * image_data[i + 1] + 0.114 * image_data[i + 2])
        
        if gray >= threshold:
            output[i] = 255
            output[i + 1] = 255
            output[i + 2] = 255
        else:
            output[i] = 0
            output[i + 1] = 0
            output[i + 2] = 0
        output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def apply_blur_box(
    image_data: bytes,
    width: int,
    height: int,
    radius: int = 5
) -> bytes:
    """Apply box blur to image.
    
    Note: This is a simple implementation. For production,
    use PIL/Pillow or CoreImage for better performance.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        radius: Blur radius
        
    Returns:
        Blurred image data
    """
    output = bytearray(len(image_data))
    
    for y in range(height):
        for x in range(width):
            r_sum = g_sum = b_sum = count = 0
            
            for dy in range(-radius, radius + 1):
                for dx in range(-radius, radius + 1):
                    nx = max(0, min(width - 1, x + dx))
                    ny = max(0, min(height - 1, y + dy))
                    
                    i = (ny * width + nx) * 4
                    r_sum += image_data[i]
                    g_sum += image_data[i + 1]
                    b_sum += image_data[i + 2]
                    count += 1
            
            i = (y * width + x) * 4
            output[i] = r_sum // count
            output[i + 1] = g_sum // count
            output[i + 2] = b_sum // count
            output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def apply_sharpen(
    image_data: bytes,
    width: int,
    height: int,
    amount: float = 1.0
) -> bytes:
    """Apply sharpening to image.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        amount: Sharpening amount
        
    Returns:
        Sharpened image data
    """
    # Simple unsharp mask
    blurred = apply_blur_box(image_data, width, height, 2)
    output = bytearray(len(image_data))
    
    for i in range(0, len(image_data), 4):
        for c in range(3):
            diff = int(image_data[i + c]) - int(blurred[i + c])
            new_val = int(image_data[i + c] + diff * amount)
            output[i + c] = max(0, min(255, new_val))
        output[i + 3] = image_data[i + 3]
    
    return bytes(output)


def apply_edge_detection(image_data: bytes, width: int, height: int) -> bytes:
    """Apply simple edge detection.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        
    Returns:
        Edge-detected image data
    """
    # Convert to grayscale first
    gray = grayscale(image_data, width, height)
    output = bytearray(len(gray))
    
    # Sobel kernels
    kx = [-1, 0, 1, -2, 0, 2, -1, 0, 1]
    ky = [-1, -2, -1, 0, 0, 0, 1, 2, 1]
    
    for y in range(1, height - 1):
        for x in range(1, width - 1):
            gx = gy = 0
            
            for ky_row in range(3):
                for kx_col in range(3):
                    nx = x + kx_col - 1
                    ny = y + ky_row - 1
                    i = (ny * width + nx)
                    
                    val = gray[i]
                    gx += val * kx[ky_row * 3 + kx_col]
                    gy += val * ky[ky_row * 3 + kx_col]
            
            magnitude = min(255, int(math.sqrt(gx * gx + gy * gy)))
            
            i = (y * width + x) * 4
            output[i] = magnitude
            output[i + 1] = magnitude
            output[i + 2] = magnitude
            output[i + 3] = gray[i + 3]
    
    return bytes(output)


import math


def create_vignette(
    image_data: bytes,
    width: int,
    height: int,
    strength: float = 0.5,
    radius: float = 0.5
) -> bytes:
    """Apply vignette effect.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        strength: Vignette strength (0-1)
        radius: Effect radius (0-1)
        
    Returns:
        Vignette-applied image data
    """
    cx = width / 2
    cy = height / 2
    max_dist = math.sqrt(cx * cx + cy * cy)
    output = bytearray(image_data)
    
    for y in range(height):
        for x in range(width):
            dx = x - cx
            dy = y - cy
            dist = math.sqrt(dx * dx + dy * dy)
            
            normalized_dist = min(1.0, dist / max_dist)
            
            # Vignette curve
            vignette = 1.0 - (normalized_dist - (1.0 - radius)) / radius
            vignette = max(0.0, min(1.0, vignette))
            vignette = vignette ** 2
            
            factor = 1.0 - (vignette * strength)
            
            i = (y * width + x) * 4
            output[i] = int(image_data[i] * factor)
            output[i + 1] = int(image_data[i + 1] * factor)
            output[i + 2] = int(image_data[i + 2] * factor)
    
    return bytes(output)


def blend_images(
    image1: bytes,
    image2: bytes,
    width: int,
    height: int,
    alpha: float = 0.5,
    mode: str = "normal"
) -> bytes:
    """Blend two images together.
    
    Args:
        image1: First RGBA image data
        image2: Second RGBA image data
        width: Image width
        height: Image height
        alpha: Blend factor (0 = image1, 1 = image2)
        mode: Blend mode ('normal', 'multiply', 'screen', 'overlay')
        
    Returns:
        Blended image data
    """
    output = bytearray(len(image1))
    
    for i in range(0, len(image1), 4):
        r1, g1, b1, a1 = image1[i], image1[i+1], image1[i+2], image1[i+3]
        r2, g2, b2, a2 = image2[i], image2[i+1], image2[i+2], image2[i+3]
        
        if mode == "normal":
            r = int(r1 * (1 - alpha) + r2 * alpha)
            g = int(g1 * (1 - alpha) + g2 * alpha)
            b = int(b1 * (1 - alpha) + b2 * alpha)
        elif mode == "multiply":
            r = int(r1 * r2 / 255)
            g = int(g1 * g2 / 255)
            b = int(b1 * b2 / 255)
        elif mode == "screen":
            r = int(255 - (255 - r1) * (255 - r2) / 255)
            g = int(255 - (255 - g1) * (255 - g2) / 255)
            b = int(255 - (255 - b1) * (255 - b2) / 255)
        elif mode == "overlay":
            def overlay(c1, c2):
                if c1 < 128:
                    return int(2 * c1 * c2 / 255)
                return int(255 - 2 * (255 - c1) * (255 - c2) / 255)
            r = overlay(r1, r2)
            g = overlay(g1, g2)
            b = overlay(b1, b2)
        else:
            r = int(r1 * (1 - alpha) + r2 * alpha)
            g = int(g1 * (1 - alpha) + g2 * alpha)
            b = int(b1 * (1 - alpha) + b2 * alpha)
        
        output[i] = max(0, min(255, r))
        output[i + 1] = max(0, min(255, g))
        output[i + 2] = max(0, min(255, b))
        output[i + 3] = max(0, min(255, a1 + a2 - a1 * a2 // 255))
    
    return bytes(output)


def crop_image(
    image_data: bytes,
    width: int,
    height: int,
    x: int,
    y: int,
    crop_width: int,
    crop_height: int
) -> bytes:
    """Crop image to a region.
    
    Args:
        image_data: RGBA image data
        width: Original width
        height: Original height
        x: Crop X
        y: Crop Y
        crop_width: Crop width
        crop_height: Crop height
        
    Returns:
        Cropped image data
    """
    output = bytearray(crop_width * crop_height * 4)
    
    for cy in range(crop_height):
        for cx in range(crop_width):
            src_x = x + cx
            src_y = y + cy
            
            if 0 <= src_x < width and 0 <= src_y < height:
                src_i = (src_y * width + src_x) * 4
                dst_i = (cy * crop_width + cx) * 4
                output[dst_i:dst_i + 4] = image_data[src_i:src_i + 4]
    
    return bytes(output)


def flip_horizontal(
    image_data: bytes,
    width: int,
    height: int
) -> bytes:
    """Flip image horizontally.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        
    Returns:
        Flipped image data
    """
    output = bytearray(len(image_data))
    
    for y in range(height):
        for x in range(width):
            src_i = (y * width + x) * 4
            dst_i = (y * width + (width - 1 - x)) * 4
            output[dst_i:dst_i + 4] = image_data[src_i:src_i + 4]
    
    return bytes(output)


def flip_vertical(
    image_data: bytes,
    width: int,
    height: int
) -> bytes:
    """Flip image vertically.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        
    Returns:
        Flipped image data
    """
    output = bytearray(len(image_data))
    
    for y in range(height):
        for x in range(width):
            src_i = (y * width + x) * 4
            dst_i = ((height - 1 - y) * width + x) * 4
            output[dst_i:dst_i + 4] = image_data[src_i:src_i + 4]
    
    return bytes(output)


def rotate_90(
    image_data: bytes,
    width: int,
    height: int,
    clockwise: bool = True
) -> Tuple[bytes, int, int]:
    """Rotate image 90 degrees.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        clockwise: Rotate clockwise
        
    Returns:
        Tuple of (rotated_data, new_width, new_height)
    """
    new_width = height
    new_height = width
    output = bytearray(new_width * new_height * 4)
    
    for y in range(height):
        for x in range(width):
            src_i = (y * width + x) * 4
            
            if clockwise:
                dst_x = height - 1 - y
                dst_y = x
            else:
                dst_x = y
                dst_y = width - 1 - x
            
            dst_i = (dst_y * new_width + dst_x) * 4
            output[dst_i:dst_i + 4] = image_data[src_i:src_i + 4]
    
    return bytes(output), new_width, new_height
