"""
Image masking and region utilities.

Provides utilities for creating and manipulating image masks,
including alpha masks, shape masks, and region-based operations.
"""

from __future__ import annotations

import math
from typing import Tuple, Optional, List, Callable


def create_rectangular_mask(
    width: int,
    height: int,
    x: int,
    y: int,
    mask_width: int,
    mask_height: int
) -> bytes:
    """Create a rectangular alpha mask.
    
    Args:
        width: Image width
        height: Image height
        x: Mask left position
        y: Mask top position
        mask_width: Mask width
        mask_height: Mask height
        
    Returns:
        8-bit grayscale mask data
    """
    mask = bytearray(width * height)
    
    for py in range(height):
        for px in range(width):
            if x <= px < x + mask_width and y <= py < y + mask_height:
                mask[py * width + px] = 255
    
    return bytes(mask)


def create_elliptical_mask(
    width: int,
    height: int,
    cx: int,
    cy: int,
    radius_x: int,
    radius_y: int
) -> bytes:
    """Create an elliptical alpha mask.
    
    Args:
        width: Image width
        height: Image height
        cx: Center X
        cy: Center Y
        radius_x: X radius (half width)
        radius_y: Y radius (half height)
        
    Returns:
        8-bit grayscale mask data
    """
    mask = bytearray(width * height)
    
    for y in range(height):
        for x in range(width):
            dx = x - cx
            dy = y - cy
            
            # Check if point is inside ellipse
            if radius_x > 0 and radius_y > 0:
                normalized = (dx * dx) / (radius_x * radius_x) + (dy * dy) / (radius_y * radius_y)
                if normalized <= 1.0:
                    # Calculate alpha based on distance from edge for smooth edge
                    if normalized > 0.8:
                        alpha = int(255 * (1.0 - (normalized - 0.8) / 0.2))
                    else:
                        alpha = 255
                    mask[y * width + x] = alpha
    
    return bytes(mask)


def create_gradient_mask(
    width: int,
    height: int,
    direction: str = "horizontal",
    gradient_type: str = "linear"
) -> bytes:
    """Create a gradient mask.
    
    Args:
        width: Mask width
        height: Mask height
        direction: 'horizontal', 'vertical', 'diagonal'
        gradient_type: 'linear', 'radial'
        
    Returns:
        8-bit grayscale mask data
    """
    mask = bytearray(width * height)
    
    if gradient_type == "linear":
        for y in range(height):
            for x in range(width):
                if direction == "horizontal":
                    t = x / max(1, width - 1)
                elif direction == "vertical":
                    t = y / max(1, height - 1)
                elif direction == "diagonal":
                    t = (x + y) / max(1, width + height - 2)
                else:
                    t = x / max(1, width - 1)
                
                mask[y * width + x] = int(t * 255)
    
    elif gradient_type == "radial":
        cx = width // 2
        cy = height // 2
        max_radius = math.sqrt(cx * cx + cy * cy)
        
        for y in range(height):
            for x in range(width):
                dx = x - cx
                dy = y - cy
                dist = math.sqrt(dx * dx + dy * dy)
                t = min(1.0, dist / max_radius)
                mask[y * width + x] = int(t * 255)
    
    return bytes(mask)


def create_rounded_rectangle_mask(
    width: int,
    height: int,
    x: int,
    y: int,
    mask_width: int,
    mask_height: int,
    corner_radius: int
) -> bytes:
    """Create a rounded rectangle mask.
    
    Args:
        width: Image width
        height: Image height
        x: Mask left position
        y: Mask top position
        mask_width: Mask width
        mask_height: Mask height
        corner_radius: Corner radius
        
    Returns:
        8-bit grayscale mask data
    """
    mask = bytearray(width * height)
    
    for py in range(height):
        for px in range(width):
            inside = False
            
            # Check if point is in the main rectangle (excluding corners)
            if (x + corner_radius <= px < x + mask_width - corner_radius and
                y <= py < y + mask_height):
                inside = True
            elif (x <= px < x + mask_width and
                  y + corner_radius <= py < y + mask_height - corner_radius):
                inside = True
            
            # Check corners
            corners = [
                (x + corner_radius, y + corner_radius),  # Top-left
                (x + mask_width - corner_radius, y + corner_radius),  # Top-right
                (x + corner_radius, y + mask_height - corner_radius),  # Bottom-left
                (x + mask_width - corner_radius, y + mask_height - corner_radius),  # Bottom-right
            ]
            
            for cx, cy in corners:
                dx = px - cx
                dy = py - cy
                if dx * dx + dy * dy <= corner_radius * corner_radius:
                    inside = True
                    break
            
            if inside:
                mask[py * width + px] = 255
    
    return bytes(mask)


def apply_mask_to_image(
    image_data: bytes,
    mask: bytes,
    width: int,
    height: int
) -> bytes:
    """Apply an alpha mask to an RGBA image.
    
    Args:
        image_data: RGBA image data
        mask: 8-bit grayscale mask
        width: Image width
        height: Image height
        
    Returns:
        Modified RGBA image data
    """
    if len(mask) != width * height:
        raise ValueError("Mask size does not match image size")
    
    output = bytearray(len(image_data))
    
    for i in range(len(image_data) // 4):
        alpha = mask[i]
        output[i * 4 + 3] = (image_data[i * 4 + 3] * alpha) // 255
    
    return bytes(output)


def invert_mask(mask: bytes) -> bytes:
    """Invert a mask (black becomes white, etc).
    
    Args:
        mask: 8-bit grayscale mask
        
    Returns:
        Inverted mask
    """
    return bytes(255 - m for m in mask)


def expand_mask(mask: bytes, width: int, height: int, amount: int) -> bytes:
    """Expand a mask by adding pixels around edges.
    
    Args:
        mask: 8-bit grayscale mask
        width: Mask width
        height: Mask height
        amount: Pixels to expand
        
    Returns:
        Expanded mask
    """
    # Simple approach: create output and set pixels that are near non-zero mask pixels
    output = bytearray(width * height)
    
    for y in range(height):
        for x in range(width):
            # Check neighborhood
            for dy in range(-amount, amount + 1):
                for dx in range(-amount, amount + 1):
                    nx = x + dx
                    ny = y + dy
                    
                    if 0 <= nx < width and 0 <= ny < height:
                        if mask[ny * width + nx] > 0:
                            output[y * width + x] = 255
                            break
    
    return bytes(output)


def erode_mask(mask: bytes, width: int, height: int, amount: int) -> bytes:
    """Erode a mask by removing edge pixels.
    
    Args:
        mask: 8-bit grayscale mask
        width: Mask width
        height: Mask height
        amount: Pixels to erode
        
    Returns:
        Eroded mask
    """
    output = bytearray(width * height)
    
    for y in range(height):
        for x in range(width):
            # Check if all neighbors in range are non-zero
            all_filled = True
            for dy in range(-amount, amount + 1):
                for dx in range(-amount, amount + 1):
                    nx = x + dx
                    ny = y + dy
                    
                    if 0 <= nx < width and 0 <= ny < height:
                        if mask[ny * width + nx] == 0:
                            all_filled = False
                            break
                if not all_filled:
                    break
            
            if all_filled:
                output[y * width + x] = 255
    
    return bytes(output)


def blur_mask(mask: bytes, width: int, height: int, radius: int) -> bytes:
    """Blur a mask (for smooth transitions).
    
    Args:
        mask: 8-bit grayscale mask
        width: Mask width
        height: Mask height
        radius: Blur radius
        
    Returns:
        Blurred mask
    """
    output = bytearray(width * height)
    
    for y in range(height):
        for x in range(width):
            total = 0
            count = 0
            
            for dy in range(-radius, radius + 1):
                for dx in range(-radius, radius + 1):
                    nx = x + dx
                    ny = y + dy
                    
                    if 0 <= nx < width and 0 <= ny < height:
                        total += mask[ny * width + nx]
                        count += 1
            
            output[y * width + x] = total // count if count > 0 else 0
    
    return bytes(output)


def threshold_mask(mask: bytes, threshold: int = 128) -> bytes:
    """Threshold a mask to binary values.
    
    Args:
        mask: 8-bit grayscale mask
        threshold: Threshold value (0-255)
        
    Returns:
        Binary mask
    """
    return bytes(255 if m >= threshold else 0 for m in mask)


def combine_masks(
    mask1: bytes,
    mask2: bytes,
    operation: str = "and"
) -> bytes:
    """Combine two masks.
    
    Args:
        mask1: First mask
        mask2: Second mask
        operation: 'and', 'or', 'xor', 'subtract'
        
    Returns:
        Combined mask
    """
    if len(mask1) != len(mask2):
        raise ValueError("Masks must be the same size")
    
    output = bytearray(len(mask1))
    
    for i in range(len(mask1)):
        if operation == "and":
            output[i] = min(mask1[i], mask2[i])
        elif operation == "or":
            output[i] = max(mask1[i], mask2[i])
        elif operation == "xor":
            output[i] = abs(mask1[i] - mask2[i])
        elif operation == "subtract":
            output[i] = max(0, mask1[i] - mask2[i])
        else:
            output[i] = mask1[i]
    
    return bytes(output)


def create_ring_mask(
    width: int,
    height: int,
    cx: int,
    cy: int,
    inner_radius: int,
    outer_radius: int
) -> bytes:
    """Create a ring (donut) mask.
    
    Args:
        width: Mask width
        height: Mask height
        cx: Center X
        cy: Center Y
        inner_radius: Inner radius
        outer_radius: Outer radius
        
    Returns:
        8-bit grayscale mask
    """
    mask = bytearray(width * height)
    
    for y in range(height):
        for x in range(width):
            dx = x - cx
            dy = y - cy
            dist_sq = dx * dx + dy * dy
            
            if inner_radius ** 2 <= dist_sq <= outer_radius ** 2:
                # Calculate alpha for smooth edge
                inner_dist = math.sqrt(dist_sq) - inner_radius
                outer_dist = outer_radius - math.sqrt(dist_sq)
                
                if inner_dist < 1 or outer_dist < 1:
                    mask[y * width + x] = int(min(inner_dist, outer_dist) * 255)
                else:
                    mask[y * width + x] = 255
    
    return bytes(mask)


def create_line_mask(
    width: int,
    height: int,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    thickness: int = 1
) -> bytes:
    """Create a line mask.
    
    Args:
        width: Mask width
        height: Mask height
        x1: Start X
        y1: Start Y
        x2: End X
        y2: End Y
        thickness: Line thickness
        
    Returns:
        8-bit grayscale mask
    """
    mask = bytearray(width * height)
    
    # Bresenham's line algorithm with thickness
    dx = abs(x2 - x1)
    dy = abs(y2 - y1)
    
    if dx > dy:
        steps = dx
    else:
        steps = dy if dy != 0 else 1
    
    for i in range(steps + 1):
        t = i / steps if steps > 0 else 0
        x = int(x1 + (x2 - x1) * t)
        y = int(y1 + (y2 - y1) * t)
        
        # Draw thickness
        for ty in range(-thickness // 2, thickness // 2 + 1):
            for tx in range(-thickness // 2, thickness // 2 + 1):
                px = x + tx
                py = y + ty
                if 0 <= px < width and 0 <= py < height:
                    mask[py * width + px] = 255
    
    return bytes(mask)


def get_mask_bounds(mask: bytes, width: int, height: int) -> Tuple[int, int, int, int]:
    """Get bounding box of non-zero pixels in mask.
    
    Args:
        mask: 8-bit grayscale mask
        width: Mask width
        height: Mask height
        
    Returns:
        Tuple of (min_x, min_y, max_x, max_y)
    """
    min_x = width
    min_y = height
    max_x = 0
    max_y = 0
    
    for y in range(height):
        for x in range(width):
            if mask[y * width + x] > 0:
                min_x = min(min_x, x)
                min_y = min(min_y, y)
                max_x = max(max_x, x)
                max_y = max(max_y, y)
    
    if min_x > max_x or min_y > max_y:
        return (0, 0, 0, 0)
    
    return (min_x, min_y, max_x, max_y)
