"""
Image blending and compositing utilities.

Provides utilities for blending, compositing, and layering images
with various blend modes and opacity control.
"""

from __future__ import annotations

from typing import Optional, Tuple, List, Callable
from dataclasses import dataclass


@dataclass
class BlendRegion:
    """Defines a rectangular region for blending operations."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def right(self) -> int:
        return self.x + self.width
    
    @property
    def bottom(self) -> int:
        return self.y + self.height
    
    def contains(self, px: int, py: int) -> bool:
        """Check if a point is within this region."""
        return self.x <= px < self.right and self.y <= py < self.bottom


@dataclass
class Layer:
    """Represents a single layer in a composite image."""
    data: bytes
    width: int
    height: int
    alpha: float = 1.0
    blend_mode: str = "normal"
    offset_x: int = 0
    offset_y: int = 0
    
    @property
    def channels(self) -> int:
        """Return bytes per pixel (assumes RGBA)."""
        return len(self.data) // (self.width * self.height) if self.width > 0 and self.height > 0 else 4


def alpha_blend(base: int, overlay: int, alpha: float) -> int:
    """Alpha blend two color values.
    
    Args:
        base: Base color value (0-255)
        overlay: Overlay color value (0-255)
        alpha: Overlay alpha (0.0-1.0)
        
    Returns:
        Blended color value
    """
    return int(base * (1 - alpha) + overlay * alpha)


def blend_pixels_rgba(
    base_r: int, base_g: int, base_b: int, base_a: int,
    overlay_r: int, overlay_g: int, overlay_b: int, overlay_a: int,
    blend_mode: str = "normal"
) -> Tuple[int, int, int, int]:
    """Blend two RGBA pixels using the specified blend mode.
    
    Args:
        base_r, base_g, base_b, base_a: Base pixel components
        overlay_r, overlay_g, overlay_b, overlay_a: Overlay pixel components
        blend_mode: Blend mode name
        
    Returns:
        Blended RGBA tuple
    """
    if overlay_a == 0:
        return (base_r, base_g, base_b, base_a)
    
    effective_alpha = overlay_a / 255.0
    
    if blend_mode == "normal":
        r = alpha_blend(base_r, overlay_r, effective_alpha)
        g = alpha_blend(base_g, overlay_g, effective_alpha)
        b = alpha_blend(base_b, overlay_b, effective_alpha)
        
    elif blend_mode == "multiply":
        r = int(base_r * overlay_r / 255)
        g = int(base_g * overlay_g / 255)
        b = int(base_b * overlay_b / 255)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "screen":
        r = int(255 - (255 - base_r) * (255 - overlay_r) / 255)
        g = int(255 - (255 - base_g) * (255 - overlay_g) / 255)
        b = int(255 - (255 - base_b) * (255 - overlay_b) / 255)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "overlay":
        def overlay_channel(c1: int, c2: int) -> int:
            if c1 < 128:
                return int(2 * c1 * c2 / 255)
            else:
                return int(255 - 2 * (255 - c1) * (255 - c2) / 255)
        r = overlay_channel(base_r, overlay_r)
        g = overlay_channel(base_g, overlay_g)
        b = overlay_channel(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "darken":
        r = min(base_r, overlay_r)
        g = min(base_g, overlay_g)
        b = min(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "lighten":
        r = max(base_r, overlay_r)
        g = max(base_g, overlay_g)
        b = max(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "color_dodge":
        def dodge(c1: int, c2: int) -> int:
            if c2 == 255:
                return 255
            return min(255, int(c1 / (255 - c2) * 255))
        r = dodge(base_r, overlay_r)
        g = dodge(base_g, overlay_g)
        b = dodge(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "color_burn":
        def burn(c1: int, c2: int) -> int:
            if c2 == 0:
                return 0
            return max(0, 255 - int((255 - c1) / c2 * 255))
        r = burn(base_r, overlay_r)
        g = burn(base_g, overlay_g)
        b = burn(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "soft_light":
        def soft_light(c1: int, c2: int) -> int:
            if c2 < 128:
                return int(c1 - (255 - 2 * c2) * c1 * (255 - c1) / 255 / 256)
            else:
                d = 0 if c1 < 64 else int(256 * ((c1 - 64) / (255 - 64)))
                return int(c1 + (2 * c2 - 255) * (d - c1) / 256)
        r = soft_light(base_r, overlay_r)
        g = soft_light(base_g, overlay_g)
        b = soft_light(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "hard_light":
        def hard_light(c1: int, c2: int) -> int:
            if c2 < 128:
                return int(2 * c1 * c2 / 255)
            else:
                return int(255 - 2 * (255 - c1) * (255 - c2) / 255)
        r = hard_light(base_r, overlay_r)
        g = hard_light(base_g, overlay_g)
        b = hard_light(base_b, overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "difference":
        r = abs(base_r - overlay_r)
        g = abs(base_g - overlay_g)
        b = abs(base_b - overlay_b)
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
        
    elif blend_mode == "exclusion":
        r = int((base_r + overlay_r) / 2) - base_r * overlay_r / 128
        g = int((base_g + overlay_g) / 2) - base_g * overlay_g / 128
        b = int((base_b + overlay_b) / 2) - base_b * overlay_b / 128
        r = alpha_blend(base_r, r, effective_alpha)
        g = alpha_blend(base_g, g, effective_alpha)
        b = alpha_blend(base_b, b, effective_alpha)
    
    else:
        r = alpha_blend(base_r, overlay_r, effective_alpha)
        g = alpha_blend(base_g, overlay_g, effective_alpha)
        b = alpha_blend(base_b, overlay_b, effective_alpha)
    
    # Combine alphas
    out_a = base_a + int((255 - base_a) * overlay_a / 255)
    
    return (r, g, b, out_a)


def composite_layers(
    layers: List[Layer],
    width: int,
    height: int,
    background: Tuple[int, int, int, int] = (0, 0, 0, 0)
) -> bytes:
    """Composite multiple layers into a single image.
    
    Args:
        layers: List of layers to composite (bottom to top)
        width: Output width
        height: Output height
        background: Background RGBA color
        
    Returns:
        Composite image data as RGBA bytes
    """
    # Initialize output with background
    output = bytearray(width * height * 4)
    
    # Fill with background
    for i in range(width * height):
        output[i * 4:(i + 1) * 4] = background
    
    # Composite each layer
    for layer in layers:
        layer_data = layer.data
        
        for py in range(layer.height):
            for px in range(layer.width):
                # Calculate output position
                out_x = px + layer.offset_x
                out_y = py + layer.offset_y
                
                if out_x < 0 or out_x >= width or out_y < 0 or out_y >= height:
                    continue
                
                # Get layer pixel (assumes RGBA)
                layer_idx = (py * layer.width + px) * 4
                if layer_idx + 3 >= len(layer_data):
                    continue
                
                lr = layer_data[layer_idx]
                lg = layer_data[layer_idx + 1]
                lb = layer_data[layer_idx + 2]
                la = int(layer_data[layer_idx + 3] * layer.alpha)
                
                # Get output pixel
                out_idx = (out_y * width + out_x) * 4
                or_val = output[out_idx]
                og_val = output[out_idx + 1]
                ob_val = output[out_idx + 2]
                oa_val = output[out_idx + 3]
                
                # Blend
                br, bg, bb, ba = blend_pixels_rgba(
                    or_val, og_val, ob_val, oa_val,
                    lr, lg, lb, la,
                    layer.blend_mode
                )
                
                output[out_idx] = br
                output[out_idx + 1] = bg
                output[out_idx + 2] = bb
                output[out_idx + 3] = ba
    
    return bytes(output)


def create_gradient_layer(
    width: int,
    height: int,
    start_color: Tuple[int, int, int, int],
    end_color: Tuple[int, int, int, int],
    direction: str = "vertical"
) -> Layer:
    """Create a gradient layer.
    
    Args:
        width: Layer width
        height: Layer height
        start_color: Starting RGBA color
        end_color: Ending RGBA color
        direction: Gradient direction ('vertical', 'horizontal', 'diagonal')
        
    Returns:
        Layer with gradient data
    """
    data = bytearray(width * height * 4)
    
    for y in range(height):
        for x in range(width):
            if direction == "vertical":
                factor = y / (height - 1) if height > 1 else 0
            elif direction == "horizontal":
                factor = x / (width - 1) if width > 1 else 0
            elif direction == "diagonal":
                factor = (x + y) / (width + height - 2)
            else:
                factor = y / (height - 1) if height > 1 else 0
            
            factor = max(0, min(1, factor))
            
            r = int(start_color[0] + (end_color[0] - start_color[0]) * factor)
            g = int(start_color[1] + (end_color[1] - start_color[1]) * factor)
            b = int(start_color[2] + (end_color[2] - start_color[2]) * factor)
            a = int(start_color[3] + (end_color[3] - start_color[3]) * factor)
            
            idx = (y * width + x) * 4
            data[idx] = r
            data[idx + 1] = g
            data[idx + 2] = b
            data[idx + 3] = a
    
    return Layer(
        data=bytes(data),
        width=width,
        height=height,
        alpha=1.0,
        blend_mode="normal"
    )


def apply_vignette(
    image_data: bytes,
    width: int,
    height: int,
    strength: float = 0.5,
    radius: float = 0.5
) -> bytes:
    """Apply a vignette effect to an image.
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        strength: Vignette strength (0.0-1.0)
        radius: Radius of effect (0.0-1.0, where 1.0 is corners)
        
    Returns:
        Modified image data
    """
    output = bytearray(image_data)
    cx = width / 2
    cy = height / 2
    max_dist = ((cx * cx) + (cy * cy)) ** 0.5
    
    for y in range(height):
        for x in range(width):
            dx = x - cx
            dy = y - cy
            dist = (dx * dx + dy * dy) ** 0.5
            
            normalized_dist = min(1.0, dist / max_dist)
            
            # Vignette curve
            vignette = 1.0 - (normalized_dist - (1.0 - radius)) / radius
            vignette = max(0.0, min(1.0, vignette))
            vignette = vignette ** 2  # Sharper falloff
            
            factor = 1.0 - (vignette * strength)
            
            idx = (y * width + x) * 4
            output[idx] = int(image_data[idx] * factor)
            output[idx + 1] = int(image_data[idx + 1] * factor)
            output[idx + 2] = int(image_data[idx + 2] * factor)
    
    return bytes(output)


def apply_blur_edge(
    image_data: bytes,
    width: int,
    height: int,
    edge_pixels: int = 5
) -> bytes:
    """Apply blur to edges of an image (for feathering).
    
    Args:
        image_data: RGBA image data
        width: Image width
        height: Image height
        edge_pixels: Number of edge pixels to blur
        
    Returns:
        Modified image data with blurred edges
    """
    output = bytearray(image_data)
    
    # Top edge
    for y in range(edge_pixels):
        factor = y / edge_pixels
        for x in range(width):
            idx = (y * width + x) * 4
            # Average with pixel below
            below_idx = ((y + 1) * width + x) * 4
            for c in range(4):
                output[idx + c] = int(image_data[idx + c] * factor + 
                                     image_data[below_idx + c] * (1 - factor))
    
    # Bottom edge
    for y in range(height - edge_pixels, height):
        offset = height - 1 - y
        factor = offset / edge_pixels
        for x in range(width):
            idx = (y * width + x) * 4
            above_idx = ((y - 1) * width + x) * 4
            for c in range(4):
                output[idx + c] = int(image_data[idx + c] * factor + 
                                     image_data[above_idx + c] * (1 - factor))
    
    # Left edge
    for x in range(edge_pixels):
        factor = x / edge_pixels
        for y in range(width):
            idx = (y * width + x) * 4
            right_idx = (y * width + x + 1) * 4
            for c in range(4):
                output[idx + c] = int(image_data[idx + c] * factor + 
                                     image_data[right_idx + c] * (1 - factor))
    
    # Right edge
    for x in range(width - edge_pixels, width):
        offset = width - 1 - x
        factor = offset / edge_pixels
        for y in range(height):
            idx = (y * width + x) * 4
            left_idx = (y * width + x - 1) * 4
            for c in range(4):
                output[idx + c] = int(image_data[idx + c] * factor + 
                                     image_data[left_idx + c] * (1 - factor))
    
    return bytes(output)


def get_blend_mode_names() -> List[str]:
    """Get list of available blend mode names.
    
    Returns:
        List of blend mode names
    """
    return [
        "normal",
        "multiply",
        "screen",
        "overlay",
        "darken",
        "lighten",
        "color_dodge",
        "color_burn",
        "soft_light",
        "hard_light",
        "difference",
        "exclusion",
    ]
