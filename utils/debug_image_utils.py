"""
Debug image utilities for UI automation testing and analysis.

Provides functions for annotating images, creating debug overlays,
and generating comparison visualizations.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Sequence


Point = Tuple[float, float]
Rect = Tuple[float, float, float, float]
Color = Tuple[int, int, int]
RectList = List[Rect]


def draw_rectangle_outline(
    image: 'Image',
    rect: Rect,
    color: Color = (255, 0, 0),
    width: int = 2,
) -> 'Image':
    """Draw rectangle outline on image.
    
    Args:
        image: PIL Image object
        rect: Rectangle (x, y, width, height)
        color: RGB color tuple
        width: Line width in pixels
    
    Returns:
        Modified image
    """
    try:
        from PIL import ImageDraw
    except ImportError:
        return image
    
    draw = ImageDraw.Draw(image)
    x, y, w, h = rect
    
    for i in range(width):
        draw.rectangle(
            [x - i, y - i, x + w + i, y + h + i],
            outline=color,
        )
    
    return image


def draw_crosshair(
    image: 'Image',
    point: Point,
    color: Color = (0, 255, 0),
    size: int = 10,
) -> 'Image':
    """Draw crosshair at point.
    
    Args:
        image: PIL Image object
        point: (x, y) center point
        color: RGB color tuple
        size: Crosshair size in pixels
    
    Returns:
        Modified image
    """
    try:
        from PIL import ImageDraw
    except ImportError:
        return image
    
    draw = ImageDraw.Draw(image)
    x, y = point
    
    draw.line([(x - size, y), (x + size, y)], fill=color)
    draw.line([(x, y - size), (x, y + size)], fill=color)
    draw.ellipse([x - 3, y - 3, x + 3, y + 3], fill=color)
    
    return image


def draw_region_labels(
    image: 'Image',
    regions: Sequence[Rect],
    labels: Optional[Sequence[str]] = None,
    color: Color = (255, 0, 0),
    font_size: int = 12,
) -> 'Image':
    """Draw labeled regions on image.
    
    Args:
        image: PIL Image object
        regions: List of rectangles
        labels: Optional list of labels for each region
        color: RGB color tuple
        font_size: Font size for labels
    
    Returns:
        Modified image
    """
    try:
        from PIL import ImageDraw, ImageFont
    except ImportError:
        return image
    
    draw = ImageDraw.Draw(image)
    
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
    except (OSError, IOError):
        font = ImageFont.load_default()
    
    for i, rect in enumerate(regions):
        x, y, w, h = rect
        
        for j in range(2):
            offset = j * 2
            draw.rectangle([x, y + offset, x + w, y + offset + 1], fill=color)
            draw.rectangle([x + offset, y, x + offset + 1, y + h], fill=color)
            draw.rectangle([x + w - offset - 1, y, x + w, y + h], fill=color)
            draw.rectangle([x, y + h - offset - 1, x + w, y + h], fill=color)
        
        if labels and i < len(labels):
            label = labels[i]
            text_x = x + 4
            text_y = max(y - font_size - 2, 2)
            draw.rectangle(
                [text_x - 2, text_y - 2, text_x + font_size * len(label), text_y + font_size],
                fill=(0, 0, 0),
            )
            draw.text((text_x, text_y), label, fill=color, font=font)
    
    return image


def highlight_regions(
    image: 'Image',
    regions: Sequence[Rect],
    color: Color = (255, 255, 0),
    alpha: float = 0.3,
) -> 'Image':
    """Highlight regions with semi-transparent overlay.
    
    Args:
        image: PIL Image object
        regions: List of rectangles to highlight
        color: RGB color tuple
        alpha: Opacity (0.0 to 1.0)
    
    Returns:
        Modified image
    """
    try:
        from PIL import ImageDraw
    except ImportError:
        return image
    
    overlay = image.copy()
    draw = ImageDraw.Draw(overlay, 'RGBA')
    
    fill_color = color + (int(255 * alpha),)
    
    for rect in regions:
        x, y, w, h = rect
        draw.rectangle([x, y, x + w, y + h], fill=fill_color)
    
    return Image.alpha_composite(image.convert('RGBA'), overlay).convert('RGB')


def create_diff_visualization(
    image1: 'Image',
    image2: 'Image',
    threshold: int = 30,
) -> 'Image':
    """Create visual diff between two images.
    
    Args:
        image1: First image
        image2: Second image
        threshold: Pixel difference threshold
    
    Returns:
        Composite image showing differences
    """
    try:
        from PIL import Image
    except ImportError:
        return image1
    
    if image1.size != image2.size:
        image2 = image2.resize(image1.size, Image.LANCZOS)
    
    img1_pixels = list(image1.getdata())
    img2_pixels = list(image2.getdata())
    
    diff_img = Image.new('RGB', image1.size, (255, 255, 255))
    diff_pixels = diff_img.load()
    
    for i, (p1, p2) in enumerate(zip(img1_pixels, img2_pixels)):
        if len(p1) == 4:
            p1 = p1[:3]
        if len(p2) == 4:
            p2 = p2[:3]
        
        diff = sum(abs(c1 - c2) for c1, c2 in zip(p1, p2))
        
        if diff > threshold:
            diff_pixels[i % image1.size[0], i // image1.size[0]] = (255, 0, 0)
    
    return diff_img


def annotate_matches(
    image: 'Image',
    matches: List[dict],
    match_color: Color = (0, 255, 0),
    rect_width: int = 2,
) -> 'Image':
    """Annotate image with detected matches.
    
    Args:
        image: PIL Image object
        matches: List of match dicts with 'rect' key
        match_color: RGB color for matches
        rect_width: Rectangle border width
    
    Returns:
        Annotated image
    """
    result = image.copy()
    
    for match in matches:
        rect = match.get('rect')
        if rect:
            result = draw_rectangle_outline(result, rect, match_color, rect_width)
    
    return result


def create_grid_overlay(
    image: 'Image',
    rows: int = 4,
    cols: int = 4,
    color: Color = (128, 128, 128),
    alpha: float = 0.5,
) -> 'Image':
    """Create grid overlay on image.
    
    Args:
        image: PIL Image object
        rows: Number of horizontal divisions
        cols: Number of vertical divisions
        color: RGB color tuple
        alpha: Opacity (0.0 to 1.0)
    
    Returns:
        Image with grid overlay
    """
    try:
        from PIL import ImageDraw
    except ImportError:
        return image
    
    overlay = Image.new('RGBA', image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay, 'RGBA')
    
    w, h = image.size
    cell_w = w / cols
    cell_h = h / rows
    
    line_color = color + (int(255 * alpha),)
    
    for i in range(1, cols):
        x = int(i * cell_w)
        draw.line([(x, 0), (x, h)], fill=line_color)
    
    for i in range(1, rows):
        y = int(i * cell_h)
        draw.line([(0, y), (w, y)], fill=line_color)
    
    return Image.alpha_composite(image.convert('RGBA'), overlay).convert('RGB')


def draw_heatmap(
    image: 'Image',
    intensity_map: List[List[float]],
    colormap: str = 'jet',
) -> 'Image':
    """Draw heatmap overlay on image.
    
    Args:
        image: PIL Image object
        intensity_map: 2D array of intensity values (0.0 to 1.0)
        colormap: Matplotlib colormap name
    
    Returns:
        Image with heatmap overlay
    """
    try:
        import numpy as np
        import matplotlib.cm as cm
        from PIL import Image as PILImage
    except ImportError:
        return image
    
    arr = np.array(intensity_map)
    if arr.dtype != np.float64:
        arr = arr.astype(np.float64)
    
    normalized = (arr - arr.min()) / (arr.max() - arr.min() + 1e-8)
    
    cmap = cm.get_cmap(colormap)
    colored = cmap(normalized)[:, :, :4]
    
    heatmap = PILImage.fromarray((colored * 255).astype(np.uint8), 'RGBA')
    heatmap = heatmap.resize(image.size, PILImage.LANCZOS)
    
    return Image.alpha_composite(image.convert('RGBA'), heatmap).convert('RGB')


def draw_similarity_indicator(
    image: 'Image',
    similarity: float,
    position: Point = (10, 10),
    size: Tuple[int, int] = (100, 10),
) -> 'Image':
    """Draw similarity score indicator bar.
    
    Args:
        image: PIL Image object
        similarity: Similarity score (0.0 to 1.0)
        position: (x, y) top-left position
        size: (width, height) of indicator
    
    Returns:
        Modified image
    """
    try:
        from PIL import ImageDraw, ImageFont
    except ImportError:
        return image
    
    result = image.copy().convert('RGB')
    draw = ImageDraw.Draw(result)
    
    x, y = position
    w, h = size
    
    draw.rectangle([x, y, x + w, y + h], fill=(50, 50, 50))
    
    fill_w = int(w * max(0.0, min(1.0, similarity)))
    
    if similarity > 0.7:
        fill_color = (0, 255, 0)
    elif similarity > 0.4:
        fill_color = (255, 255, 0)
    else:
        fill_color = (255, 0, 0)
    
    draw.rectangle([x, y, x + fill_w, y + h], fill=fill_color)
    
    return result


def crop_to_region(
    image: 'Image',
    rect: Rect,
    padding: int = 10,
) -> 'Image':
    """Crop image to region with optional padding.
    
    Args:
        image: PIL Image object
        rect: Rectangle (x, y, width, height)
        padding: Padding to add around region
    
    Returns:
        Cropped image
    """
    x, y, w, h = rect
    img_w, img_h = image.size
    
    x1 = max(0, int(x) - padding)
    y1 = max(0, int(y) - padding)
    x2 = min(img_w, int(x + w) + padding)
    y2 = min(img_h, int(y + h) + padding)
    
    return image.crop((x1, y1, x2, y2))
