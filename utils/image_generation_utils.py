"""Image generation utilities for creating visual assets programmatically.

Provides helpers for creating button images, icon placeholders,
annotated screenshots, and other programmatically generated
visuals useful for UI automation demonstrations.

Example:
    >>> from utils.image_generation_utils import create_button, add_annotations
    >>> img = create_button('Click Me', size=(120, 40), color=(0, 120, 255))
    >>> save_image(img, '/tmp/button.png')
"""

from __future__ import annotations

from typing import Optional, Tuple

__all__ = [
    "create_button",
    "create_placeholder_image",
    "add_annotations",
    "create_color_swatch",
    "overlay_icon",
    "resize_image",
    "ImageGenerationError",
]


class ImageGenerationError(Exception):
    """Raised when image generation fails."""
    pass


def _get_pil():
    try:
        from PIL import Image, ImageDraw, ImageFont
        return Image, ImageDraw, ImageFont
    except ImportError:
        raise ImageGenerationError("Pillow not installed: pip install Pillow")


def create_button(
    text: str,
    size: Tuple[int, int] = (120, 40),
    color: Tuple[int, int, int] = (70, 130, 255),
    text_color: Tuple[int, int, int] = (255, 255, 255),
    corner_radius: int = 8,
    font_size: int = 14,
) -> "Image.Image":
    """Create a styled button image with text.

    Args:
        text: Button label text.
        size: (width, height) in pixels.
        color: RGB button background color.
        text_color: RGB text color.
        corner_radius: Corner radius in pixels.
        font_size: Font size in points.

    Returns:
        PIL Image object.
    """
    Image, ImageDraw, ImageFont = _get_pil()

    w, h = size
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Draw rounded rectangle
    draw.rounded_rectangle(
        [(0, 0), (w - 1, h - 1)],
        radius=corner_radius,
        fill=color + (255,),
        outline=color,
    )

    # Try to use a nice font, fall back to default
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
    except Exception:
        font = ImageFont.load_default()

    # Center text
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    x = (w - text_w) // 2
    y = (h - text_h) // 2 - 2

    draw.text((x, y), text, fill=text_color + (255,), font=font)
    return img


def create_placeholder_image(
    size: Tuple[int, int] = (200, 150),
    text: str = "Placeholder",
    bg_color: Tuple[int, int, int] = (100, 100, 100),
    text_color: Tuple[int, int, int] = (200, 200, 200),
) -> "Image.Image":
    """Create a placeholder image with text.

    Args:
        size: (width, height) in pixels.
        text: Placeholder text.
        bg_color: RGB background color.
        text_color: RGB text color.

    Returns:
        PIL Image object.
    """
    Image, ImageDraw, ImageFont = _get_pil()

    w, h = size
    img = Image.new("RGB", (w, h), bg_color)
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 16)
    except Exception:
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    x = (w - text_w) // 2
    y = (h - text_h) // 2

    draw.text((x, y), text, fill=text_color, font=font)
    return img


def add_annotations(
    image: "Image.Image",
    annotations: list[dict],
) -> "Image.Image":
    """Add visual annotations to an image.

    Args:
        image: PIL Image to annotate.
        annotations: List of annotation dicts with 'type',
            'position', and type-specific fields.

    Returns:
        Annotated PIL Image.
    """
    Image, ImageDraw, ImageFont = _get_pil()

    img = image.copy()
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
    except Exception:
        font = ImageFont.load_default()

    for ann in annotations:
        ann_type = ann.get("type")

        if ann_type == "arrow":
            x1 = ann["x1"]
            y1 = ann["y1"]
            x2 = ann["x2"]
            y2 = ann["y2"]
            color = ann.get("color", (255, 0, 0))
            draw.line([(x1, y1), (x2, y2)], fill=color, width=2)

        elif ann_type == "box":
            x = ann["x"]
            y = ann["y"]
            w = ann["width"]
            h = ann["height"]
            color = ann.get("color", (255, 0, 0))
            draw.rectangle([x, y, x + w, y + h], outline=color, width=2)

        elif ann_type == "text":
            x = ann["x"]
            y = ann["y"]
            text = ann["text"]
            color = ann.get("color", (255, 255, 255))
            bg_color = ann.get("bg_color", (0, 0, 0))
            bbox = draw.textbbox((x, y), text, font=font)
            draw.rectangle(bbox, fill=bg_color)
            draw.text((x, y), text, fill=color, font=font)

        elif ann_type == "circle":
            x = ann["x"]
            y = ann["y"]
            r = ann["radius"]
            color = ann.get("color", (255, 0, 0))
            draw.ellipse([x - r, y - r, x + r, y + r], outline=color, width=2)

    return img


def create_color_swatch(
    color: Tuple[int, int, int],
    size: Tuple[int, int] = (100, 100),
    label: Optional[str] = None,
) -> "Image.Image":
    """Create a color swatch image.

    Args:
        color: RGB color tuple.
        size: (width, height).
        label: Optional text label.

    Returns:
        PIL Image object.
    """
    Image, ImageDraw, ImageFont = _get_pil()

    w, h = size
    img = Image.new("RGB", (w, h), color)
    draw = ImageDraw.Draw(img)

    if label:
        hex_label = "#{:02x}{:02x}{:02x}".format(*color)
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
        except Exception:
            font = ImageFont.load_default()

        bbox = draw.textbbox((0, 0), hex_label, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        x = (w - text_w) // 2
        y = (h - text_h) // 2

        # White or black text depending on brightness
        brightness = 0.299 * color[0] + 0.587 * color[1] + 0.114 * color[2]
        text_color = (0, 0, 0) if brightness > 128 else (255, 255, 255)

        draw.text((x, y), hex_label, fill=text_color, font=font)

    return img


def overlay_icon(
    background: "Image.Image",
    icon: "Image.Image",
    position: str = "top_right",
    padding: int = 10,
    size: Optional[Tuple[int, int]] = None,
) -> "Image.Image":
    """Overlay an icon onto a background image.

    Args:
        background: Background PIL Image.
        icon: Icon PIL Image to overlay.
        position: 'top_left', 'top_right', 'bottom_left', 'bottom_right', 'center'.
        padding: Pixel padding from edges.
        size: Optional (width, height) to resize icon.

    Returns:
        Composite PIL Image.
    """
    img = background.copy()

    if size:
        icon = icon.resize(size)

    iw, ih = icon.size
    bw, bh = img.size

    if position == "top_left":
        x, y = padding, padding
    elif position == "top_right":
        x, y = bw - iw - padding, padding
    elif position == "bottom_left":
        x, y = padding, bh - ih - padding
    elif position == "bottom_right":
        x, y = bw - iw - padding, bh - ih - padding
    elif position == "center":
        x, y = (bw - iw) // 2, (bh - ih) // 2
    else:
        x, y = padding, padding

    img.paste(icon, (x, y), mask=icon if icon.mode == "RGBA" else None)
    return img


def resize_image(
    image: "Image.Image",
    size: Tuple[int, int],
    keep_aspect: bool = True,
) -> "Image.Image":
    """Resize an image with optional aspect ratio preservation.

    Args:
        image: PIL Image to resize.
        size: Target (width, height).
        keep_aspect: If True, fit within size maintaining aspect ratio.

    Returns:
        Resized PIL Image.
    """
    target_w, target_h = size
    if keep_aspect:
        img_w, img_h = image.size
        ratio = min(target_w / img_w, target_h / img_h)
        new_w = int(img_w * ratio)
        new_h = int(img_h * ratio)
        return image.resize((new_w, new_h), Image.LANCZOS)
    return image.resize(size, Image.LANCZOS)


def save_image(image: "Image.Image", path: str, format: str = "PNG") -> bool:
    """Save a PIL Image to a file.

    Args:
        image: PIL Image.
        path: Output file path.
        format: Image format ('PNG', 'JPEG', etc.).

    Returns:
        True if successful.
    """
    try:
        image.save(path, format=format)
        return True
    except Exception:
        return False
