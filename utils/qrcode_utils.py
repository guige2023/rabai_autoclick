"""
QR code generation and parsing utilities.

Provides:
- QR code generation with configurable error correction
- QR code parsing/decoding from images
- Styled QR codes (colors, logos, rounded corners)
- Batch QR code generation
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

try:
    import qrcode
    from PIL import Image

    QRCODE_AVAILABLE = True
except ImportError:
    QRCODE_AVAILABLE = False


class ErrorCorrectionLevel(Enum):
    """QR code error correction levels."""

    L = 1  # ~7% recovery
    M = 0  # ~15% recovery
    Q = 3  # ~25% recovery
    H = 2  # ~30% recovery


@dataclass
class QRCodeConfig:
    """Configuration for QR code generation."""

    version: int = 1  # 1-40, auto if 0
    error_correction: ErrorCorrectionLevel = ErrorCorrectionLevel.M
    box_size: int = 10
    border: int = 4
    fill_color: str = "black"
    back_color: str = "white"
    fit: bool = True


@dataclass
class ParsedQRResult:
    """Result of QR code parsing."""

    data: str
    confidence: float = 1.0
    bbox: Optional[tuple[int, int, int, int]] = None  # (x, y, width, height)


def generate_qr_code(
    data: str,
    config: Optional[QRCodeConfig] = None,
) -> "Image.Image":
    """
    Generate a QR code image.

    Args:
        data: String data to encode
        config: Optional configuration

    Returns:
        PIL Image of the QR code

    Raises:
        ImportError: If qrcode or PIL is not installed
    """
    if not QRCODE_AVAILABLE:
        raise ImportError("qrcode and PIL are required. Install with: pip install qrcode pillow")

    if config is None:
        config = QRCodeConfig()

    ec_level_map = {
        ErrorCorrectionLevel.L: qrcode.constants.ERROR_CORRECT_L,
        ErrorCorrectionLevel.M: qrcode.constants.ERROR_CORRECT_M,
        ErrorCorrectionLevel.Q: qrcode.constants.ERROR_CORRECT_Q,
        ErrorCorrectionLevel.H: qrcode.constants.ERROR_CORRECT_H,
    }

    qr = qrcode.QRCode(
        version=config.version if config.version > 0 else None,
        error_correction=ec_level_map[config.error_correction],
        box_size=config.box_size,
        border=config.border,
        image_factory=None,
    )
    qr.add_data(data)
    if config.fit:
        qr.make(fit=True)
    else:
        qr.make()

    img = qr.make_image(fill_color=config.fill_color, back_color=config.back_color)
    return img


def generate_qr_bytes(
    data: str,
    config: Optional[QRCodeConfig] = None,
    format: str = "PNG",
) -> bytes:
    """
    Generate QR code and return as bytes.

    Args:
        data: String data to encode
        config: Optional configuration
        format: Output format (PNG, JPEG, etc.)

    Returns:
        QR code image as bytes
    """
    img = generate_qr_code(data, config)
    buf = io.BytesIO()
    img.save(buf, format=format.upper())
    return buf.getvalue()


def generateStyledQRCode(
    data: str,
    logo_path: Optional[str] = None,
    corner_radius: int = 0,
    gradient_start: Optional[str] = None,
    gradient_end: Optional[str] = None,
    config: Optional[QRCodeConfig] = None,
) -> "Image.Image":
    """
    Generate a styled QR code with optional logo and gradient.

    Args:
        data: String data to encode
        logo_path: Optional path to logo image
        corner_radius: Rounded corner radius (0 for none)
        gradient_start: Optional start color for gradient
        gradient_end: Optional end color for gradient
        config: Optional QR code configuration

    Returns:
        Styled PIL Image
    """
    if not QRCODE_AVAILABLE:
        raise ImportError("qrcode and PIL are required")

    if config is None:
        config = QRCodeConfig()

    img = generate_qr_code(data, config)

    if corner_radius > 0:
        img = _apply_rounded_corners(img, corner_radius)

    if gradient_start and gradient_end:
        img = _apply_gradient(img, gradient_start, gradient_end)
        config = QRCodeConfig(fill_color="white", back_color="white")

    if logo_path:
        img = _add_logo_to_qr(img, logo_path)

    return img


def _apply_rounded_corners(img: "Image.Image", radius: int) -> "Image.Image":
    """Apply rounded corners to an image."""
    circular = Image.new("RGBA", img.size, (255, 255, 255, 0))
    from PIL import ImageDraw

    mask = Image.new("L", img.size, 255)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle([(0, 0), img.size], radius=radius, fill=0)
    circular.paste(img, (0, 0))
    circular.putalpha(mask)
    return circular


def _apply_gradient(img: "Image.Image", start_color: str, end_color: str) -> "Image.Image":
    """Apply a vertical gradient overlay to an image."""
    from PIL import ImageDraw

    base = img.convert("RGBA")
    gradient = Image.new("RGBA", base.size)
    draw = ImageDraw.Draw(gradient)

    r1, g1, b1 = img.getpixel((0, 0))[:3]
    r2, g2, b2 = img.getpixel((0, base.height - 1))[:3]

    for y in range(base.height):
        ratio = y / base.height
        r = int(r1 + (r2 - r1) * ratio)
        g = int(g1 + (g2 - g1) * ratio)
        b = int(b1 + (b2 - b1) * ratio)
        alpha = 50
        draw.line([(0, y), (base.width, y)], fill=(r, g, b, alpha))

    return Image.alpha_composite(base, gradient)


def _add_logo_to_qr(img: "Image.Image", logo_path: str, logo_size_ratio: float = 0.2) -> "Image.Image":
    """Add a centered logo to a QR code."""
    from PIL import ImageOps

    logo = Image.open(logo_path).convert("RGBA")
    max_logo_size = int(min(img.size) * logo_size_ratio)
    logo.thumbnail((max_logo_size, max_logo_size), Image.LANCZOS)

    pad = max_logo_size // 10
    mask = Image.new("L", img.size, 255)
    center_x, center_y = img.width // 2, img.height // 2
    from PIL import ImageDraw

    draw = ImageDraw.Draw(mask)
    draw.ellipse(
        [
            (center_x - max_logo_size // 2 - pad, center_y - max_logo_size // 2 - pad),
            (center_x + max_logo_size // 2 + pad, center_y + max_logo_size // 2 + pad),
        ],
        fill=0,
    )

   qr_with_space = img.convert("RGBA")
    qr_with_space.paste(logo, (center_x - logo.width // 2, center_y - logo.height // 2), logo)
    return qr_with_space


def parse_qr_code(image_source: "Image.Image | str | bytes") -> list[ParsedQRResult]:
    """
    Parse QR codes from an image.

    Args:
        image_source: PIL Image, file path, or image bytes

    Returns:
        List of ParsedQRResult objects

    Raises:
        ImportError: If required libraries not installed
    """
    if not QRCODE_AVAILABLE:
        raise ImportError("qrcode and PIL are required")

    try:
        from pyzbar.pyzbar import decode as zbar_decode
    except ImportError:
        raise ImportError("pyzbar is required for parsing. Install with: pip install pyzbar")

    if isinstance(image_source, (str, bytes)):
        img = Image.open(io.BytesIO(image_source) if isinstance(image_source, bytes) else image_source)
    else:
        img = image_source

    results = []
    decoded = zbar_decode(img)

    for item in decoded:
        results.append(
            ParsedQRResult(
                data=item.data.decode("utf-8"),
                confidence=1.0,
                bbox=(item.rect.left, item.rect.top, item.rect.width, item.rect.height),
            )
        )

    return results


def batch_generate_qr(
    data_list: list[str],
    config: Optional[QRCodeConfig] = None,
) -> dict[str, "Image.Image"]:
    """
    Generate QR codes for multiple data strings.

    Args:
        data_list: List of strings to encode
        config: Optional shared configuration

    Returns:
        Dictionary mapping data string to QR code Image
    """
    return {data: generate_qr_code(data, config) for data in data_list}


def create_qr_sticker(
    data: str,
    emoji: Optional[str] = None,
    label: Optional[str] = None,
    config: Optional[QRCodeConfig] = None,
    sticker_size: int = 400,
) -> "Image.Image":
    """
    Create a QR code sticker with emoji and/or label.

    Args:
        data: String data to encode
        emoji: Optional emoji to overlay
        label: Optional text label below QR code
        config: Optional QR code configuration
        sticker_size: Output size in pixels

    Returns:
        PIL Image of the QR code sticker
    """
    if not QRCODE_AVAILABLE:
        raise ImportError("qrcode and PIL are required")

    from PIL import ImageDraw

    img = generate_qr_code(data, config)

    if emoji or label:
        canvas_size = sticker_size
        canvas = Image.new("RGB", (canvas_size, canvas_size + (60 if label else 0)), "white")

        qr_size = canvas_size - 40
        img = img.resize((qr_size, qr_size), Image.LANCZOS)
        canvas.paste(img, (20, 20))

        if emoji:
            from PIL import ImageFont

            emoji_size = int(qr_size * 0.3)
            emoji_img = Image.new("RGBA", (emoji_size, emoji_size), (255, 255, 255, 0))
            emoji_draw = ImageDraw.Draw(emoji_img)
            try:
                font = ImageFont.truetype("/System/Library/Fonts/NotoColorEmoji.ttf", emoji_size)
            except OSError:
                font = ImageFont.load_default()
            emoji_draw.text((0, 0), emoji, font=font, embedded_color=True)
            emoji_img = emoji_img.resize((emoji_size, emoji_size), Image.LANCZOS)
            canvas.paste(emoji_img, (canvas_size // 2 - emoji_size // 2, canvas_size // 2 - emoji_size // 2), emoji_img)

        if label:
            draw = ImageDraw.Draw(canvas)
            try:
                font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 24)
            except OSError:
                font = ImageFont.load_default()
            bbox = draw.textbbox((0, 0), label, font=font)
            text_width = bbox[2] - bbox[0]
            draw.text(((canvas_size - text_width) // 2, canvas_size + 15), label, fill="black", font=font)

        return canvas

    img = img.resize((sticker_size, sticker_size), Image.LANCZOS)
    return img
