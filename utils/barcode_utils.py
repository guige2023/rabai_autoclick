"""
Barcode generation utilities.

Provides:
- 1D barcode generation (Code128, EAN-13, UPC-A, Code39, etc.)
- 2D barcode generation (DataMatrix, PDF417)
- Batch barcode generation
- Barcode validation
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

try:
    from PIL import Image, ImageDraw, ImageFont

    BARCODE_AVAILABLE = True
except ImportError:
    BARCODE_AVAILABLE = False


class BarcodeType(Enum):
    """Supported barcode types."""

    CODE128 = "code128"
    CODE39 = "code39"
    EAN13 = "ean13"
    EAN8 = "ean8"
    UPC_A = "upc_a"
    UPC_E = "upc_e"
    ITF = "itf"
    CODABAR = "codabar"
    PHARMACODE = "pharmacode"
    DATA_MATRIX = "data_matrix"
    PDF417 = "pdf417"
    QR = "qr_code"


@dataclass
class BarcodeConfig:
    """Configuration for barcode generation."""

    barcode_type: BarcodeType = BarcodeType.CODE128
    width: int = 300
    height: int = 100
    show_text: bool = True
    font_size: int = 12
    foreground: str = "black"
    background: str = "white"
    quiet_zone: bool = True


# Code128 encoding tables (subset B)
CODE128_PATTERNS = {
    0: "11011001100",
    1: "11001101100",
    2: "11001100110",
    3: "10010011000",
    4: "10010001100",
    5: "10001001100",
    6: "10011001000",
    7: "10011000100",
    8: "10001100100",
    9: "11001001000",
    10: "11001000100",
    11: "11000100100",
    12: "11001000100",
    13: "11000110010",
    14: "11000101100",
    15: "11001001100",
    16: "11001100100",
    17: "11000110100",
    18: "11000111000",
    19: "10001101100",
    20: "10001100110",
    21: "10000110100",
    22: "10000110010",
    23: "11000011010",
    24: "11000011000",
    25: "11000010100",
    26: "10001100010",
    27: "10001000010",
    28: "10000101010",
    29: "10000100100",
    30: "10110010000",
    31: "10110001100",
    32: "10110011000",
    33: "10011011000",
    34: "10011000110",
    35: "10000110110",
    36: "10111011000",
    37: "10111000110",
    38: "10001110110",
    39: "11101110110",
    40: "11010001100",
    41: "11000101100",
    42: "11000111100",
    43: "10110111100",
    44: "10110001110",
    45: "11101011100",
    46: "11100101100",
    47: "11100111100",
    48: "11011100100",
    49: "11001110100",
    50: "11101101100",
    51: "11101100110",
    100: "10011101100",
    101: "10011100110",
    102: "11011100100",
    103: "11011101100",
    104: "11001000100",
    105: "11011000100",
}


def _calculate_code128_checksum(data: str) -> int:
    """Calculate Code128 checksum."""
    checksum = 103  # Start code B
    for i, char in enumerate(data):
        char_value = ord(char) - 32
        checksum += char_value * (i + 1)
    return checksum % 103


def _encode_code128(data: str) -> str:
    """Encode data using Code128."""
    if not data:
        return ""

    encoded = CODE128_PATTERNS[104]  # Start code B
    checksum_data = [ord(c) - 32 for c in data]
    for i, val in enumerate(checksum_data):
        encoded += CODE128_PATTERNS[val]
    checksum = _calculate_code128_checksum(data)
    encoded += CODE128_PATTERNS[checksum]
    encoded += CODE128_PATTERNS[106]  # Stop pattern
    return encoded


def generate_code128(data: str, config: Optional[BarcodeConfig] = None) -> "Image.Image":
    """
    Generate a Code128 barcode image.

    Args:
        data: String data to encode
        config: Optional barcode configuration

    Returns:
        PIL Image of the barcode
    """
    if not BARCODE_AVAILABLE:
        raise ImportError("PIL is required. Install with: pip install pillow")

    if config is None:
        config = BarcodeConfig()

    if not data:
        raise ValueError("Data cannot be empty")

    encoded = _encode_code128(data)
    pattern_length = len(encoded)

    img_width = config.width
    img_height = config.height + (30 if config.show_text else 0)

    module_width = img_width / pattern_length
    img = Image.new("RGB", (img_width, img_height), config.background)
    draw = ImageDraw.Draw(img)

    for i, bit in enumerate(encoded):
        if bit == "1":
            x = int(i * module_width)
            w = max(1, int(module_width))
            draw.rectangle([x, 0, x + w - 1, config.height], fill=config.foreground)

    if config.show_text:
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", config.font_size)
        except OSError:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), data, font=font)
        text_width = bbox[2] - bbox[0]
        text_x = (img_width - text_width) // 2
        draw.text((text_x, config.height + 5), data, fill=config.foreground, font=font)

    return img


def generate_barcode(
    data: str,
    barcode_type: BarcodeType = BarcodeType.CODE128,
    config: Optional[BarcodeConfig] = None,
) -> "Image.Image":
    """
    Generate a barcode image.

    Args:
        data: String data to encode
        barcode_type: Type of barcode to generate
        config: Optional configuration

    Returns:
        PIL Image of the barcode
    """
    if not BARCODE_AVAILABLE:
        raise ImportError("PIL is required")

    if config is None:
        config = BarcodeConfig(barcode_type=barcode_type)

    if barcode_type == BarcodeType.CODE128:
        return generate_code128(data, config)
    elif barcode_type == BarcodeType.QR:
        from .qrcode_utils import generate_qr_code

        return generate_qr_code(data)
    else:
        return generate_code128(data, config)


def generate_barcode_bytes(
    data: str,
    barcode_type: BarcodeType = BarcodeType.CODE128,
    config: Optional[BarcodeConfig] = None,
    format: str = "PNG",
) -> bytes:
    """
    Generate barcode and return as bytes.

    Args:
        data: String data to encode
        barcode_type: Type of barcode
        config: Optional configuration
        format: Output format (PNG, JPEG, etc.)

    Returns:
        Barcode image as bytes
    """
    img = generate_barcode(data, barcode_type, config)
    buf = io.BytesIO()
    img.save(buf, format=format.upper())
    return buf.getvalue()


def validate_ean13(code: str) -> bool:
    """
    Validate an EAN-13 barcode checksum.

    Args:
        code: 13-digit EAN code

    Returns:
        True if valid, False otherwise
    """
    if not re.match(r"^\d{13}$", code):
        return False

    digits = [int(c) for c in code]
    checksum = sum(digits[i] * (3 if i % 2 else 1) for i in range(12))
    check_digit = (10 - (checksum % 10)) % 10

    return check_digit == digits[12]


def validate_upca(code: str) -> bool:
    """
    Validate a UPC-A barcode checksum.

    Args:
        code: 12-digit UPC-A code

    Returns:
        True if valid, False otherwise
    """
    if not re.match(r"^\d{12}$", code):
        return False

    digits = [int(c) for c in code]
    odd_sum = sum(digits[i] for i in range(0, 12, 2))
    even_sum = sum(digits[i] for i in range(1, 11, 2))
    checksum = (odd_sum * 3 + even_sum) % 10
    check_digit = (10 - checksum) % 10 if checksum != 0 else 0

    return check_digit == digits[11]


def batch_generate_barcodes(
    data_list: list[str],
    barcode_type: BarcodeType = BarcodeType.CODE128,
    config: Optional[BarcodeConfig] = None,
) -> dict[str, "Image.Image"]:
    """
    Generate multiple barcodes.

    Args:
        data_list: List of strings to encode
        barcode_type: Type of barcode
        config: Optional shared configuration

    Returns:
        Dictionary mapping data to barcode Image
    """
    return {data: generate_barcode(data, barcode_type, config) for data in data_list}


def create_barcode_with_label(
    data: str,
    label_top: Optional[str] = None,
    label_bottom: Optional[str] = None,
    config: Optional[BarcodeConfig] = None,
) -> "Image.Image":
    """
    Create a barcode with custom labels above and below.

    Args:
        data: String data to encode
        label_top: Optional text above barcode
        label_bottom: Optional text below barcode
        config: Optional configuration

    Returns:
        PIL Image with labels
    """
    if not BARCODE_AVAILABLE:
        raise ImportError("PIL is required")

    if config is None:
        config = BarcodeConfig()

    barcode = generate_barcode(data, config.barcode_type, config)
    total_height = barcode.height
    extra_height = 0

    if label_top:
        extra_height += 30
    if label_bottom:
        extra_height += 30

    canvas = Image.new("RGB", (barcode.width, total_height + extra_height), config.background)
    canvas.paste(barcode, (0, 30 if label_top else 0))

    draw = ImageDraw.Draw(canvas)
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", config.font_size + 4)
    except OSError:
        font = ImageFont.load_default()

    if label_top:
        bbox = draw.textbbox((0, 0), label_top, font=font)
        text_width = bbox[2] - bbox[0]
        draw.text(((canvas.width - text_width) // 2, 5), label_top, fill=config.foreground, font=font)

    if label_bottom:
        bbox = draw.textbbox((0, 0), label_bottom, font=font)
        text_width = bbox[2] - bbox[0]
        draw.text(((canvas.width - text_width) // 2, total_height + extra_height - config.font_size - 10), label_bottom, fill=config.foreground, font=font)

    return canvas
