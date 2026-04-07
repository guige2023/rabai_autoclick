"""
QR code generation and parsing utilities.
"""

from typing import Optional, Union
import io
import base64

try:
    import qrcode
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False


def generate_qr_image_bytes(
    data: str,
    version: Optional[int] = None,
    error_correction: str = "M",
    box_size: int = 10,
    border: int = 4,
    fill_color: str = "black",
    back_color: str = "white",
    image_format: str = "PNG"
) -> bytes:
    """
    Generate QR code as raw image bytes.

    Args:
        data: Data to encode
        version: QR version (None for auto)
        error_correction: Error correction level ("L", "M", "Q", "H")
        box_size: Box size in pixels
        border: Border width
        fill_color: QR code color
        back_color: Background color
        image_format: Image format (PNG, JPEG, WEBP)
    """
    if not QR_AVAILABLE:
        raise ImportError("qrcode library required: pip install qrcode pillow")

    error_levels = {
        "L": qrcode.constants.ERROR_CORRECT_L,
        "M": qrcode.constants.ERROR_CORRECT_M,
        "Q": qrcode.constants.ERROR_CORRECT_Q,
        "H": qrcode.constants.ERROR_CORRECT_H,
    }

    qr = qrcode.QRCode(
        version=version,
        error_correction=error_levels.get(error_correction.upper(), qrcode.constants.ERROR_CORRECT_M),
        box_size=box_size,
        border=border,
    )
    qr.add_data(data)
    qr.make(fit=True)

    img = qr.make_image(fill_color=fill_color, back_color=back_color)
    buf = io.BytesIO()
    img.save(buf, format=image_format.upper())
    return buf.getvalue()
