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


def generate_qr_base64(
    data: str,
    version: Optional[int] = None,
    error_correction: str = "M",
    box_size: int = 10,
    border: int = 4,
    fill_color: str = "black",
    back_color: str = "white",
    image_format: str = "PNG"
) -> str:
    """
    Generate QR code and return as base64 data URL.
    """
    img_bytes = generate_qr_image_bytes(
        data, version, error_correction, box_size, border,
        fill_color, back_color, image_format
    )
    mime_types = {"PNG": "image/png", "JPEG": "image/jpeg", "WEBP": "image/webp"}
    mime = mime_types.get(image_format.upper(), "image/png")
    b64 = base64.b64encode(img_bytes).decode()
    return f"data:{mime};base64,{b64}"


def generate_qr_svg(
    data: str,
    version: Optional[int] = None,
    error_correction: str = "M",
    box_size: int = 10,
    border: int = 4
) -> str:
    """Generate QR code as SVG string."""
    if not QR_AVAILABLE:
        raise ImportError("qrcode library required")

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

    img = qr.make_image(fill_color="black", back_color="white")
    pil_image = img.get_image()

    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" version="1.1" '
        f'viewBox="0 0 {pil_image.width} {pil_image.height}">',
        f'<rect width="100%" height="100%" fill="white"/>',
    ]

    for y, row in enumerate(img.matrix):
        for x, val in enumerate(row):
            if val:
                svg_parts.append(
                    f'<rect x="{x * box_size + border}" y="{y * box_size + border}" '
                    f'width="{box_size}" height="{box_size}"/>'
                )

    svg_parts.append("</svg>")
    return "\n".join(svg_parts)


def get_qr_matrix(data: str):
    """Get QR code as a 2D boolean matrix."""
    if not QR_AVAILABLE:
        raise ImportError("qrcode library required")
    qr = qrcode.QRCode()
    qr.add_data(data)
    qr.make(fit=True)
    return qr.make_image().tolist()
