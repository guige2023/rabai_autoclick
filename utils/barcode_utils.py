"""
Barcode generation utilities supporting multiple formats.
"""

from typing import Optional
import io
import base64

try:
    from barcode import get_barcode_class
    from barcode.writer import ImageWriter, SVGWriter
    BARCODE_AVAILABLE = True
except ImportError:
    BARCODE_AVAILABLE = False


SUPPORTED_FORMATS = ["ean13", "ean8", "upc", "code39", "code128", "pzn", "jan"]


def generate_barcode_image(
    data: str,
    format: str = "code128",
    output: str = "PNG",
    writer_options: Optional[dict] = None
) -> bytes:
    """
    Generate barcode as image bytes.

    Args:
        data: Data to encode
        format: Barcode format (code128, ean13, code39, etc.)
        output: Output format (PNG, SVG)
        writer_options: Options passed to the writer
    """
    if not BARCODE_AVAILABLE:
        raise ImportError("python-barcode required: pip install python-barcode")

    if writer_options is None:
        writer_options = {}

    writer = SVGWriter() if output.upper() == "SVG" else ImageWriter()
    barcode_class = get_barcode_class(format.lower())
    rv = barcode_class(data, writer=writer)
    buf = io.BytesIO()
    rv.write(buf, writer_options)
    return buf.getvalue()


def generate_barcode_base64(
    data: str,
    format: str = "code128",
    output: str = "PNG",
    writer_options: Optional[dict] = None
) -> str:
    """Generate barcode and return as base64 data URL."""
    img_bytes = generate_barcode_image(data, format, output, writer_options)
    mime = "image/svg+xml" if output.upper() == "SVG" else "image/png"
    b64 = base64.b64encode(img_bytes).decode()
    return f"data:{mime};base64,{b64}"


def generate_code128(data: str, save_path: Optional[str] = None) -> Optional[bytes]:
    """Generate Code 128 barcode."""
    return generate_barcode_image(data, "code128", "PNG")


def generate_ean13(data: str) -> bytes:
    """Generate EAN-13 barcode (12 digits, check digit auto-computed)."""
    data = data.strip()[:12]
    return generate_barcode_image(data, "ean13", "PNG")
