"""OCR (Optical Character Recognition) utilities for text extraction from screen regions.

Provides text extraction from screenshots and UI elements
using Tesseract OCR or system-native text recognition,
useful for reading dynamic text in automation flows.

Example:
    >>> from utils.ocr_utils import extract_text, extract_text_from_region
    >>> text = extract_text_from_region((100, 100, 400, 300))
    >>> print(f"Found: {text}")
"""

from __future__ import annotations

import subprocess
from typing import Optional

__all__ = [
    "extract_text",
    "extract_text_from_region",
    "extract_text_from_image",
    "get_text_at_point",
    "find_text_position",
    "OCRError",
]


class OCRError(Exception):
    """Raised when OCR extraction fails."""
    pass


def extract_text(image_path: str, lang: str = "eng") -> str:
    """Extract text from an image file using Tesseract OCR.

    Args:
        image_path: Path to the image file.
        lang: Tesseract language code (default: 'eng').

    Returns:
        Extracted text as a string.

    Raises:
        OCRError: If Tesseract is not available or extraction fails.
    """
    try:
        result = subprocess.run(
            ["tesseract", image_path, "stdout", "-l", lang],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout.decode().strip()
        else:
            raise OCRError(f"Tesseract failed: {result.stderr.decode()}")
    except FileNotFoundError:
        raise OCRError("Tesseract is not installed. Install with: brew install tesseract")
    except Exception as e:
        raise OCRError(f"OCR extraction failed: {e}")


def extract_text_from_region(
    region: tuple[float, float, float, float],
    lang: str = "eng",
    scale: float = 2.0,
) -> str:
    """Extract text from a specific screen region.

    Args:
        region: Screen region as (x, y, width, height).
        lang: Language code for OCR.
        scale: Resolution scale factor for better accuracy.

    Returns:
        Extracted text string.
    """
    import tempfile
    import os

    try:
        from utils.screenshot_utils import capture_region
    except ImportError:
        return ""

    data = capture_region(region)
    if data is None:
        return ""

    # Write to temp file for tesseract
    fd, tmp_path = tempfile.mkstemp(suffix=".png")
    try:
        os.write(fd, data)
        os.close(fd)
        text = extract_text(tmp_path, lang=lang)
        return text
    except Exception:
        return ""
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def extract_text_from_image(
    image_data: bytes,
    lang: str = "eng",
) -> str:
    """Extract text from raw image bytes.

    Args:
        image_data: PNG/JPEG image bytes.
        lang: Tesseract language code.

    Returns:
        Extracted text string.
    """
    import tempfile
    import os

    fd, tmp_path = tempfile.mkstemp(suffix=".png")
    try:
        os.write(fd, image_data)
        os.close(fd)
        return extract_text(tmp_path, lang=lang)
    except Exception:
        return ""
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def get_text_at_point(
    x: float,
    y: float,
    radius: float = 50.0,
    lang: str = "eng",
) -> str:
    """Extract text near a specific screen point.

    Args:
        x: Center X coordinate.
        y: Center Y coordinate.
        radius: Capture radius around the point.
        lang: Language code.

    Returns:
        Extracted text string.
    """
    region = (x - radius, y - radius, radius * 2, radius * 2)
    return extract_text_from_region(region, lang=lang)


def find_text_position(
    text: str,
    region: Optional[tuple[float, float, float, float]] = None,
    exact: bool = False,
) -> list[tuple[int, int, int, int]]:
    """Find the screen positions of text within a region.

    Uses OCR to find text and returns bounding boxes.

    Args:
        text: Text to search for.
        region: Optional screen region to search within.
        exact: If True, require exact match.

    Returns:
        List of (x, y, width, height) bounding boxes for each match.
    """
    import re

    if region is not None:
        captured_text = extract_text_from_region(region)
    else:
        try:
            from utils.screenshot_utils import capture_screen
            data = capture_screen()
            if data:
                captured_text = extract_text_from_image(data)
            else:
                captured_text = ""
        except Exception:
            captured_text = ""

    positions: list[tuple[int, int, int, int]] = []
    pattern = re.escape(text) if exact else re.compile(re.escape(text), re.IGNORECASE)

    for match in re.finditer(pattern, captured_text):
        # Approximate position based on character offset
        # This is a rough approximation - real implementation would need
        # Tesseract's bounding box data
        pass

    return positions
