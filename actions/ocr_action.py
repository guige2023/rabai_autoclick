"""OCR (Optical Character Recognition) action.

This module provides OCR capabilities for extracting text
from images and screenshots using Tesseract.

Example:
    >>> action = OCRAction()
    >>> result = action.execute(image_path="/tmp/screenshot.png")
"""

from __future__ import annotations

import base64
import io
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class OCRResult:
    """OCR extraction result."""
    text: str
    confidence: float
    boxes: list[dict[str, Any]]


class OCRAction:
    """OCR (Optical Character Recognition) action.

    Extracts text from images using Tesseract OCR
    with support for multiple languages and preprocessing.

    Example:
        >>> action = OCRAction()
        >>> result = action.execute(
        ...     image_path="/tmp/screenshot.png",
        ...     lang="eng"
        ... )
    """

    def __init__(self) -> None:
        """Initialize OCR action."""
        self._last_result: Optional[OCRResult] = None

    def execute(
        self,
        image_path: Optional[str] = None,
        image_data: Optional[str] = None,
        lang: str = "eng",
        psm: int = 3,
        whitelist: Optional[str] = None,
        preprocess: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute OCR on image.

        Args:
            image_path: Path to image file.
            image_data: Base64 encoded image data.
            lang: Language code (eng, chi_sim, etc.).
            psm: Page segmentation mode (0-13).
            whitelist: Allowed characters.
            preprocess: Whether to preprocess image.
            **kwargs: Additional parameters.

        Returns:
            OCR result dictionary.

        Raises:
            ValueError: If image is not provided.
        """
        try:
            import pytesseract
            from PIL import Image
        except ImportError:
            return {
                "success": False,
                "error": "pytesseract or Pillow not installed. Run: pip install pytesseract pillow",
            }

        if not image_path and not image_data:
            raise ValueError("image_path or image_data required")

        result: dict[str, Any] = {"success": True, "lang": lang}

        try:
            # Load image
            if image_path:
                img = Image.open(image_path)
            else:
                img_data = base64.b64decode(image_data)
                img = Image.open(io.BytesIO(img_data))

            # Preprocess
            if preprocess:
                img = self._preprocess_image(img)

            # Configure Tesseract
            config = f"--psm {psm}"
            if whitelist:
                config += f" -c tessedit_char_whitelist={whitelist}"

            # Extract text
            text = pytesseract.image_to_string(img, lang=lang, config=config)
            result["text"] = text.strip()

            # Get confidence
            data = pytesseract.image_to_data(img, lang=lang, config=config, output_type=pytesseract.Output.DICT)
            confidences = [int(c) for c in data["conf"] if c != "-1"]
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0
            result["confidence"] = avg_confidence

            # Get bounding boxes
            boxes = self._get_boxes(data)
            result["boxes"] = boxes
            result["word_count"] = len(boxes)

            self._last_result = OCRResult(
                text=text.strip(),
                confidence=avg_confidence,
                boxes=boxes,
            )

        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _preprocess_image(self, img: Any) -> Any:
        """Preprocess image for better OCR.

        Args:
            img: PIL Image.

        Returns:
            Preprocessed image.
        """
        from PIL import ImageFilter, ImageEnhance

        # Convert to grayscale
        img = img.convert("L")

        # Increase contrast
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.5)

        # Sharpen
        img = img.filter(ImageFilter.SHARPEN)

        # Resize if too small
        if img.width < 300:
            scale = 300 / img.width
            new_size = (int(img.width * scale), int(img.height * scale))
            img = img.resize(new_size, Image.LANCZOS)

        return img

    def _get_boxes(self, data: dict) -> list[dict[str, Any]]:
        """Extract bounding boxes from Tesseract output.

        Args:
            data: Tesseract output data dict.

        Returns:
            List of word bounding boxes.
        """
        boxes = []
        n_boxes = len(data["text"])

        for i in range(n_boxes):
            text = data["text"][i].strip()
            if text:
                boxes.append({
                    "text": text,
                    "x": data["left"][i],
                    "y": data["top"][i],
                    "width": data["width"][i],
                    "height": data["height"][i],
                    "confidence": int(data["conf"][i]) if data["conf"][i] != "-1" else 0,
                })

        return boxes

    def extract_numbers(self) -> list[str]:
        """Extract only numbers from last OCR result.

        Returns:
            List of number strings.
        """
        if not self._last_result:
            return []
        import re
        return re.findall(r"\d+(?:\.\d+)?", self._last_result.text)

    def extract_emails(self) -> list[str]:
        """Extract emails from last OCR result.

        Returns:
            List of email addresses.
        """
        if not self._last_result:
            return []
        import re
        return re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", self._last_result.text)

    def extract_urls(self) -> list[str]:
        """Extract URLs from last OCR result.

        Returns:
            List of URLs.
        """
        if not self._last_result:
            return []
        import re
        return re.findall(r"https?://[^\s<>\"]+", self._last_result.text)

    def search_text(self, query: str) -> list[dict[str, Any]]:
        """Search for text in OCR result.

        Args:
            query: Search query.

        Returns:
            List of matching boxes.
        """
        if not self._last_result:
            return []

        query_lower = query.lower()
        matches = []

        for box in self._last_result.boxes:
            if query_lower in box["text"].lower():
                matches.append(box)

        return matches

    def get_confidence(self) -> float:
        """Get average confidence of last OCR result.

        Returns:
            Confidence score (0-100).
        """
        if self._last_result:
            return self._last_result.confidence
        return 0.0
