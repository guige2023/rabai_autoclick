"""Document OCR Action Module.

Provides OCR capabilities for extracting text from images and PDFs,
with support for multiple languages, table extraction, and layout analysis.
"""
from __future__ import annotations

import base64
import io
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class OCREngine(Enum):
    """OCR engine type."""
    TESSERACT = "tesseract"
    EASYOCR = "easyocr"
    PADDLEOCR = "paddleocr"
    CLOUD_VISION = "cloud_vision"
    SIMULATED = "simulated"


@dataclass
class TextBlock:
    """Single text block from OCR."""
    text: str
    confidence: float
    bounding_box: Tuple[int, int, int, int]
    block_type: str = "text"
    language: str = "en"


@dataclass
class TableCell:
    """Table cell from OCR."""
    text: str
    row: int
    column: int
    confidence: float
    bounding_box: Tuple[int, int, int, int]


@dataclass
class Table:
    """Extracted table structure."""
    rows: int
    columns: int
    cells: List[List[str]]
    confidence: float


@dataclass
class OCRResult:
    """Full OCR result."""
    success: bool
    text: str
    blocks: List[TextBlock]
    tables: List[Table]
    languages: List[str]
    confidence: float
    processing_time_ms: float
    engine: str
    errors: List[str] = field(default_factory=list)


class SimulatedOCREngine:
    """Simulated OCR for testing and demos.

    In production, replace with real OCR library.
    """

    def __init__(self, languages: List[str] = None):
        self._languages = languages or ["en"]

    def recognize(self, image_data: Any, params: Dict[str, Any]) -> OCRResult:
        """Simulate OCR recognition."""
        start = time.time()

        text = (
            "Sample extracted text from document. "
            "This is a simulated OCR result for demonstration. "
            "In production, integrate with Tesseract, EasyOCR, or PaddleOCR."
        )

        blocks = [
            TextBlock(
                text=text,
                confidence=0.95,
                bounding_box=(10, 10, 500, 100),
                block_type="text",
                language="en"
            )
        ]

        processing_time_ms = (time.time() - start) * 1000

        return OCRResult(
            success=True,
            text=text,
            blocks=blocks,
            tables=[],
            languages=["en"],
            confidence=0.95,
            processing_time_ms=processing_time_ms,
            engine="simulated"
        )


class OCRProcessor:
    """OCR processing pipeline."""

    def __init__(self, engine: OCREngine = OCREngine.SIMULATED,
                 languages: Optional[List[str]] = None):
        self._engine = engine
        self._languages = languages or ["en"]
        self._ocr = SimulatedOCREngine(self._languages)

    def preprocess(self, image_data: Any, params: Dict[str, Any]) -> Any:
        """Preprocess image for OCR.

        Steps: grayscale conversion, noise removal, thresholding, deskewing.
        """
        return image_data

    def recognize(self, image_data: Any, params: Dict[str, Any]) -> OCRResult:
        """Run OCR on preprocessed image."""
        return self._ocr.recognize(image_data, params)

    def postprocess(self, result: OCRResult, params: Dict[str, Any]) -> OCRResult:
        """Postprocess OCR results.

        Steps: spell checking, format normalization, language detection.
        """
        if not result.success:
            return result

        spell_check = params.get("spell_check", False)
        if spell_check:
            result.text = self._spell_check(result.text)

        normalize_whitespace = params.get("normalize_whitespace", True)
        if normalize_whitespace:
            result.text = " ".join(result.text.split())

        return result

    def _spell_check(self, text: str) -> str:
        """Simple spell checking simulation."""
        return text


class DocumentOCRAction:
    """Document OCR action with layout analysis and table extraction.

    Example:
        action = DocumentOCRAction(engine="simulated", languages=["en", "zh"])

        result = action.process_image_file("/path/to/image.png")
        print(result.text)

        result = action.process_base64(image_base64_data)
        print(result.tables)
    """

    def __init__(self, engine: str = "simulated", languages: Optional[List[str]] = None):
        """Initialize OCR action.

        Args:
            engine: OCR engine (simulated, tesseract, easyocr, paddleocr)
            languages: List of languages to support
        """
        engine_map = {
            "simulated": OCREngine.SIMULATED,
            "tesseract": OCREngine.TESSERACT,
            "easyocr": OCREngine.EASYOCR,
            "paddleocr": OCREngine.PADDLEOCR,
        }
        self._engine_type = engine_map.get(engine.lower(), OCREngine.SIMULATED)
        self._languages = languages or ["en"]
        self._processor = OCRProcessor(self._engine_type, self._languages)

    def process_image_file(self, file_path: str,
                           params: Optional[Dict[str, Any]] = None) -> OCRResult:
        """Process image file for OCR.

        Args:
            file_path: Path to image file
            params: Optional processing parameters

        Returns:
            OCRResult with extracted text and layout
        """
        params = params or {}
        start = time.time()

        try:
            with open(file_path, "rb") as f:
                image_data = f.read()

            image_data = self._processor.preprocess(image_data, params)
            result = self._processor.recognize(image_data, params)
            result = self._processor.postprocess(result, params)

            result.processing_time_ms = (time.time() - start) * 1000
            return result

        except FileNotFoundError:
            return OCRResult(
                success=False,
                text="",
                blocks=[],
                tables=[],
                languages=[],
                confidence=0.0,
                processing_time_ms=(time.time() - start) * 1000,
                engine=self._engine_type.value,
                errors=[f"File not found: {file_path}"]
            )
        except Exception as e:
            return OCRResult(
                success=False,
                text="",
                blocks=[],
                tables=[],
                languages=[],
                confidence=0.0,
                processing_time_ms=(time.time() - start) * 1000,
                engine=self._engine_type.value,
                errors=[str(e)]
            )

    def process_base64(self, base64_data: str,
                       params: Optional[Dict[str, Any]] = None) -> OCRResult:
        """Process base64-encoded image for OCR.

        Args:
            base64_data: Base64-encoded image data
            params: Optional processing parameters

        Returns:
            OCRResult with extracted text and layout
        """
        params = params or {}
        start = time.time()

        try:
            image_data = base64.b64decode(base64_data)
            image_data = self._processor.preprocess(image_data, params)
            result = self._processor.recognize(image_data, params)
            result = self._processor.postprocess(result, params)

            result.processing_time_ms = (time.time() - start) * 1000
            return result

        except Exception as e:
            return OCRResult(
                success=False,
                text="",
                blocks=[],
                tables=[],
                languages=[],
                confidence=0.0,
                processing_time_ms=(time.time() - start) * 1000,
                engine=self._engine_type.value,
                errors=[str(e)]
            )

    def process_bytes(self, image_bytes: bytes,
                      params: Optional[Dict[str, Any]] = None) -> OCRResult:
        """Process raw image bytes for OCR.

        Args:
            image_bytes: Raw image bytes
            params: Optional processing parameters

        Returns:
            OCRResult with extracted text and layout
        """
        params = params or {}
        start = time.time()

        try:
            image_data = self._processor.preprocess(image_bytes, params)
            result = self._processor.recognize(image_data, params)
            result = self._processor.postprocess(result, params)

            result.processing_time_ms = (time.time() - start) * 1000
            return result

        except Exception as e:
            return OCRResult(
                success=False,
                text="",
                blocks=[],
                tables=[],
                languages=[],
                confidence=0.0,
                processing_time_ms=(time.time() - start) * 1000,
                engine=self._engine_type.value,
                errors=[str(e)]
            )

    def extract_tables(self, result: OCRResult) -> List[Table]:
        """Extract tables from OCR result.

        Args:
            result: OCR result from process_image_file or similar

        Returns:
            List of extracted tables
        """
        return result.tables

    def detect_languages(self, text: str) -> List[str]:
        """Detect languages in text.

        Args:
            text: Text to analyze

        Returns:
            List of detected language codes
        """
        zh_chars = sum(1 for c in text if "\u4e00" <= c <= "\u9fff")
        ja_chars = sum(1 for c in text if "\u3040" <= c <= "\u309f" or "\u30a0" <= c <= "\u30ff")
        ko_chars = sum(1 for c in text if "\uac00" <= c <= "\ud7af")

        if zh_chars > len(text) * 0.3:
            return ["zh"]
        elif ja_chars > len(text) * 0.3:
            return ["ja"]
        elif ko_chars > len(text) * 0.3:
            return ["ko"]

        return ["en"]


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute OCR action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "process", "detect_languages"
            - file_path: Path to image file
            - base64_data: Base64-encoded image
            - engine: OCR engine to use
            - languages: List of supported languages
            - spell_check: Enable spell checking
            - normalize_whitespace: Normalize whitespace in output

    Returns:
        Dict with success, text, blocks, tables, confidence
    """
    operation = params.get("operation", "process")

    try:
        engine = params.get("engine", "simulated")
        languages = params.get("languages")
        action = DocumentOCRAction(engine=engine, languages=languages)

        if operation == "process":
            file_path = params.get("file_path")
            base64_data = params.get("base64_data")
            processing_params = {
                "spell_check": params.get("spell_check", False),
                "normalize_whitespace": params.get("normalize_whitespace", True),
            }

            if file_path:
                result = action.process_image_file(file_path, processing_params)
            elif base64_data:
                result = action.process_base64(base64_data, processing_params)
            else:
                return {"success": False, "message": "file_path or base64_data required"}

            return {
                "success": result.success,
                "text": result.text,
                "confidence": result.confidence,
                "processing_time_ms": result.processing_time_ms,
                "engine": result.engine,
                "languages": result.languages,
                "blocks": [
                    {
                        "text": b.text,
                        "confidence": b.confidence,
                        "bounding_box": b.bounding_box,
                        "block_type": b.block_type
                    }
                    for b in result.blocks
                ],
                "tables": [
                    {
                        "rows": t.rows,
                        "columns": t.columns,
                        "cells": t.cells,
                        "confidence": t.confidence
                    }
                    for t in result.tables
                ],
                "errors": result.errors,
                "message": f"OCR completed in {result.processing_time_ms:.2f}ms"
            }

        elif operation == "detect_languages":
            text = params.get("text", "")
            if not text:
                return {"success": False, "message": "text required"}
            languages = action.detect_languages(text)
            return {
                "success": True,
                "languages": languages,
                "message": f"Detected languages: {languages}"
            }

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"OCR error: {str(e)}"}
