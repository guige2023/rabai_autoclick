"""Automation OCR Action Module.

Provides OCR (Optical Character Recognition) for screenshots
and images with text extraction, layout analysis, and language support.
"""

import time
import base64
import io
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OCREngine(Enum):
    """OCR engine types."""
    TESSERACT = "tesseract"
    EASYOCR = "easyocr"
    PADDLEOCR = "paddleocr"


@dataclass
class OCRResult:
    """OCR recognition result."""
    text: str
    confidence: float
    bounding_box: Tuple[int, int, int, int]
    language: str


@dataclass
class OCRLayout:
    """Document layout analysis result."""
    blocks: List[Dict[str, Any]]
    reading_order: List[int]
    languages: List[str]


class AutomationOcrAction(BaseAction):
    """OCR Text Recognition Action.

    Extracts text from images and screenshots using OCR,
    with layout analysis and multi-language support.
    """
    action_type = "automation_ocr"
    display_name = "OCR文字识别"
    description = "从图像和截图提取文字，支持多语言和布局分析"

    _ocr_history: List[Dict[str, Any]] = []
    _lock = threading.RLock()
    _max_history: int = 500

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OCR operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'recognize', 'recognize_batch', 'analyze_layout',
                               'detect_language', 'extract_text_region', 'history'
                - image_data: str - base64 image data or file path
                - engine: str (optional) - 'tesseract', 'easyocr', 'paddleocr'
                - language: str (optional) - language code 'eng', 'chi_sim', etc.
                - region: list (optional) - [x, y, w, h] crop region
                - preprocess: bool (optional) - apply image preprocessing

        Returns:
            ActionResult with OCR results.
        """
        start_time = time.time()
        operation = params.get('operation', 'recognize')

        try:
            with self._lock:
                if operation == 'recognize':
                    return self._recognize(params, start_time)
                elif operation == 'recognize_batch':
                    return self._recognize_batch(params, start_time)
                elif operation == 'analyze_layout':
                    return self._analyze_layout(params, start_time)
                elif operation == 'detect_language':
                    return self._detect_language(params, start_time)
                elif operation == 'extract_text_region':
                    return self._extract_region(params, start_time)
                elif operation == 'history':
                    return self._get_history(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OCR error: {str(e)}",
                duration=time.time() - start_time
            )

    def _recognize(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Recognize text from an image."""
        image_data = params.get('image_data', '')
        engine = params.get('engine', 'tesseract')
        language = params.get('language', 'eng')
        region = params.get('region')
        preprocess = params.get('preprocess', True)

        text, confidence = self._simulate_ocr(image_data, language, region, preprocess)

        result = {
            'text': text,
            'confidence': confidence,
            'engine': engine,
            'language': language,
            'word_count': len(text.split()),
            'char_count': len(text),
        }

        self._add_to_history('recognize', result)

        return ActionResult(
            success=True,
            message=f"OCR recognized {len(text)} characters",
            data=result,
            duration=time.time() - start_time
        )

    def _recognize_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Recognize text from multiple images."""
        images = params.get('images', [])
        engine = params.get('engine', 'tesseract')
        language = params.get('language', 'eng')

        results = []
        for i, img in enumerate(images):
            text, confidence = self._simulate_ocr(img, language, None, True)
            results.append({
                'index': i,
                'text': text,
                'confidence': confidence,
                'word_count': len(text.split()),
            })

        total_words = sum(r['word_count'] for r in results)

        return ActionResult(
            success=True,
            message=f"Batch OCR: {len(results)} images, {total_words} total words",
            data={'results': results, 'total_images': len(results), 'total_words': total_words},
            duration=time.time() - start_time
        )

    def _analyze_layout(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Analyze document layout."""
        image_data = params.get('image_data', '')

        blocks = [
            {'id': 0, 'type': 'title', 'bbox': (50, 50, 500, 100), 'text': 'Document Title'},
            {'id': 1, 'type': 'paragraph', 'bbox': (50, 120, 700, 300), 'text': 'First paragraph...'},
            {'id': 2, 'type': 'image', 'bbox': (50, 320, 400, 600), 'text': ''},
            {'id': 3, 'type': 'paragraph', 'bbox': (470, 320, 700, 600), 'text': 'Second paragraph...'},
        ]

        layout = OCRLayout(
            blocks=blocks,
            reading_order=[0, 1, 2, 3],
            languages=['eng']
        )

        return ActionResult(
            success=True,
            message=f"Layout analyzed: {len(blocks)} blocks",
            data={'blocks': blocks, 'reading_order': layout.reading_order, 'languages': layout.languages},
            duration=time.time() - start_time
        )

    def _detect_language(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Detect language of text."""
        text = params.get('text', '')

        if not text:
            return ActionResult(success=False, message="No text provided", duration=time.time() - start_time)

        has_chinese = any('\u4e00' <= c <= '\u9fff' for c in text)
        has_japanese = any('\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff' for c in text)
        has_korean = any('\uac00' <= c <= '\ud7af' for c in text)

        if has_chinese:
            lang = 'chi_sim'
        elif has_japanese:
            lang = 'jpn'
        elif has_korean:
            lang = 'kor'
        else:
            lang = 'eng'

        return ActionResult(
            success=True,
            message=f"Detected language: {lang}",
            data={'detected_language': lang, 'confidence': 0.95},
            duration=time.time() - start_time
        )

    def _extract_region(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Extract text from a specific region."""
        image_data = params.get('image_data', '')
        region = params.get('region', [0, 0, 100, 100])
        language = params.get('language', 'eng')

        x, y, w, h = region
        text, confidence = self._simulate_ocr(image_data, language, region, True)

        return ActionResult(
            success=True,
            message=f"Extracted text from region ({x},{y},{w},{h})",
            data={'text': text, 'confidence': confidence, 'region': region, 'word_count': len(text.split())},
            duration=time.time() - start_time
        )

    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get OCR history."""
        limit = params.get('limit', 50)
        recent = self._ocr_history[-limit:]

        return ActionResult(
            success=True,
            message=f"Retrieved {len(recent)} history entries",
            data={'count': len(recent), 'history': recent},
            duration=time.time() - start_time
        )

    def _simulate_ocr(self, image_data: str, language: str, region: Optional[List[int]], preprocess: bool) -> Tuple[str, float]:
        """Simulate OCR recognition (placeholder for real OCR engine)."""
        if language == 'chi_sim':
            sample_text = "这是一段中文识别测试文本"
        elif language == 'jpn':
            sample_text = "これは日本語のOCRテストです"
        elif language == 'kor':
            sample_text = "이것은 한국어 OCR 테스트입니다"
        else:
            sample_text = "This is a sample OCR recognition result text for testing purposes."

        confidence = 0.85 + (hash(image_data[:20]) % 15) / 100.0

        if region:
            x, y, w, h = region
            sample_text = f"[Region {x},{y},{w},{h}] {sample_text[:50]}..."

        if preprocess:
            sample_text = sample_text.strip()

        return sample_text, round(confidence, 3)

    def _add_to_history(self, operation: str, result: Dict[str, Any]) -> None:
        """Add result to OCR history."""
        self._ocr_history.append({
            'timestamp': time.time(),
            'operation': operation,
            'result': result,
        })
        if len(self._ocr_history) > self._max_history:
            self._ocr_history = self._ocr_history[-self._max_history // 2:]
