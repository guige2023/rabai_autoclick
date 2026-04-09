"""OCR action module for RabAI AutoClick.

Provides OCR-based text recognition and click actions using:
- RapidOCR (primary, ONNX-based)
- EasyOCR (fallback)
- PaddleOCR (deprecated fallback)

Features:
- Text recognition in screen regions
- Click at recognized text positions
- Multiple preprocessing modes (auto, contrast, binary, denoise)
- Exact and fuzzy text matching
- Multi-result merging
"""

from __future__ import annotations

import threading
import time
import warnings
import cv2
import numpy as np
import pyautogui
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from rabai_autoclick.core.base_action import BaseAction, ActionResult
from rabai_autoclick.utils.mouse_utils import macos_click


# Valid OCR preprocessing modes
VALID_PREPROCESS_MODES: List[str] = [
    'auto', 'all', 'contrast', 'binary', 'denoise', 'original'
]

# Valid OCR backends
VALID_OCR_BACKENDS: List[str] = ['rapidocr', 'easyocr', 'paddleocr']

# Confidence range bounds
MIN_CONFIDENCE: float = 0.0
MAX_CONFIDENCE: float = 1.0


# Thread-local storage for per-thread OCR instances
_thread_local = threading.local()


def _get_thread_ocr():
    """Get thread-local OCR instance and backend."""
    return getattr(_thread_local, 'ocr', None), getattr(_thread_local, 'backend', None)


def _set_thread_ocr(ocr: Any, backend: Optional[str]) -> None:
    """Set thread-local OCR instance and backend."""
    _thread_local.ocr = ocr
    _thread_local.backend = backend


# ---------------------------------------------------------------------------
# OCR Backend ABC
# ---------------------------------------------------------------------------

class OCRBackend(ABC):
    """Abstract base class for OCR backends."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the backend name string."""
        ...

    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the OCR engine. Return True on success."""
        ...

    @abstractmethod
    def execute(self, img_array: np.ndarray) -> List[Dict]:
        """Run OCR on the given image. Return list of result dicts."""
        ...

    @property
    def is_available(self) -> bool:
        """Check if this backend is available (can be initialized)."""
        try:
            return self.initialize()
        except Exception:
            return False


# ---------------------------------------------------------------------------
# RapidOCR Backend
# ---------------------------------------------------------------------------

class RapidOCRBackend(OCRBackend):
    name = 'rapidocr'

    def __init__(self) -> None:
        self._engine: Any = None

    def initialize(self) -> bool:
        if self._engine is not None:
            return True
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                from rapidocr_onnxruntime import RapidOCR
                self._engine = RapidOCR()
                return True
        except Exception:
            self._engine = None
            return False

    def execute(self, img_array: np.ndarray) -> List[Dict]:
        if self._engine is None:
            raise RuntimeError("RapidOCR not initialized")
        results: List[Dict] = []
        ocr_result, _ = self._engine(img_array)
        if ocr_result is not None and len(ocr_result) > 0:
            for item in ocr_result:
                box = item[0]
                text = item[1]
                confidence = item[2]
                center_x = int((box[0][0] + box[2][0]) / 2)
                center_y = int((box[0][1] + box[2][1]) / 2)
                results.append({
                    'text': text,
                    'confidence': float(confidence),
                    'x': center_x,
                    'y': center_y,
                    'box': box.tolist() if hasattr(box, 'tolist') else box
                })
        return results


# ---------------------------------------------------------------------------
# EasyOCR Backend
# ---------------------------------------------------------------------------

class EasyOCRBackend(OCRBackend):
    name = 'easyocr'

    def __init__(self) -> None:
        self._engine: Any = None

    def initialize(self) -> bool:
        if self._engine is not None:
            return True
        try:
            import easyocr
            self._engine = easyocr.Reader(
                ['ch_sim', 'en'], gpu=False, verbose=False
            )
            return True
        except Exception:
            self._engine = None
            return False

    def execute(self, img_array: np.ndarray) -> List[Dict]:
        if self._engine is None:
            raise RuntimeError("EasyOCR not initialized")
        results: List[Dict] = []
        easy_results = self._engine.readtext(img_array)
        for detection in easy_results:
            box = detection[0]
            text = detection[1]
            confidence = detection[2]
            center_x = int((box[0][0] + box[2][0]) / 2)
            center_y = int((box[0][1] + box[2][1]) / 2)
            results.append({
                'text': text,
                'confidence': confidence,
                'x': center_x,
                'y': center_y,
                'box': box
            })
        return results


# ---------------------------------------------------------------------------
# PaddleOCR Backend
# ---------------------------------------------------------------------------

class PaddleOCRBackend(OCRBackend):
    name = 'paddleocr'

    def __init__(self) -> None:
        self._engine: Any = None

    def initialize(self) -> bool:
        if self._engine is not None:
            return True
        try:
            from paddleocr import PaddleOCR
            self._engine = PaddleOCR(use_angle_cls=True, lang='en', show_log=False)
            return True
        except Exception:
            self._engine = None
            return False

    def execute(self, img_array: np.ndarray) -> List[Dict]:
        if self._engine is None:
            raise RuntimeError("PaddleOCR not initialized")
        results: List[Dict] = []
        paddle_result = self._engine.ocr(img_array, cls=True)
        if paddle_result and paddle_result[0]:
            for line in paddle_result[0]:
                box = line[0]
                text = line[1][0]
                confidence = line[1][1]
                center_x = int((box[0][0] + box[2][0]) / 2)
                center_y = int((box[0][1] + box[2][1]) / 2)
                results.append({
                    'text': text,
                    'confidence': confidence,
                    'x': center_x,
                    'y': center_y,
                    'box': box
                })
        return results


# ---------------------------------------------------------------------------
# OCR Backend Factory
# ---------------------------------------------------------------------------

_ALL_BACKENDS: List[OCRBackend] = [
    RapidOCRBackend(),
    EasyOCRBackend(),
    PaddleOCRBackend(),
]


def _create_ocr_backend() -> Tuple[Optional[OCRBackend], Optional[str]]:
    """Initialize and return the first available OCR backend."""
    for backend in _ALL_BACKENDS:
        if backend.initialize():
            return backend, backend.name
    return None, None


# ---------------------------------------------------------------------------
# Image Preprocessing
# ---------------------------------------------------------------------------

def preprocess_image_enhanced(
    img_array: np.ndarray,
    mode: str = 'auto'
) -> List[Tuple[str, np.ndarray]]:
    """Enhanced image preprocessing with multiple modes.

    Args:
        img_array: Input image as numpy array.
        mode: Preprocessing mode ('auto', 'all', 'contrast', 'binary',
              'denoise', 'original').

    Returns:
        List of (mode_name, processed_image) tuples.
    """
    try:
        if len(img_array.shape) == 3:
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
            gray = cv2.cvtColor(gray, cv2.COLOR_BGR2GRAY)
        else:
            gray = img_array.copy()

        processed_images: List[Tuple[str, np.ndarray]] = []

        if mode == 'auto' or mode == 'original':
            processed_images.append(('original', img_array))

        elif mode == 'all':
            processed_images.append(('original', img_array))

            clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(gray)
            processed_images.append(
                ('clahe', cv2.cvtColor(enhanced, cv2.COLOR_GRAY2RGB))
            )

            _, binary_otsu = cv2.threshold(
                enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
            )
            processed_images.append(
                ('otsu', cv2.cvtColor(binary_otsu, cv2.COLOR_GRAY2RGB))
            )

            binary_adaptive = cv2.adaptiveThreshold(
                enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY, 11, 2
            )
            processed_images.append(
                ('adaptive', cv2.cvtColor(binary_adaptive, cv2.COLOR_GRAY2RGB))
            )

            kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
            sharpened = cv2.filter2D(gray, -1, kernel)
            processed_images.append(
                ('sharpen', cv2.cvtColor(sharpened, cv2.COLOR_GRAY2RGB))
            )

            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            processed_images.append(
                ('denoise', cv2.cvtColor(denoised, cv2.COLOR_GRAY2RGB))
            )

        elif mode == 'contrast':
            clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(gray)
            processed_images.append(
                ('contrast', cv2.cvtColor(enhanced, cv2.COLOR_GRAY2RGB))
            )

        elif mode == 'binary':
            _, binary = cv2.threshold(
                gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
            )
            processed_images.append(
                ('binary', cv2.cvtColor(binary, cv2.COLOR_GRAY2RGB))
            )

        elif mode == 'denoise':
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            processed_images.append(
                ('denoise', cv2.cvtColor(denoised, cv2.COLOR_GRAY2RGB))
            )

        else:
            processed_images.append(('original', img_array))

        return processed_images

    except Exception:
        return [('original', img_array)]


# ---------------------------------------------------------------------------
# OCRAction
# ---------------------------------------------------------------------------

class OCRAction(BaseAction):
    """OCR text recognition with optional click actions.

    Supports multiple OCR backends (RapidOCR, EasyOCR, PaddleOCR),
    text matching (exact and fuzzy), and image preprocessing modes.
    """
    action_type = "ocr"
    display_name = "OCR文字识别"
    description = "识别屏幕指定区域的文字内容，支持精确匹配和模糊匹配"

    def __init__(self) -> None:
        super().__init__()
        self._backend: Optional[OCRBackend] = None

    @property
    def _ocr_backend(self) -> Optional[str]:
        """Return the current backend name."""
        if self._backend is not None:
            return self._backend.name
        return None

    def _ensure_backend(self) -> Tuple[Optional[OCRBackend], Optional[str]]:
        """Lazily initialize the OCR backend for this instance."""
        if self._backend is not None:
            return self._backend, self._backend.name

        # Try thread-local cache first
        ocr, backend_name = _get_thread_ocr()
        if ocr is not None:
            self._backend = ocr
            return ocr, backend_name

        # Initialize fresh
        backend, backend_name = _create_ocr_backend()
        if backend is not None:
            self._backend = backend
            _set_thread_ocr(backend, backend_name)
        return backend, backend_name

    def _sort_by_position(self, results: List[Dict]) -> List[Dict]:
        """Sort OCR results by position: top-to-bottom, left-to-right.

        Args:
            results: List of OCR result dicts with 'x' and 'y' keys.

        Returns:
            Sorted list of results.
        """
        return sorted(results, key=lambda r: (r['y'], r['x']))

    def _find_matches(
        self,
        results: List[Dict],
        text: str,
        exact_match: bool = False
    ) -> List[Dict]:
        """Find OCR results matching the specified text.

        Args:
            results: List of OCR result dicts.
            text: Text to search for.
            exact_match: If True, require exact match; else substring match.

        Returns:
            List of matching result dicts.
        """
        if exact_match:
            matched = [r for r in results if r['text'] == text]
        else:
            matched = [r for r in results if text in r['text']]

        return self._sort_by_position(matched)

    def _merge_results(
        self,
        all_results: List[Tuple[str, List[Dict]]]
    ) -> List[Dict]:
        """Merge multiple OCR results, deduplicating and keeping best confidence.

        Args:
            all_results: List of (mode_name, results) tuples.

        Returns:
            Merged list of unique results.
        """
        merged: Dict[Tuple, Dict] = {}

        for mode, results in all_results:
            for r in results:
                text = r['text']
                key = (r['x'], r['y'], text)

                if key not in merged or r['confidence'] > merged[key]['confidence']:
                    merged[key] = r

        return list(merged.values())

    def _do_ocr(
        self,
        img_array: np.ndarray,
        backend: OCRBackend
    ) -> List[Dict]:
        """Execute OCR on an image array using the given backend.

        Args:
            img_array: Image as numpy array (RGB format).
            backend: OCR backend instance.

        Returns:
            List of OCR result dicts with 'text', 'confidence', 'x', 'y', 'box'.
        """
        return backend.execute(img_array)

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OCR recognition with optional click action.

        Args:
            context: Execution context.
            params: Dict with region, click_text, click_index, contains,
                   exact_match, move_duration, preprocess_mode, retry_count,
                   click_count, button, offset_x, offset_y.

        Returns:
            ActionResult with OCR results and optional click result.
        """
        backend, ocr_backend = self._ensure_backend()

        if backend is None:
            return ActionResult(
                success=False,
                message="OCR未安装，请运行: pip install rapidocr-onnxruntime"
            )

        region = params.get('region', None)
        click_text = params.get('click_text', None)
        click_index = params.get('click_index', 0)
        contains = params.get('contains', None)
        exact_match = params.get('exact_match', False)
        move_duration = params.get('move_duration', 0.2)
        preprocess_mode = params.get('preprocess_mode', 'auto')
        retry_count = params.get('retry_count', 3)
        click_count = params.get('click_count', 1)
        button = params.get('button', 'left')
        offset_x = params.get('offset_x', 0)
        offset_y = params.get('offset_y', 0)

        # Validate click_index
        valid, msg = self.validate_type(click_index, int, 'click_index')
        if not valid:
            return ActionResult(success=False, message=msg)
        if click_index < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'click_index' must be >= 0, got {click_index}"
            )

        # Validate retry_count
        valid, msg = self.validate_type(retry_count, int, 'retry_count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if retry_count < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'retry_count' must be >= 1, got {retry_count}"
            )

        # Validate click_count
        valid, msg = self.validate_type(click_count, int, 'click_count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if click_count < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'click_count' must be >= 1, got {click_count}"
            )

        # Validate exact_match
        valid, msg = self.validate_type(exact_match, bool, 'exact_match')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate button
        valid, msg = self.validate_in(button, self.VALID_BUTTONS, 'button')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate move_duration
        valid, msg = self.validate_type(move_duration, (int, float), 'move_duration')
        if not valid:
            return ActionResult(success=False, message=msg)
        if move_duration < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'move_duration' must be >= 0, got {move_duration}"
            )

        # Validate preprocess_mode
        valid, msg = self.validate_in(
            preprocess_mode, VALID_PREPROCESS_MODES, 'preprocess_mode'
        )
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            screenshot = pyautogui.screenshot(region=region)
            img_array = np.array(screenshot)

            processed_images = preprocess_image_enhanced(img_array, preprocess_mode)

            all_results: List[Tuple[str, List[Dict]]] = []

            for mode, processed_img in processed_images[:retry_count]:
                results = self._do_ocr(processed_img, backend)
                if results:
                    for r in results:
                        if region:
                            r['x'] += region[0]
                            r['y'] += region[1]
                    all_results.append((mode, results))

            if not all_results:
                return ActionResult(
                    success=True,
                    message="未识别到文字",
                    data={'text': '', 'results': [], 'found': False}
                )

            ocr_results = self._merge_results(all_results)

            if not ocr_results:
                return ActionResult(
                    success=True,
                    message="未识别到文字",
                    data={'text': '', 'results': [], 'found': False}
                )

            all_text = [r['text'] for r in ocr_results]
            full_text = '\n'.join(all_text)
            ocr_results = self._sort_by_position(ocr_results)

            # Handle click_text action
            if click_text:
                matched_results = self._find_matches(ocr_results, click_text, exact_match)

                if not matched_results:
                    display_text = full_text[:200] + '...' if len(full_text) > 200 else full_text
                    return ActionResult(
                        success=True,
                        message=f"未找到 '{click_text}' (精确匹配: {exact_match})，"
                                f"已识别 {len(ocr_results)} 项: {display_text}",
                        data={
                            'text': full_text,
                            'results': ocr_results,
                            'found': False,
                            'count': len(ocr_results)
                        }
                    )

                # Clamp click_index to valid range
                if click_index < 0:
                    click_index = 0
                elif click_index >= len(matched_results):
                    click_index = len(matched_results) - 1

                target = matched_results[click_index]
                click_x = int(target['x']) + int(offset_x)
                click_y = int(target['y']) + int(offset_y)

                pyautogui.moveTo(click_x, click_y, duration=move_duration)
                time.sleep(0.2)

                # Perform click (using double-click count)
                for _ in range(click_count):
                    macos_click(click_x, click_y, 1, button)
                    time.sleep(0.05)

                click_type = "双击" if click_count == 2 else "单击"
                button_name = "右键" if button == 'right' else "左键" if button == 'left' else "中键"
                match_info = f"第{click_index + 1}个匹配项(共{len(matched_results)}个)"
                return ActionResult(
                    success=True,
                    message=f"{click_type}{button_name}成功: '{target['text']}' "
                            f"[{match_info}] 偏移({offset_x},{offset_y})",
                    data={
                        'text': full_text,
                        'results': ocr_results,
                        'clicked': target,
                        'found': True,
                        'match_count': len(matched_results),
                        'match_index': click_index
                    }
                )

            # Handle contains (detect-only) action
            if contains:
                matched = self._find_matches(ocr_results, contains, exact_match)
                if matched:
                    return ActionResult(
                        success=True,
                        message=f"找到 {len(matched)} 个包含 '{contains}' 的文字",
                        data={
                            'text': full_text,
                            'results': ocr_results,
                            'matched': matched,
                            'found': True
                        }
                    )
                else:
                    return ActionResult(
                        success=True,
                        message=f"未找到包含 '{contains}' 的文字",
                        data={
                            'text': full_text,
                            'results': ocr_results,
                            'found': False
                        }
                    )

            # Default: just return OCR results
            return ActionResult(
                success=True,
                message=f"识别成功，共 {len(ocr_results)} 行文字 (使用{self._ocr_backend})",
                data={'text': full_text, 'results': ocr_results}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OCR识别异常: {str(e)}",
                data={'found': False}
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'region': None,
            'click_text': None,
            'click_index': 0,
            'exact_match': False,
            'contains': None,
            'move_duration': 0.2,
            'preprocess_mode': 'auto',
            'retry_count': 3,
            'click_count': 1,
            'button': 'left',
            'offset_x': 0,
            'offset_y': 0
        }
