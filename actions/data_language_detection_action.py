"""Data Language Detection Action Module.

Provides language detection for text with confidence scoring,
script detection, and multi-lingual text handling.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LanguageScript(Enum):
    """Language script types."""
    LATIN = "latin"
    CYRILLIC = "cyrillic"
    CJK = "cjk"
    ARABIC = "arabic"
    HINDI = "hindi"
    KOREAN = "korean"
    THAI = "thai"
   GREEK = "greek"
    HEBREW = "hebrew"


@dataclass
class LanguagePrediction:
    """Language prediction result."""
    language: str
    confidence: float
    script: LanguageScript
    is_reliable: bool


@dataclass
class MultiLingualSegment:
    """Segment of text in a specific language."""
    text: str
    language: str
    start_pos: int
    end_pos: int


class DataLanguageDetectionAction(BaseAction):
    """Language Detection Action.

    Detects languages in text with confidence scoring,
    script detection, and multi-lingual segment handling.
    """
    action_type = "data_language_detection"
    display_name = "语言检测"
    description = "文本语言检测，支持多语言分段"

    _detection_cache: Dict[str, LanguagePrediction] = {}
    _lock = threading.RLock()

    _language_signatures = {
        'en': {'the', 'and', 'is', 'are', 'was', 'were', 'have', 'has', 'had', 'this', 'that', 'it', 'in', 'on', 'at'},
        'zh': {'的', '是', '在', '有', '和', '了', '我', '你', '他', '她', '它', '们', '这', '那', '什', '么'},
        'ja': {'の', 'は', 'が', 'て', 'に', 'を', 'は', 'です', 'ます', 'した', 'して', 'され', 'これ', 'それ'},
        'ko': {'의', '이', '가', '을', '를', '에', '는', '한', '과', '도', '으로', '에서', '입', '니'},
        'fr': {'le', 'la', 'les', 'un', 'une', 'des', 'est', 'sont', 'et', 'en', 'du', 'que', 'qui', 'dans'},
        'de': {'der', 'die', 'das', 'ein', 'eine', 'und', 'ist', 'sind', 'von', 'mit', 'auf', 'nicht', 'sich'},
        'es': {'el', 'la', 'los', 'las', 'un', 'una', 'es', 'son', 'y', 'en', 'de', 'que', 'del', 'los'},
        'pt': {'o', 'a', 'os', 'as', 'um', 'uma', 'e', 'de', 'em', 'que', 'do', 'da', 'na', 'os'},
        'ru': {'и', 'в', 'на', 'не', 'что', 'он', 'с', 'как', 'а', 'то', 'все', 'она', 'так', 'это'},
        'ar': {'في', 'من', 'على', 'أن', 'هذا', 'التي', 'الذي', 'هو', 'عن', 'مع', 'كانت', 'لها'},
        'hi': {'की', 'है', 'के', 'में', 'और', 'को', 'से', 'पर', 'यह', 'एक', 'हैं', 'था', 'थी', 'थे'},
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute language detection operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'detect', 'detect_batch', 'segment_multilingual',
                               'get_script', 'list_scripts', 'history'
                - text: str - text to analyze
                - texts: list (optional) - batch texts
                - return_all: bool (optional) - return all language predictions

        Returns:
            ActionResult with detection results.
        """
        start_time = time.time()
        operation = params.get('operation', 'detect')

        try:
            with self._lock:
                if operation == 'detect':
                    return self._detect(params, start_time)
                elif operation == 'detect_batch':
                    return self._detect_batch(params, start_time)
                elif operation == 'segment_multilingual':
                    return self._segment_multilingual(params, start_time)
                elif operation == 'get_script':
                    return self._get_script(params, start_time)
                elif operation == 'list_scripts':
                    return self._list_scripts(params, start_time)
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
                message=f"Language detection error: {str(e)}",
                duration=time.time() - start_time
            )

    def _detect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Detect language of text."""
        text = params.get('text', '')
        return_all = params.get('return_all', False)

        if not text:
            return ActionResult(success=False, message="No text provided", duration=time.time() - start_time)

        prediction = self._detect_language(text)
        cache_key = text[:100]
        self._detection_cache[cache_key] = prediction

        if return_all:
            all_predictions = self._get_all_predictions(text)
            return ActionResult(
                success=True,
                message=f"Primary: {prediction.language} ({prediction.confidence:.2f})",
                data={
                    'primary': {'language': prediction.language, 'confidence': prediction.confidence, 'script': prediction.script.value, 'is_reliable': prediction.is_reliable},
                    'all_predictions': [{'language': p.language, 'confidence': p.confidence, 'script': p.script.value} for p in all_predictions],
                },
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message=f"Detected: {prediction.language} ({prediction.confidence:.2f})",
            data={
                'language': prediction.language,
                'confidence': prediction.confidence,
                'script': prediction.script.value,
                'is_reliable': prediction.is_reliable,
            },
            duration=time.time() - start_time
        )

    def _detect_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Detect language for multiple texts."""
        texts = params.get('texts', [])

        results = []
        for i, text in enumerate(texts):
            prediction = self._detect_language(text)
            results.append({
                'index': i,
                'text_preview': text[:50],
                'language': prediction.language,
                'confidence': prediction.confidence,
                'script': prediction.script.value,
            })

        return ActionResult(
            success=True,
            message=f"Batch detected {len(results)} texts",
            data={'results': results, 'count': len(results)},
            duration=time.time() - start_time
        )

    def _segment_multilingual(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Segment multi-lingual text into language-specific segments."""
        text = params.get('text', '')

        segments: List[MultiLingualSegment] = []
        current_pos = 0

        cjk_chars = []
        non_cjk = []

        for char in text:
            if '\u4e00' <= char <= '\u9fff' or '\u3040' <= char <= '\u309f' or '\u30a0' <= char <= '\u30ff' or '\uac00' <= char <= '\ud7af':
                cjk_chars.append(char)
            else:
                if cjk_chars:
                    cjk_text = ''.join(cjk_chars)
                    detected = self._detect_language(cjk_text)
                    segments.append(MultiLingualSegment(cjk_text, detected.language, current_pos - len(cjk_text), current_pos))
                    cjk_chars = []
                non_cjk.append(char)

        if cjk_chars:
            cjk_text = ''.join(cjk_chars)
            detected = self._detect_language(cjk_text)
            segments.append(MultiLingualSegment(cjk_text, detected.language, len(text) - len(cjk_chars), len(text)))

        if non_cjk:
            non_cjk_text = ''.join(non_cjk)
            detected = self._detect_language(non_cjk_text)
            segments.append(MultiLingualSegment(non_cjk_text, detected.language, 0, len(non_cjk_text)))

        segments.sort(key=lambda s: s.start_pos)

        return ActionResult(
            success=True,
            message=f"Segmented into {len(segments)} language segments",
            data={
                'segments': [
                    {'text': s.text[:50], 'language': s.language, 'start': s.start_pos, 'end': s.end_pos}
                    for s in segments
                ],
                'count': len(segments),
            },
            duration=time.time() - start_time
        )

    def _get_script(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get the primary script of text."""
        text = params.get('text', '')

        script = self._detect_script(text)

        return ActionResult(
            success=True,
            message=f"Script detected: {script.value}",
            data={'script': script.value},
            duration=time.time() - start_time
        )

    def _list_scripts(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all supported scripts."""
        scripts = {s.value: s.name.lower() for s in LanguageScript}
        return ActionResult(
            success=True,
            message=f"Supported scripts: {len(scripts)}",
            data={'scripts': scripts},
            duration=time.time() - start_time
        )

    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get detection history."""
        limit = params.get('limit', 50)
        recent = list(self._detection_cache.items())[-limit:]
        return ActionResult(
            success=True,
            message=f"Retrieved {len(recent)} history entries",
            data={'count': len(recent), 'history': [{'text': k, 'language': v.language, 'confidence': v.confidence} for k, v in recent]},
            duration=time.time() - start_time
        )

    def _detect_language(self, text: str) -> LanguagePrediction:
        """Detect the primary language of text."""
        script = self._detect_script(text)

        if script == LanguageScript.CJK:
            if any('\u4e00' <= c <= '\u9fff' for c in text):
                return LanguagePrediction('zh', 0.95, script, True)
            elif any('\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff' for c in text):
                return LanguagePrediction('ja', 0.95, script, True)
            elif any('\uac00' <= c <= '\ud7af' for c in text):
                return LanguagePrediction('ko', 0.95, script, True)

        if script == LanguageScript.CYRILLIC:
            return LanguagePrediction('ru', 0.90, script, True)

        if script == LanguageScript.ARABIC:
            return LanguagePrediction('ar', 0.90, script, True)

        if script == LanguageScript.HINDI:
            return LanguagePrediction('hi', 0.90, script, True)

        words = text.lower().split()
        scores: Dict[str, float] = {}

        for lang, signature in self._language_signatures.items():
            matches = sum(1 for w in words if w in signature)
            if words:
                scores[lang] = matches / min(len(words), 50)
            else:
                scores[lang] = 0.0

        if not scores or max(scores.values()) == 0:
            return LanguagePrediction('en', 0.30, script, False)

        best_lang = max(scores.items(), key=lambda x: x[1])
        confidence = min(best_lang[1] * 2, 1.0)

        return LanguagePrediction(best_lang[0], round(confidence, 3), script, confidence > 0.5)

    def _get_all_predictions(self, text: str) -> List[LanguagePrediction]:
        """Get all language predictions sorted by confidence."""
        predictions: List[Tuple[str, float]] = []

        for lang, signature in self._language_signatures.items():
            words = text.lower().split()
            matches = sum(1 for w in words if w in signature)
            if words and matches > 0:
                score = matches / min(len(words), 50)
                predictions.append((lang, min(score * 2, 1.0)))

        predictions.sort(key=lambda x: x[1], reverse=True)
        script = self._detect_script(text)

        return [LanguagePrediction(lang, round(conf, 3), script, conf > 0.5) for lang, conf in predictions[:5]]

    def _detect_script(self, text: str) -> LanguageScript:
        """Detect the primary script of text."""
        has_cyrillic = any('\u0400' <= c <= '\u04ff' for c in text)
        has_cjk = any('\u4e00' <= c <= '\u9fff' for c in text) or any('\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff' for c in text)
        has_korean = any('\uac00' <= c <= '\ud7af' for c in text)
        has_arabic = any('\u0600' <= c <= '\u06ff' for c in text)
        has_hindi = any('\u0900' <= c <= '\u097f' for c in text)
        has_thai = any('\u0e00' <= c <= '\u0e7f' for c in text)
        has_greek = any('\u0370' <= c <= '\u03ff' for c in text)
        has_hebrew = any('\u0590' <= c <= '\u05ff' for c in text)

        if has_cyrillic:
            return LanguageScript.CYRILLIC
        elif has_cjk:
            return LanguageScript.CJK
        elif has_korean:
            return LanguageScript.KOREAN
        elif has_arabic:
            return LanguageScript.ARABIC
        elif has_hindi:
            return LanguageScript.HINDI
        elif has_thai:
            return LanguageScript.THAI
        elif has_greek:
            return LanguageScript.GREEK
        elif has_hebrew:
            return LanguageScript.HEBREW
        else:
            return LanguageScript.LATIN
