"""Automation Translate Action Module.

Provides machine translation for text with language detection,
batch translation, and glossary support.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TranslationEngine(Enum):
    """Translation engine types."""
    GOOGLE = "google"
    DEEPL = "deepl"
    OPENAI = "openai"
    DEEPSEEK = "deepseek"
    VOLCENGINE = "volcengine"


@dataclass
class TranslationResult:
    """Translation result."""
    source_text: str
    translated_text: str
    source_lang: str
    target_lang: str
    confidence: float
    engine: str


class AutomationTranslateAction(BaseAction):
    """Machine Translation Action.

    Translates text between languages with multiple engine support,
    batch processing, and glossary management.
    """
    action_type = "automation_translate"
    display_name = "机器翻译"
    description = "多引擎机器翻译，支持批量处理和术语表"

    _translation_history: List[Dict[str, Any]] = []
    _glossaries: Dict[str, Dict[str, str]] = {}
    _lock = threading.RLock()
    _max_history: int = 500

    _language_names = {
        'en': 'English', 'zh': 'Chinese', 'ja': 'Japanese', 'ko': 'Korean',
        'fr': 'French', 'de': 'German', 'es': 'Spanish', 'it': 'Italian',
        'pt': 'Portuguese', 'ru': 'Russian', 'ar': 'Arabic', 'hi': 'Hindi',
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute translation operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'translate', 'translate_batch', 'detect_language',
                               'list_languages', 'add_glossary', 'get_glossary', 'history'
                - text: str - text to translate
                - source_lang: str - source language code
                - target_lang: str - target language code
                - engine: str (optional) - translation engine
                - texts: list (optional) - batch texts
                - glossary_name: str (optional) - glossary to use

        Returns:
            ActionResult with translation result.
        """
        start_time = time.time()
        operation = params.get('operation', 'translate')

        try:
            with self._lock:
                if operation == 'translate':
                    return self._translate(params, start_time)
                elif operation == 'translate_batch':
                    return self._translate_batch(params, start_time)
                elif operation == 'detect_language':
                    return self._detect_language(params, start_time)
                elif operation == 'list_languages':
                    return self._list_languages(params, start_time)
                elif operation == 'add_glossary':
                    return self._add_glossary(params, start_time)
                elif operation == 'get_glossary':
                    return self._get_glossary(params, start_time)
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
                message=f"Translation error: {str(e)}",
                duration=time.time() - start_time
            )

    def _translate(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Translate a single text."""
        text = params.get('text', '')
        source_lang = params.get('source_lang', 'auto')
        target_lang = params.get('target_lang', 'en')
        engine = params.get('engine', 'deepl')
        glossary_name = params.get('glossary_name')

        if source_lang == 'auto':
            source_lang = self._detect_text_language(text)

        translated = self._simulate_translation(text, source_lang, target_lang, glossary_name)
        confidence = 0.90 + (hash(text[:20]) % 10) / 100.0

        result = {
            'source_text': text,
            'translated_text': translated,
            'source_lang': source_lang,
            'target_lang': target_lang,
            'engine': engine,
            'confidence': round(confidence, 3),
        }

        self._add_to_history(result)

        return ActionResult(
            success=True,
            message=f"Translated {source_lang} -> {target_lang}",
            data=result,
            duration=time.time() - start_time
        )

    def _translate_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Translate multiple texts."""
        texts = params.get('texts', [])
        source_lang = params.get('source_lang', 'auto')
        target_lang = params.get('target_lang', 'en')
        engine = params.get('engine', 'deepl')

        if source_lang == 'auto' and texts:
            source_lang = self._detect_text_language(texts[0])

        results = []
        for i, text in enumerate(texts):
            translated = self._simulate_translation(text, source_lang, target_lang, None)
            results.append({
                'index': i,
                'source_text': text,
                'translated_text': translated,
                'source_lang': source_lang,
                'target_lang': target_lang,
            })

        return ActionResult(
            success=True,
            message=f"Batch translated {len(results)} texts",
            data={'results': results, 'count': len(results)},
            duration=time.time() - start_time
        )

    def _detect_language(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Detect the language of text."""
        text = params.get('text', '')

        if not text:
            return ActionResult(success=False, message="No text provided", duration=time.time() - start_time)

        lang = self._detect_text_language(text)

        return ActionResult(
            success=True,
            message=f"Detected language: {lang}",
            data={'detected_lang': lang, 'language_name': self._language_names.get(lang, lang), 'confidence': 0.95},
            duration=time.time() - start_time
        )

    def _list_languages(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List supported languages."""
        return ActionResult(
            success=True,
            message=f"Supported languages: {len(self._language_names)}",
            data={'languages': self._language_names},
            duration=time.time() - start_time
        )

    def _add_glossary(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a translation glossary."""
        glossary_name = params.get('glossary_name', 'default')
        terms = params.get('terms', {})

        self._glossaries[glossary_name] = terms

        return ActionResult(
            success=True,
            message=f"Added glossary '{glossary_name}' with {len(terms)} terms",
            data={'glossary_name': glossary_name, 'term_count': len(terms)},
            duration=time.time() - start_time
        )

    def _get_glossary(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a translation glossary."""
        glossary_name = params.get('glossary_name', 'default')

        if glossary_name not in self._glossaries:
            return ActionResult(success=False, message=f"Glossary '{glossary_name}' not found", duration=time.time() - start_time)

        return ActionResult(
            success=True,
            message=f"Retrieved glossary '{glossary_name}'",
            data={'glossary_name': glossary_name, 'terms': self._glossaries[glossary_name]},
            duration=time.time() - start_time
        )

    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get translation history."""
        limit = params.get('limit', 50)
        recent = self._translation_history[-limit:]

        return ActionResult(
            success=True,
            message=f"Retrieved {len(recent)} history entries",
            data={'count': len(recent), 'history': recent},
            duration=time.time() - start_time
        )

    def _detect_text_language(self, text: str) -> str:
        """Detect text language."""
        has_chinese = any('\u4e00' <= c <= '\u9fff' for c in text)
        has_japanese = any('\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff' for c in text)
        has_korean = any('\uac00' <= c <= '\ud7af' for c in text)
        has_cyrillic = any('\u0400' <= c <= '\u04ff' for c in text)

        if has_chinese:
            return 'zh'
        elif has_japanese:
            return 'ja'
        elif has_korean:
            return 'ko'
        elif has_cyrillic:
            return 'ru'

        words = text.lower().split()
        en_articles = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'have', 'has'}
        fr_articles = {'le', 'la', 'les', 'un', 'une', 'des', 'est', 'sont'}
        de_articles = {'der', 'die', 'das', 'ein', 'eine', 'ist', 'sind'}

        if any(w in en_articles for w in words[:5]):
            return 'en'
        elif any(w in fr_articles for w in words[:5]):
            return 'fr'
        elif any(w in de_articles for w in words[:5]):
            return 'de'

        return 'en'

    def _simulate_translation(self, text: str, source_lang: str, target_lang: str, glossary_name: Optional[str]) -> str:
        """Simulate translation (placeholder for real engine)."""
        translations = {
            ('zh', 'en'): "This is the Chinese to English translation",
            ('en', 'zh'): "这是中文翻译",
            ('ja', 'en'): "This is the Japanese to English translation",
            ('en', 'ja'): "日本語翻訳",
            ('en', 'fr'): "Ceci est la traduction",
            ('fr', 'en'): "This is the French to English translation",
        }

        key = (source_lang, target_lang)
        if key in translations:
            return translations[key] + f": {text[:30]}"

        return f"[Translated to {target_lang}]: {text[:50]}"

    def _add_to_history(self, result: Dict[str, Any]) -> None:
        """Add result to translation history."""
        self._translation_history.append({
            'timestamp': time.time(),
            'result': result,
        })
        if len(self._translation_history) > self._max_history:
            self._translation_history = self._translation_history[-self._max_history // 2:]
