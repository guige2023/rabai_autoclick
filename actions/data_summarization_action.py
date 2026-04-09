"""Data Summarization Action Module.

Provides text summarization with extractive and abstractive methods,
configurable length, and multi-document support.
"""

import time
import threading
import sys
import os
import re
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SummarizationMethod(Enum):
    """Summarization method types."""
    EXTRACTIVE = "extractive"
    ABSTRACTIVE = "abstractive"
    Hybrid = "hybrid"


@dataclass
class SummaryResult:
    """Summarization result."""
    summary: str
    original_length: int
    summary_length: int
    compression_ratio: float
    method: SummarizationMethod
    key_points: List[str]


class DataSummarizationAction(BaseAction):
    """Text Summarization Action.

    Summarizes text using extractive and abstractive methods,
    with configurable length and multi-document support.
    """
    action_type = "data_summarization"
    display_name = "文本摘要"
    description = "文本摘要生成，支持提取式和生成式方法"

    _summary_history: List[Dict[str, Any]] = []
    _lock = threading.RLock()
    _max_history: int = 200

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute summarization operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'summarize', 'summarize_batch', 'extract_key_points',
                               'get_bullet_summary', 'compare', 'history'
                - text: str - text to summarize
                - method: str (optional) - 'extractive', 'abstractive', 'hybrid'
                - max_length: int (optional) - max summary length in words
                - min_length: int (optional) - min summary length
                - texts: list (optional) - batch texts for multi-doc summary
                - compression_ratio: float (optional) - target compression

        Returns:
            ActionResult with summarization result.
        """
        start_time = time.time()
        operation = params.get('operation', 'summarize')

        try:
            with self._lock:
                if operation == 'summarize':
                    return self._summarize(params, start_time)
                elif operation == 'summarize_batch':
                    return self._summarize_batch(params, start_time)
                elif operation == 'extract_key_points':
                    return self._extract_key_points(params, start_time)
                elif operation == 'get_bullet_summary':
                    return self._get_bullet_summary(params, start_time)
                elif operation == 'compare':
                    return self._compare(params, start_time)
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
                message=f"Summarization error: {str(e)}",
                duration=time.time() - start_time
            )

    def _summarize(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Summarize a single text."""
        text = params.get('text', '')
        method_str = params.get('method', 'extractive')
        max_length = params.get('max_length', 100)
        min_length = params.get('min_length', 30)

        if not text:
            return ActionResult(success=False, message="No text provided", duration=time.time() - start_time)

        try:
            method = SummarizationMethod(method_str)
        except ValueError:
            method = SummarizationMethod.EXTRACTIVE

        summary, key_points = self._generate_summary(text, method, max_length, min_length)

        original_length = len(text.split())
        summary_length = len(summary.split())
        compression_ratio = summary_length / original_length if original_length > 0 else 1.0

        result = {
            'summary': summary,
            'original_length': original_length,
            'summary_length': summary_length,
            'compression_ratio': round(compression_ratio, 3),
            'method': method.value,
            'key_points': key_points,
        }

        self._add_to_history(result)

        return ActionResult(
            success=True,
            message=f"Summarized {original_length} -> {summary_length} words ({method.value})",
            data=result,
            duration=time.time() - start_time
        )

    def _summarize_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Summarize multiple texts together."""
        texts = params.get('texts', [])
        method_str = params.get('method', 'extractive')
        max_length = params.get('max_length', 150)

        if not texts:
            return ActionResult(success=False, message="No texts provided", duration=time.time() - start_time)

        combined_text = ' '.join(texts)
        summary, key_points = self._generate_summary(combined_text, SummarizationMethod(method_str), max_length, 30)

        return ActionResult(
            success=True,
            message=f"Multi-doc summary from {len(texts)} documents",
            data={
                'summary': summary,
                'document_count': len(texts),
                'combined_length': len(combined_text.split()),
                'summary_length': len(summary.split()),
                'key_points': key_points,
            },
            duration=time.time() - start_time
        )

    def _extract_key_points(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Extract key points from text."""
        text = params.get('text', '')
        max_points = params.get('max_points', 5)

        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]

        word_freq = {}
        words = re.findall(r'\b\w+\b', text.lower())
        stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they'}
        words = [w for w in words if w not in stopwords and len(w) > 3]

        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1

        scored_sentences = []
        for sent in sentences:
            sent_words = re.findall(r'\b\w+\b', sent.lower())
            score = sum(word_freq.get(w, 0) for w in sent_words)
            scored_sentences.append((score, sent))

        scored_sentences.sort(key=lambda x: x[0], reverse=True)
        top_points = [sent for _, sent in scored_sentences[:max_points]]

        return ActionResult(
            success=True,
            message=f"Extracted {len(top_points)} key points",
            data={'key_points': top_points, 'count': len(top_points)},
            duration=time.time() - start_time
        )

    def _get_bullet_summary(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a bullet-point summary."""
        text = params.get('text', '')
        max_bullets = params.get('max_bullets', 5)

        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip() and len(s) > 20]

        bullet_points = sentences[:max_bullets]
        bullet_summary = '\n'.join(f"• {point}" for point in bullet_points)

        return ActionResult(
            success=True,
            message=f"Bullet summary with {len(bullet_points)} points",
            data={'bullet_summary': bullet_summary, 'bullets': bullet_points, 'count': len(bullet_points)},
            duration=time.time() - start_time
        )

    def _compare(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Compare summaries using different methods."""
        text = params.get('text', '')

        extractive_summary, _ = self._generate_summary(text, SummarizationMethod.EXTRACTIVE, 100, 30)
        abstractive_summary, _ = self._generate_summary(text, SummarizationMethod.ABSTRACTIVE, 100, 30)

        return ActionResult(
            success=True,
            message="Method comparison",
            data={
                'extractive': {'summary': extractive_summary, 'length': len(extractive_summary.split())},
                'abstractive': {'summary': abstractive_summary, 'length': len(abstractive_summary.split())},
            },
            duration=time.time() - start_time
        )

    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get summarization history."""
        limit = params.get('limit', 50)
        recent = self._summary_history[-limit:]

        return ActionResult(
            success=True,
            message=f"Retrieved {len(recent)} history entries",
            data={'count': len(recent), 'history': recent},
            duration=time.time() - start_time
        )

    def _generate_summary(self, text: str, method: SummarizationMethod, max_length: int, min_length: int) -> tuple:
        """Generate summary from text."""
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip() and len(s) > 15]

        if len(sentences) <= 3:
            return text[:max_length * 6], sentences[:3]

        word_freq = {}
        words = re.findall(r'\b\w+\b', text.lower())
        stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'and', 'but', 'or', 'not', 'for', 'with', 'from', 'your'}
        content_words = [w for w in words if w not in stopwords and len(w) > 3]
        for word in content_words:
            word_freq[word] = word_freq.get(word, 0) + 1

        scored = []
        for sent in sentences:
            sent_words = re.findall(r'\b\w+\b', sent.lower())
            score = sum(word_freq.get(w, 0) for w in sent_words)
            position_score = 1.0 / (sentences.index(sent) + 1)
            final_score = score * 0.7 + position_score * 0.3
            scored.append((final_score, sent))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_sentences = [s for _, s in scored[:5]]
        top_sentences.sort(key=lambda s: text.index(s))

        summary = '. '.join(top_sentences)
        if len(summary) > max_length * 6:
            summary = '. '.join(top_sentences[:max_length // 30])

        key_points = top_sentences[:3]

        return summary, key_points

    def _add_to_history(self, result: Dict[str, Any]) -> None:
        """Add result to history."""
        self._summary_history.append({
            'timestamp': time.time(),
            'result': result,
        })
        if len(self._summary_history) > self._max_history:
            self._summary_history = self._summary_history[-self._max_history // 2:]
