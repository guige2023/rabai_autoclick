"""Data Sentiment Action Module.

Provides sentiment analysis for text with polarity detection,
aspect-based sentiment, and multi-language support.
"""

import time
import threading
import sys
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SentimentPolarity(Enum):
    """Sentiment polarity values."""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    MIXED = "mixed"


@dataclass
class SentimentScore:
    """Sentiment score result."""
    polarity: SentimentPolarity
    score: float
    confidence: float
    positive_score: float
    negative_score: float
    neutral_score: float


@dataclass
class AspectSentiment:
    """Aspect-based sentiment result."""
    aspect: str
    polarity: SentimentPolarity
    score: float
    evidence: List[str]


class DataSentimentAction(BaseAction):
    """Sentiment Analysis Action.

    Analyzes sentiment in text with polarity classification,
    aspect-based analysis, and multi-language support.
    """
    action_type = "data_sentiment"
    display_name = "情感分析"
    description = "文本情感分析，支持方面级情感检测"

    _analysis_cache: Dict[str, SentimentScore] = {}
    _lock = threading.RLock()

    _positive_words = {'good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic', 'love', 'happy', 'joy', 'best', 'beautiful', 'perfect', 'awesome', 'brilliant', 'outstanding'}
    _negative_words = {'bad', 'terrible', 'awful', 'horrible', 'hate', 'sad', 'angry', 'worst', 'poor', 'disappointing', 'fail', 'broken', 'useless', 'pathetic', 'dreadful'}
    _intensifiers = {'very', 'really', 'extremely', 'absolutely', 'completely', 'totally', 'incredibly'}
    _negators = {'not', "n't", 'never', 'no', 'neither', 'nobody', 'nothing'}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sentiment operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'analyze', 'analyze_batch', 'aspect_analysis',
                               'compare', 'get_trend', 'clear_cache'
                - text: str - text to analyze
                - texts: list (optional) - batch texts
                - language: str (optional) - language code
                - aspects: list (optional) - specific aspects to analyze

        Returns:
            ActionResult with sentiment analysis result.
        """
        start_time = time.time()
        operation = params.get('operation', 'analyze')

        try:
            with self._lock:
                if operation == 'analyze':
                    return self._analyze(params, start_time)
                elif operation == 'analyze_batch':
                    return self._analyze_batch(params, start_time)
                elif operation == 'aspect_analysis':
                    return self._aspect_analysis(params, start_time)
                elif operation == 'compare':
                    return self._compare(params, start_time)
                elif operation == 'get_trend':
                    return self._get_trend(params, start_time)
                elif operation == 'clear_cache':
                    return self._clear_cache(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Sentiment error: {str(e)}",
                duration=time.time() - start_time
            )

    def _analyze(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Analyze sentiment of a single text."""
        text = params.get('text', '')
        language = params.get('language', 'en')

        if not text:
            return ActionResult(success=False, message="No text provided", duration=time.time() - start_time)

        score = self._analyze_sentiment(text)
        cache_key = text[:100]
        self._analysis_cache[cache_key] = score

        return ActionResult(
            success=True,
            message=f"Sentiment: {score.polarity.value} ({score.score:.2f})",
            data={
                'text': text,
                'polarity': score.polarity.value,
                'score': score.score,
                'confidence': score.confidence,
                'positive_score': score.positive_score,
                'negative_score': score.negative_score,
                'neutral_score': score.neutral_score,
                'language': language,
            },
            duration=time.time() - start_time
        )

    def _analyze_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Analyze sentiment of multiple texts."""
        texts = params.get('texts', [])
        language = params.get('language', 'en')

        results = []
        positive_count = 0
        negative_count = 0
        neutral_count = 0

        for i, text in enumerate(texts):
            score = self._analyze_sentiment(text)
            results.append({
                'index': i,
                'polarity': score.polarity.value,
                'score': score.score,
                'confidence': score.confidence,
            })
            if score.polarity == SentimentPolarity.POSITIVE:
                positive_count += 1
            elif score.polarity == SentimentPolarity.NEGATIVE:
                negative_count += 1
            else:
                neutral_count += 1

        avg_score = sum(r['score'] for r in results) / len(results) if results else 0.0

        return ActionResult(
            success=True,
            message=f"Batch analyzed {len(results)} texts",
            data={
                'results': results,
                'count': len(results),
                'positive_count': positive_count,
                'negative_count': negative_count,
                'neutral_count': neutral_count,
                'average_score': round(avg_score, 3),
            },
            duration=time.time() - start_time
        )

    def _aspect_analysis(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform aspect-based sentiment analysis."""
        text = params.get('text', '')
        aspects = params.get('aspects', ['price', 'quality', 'service', 'speed'])

        aspect_keywords = {
            'price': ['price', 'cost', 'cheap', 'expensive', 'affordable', 'value', 'worth'],
            'quality': ['quality', 'good', 'bad', 'better', 'best', 'excellent', 'poor'],
            'service': ['service', 'support', 'help', 'staff', 'customer', 'response'],
            'speed': ['fast', 'slow', 'quick', 'rapid', 'speed', 'delivery', 'shipping'],
        }

        aspect_results: List[AspectSentiment] = []

        for aspect in aspects:
            keywords = aspect_keywords.get(aspect, [aspect])
            relevant_sentences = []

            sentences = text.replace('!', '.').replace('?', '.').split('.')
            for sentence in sentences:
                sentence_lower = sentence.lower()
                if any(kw in sentence_lower for kw in keywords):
                    relevant_sentences.append(sentence.strip())

            if relevant_sentences:
                combined = ' '.join(relevant_sentences)
                score = self._analyze_sentiment(combined)
                aspect_results.append(AspectSentiment(
                    aspect=aspect,
                    polarity=score.polarity,
                    score=score.score,
                    evidence=relevant_sentences[:3]
                ))
            else:
                aspect_results.append(AspectSentiment(
                    aspect=aspect,
                    polarity=SentimentPolarity.NEUTRAL,
                    score=0.0,
                    evidence=[]
                ))

        return ActionResult(
            success=True,
            message=f"Aspect analysis for {len(aspect_results)} aspects",
            data={
                'aspects': [
                    {'aspect': a.aspect, 'polarity': a.polarity.value, 'score': a.score, 'evidence': a.evidence}
                    for a in aspect_results
                ]
            },
            duration=time.time() - start_time
        )

    def _compare(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Compare sentiments of two texts."""
        text1 = params.get('text1', '')
        text2 = params.get('text2', '')

        score1 = self._analyze_sentiment(text1)
        score2 = self._analyze_sentiment(text2)

        return ActionResult(
            success=True,
            message="Sentiment comparison",
            data={
                'text1': {'polarity': score1.polarity.value, 'score': score1.score},
                'text2': {'polarity': score2.polarity.value, 'score': score2.score},
                'difference': abs(score1.score - score2.score),
            },
            duration=time.time() - start_time
        )

    def _get_trend(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get sentiment trend from cache."""
        limit = params.get('limit', 100)
        recent = list(self._analysis_cache.items())[-limit:]

        if not recent:
            return ActionResult(success=True, message="No trend data", data={'trend': 'neutral', 'data_points': 0}, duration=time.time() - start_time)

        avg_score = sum(s.score for _, s in recent) / len(recent)
        scores_over_time = [(k[:20], s.score) for k, s in recent]

        trend = 'increasing' if len(scores_over_time) > 1 and scores_over_time[-1][1] > scores_over_time[0][1] else 'decreasing' if len(scores_over_time) > 1 and scores_over_time[-1][1] < scores_over_time[0][1] else 'stable'

        return ActionResult(
            success=True,
            message=f"Sentiment trend: {trend}",
            data={'trend': trend, 'average_score': round(avg_score, 3), 'data_points': len(recent)},
            duration=time.time() - start_time
        )

    def _clear_cache(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clear sentiment analysis cache."""
        cleared = len(self._analysis_cache)
        self._analysis_cache.clear()

        return ActionResult(
            success=True,
            message=f"Cleared {cleared} cached results",
            data={'cleared_count': cleared},
            duration=time.time() - start_time
        )

    def _analyze_sentiment(self, text: str) -> SentimentScore:
        """Analyze sentiment of text."""
        words = re.findall(r'\b\w+\b', text.lower())
        pos_count = sum(1 for w in words if w in self._positive_words)
        neg_count = sum(1 for w in words if w in self._negative_words)

        negator_active = False
        for i, word in enumerate(words):
            if word in self._negators:
                negator_active = True
            if word in self._intensifiers and i + 1 < len(words) and words[i + 1] in self._positive_words:
                pos_count += 0.5
            if negator_active and word in self._positive_words:
                pos_count -= 0.5
                negator_active = False

        total = pos_count + neg_count
        if total == 0:
            return SentimentScore(SentimentPolarity.NEUTRAL, 0.0, 0.5, 0.0, 0.0, 1.0)

        pos_score = pos_count / total
        neg_score = neg_count / total
        net_score = pos_score - neg_score

        if net_score > 0.1:
            polarity = SentimentPolarity.POSITIVE
        elif net_score < -0.1:
            polarity = SentimentPolarity.NEGATIVE
        else:
            polarity = SentimentPolarity.NEUTRAL

        confidence = min(0.5 + abs(net_score) * 0.5, 1.0)

        return SentimentScore(
            polarity=polarity,
            score=round(net_score, 3),
            confidence=round(confidence, 3),
            positive_score=round(pos_score, 3),
            negative_score=round(neg_score, 3),
            neutral_score=round(1.0 - pos_score - neg_score, 3)
        )
