"""Data Text Action Module.

Provides comprehensive text processing including tokenization,
lemmatization, stemming, Named Entity Recognition, and text statistics.
"""

import time
import threading
import sys
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EntityType(Enum):
    """NER entity types."""
    PERSON = "person"
    ORGANIZATION = "organization"
    LOCATION = "location"
    DATE = "date"
    TIME = "time"
    MONEY = "money"
    PERCENT = "percent"
    EMAIL = "email"
    URL = "url"


@dataclass
class TextStatistics:
    """Text statistics result."""
    char_count: int
    word_count: int
    sentence_count: int
    paragraph_count: int
    avg_word_length: float
    avg_sentence_length: float
    unique_words: int
    vocabulary_richness: float


@dataclass
class TokenInfo:
    """Token information."""
    token: str
    lemma: str
    pos_tag: str
    is_stopword: bool
    char_start: int
    char_end: int


@dataclass
class EntityInfo:
    """Named entity information."""
    text: str
    entity_type: EntityType
    start_pos: int
    end_pos: int
    confidence: float


class DataTextAction(BaseAction):
    """Text Processing Action.

    Comprehensive text processing with tokenization, NER,
    lemmatization, and text statistics.
    """
    action_type = "data_text"
    display_name = "文本处理"
    description = "文本处理：分词、命名实体识别、词性标注、统计"

    _stopwords: Set[str] = field(default_factory=lambda: {
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'must', 'shall', 'can', 'this', 'that',
        'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
        'and', 'but', 'or', 'not', 'for', 'with', 'from', 'your', 'my',
        'his', 'her', 'its', 'our', 'their', 'what', 'which', 'who', 'whom',
        'when', 'where', 'why', 'how', 'all', 'each', 'every', 'both',
        'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'too',
        'very', 'just', 'also', 'now', 'here', 'there', 'then', 'once'
    })

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text processing operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'tokenize', 'ner', 'statistics', 'pos_tag',
                               'lemmatize', 'stem', 'clean', 'split_sentences'
                - text: str - text to process
                - language: str (optional) - language code

        Returns:
            ActionResult with processing result.
        """
        start_time = time.time()
        operation = params.get('operation', 'statistics')

        try:
            if operation == 'tokenize':
                return self._tokenize(params, start_time)
            elif operation == 'ner':
                return self._ner(params, start_time)
            elif operation == 'statistics':
                return self._statistics(params, start_time)
            elif operation == 'pos_tag':
                return self._pos_tag(params, start_time)
            elif operation == 'lemmatize':
                return self._lemmatize(params, start_time)
            elif operation == 'stem':
                return self._stem(params, start_time)
            elif operation == 'clean':
                return self._clean(params, start_time)
            elif operation == 'split_sentences':
                return self._split_sentences(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Text processing error: {str(e)}",
                duration=time.time() - start_time
            )

    def _tokenize(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Tokenize text into words."""
        text = params.get('text', '')
        remove_stopwords = params.get('remove_stopwords', False)

        tokens = re.findall(r'\b\w+\b', text.lower())
        if remove_stopwords:
            tokens = [t for t in tokens if t not in self._stopwords]

        return ActionResult(
            success=True,
            message=f"Tokenized into {len(tokens)} tokens",
            data={'tokens': tokens, 'count': len(tokens), 'unique_count': len(set(tokens))},
            duration=time.time() - start_time
        )

    def _ner(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform Named Entity Recognition."""
        text = params.get('text', '')

        entities: List[EntityInfo] = []

        person_patterns = re.finditer(r'\b([A-Z][a-z]+ [A-Z][a-z]+)\b', text)
        for match in person_patterns:
            entities.append(EntityInfo(match.group(), EntityType.PERSON, match.start(), match.end(), 0.95))

        org_patterns = re.finditer(r'\b([A-Z][a-z]+ (Inc|Corp|Ltd|LLC|Company|Technologies))\b', text)
        for match in org_patterns:
            entities.append(EntityInfo(match.group(), EntityType.ORGANIZATION, match.start(), match.end(), 0.90))

        location_patterns = re.finditer(r'\b(in|at|to|from) ([A-Z][a-z]+)\b', text)
        for match in location_patterns:
            entities.append(EntityInfo(match.group(2), EntityType.LOCATION, match.start(2), match.end(2), 0.85))

        date_patterns = re.finditer(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\w+ \d{1,2},? \d{4})\b', text)
        for match in date_patterns:
            entities.append(EntityInfo(match.group(), EntityType.DATE, match.start(), match.end(), 0.95))

        email_patterns = re.finditer(r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', text)
        for match in email_patterns:
            entities.append(EntityInfo(match.group(), EntityType.EMAIL, match.start(), match.end(), 0.99))

        url_patterns = re.finditer(r'\b(https?://[^\s]+)\b', text)
        for match in url_patterns:
            entities.append(EntityInfo(match.group(), EntityType.URL, match.start(), match.end(), 0.99))

        money_patterns = re.finditer(r'\b(\$[\d,]+(?:\.\d{2})?|\d+ (?:dollars?))\b', text)
        for match in money_patterns:
            entities.append(EntityInfo(match.group(), EntityType.MONEY, match.start(), match.end(), 0.95))

        return ActionResult(
            success=True,
            message=f"Found {len(entities)} entities",
            data={
                'entities': [
                    {'text': e.text, 'type': e.entity_type.value, 'start': e.start_pos, 'end': e.end_pos, 'confidence': e.confidence}
                    for e in entities
                ],
                'by_type': {et.value: [e.text for e in entities if e.entity_type == et] for et in set(e.entity_type for e in entities)}
            },
            duration=time.time() - start_time
        )

    def _statistics(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate text statistics."""
        text = params.get('text', '')

        char_count = len(text)
        words = re.findall(r'\b\w+\b', text)
        word_count = len(words)
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        sentence_count = len(sentences)
        paragraphs = text.split('\n\n')
        paragraph_count = len([p for p in paragraphs if p.strip()])

        unique_words = len(set(w.lower() for w in words))
        vocab_richness = unique_words / word_count if word_count > 0 else 0.0

        avg_word_len = sum(len(w) for w in words) / word_count if word_count > 0 else 0.0
        avg_sent_len = word_count / sentence_count if sentence_count > 0 else 0.0

        stats = TextStatistics(
            char_count=char_count,
            word_count=word_count,
            sentence_count=sentence_count,
            paragraph_count=paragraph_count,
            avg_word_length=round(avg_word_len, 2),
            avg_sentence_length=round(avg_sent_len, 1),
            unique_words=unique_words,
            vocabulary_richness=round(vocab_richness, 3)
        )

        return ActionResult(
            success=True,
            message="Text statistics calculated",
            data={
                'char_count': stats.char_count,
                'word_count': stats.word_count,
                'sentence_count': stats.sentence_count,
                'paragraph_count': stats.paragraph_count,
                'avg_word_length': stats.avg_word_length,
                'avg_sentence_length': stats.avg_sentence_length,
                'unique_words': stats.unique_words,
                'vocabulary_richness': stats.vocabulary_richness,
            },
            duration=time.time() - start_time
        )

    def _pos_tag(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Part-of-speech tagging."""
        text = params.get('text', '')
        tokens = re.findall(r'\b\w+\b', text)

        pos_tags = []
        for i, token in enumerate(tokens):
            token_lower = token.lower()
            if token_lower in {'the', 'a', 'an'}:
                tag = 'DET'
            elif token_lower in {'is', 'are', 'was', 'were', 'be', 'been', 'being'}:
                tag = 'VERB'
            elif token_lower in {'and', 'but', 'or', 'not'}:
                tag = 'CONJ'
            elif token_lower in {'in', 'on', 'at', 'to', 'for', 'with', 'from', 'by'}:
                tag = 'ADP'
            elif token[0].isupper():
                tag = 'PROPN'
            elif token_lower.endswith('ly'):
                tag = 'ADV'
            elif token_lower.endswith('ing') or token_lower.endswith('ed'):
                tag = 'VERB'
            elif token_lower.endswith('s') and len(token) > 3:
                tag = 'NOUN'
            else:
                tag = 'NOUN'

            pos_tags.append({'token': token, 'tag': tag, 'index': i})

        return ActionResult(
            success=True,
            message=f"POS tagged {len(pos_tags)} tokens",
            data={'pos_tags': pos_tags, 'count': len(pos_tags)},
            duration=time.time() - start_time
        )

    def _lemmatize(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Lemmatize text."""
        text = params.get('text', '')
        tokens = re.findall(r'\b\w+\b', text.lower())

        lemmas = []
        for token in tokens:
            lemma = token
            if token.endswith('ies'):
                lemma = token[:-3] + 'y'
            elif token.endswith('ing'):
                lemma = token[:-3]
            elif token.endswith('ed') and len(token) > 4:
                lemma = token[:-2]
            elif token.endswith('s') and len(token) > 3 and not token.endswith('ss'):
                lemma = token[:-1]
            lemmas.append(lemma)

        return ActionResult(
            success=True,
            message=f"Lemmatized {len(lemmas)} tokens",
            data={'lemmas': lemmas, 'count': len(lemmas)},
            duration=time.time() - start_time
        )

    def _stem(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Stem text using Porter-like algorithm."""
        text = params.get('text', '')
        tokens = re.findall(r'\b\w+\b', text.lower())

        def simple_stem(word: str) -> str:
            if len(word) < 4:
                return word
            if word.endswith('ing'):
                return word[:-3]
            if word.endswith('ed'):
                return word[:-2]
            if word.endswith('ly'):
                return word[:-2]
            if word.endswith('tion'):
                return word[:-4]
            if word.endswith('ness'):
                return word[:-4]
            if word.endswith('ment'):
                return word[:-4]
            if word.endswith('able') or word.endswith('ible'):
                return word[:-4]
            if word.endswith('ful'):
                return word[:-3]
            if word.endswith('less'):
                return word[:-4]
            if word.endswith('est'):
                return word[:-3]
            if word.endswith('er'):
                return word[:-2]
            if word.endswith('s') and len(word) > 3:
                return word[:-1]
            return word

        stems = [simple_stem(t) for t in tokens]

        return ActionResult(
            success=True,
            message=f"Stemmed {len(stems)} tokens",
            data={'stems': stems, 'count': len(stems)},
            duration=time.time() - start_time
        )

    def _clean(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clean text."""
        text = params.get('text', '')
        remove_urls = params.get('remove_urls', True)
        remove_emails = params.get('remove_emails', True)
        remove_numbers = params.get('remove_numbers', False)
        remove_extra_spaces = params.get('remove_extra_spaces', True)
        lowercase = params.get('lowercase', False)

        cleaned = text
        if remove_urls:
            cleaned = re.sub(r'https?://\S+', '', cleaned)
        if remove_emails:
            cleaned = re.sub(r'\S+@\S+', '', cleaned)
        if remove_numbers:
            cleaned = re.sub(r'\d+', '', cleaned)
        if remove_extra_spaces:
            cleaned = re.sub(r'\s+', ' ', cleaned)
        if lowercase:
            cleaned = cleaned.lower()

        return ActionResult(
            success=True,
            message="Text cleaned",
            data={'cleaned_text': cleaned.strip(), 'original_length': len(text), 'cleaned_length': len(cleaned.strip())},
            duration=time.time() - start_time
        )

    def _split_sentences(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Split text into sentences."""
        text = params.get('text', '')
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]

        return ActionResult(
            success=True,
            message=f"Split into {len(sentences)} sentences",
            data={'sentences': sentences, 'count': len(sentences)},
            duration=time.time() - start_time
        )
