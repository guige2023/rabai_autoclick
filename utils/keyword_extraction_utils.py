"""
Keyword extraction and text summarization utilities.

Provides TF-IDF, RAKE, and TextRank-based keyword extraction
for document analysis and topic identification.

Example:
    >>> from utils.keyword_extraction_utils import extract_keywords, TFIDFExtractor
    >>> keywords = extract_keywords(document, top_k=10)
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


@dataclass
class KeywordResult:
    """Keyword extraction result."""
    word: str
    score: float
    rank: int


class TextPreprocessor:
    """
    Text preprocessing for keyword extraction.

    Handles tokenization, stop word removal, and normalization.
    """

    DEFAULT_STOPWORDS: Set[str] = {
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
        "be", "have", "has", "had", "do", "does", "did", "will", "would",
        "should", "could", "may", "might", "must", "can", "this", "that",
        "these", "those", "i", "you", "he", "she", "it", "we", "they",
        "what", "which", "who", "whom", "whose", "where", "when", "why",
        "how", "all", "each", "every", "both", "few", "more", "most",
        "other", "some", "such", "no", "nor", "not", "only", "own",
        "same", "so", "than", "too", "very", "just", "also", "now",
    }

    def __init__(
        self,
        stopwords: Optional[Set[str]] = None,
        lowercase: bool = True,
        min_length: int = 3,
        max_length: int = 50,
    ) -> None:
        """
        Initialize the preprocessor.

        Args:
            stopwords: Set of stopwords to remove.
            lowercase: Convert to lowercase.
            min_length: Minimum token length.
            max_length: Maximum token length.
        """
        self.stopwords = stopwords or self.DEFAULT_STOPWORDS.copy()
        self.lowercase = lowercase
        self.min_length = min_length
        self.max_length = max_length

    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into words.

        Args:
            text: Input text.

        Returns:
            List of tokens.
        """
        if self.lowercase:
            text = text.lower()

        tokens = re.findall(r"\b[a-zA-Z]+\b", text)

        tokens = [
            t for t in tokens
            if self.min_length <= len(t) <= self.max_length
            and t not in self.stopwords
        ]

        return tokens

    def tokenize_ngrams(
        self,
        text: str,
        n: int = 2,
    ) -> List[str]:
        """
        Tokenize text into n-grams.

        Args:
            text: Input text.
            n: N-gram size.

        Returns:
            List of n-grams.
        """
        tokens = self.tokenize(text)
        ngrams: List[str] = []

        for i in range(len(tokens) - n + 1):
            ngram = " ".join(tokens[i : i + n])
            ngrams.append(ngram)

        return ngrams


class TFIDFExtractor:
    """
    TF-IDF based keyword extractor.

    Extracts keywords based on term frequency-inverse document
    frequency scoring.
    """

    def __init__(
        self,
        preprocessor: Optional[TextPreprocessor] = None,
        max_features: int = 100,
    ) -> None:
        """
        Initialize the TF-IDF extractor.

        Args:
            preprocessor: Text preprocessor.
            max_features: Maximum number of features to extract.
        """
        self.preprocessor = preprocessor or TextPreprocessor()
        self.max_features = max_features
        self._document_freq: Dict[str, int] = defaultdict(int)
        self._num_docs = 0
        self._vocabulary: Set[str] = set()

    def fit(self, documents: List[str]) -> "TFIDFExtractor":
        """
        Build vocabulary and document frequencies.

        Args:
            documents: List of documents.

        Returns:
            Self for chaining.
        """
        self._num_docs = len(documents)
        self._document_freq.clear()
        self._vocabulary.clear()

        for doc in documents:
            tokens = set(self.preprocessor.tokenize(doc))
            for token in tokens:
                self._document_freq[token] += 1
            self._vocabulary.update(tokens)

        return self

    def extract_keywords(
        self,
        document: str,
        top_k: int = 10,
    ) -> List[KeywordResult]:
        """
        Extract keywords from a document.

        Args:
            document: Input document.
            top_k: Number of keywords to extract.

        Returns:
            List of KeywordResult objects.
        """
        tokens = self.preprocessor.tokenize(document)
        token_counts = Counter(tokens)

        scores: Dict[str, float] = {}

        for token, count in token_counts.items():
            tf = count
            df = self._document_freq.get(token, 1)
            idf = math.log(self._num_docs / df) if df > 0 else 0
            scores[token] = tf * idf

        sorted_tokens = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        results: List[KeywordResult] = []
        for rank, (word, score) in enumerate(sorted_tokens[:top_k], 1):
            results.append(KeywordResult(word=word, score=score, rank=rank))

        return results

    def get_tfidf(
        self,
        document: str,
    ) -> Dict[str, float]:
        """
        Get TF-IDF scores for all tokens in a document.

        Args:
            document: Input document.

        Returns:
            Dictionary of token -> TF-IDF score.
        """
        tokens = self.preprocessor.tokenize(document)
        token_counts = Counter(tokens)
        scores: Dict[str, float] = {}

        for token, count in token_counts.items():
            tf = count
            df = self._document_freq.get(token, 1)
            idf = math.log(self._num_docs / df) if df > 0 else 0
            scores[token] = tf * idf

        return scores


class RAKEExtractor:
    """
    Rapid Automatic Keyword Extraction (RAKE).

    Extracts keywords based on word co-occurrence in candidate
    phrases, without requiring document frequency information.
    """

    def __init__(
        self,
        preprocessor: Optional[TextPreprocessor] = None,
        min_word_freq: int = 1,
        min_phrase_length: int = 2,
        max_phrase_length: int = 5,
    ) -> None:
        """
        Initialize the RAKE extractor.

        Args:
            preprocessor: Text preprocessor.
            min_word_freq: Minimum word frequency.
            min_phrase_length: Minimum phrase length.
            max_phrase_length: Maximum phrase length.
        """
        self.preprocessor = preprocessor or TextPreprocessor()
        self.min_word_freq = min_word_freq
        self.min_phrase_length = min_phrase_length
        self.max_phrase_length = max_phrase_length

    def extract_keywords(
        self,
        document: str,
        top_k: int = 10,
    ) -> List[KeywordResult]:
        """
        Extract keywords using RAKE algorithm.

        Args:
            document: Input document.
            top_k: Number of keywords to extract.

        Returns:
            List of KeywordResult objects.
        """
        sentences = re.split(r"[.!?\n]", document)
        sentences = [s.strip() for s in sentences if s.strip()]

        word_freq = Counter()
        word_scores: Dict[str, float] = {}
        phrase_scores: Dict[str, float] = defaultdict(float)

        for sentence in sentences:
            words = re.findall(r"\b[a-zA-Z]+\b", sentence.lower())
            words = [w for w in words if len(w) >= self.min_phrase_length]

            for word in words:
                word_freq[word] += 1

            for i in range(len(words)):
                for j in range(i + 1, min(len(words), i + self.max_phrase_length)):
                    phrase = " ".join(words[i:j])
                    phrase_scores[phrase] += 1

        for word, freq in word_freq.items():
            if freq >= self.min_word_freq:
                word_scores[word] = freq

        for phrase, count in phrase_scores.items():
            phrase_words = phrase.split()
            if len(phrase_words) >= self.min_phrase_length:
                score = sum(word_scores.get(w, 0) for w in phrase_words) / (len(phrase_words) ** 2)
                phrase_scores[phrase] = score

        sorted_phrases = sorted(phrase_scores.items(), key=lambda x: x[1], reverse=True)

        results: List[KeywordResult] = []
        seen: Set[str] = set()

        for phrase, score in sorted_phrases:
            if phrase.lower() not in seen:
                seen.add(phrase.lower())
                results.append(KeywordResult(
                    word=phrase,
                    score=score,
                    rank=len(results) + 1,
                ))
                if len(results) >= top_k:
                    break

        return results


def extract_keywords(
    document: str,
    method: str = "tfidf",
    top_k: int = 10,
    documents: Optional[List[str]] = None,
) -> List[KeywordResult]:
    """
    Convenience function to extract keywords.

    Args:
        document: Input document.
        method: Extraction method ('tfidf', 'rake').
        top_k: Number of keywords to extract.
        documents: Documents for TF-IDF fitting.

    Returns:
        List of KeywordResult objects.
    """
    if method == "tfidf":
        extractor = TFIDFExtractor()
        if documents:
            extractor.fit(documents)
        return extractor.extract_keywords(document, top_k=top_k)
    elif method == "rake":
        extractor = RAKEExtractor()
        return extractor.extract_keywords(document, top_k=top_k)
    else:
        raise ValueError(f"Unknown method: {method}")
