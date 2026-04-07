"""
Text processing and NLP utilities.

Provides text cleaning, parsing, sentiment analysis utilities,
and common NLP preprocessing functions.
"""
from __future__ import annotations

import html
import re
import unicodedata
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np


def remove_html_tags(text: str) -> str:
    """
    Remove HTML tags from text.

    Args:
        text: Input text

    Returns:
        Text without HTML tags

    Example:
        >>> remove_html_tags("<p>Hello <b>world</b></p>")
        'Hello world'
    """
    return re.sub(r"<[^>]+>", "", text)


def decode_html_entities(text: str) -> str:
    """
    Decode HTML entities.

    Args:
        text: Input text with HTML entities

    Returns:
        Decoded text

    Example:
        >>> decode_html_entities("&lt;3 &amp; &quot;quotes&quot;")
        '<3 & "quotes"'
    """
    return html.unescape(text)


def remove_urls(text: str) -> str:
    """
    Remove URLs from text.

    Args:
        text: Input text

    Returns:
        Text without URLs

    Example:
        >>> remove_urls("Check https://example.com for info")
        'Check  for info'
    """
    return re.sub(r"https?://\S+|www\.\S+", "", text)


def remove_emails(text: str) -> str:
    """
    Remove email addresses from text.

    Args:
        text: Input text

    Returns:
        Text without emails

    Example:
        >>> remove_emails("Contact user@example.com for help")
        'Contact  for help'
    """
    return re.sub(r"\S+@\S+", "", text)


def remove_mentions(text: str) -> str:
    """
    Remove @mentions from text.

    Args:
        text: Input text

    Returns:
        Text without mentions
    """
    return re.sub(r"@\w+", "", text)


def remove_hashtags(text: str) -> str:
    """
    Remove #hashtags from text.

    Args:
        text: Input text

    Returns:
        Text without hashtags
    """
    return re.sub(r"#\w+", "", text)


def normalize_unicode(text: str) -> str:
    """
    Normalize unicode characters.

    Args:
        text: Input text

    Returns:
        Normalized text

    Example:
        >>> normalize_unicode("café")
        'café'
    """
    return unicodedata.normalize("NFKD", text)


def remove_accents(text: str) -> str:
    """
    Remove accent marks from text.

    Args:
        text: Input text

    Returns:
        Text without accents

    Example:
        >>> remove_accents("café")
        'cafe'
    """
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def expand_contractions(text: str) -> str:
    """
    Expand common English contractions.

    Args:
        text: Input text

    Returns:
        Text with expanded contractions

    Example:
        >>> expand_contractions("I'm don't won't")
        'I am do not will not'
    """
    contractions = {
        "n't": " not",
        "'re": " are",
        "'s": " is",
        "'d": " would",
        "'ll": " will",
        "'ve": " have",
        "'m": " am",
    }
    for contraction, expansion in contractions.items():
        text = text.replace(contraction, expansion)
    return text


def remove_repeated_chars(text: str, max_repeat: int = 2) -> str:
    """
    Remove repeated characters beyond max_repeat.

    Args:
        text: Input text
        max_repeat: Maximum allowed repetitions

    Returns:
        Cleaned text

    Example:
        >>> remove_repeated_chars("helloooooo")
        'helloo'
    """
    return re.sub(r"(.)\1{%d,}" % max_repeat, r"\1" * max_repeat, text)


def extract_emojis(text: str) -> List[str]:
    """
    Extract all emojis from text.

    Args:
        text: Input text

    Returns:
        List of emojis

    Example:
        >>> extract_emojis("Hello 👋🎉")
        ['👋', '🎉']
    """
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.findall(text)


def count_words(text: str) -> int:
    """Count words in text."""
    return len(re.findall(r"\b\w+\b", text))


def count_sentences(text: str) -> int:
    """Count sentences in text."""
    return len(re.split(r"[.!?]+", text))


def count_syllables(word: str) -> int:
    """
    Estimate syllable count for a word.

    Args:
        word: Input word

    Returns:
        Estimated syllable count
    """
    word = word.lower()
    count = 0
    vowels = "aeiouy"
    if word[0] in vowels:
        count += 1
    for i in range(1, len(word)):
        if word[i] in vowels and word[i - 1] not in vowels:
            count += 1
    if word.endswith("e"):
        count -= 1
    if word.endswith("le") and len(word) > 2 and word[-3] not in vowels:
        count += 1
    if count == 0:
        count = 1
    return count


def flesch_reading_ease(text: str) -> float:
    """
    Calculate Flesch Reading Ease score.

    Args:
        text: Input text

    Returns:
        Reading ease score (higher = easier)
    """
    words = re.findall(r"\b\w+\b", text)
    sentences = len(re.split(r"[.!?]+", text)) - 1
    syllables = sum(count_syllables(w) for w in words)
    if len(words) == 0 or sentences == 0:
        return 0.0
    return (
        206.835
        - 1.015 * (len(words) / sentences)
        - 84.6 * (syllables / len(words))
    )


def simple_sentiment(text: str) -> float:
    """
    Simple lexicon-based sentiment analysis.

    Args:
        text: Input text

    Returns:
        Sentiment score (-1 to 1)

    Note:
        This is a placeholder. Real implementation requires
        a sentiment lexicon or trained model.
    """
    positive_words = {
        "good", "great", "excellent", "amazing", "wonderful", "fantastic",
        "love", "like", "best", "happy", "joy", "beautiful", "perfect",
        "awesome", "brilliant", "superb", "outstanding", "positive",
    }
    negative_words = {
        "bad", "terrible", "awful", "horrible", "hate", "worst",
        "poor", "sad", "disappointing", "disappoint", "negative",
        "ugly", "boring", "dreadful", "inferior", "wrong",
    }
    words = text.lower().split()
    pos_count = sum(1 for w in words if w in positive_words)
    neg_count = sum(1 for w in words if w in negative_words)
    total = pos_count + neg_count
    if total == 0:
        return 0.0
    return (pos_count - neg_count) / total


class TextCleaner:
    """Composable text cleaning pipeline."""

    def __init__(self, steps: List[Callable] = None):
        self.steps = steps or []

    def add(self, step: Callable) -> "TextCleaner":
        """Add a cleaning step."""
        self.steps.append(step)
        return self

    def clean(self, text: str) -> str:
        """Apply all cleaning steps."""
        for step in self.steps:
            text = step(text)
        return text

    def __call__(self, text: str) -> str:
        return self.clean(text)


def create_text_cleaner(mode: str = "basic") -> TextCleaner:
    """
    Create a pre-configured text cleaner.

    Args:
        mode: Cleaning mode ('basic', 'aggressive', 'minimal')

    Returns:
        Configured TextCleaner
    """
    cleaner = TextCleaner()
    if mode == "basic":
        cleaner.add(remove_html_tags)
        cleaner.add(decode_html_entities)
        cleaner.add(normalize_unicode)
    elif mode == "aggressive":
        cleaner.add(remove_html_tags)
        cleaner.add(decode_html_entities)
        cleaner.add(normalize_unicode)
        cleaner.add(remove_urls)
        cleaner.add(remove_emails)
        cleaner.add(remove_mentions)
        cleaner.add(remove_hashtags)
        cleaner.add(expand_contractions)
        cleaner.add(remove_repeated_chars)
        cleaner.add(remove_punctuation)
    elif mode == "minimal":
        cleaner.add(normalize_unicode)
    return cleaner


def remove_punctuation(text: str) -> str:
    """Remove punctuation from text."""
    return re.sub(r"[^\w\s]", " ", text)


def tokenize_by_character(
    text: str, n_gram: int = 1, stride: int = 1
) -> List[str]:
    """
    Character-level tokenization.

    Args:
        text: Input text
        n_gram: Character n-gram size
        stride: Step size between n-grams

    Returns:
        List of character n-grams

    Example:
        >>> tokenize_by_character("hello", n_gram=2)
        ['he', 'el', 'll', 'lo']
    """
    text = text.replace(" ", "")
    ngrams = []
    for i in range(0, len(text) - n_gram + 1, stride):
        ngrams.append(text[i : i + n_gram])
    return ngrams


def extract_keywords_tfidf(
    documents: List[str], top_k: int = 10
) -> List[List[Tuple[str, float]]]:
    """
    Extract keywords using TF-IDF.

    Args:
        documents: List of documents
        top_k: Number of top keywords per document

    Returns:
        List of (word, score) tuples per document
    """
    from utils.feature_extraction_utils import tf_idf
    doc_words = [text.lower().split() for text in documents]
    all_words = set(word for doc in doc_words for word in doc)
    word_to_idx = {w: i for i, w in enumerate(all_words)}
    tfidf, vocab = tf_idf(documents, max_features=len(all_words))
    results = []
    for doc_tfidf in tfidf:
        top_indices = np.argsort(doc_tfidf)[-top_k:][::-1]
        results.append([(vocab[i], doc_tfidf[i]) for i in top_indices if doc_tfidf[i] > 0])
    return results


def extract_keywords_text_rank(text: str, top_k: int = 10) -> List[str]:
    """
    Extract keywords using TextRank algorithm.

    Args:
        text: Input text
        top_k: Number of keywords

    Returns:
        List of keywords

    Note:
        Simplified version. Real TextRank uses graph-based ranking.
    """
    from utils.text_similarity_utils import jaccard_similarity
    words = re.findall(r"\b\w+\b", text.lower())
    unique_words = list(set(words))
    if len(unique_words) <= top_k:
        return unique_words
    scores = {}
    for word in unique_words:
        score = sum(1 for w in unique_words if w != word and word in w or jaccard_similarity([word], [w]) > 0.3)
        scores[word] = score
    return sorted(scores.keys(), key=lambda w: scores[w], reverse=True)[:top_k]


def detect_language_simple(text: str) -> str:
    """
    Simple language detection based on character distributions.

    Args:
        text: Input text

    Returns:
        Detected language code ('en', 'es', 'fr', 'de', etc.)

    Note:
        This is a heuristic placeholder. Use langdetect for production.
    """
    lang_patterns = {
        "en": {"the", "is", "are", "and", "to", "of", "a", "in"},
        "es": {"el", "la", "de", "que", "es", "en", "los", "las"},
        "fr": {"le", "la", "les", "de", "est", "et", "un", "une"},
        "de": {"der", "die", "und", "das", "ist", "in", "ein", "eine"},
    }
    text_lower = text.lower()
    scores = {}
    for lang, chars in lang_patterns.items():
        score = sum(1 for w in chars if f" {w} " in f" {text_lower} ")
        scores[lang] = score
    return max(scores.keys(), key=lambda k: scores[k]) if scores else "en"


def to_snake_case(text: str) -> str:
    """
    Convert text to snake_case.

    Args:
        text: Input text

    Returns:
        snake_case text

    Example:
        >>> to_snake_case("HelloWorld")
        'hello_world'
    """
    text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub("([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower().replace("__", "_").strip("_")


def to_camel_case(text: str) -> str:
    """
    Convert text to camelCase.

    Args:
        text: Input text

    Returns:
        camelCase text

    Example:
        >>> to_camel_case("hello_world")
        'helloWorld'
    """
    words = text.split("_")
    return words[0] + "".join(w.capitalize() for w in words[1:])


def to_pascal_case(text: str) -> str:
    """
    Convert text to PascalCase.

    Args:
        text: Input text

    Returns:
        PascalCase text

    Example:
        >>> to_pascal_case("hello_world")
        'HelloWorld'
    """
    words = text.split("_")
    return "".join(w.capitalize() for w in words)


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate text to maximum length.

    Args:
        text: Input text
        max_length: Maximum length
        suffix: Suffix for truncated text

    Returns:
        Truncated text

    Example:
        >>> truncate_text("Hello world", max_length=8)
        'Hello...'
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix
