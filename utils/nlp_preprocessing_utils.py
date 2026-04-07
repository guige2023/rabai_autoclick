"""
NLP preprocessing utilities.

Provides text cleaning, tokenization, and preprocessing functions
for natural language processing tasks.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Callable, List, Optional, Sequence, Tuple

import numpy as np


def lowercase(text: str) -> str:
    """Convert text to lowercase."""
    return text.lower()


def uppercase(text: str) -> str:
    """Convert text to uppercase."""
    return text.upper()


def remove_punctuation(text: str, keep_patterns: str = "") -> str:
    """
    Remove punctuation from text.

    Args:
        text: Input text
        keep_patterns: Patterns to keep (e.g., "!?.,")

    Returns:
        Text without punctuation

    Example:
        >>> remove_punctuation("Hello, world!")
        'Hello world'
    """
    pattern = r"[^\w\s" + re.escape(keep_patterns) + r"]"
    return re.sub(pattern, "", text)


def remove_whitespace(text: str) -> str:
    """Remove all whitespace from text."""
    return re.sub(r"\s+", "", text)


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace to single spaces."""
    return re.sub(r"\s+", " ", text).strip()


def remove_numbers(text: str) -> str:
    """Remove all digits from text."""
    return re.sub(r"\d+", "", text)


def remove_special_chars(text: str, keep_patterns: str = "") -> str:
    """
    Remove special characters.

    Args:
        text: Input text
        keep_patterns: Regex patterns to keep

    Returns:
        Cleaned text

    Example:
        >>> remove_special_chars("Hello! @world #123", keep_patterns="@")
        'Hello  @world '
    """
    pattern = r"[^a-zA-Z0-9\s" + re.escape(keep_patterns) + r"]"
    return re.sub(pattern, "", text)


def normalize_unicode(text: str, form: str = "NFKD") -> str:
    """
    Normalize unicode characters.

    Args:
        text: Input text
        form: Normalization form (NFKD, NFC, NFKC, NFKC)

    Returns:
        Normalized text

    Example:
        >>> normalize_unicode("café")
        'café'
    """
    return unicodedata.normalize(form, text)


def remove_accents(text: str) -> str:
    """Remove accent marks from text."""
    nfd = unicodedata.normalize("NFD", text)
    return "".join(char for char in nfd if unicodedata.category(char) != "Mn")


def tokenize_words(text: str) -> List[str]:
    """
    Tokenize text into words.

    Args:
        text: Input text

    Returns:
        List of word tokens

    Example:
        >>> tokenize_words("Hello, world!")
        ['Hello', 'world']
    """
    return re.findall(r"\b\w+\b", text.lower())


def tokenize_sentences(text: str) -> List[str]:
    """
    Tokenize text into sentences.

    Args:
        text: Input text

    Returns:
        List of sentences

    Example:
        >>> tokenize_sentences("Hello! How are you? I'm fine.")
        ['Hello!', 'How are you?', "I'm fine."]
    """
    return re.split(r"(?<=[.!?])\s+", text)


def tokenize_ngrams(
    text: str, n: int = 2, tokenizer: Callable[[str], List[str]] = None
) -> List[Tuple[str, ...]]:
    """
    Create n-grams from text.

    Args:
        text: Input text
        n: N-gram size
        tokenizer: Custom tokenizer function

    Returns:
        List of n-gram tuples

    Example:
        >>> tokenize_ngrams("the quick brown fox", n=2)
        [('the', 'quick'), ('quick', 'brown'), ('brown', 'fox')]
    """
    if tokenizer is None:
        tokens = tokenize_words(text)
    else:
        tokens = tokenizer(text)
    return [tuple(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


def pad_sequence(
    sequence: List[int], max_length: int, pad_token: int = 0, truncating: str = "post"
) -> List[int]:
    """
    Pad or truncate sequence to fixed length.

    Args:
        sequence: Input sequence of token IDs
        max_length: Target length
        pad_token: Token to use for padding
        truncating: Truncation strategy (post or pre)

    Returns:
        Padded/truncated sequence

    Example:
        >>> pad_sequence([1, 2, 3, 4, 5], 3)
        [1, 2, 3]
        >>> pad_sequence([1, 2], 4, pad_token=0)
        [1, 2, 0, 0]
    """
    if len(sequence) > max_length:
        if truncating == "post":
            return sequence[:max_length]
        else:
            return sequence[-max_length:]
    padding = [pad_token] * (max_length - len(sequence))
    return sequence + padding


def build_vocab(
    texts: Sequence[str],
    min_freq: int = 1,
    max_vocab: int = None,
    tokenizer: Callable[[str], List[str]] = None,
) -> Tuple[dict, dict]:
    """
    Build vocabulary from texts.

    Args:
        texts: Collection of texts
        min_freq: Minimum word frequency
        max_vocab: Maximum vocabulary size
        tokenizer: Custom tokenizer

    Returns:
        Tuple of (word_to_idx, idx_to_word)

    Example:
        >>> word2idx, idx2word = build_vocab(["hello world", "hello"])
        >>> word2idx
        {'<unk>': 0, '<pad>': 1, 'hello': 2, 'world': 3}
    """
    if tokenizer is None:
        tokenizer = tokenize_words
    word_counts = {}
    for text in texts:
        for word in tokenizer(text.lower()):
            word_counts[word] = word_counts.get(word, 0) + 1
    vocab = [("<unk>", 0), ("<pad>", 1)]
    for word, count in sorted(word_counts.items(), key=lambda x: -x[1]):
        if count >= min_freq:
            vocab.append((word, count))
        if max_vocab and len(vocab) >= max_vocab:
            break
    word_to_idx = {w: i for i, w in enumerate(vocab)}
    idx_to_word = {i: w for w, i in word_to_idx.items()}
    return word_to_idx, idx_to_word


def text_to_sequence(
    text: str,
    word_to_idx: dict,
    tokenizer: Callable[[str], List[str]] = None,
    unknown_token: str = "<unk>",
) -> List[int]:
    """
    Convert text to sequence of indices.

    Args:
        text: Input text
        word_to_idx: Vocabulary mapping
        tokenizer: Custom tokenizer
        unknown_token: Token for unknown words

    Returns:
        List of token indices

    Example:
        >>> text_to_sequence("hello world", {"hello": 2, "world": 3})
        [2, 3]
    """
    if tokenizer is None:
        tokens = tokenize_words(text.lower())
    else:
        tokens = tokenizer(text.lower())
    unk_idx = word_to_idx.get(unknown_token, 0)
    return [word_to_idx.get(word, unk_idx) for word in tokens]


def sliding_window(
    sequence: List[int], window_size: int, step: int = 1
) -> List[List[int]]:
    """
    Create sliding windows over sequence.

    Args:
        sequence: Input sequence
        window_size: Size of each window
        step: Step size between windows

    Returns:
        List of window sequences

    Example:
        >>> sliding_window([1, 2, 3, 4, 5], 3)
        [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    """
    windows = []
    for i in range(0, len(sequence) - window_size + 1, step):
        windows.append(sequence[i : i + window_size])
    return windows


def text_augmentation_synonym(
    text: str, n_augmentations: int = 1
) -> List[str]:
    """
    Simple synonym-based text augmentation (placeholder).
    In production, use libraries like nltk or textattack.

    Args:
        text: Input text
        n_augmentations: Number of augmented texts to generate

    Returns:
        List of augmented texts

    Note:
        This is a placeholder. Real implementation requires
        a synonym dictionary or word embedding model.
    """
    synonyms = {
        "happy": ["joyful", "cheerful", "pleased"],
        "sad": ["unhappy", "sorrowful", "down"],
        "big": ["large", "huge", "enormous"],
        "small": ["tiny", "little", "compact"],
    }
    words = tokenize_words(text.lower())
    augmented = []
    for _ in range(n_augmentations):
        new_words = []
        for word in words:
            if word in synonyms:
                import random
                new_words.append(random.choice(synonyms[word]))
            else:
                new_words.append(word)
        augmented.append(" ".join(new_words))
    return augmented


class TextPreprocessor:
    """Composable text preprocessing pipeline."""

    def __init__(self, steps: List[Callable[[str], str]] = None):
        self.steps = steps or []

    def add(self, step: Callable[[str], str]) -> "TextPreprocessor":
        """Add a preprocessing step."""
        self.steps.append(step)
        return self

    def process(self, text: str) -> str:
        """Apply all preprocessing steps."""
        for step in self.steps:
            text = step(text)
        return text

    def __call__(self, text: str) -> str:
        return self.process(text)


def create_basic_preprocessor(
    lowercase: bool = True,
    remove_punct: bool = True,
    normalize_unicode: bool = True,
) -> TextPreprocessor:
    """
    Create basic text preprocessor.

    Args:
        lowercase: Convert to lowercase
        remove_punct: Remove punctuation
        normalize_unicode: Normalize unicode

    Returns:
        Configured TextPreprocessor

    Example:
        >>> preprocessor = create_basic_preprocessor()
        >>> preprocessor("Hello, WORLD! café")
        'hello world cafe'
    """
    p = TextPreprocessor()
    if normalize_unicode:
        p.add(normalize_unicode)
        p.add(remove_accents)
    if lowercase:
        p.add(lowercase)
    if remove_punct:
        p.add(remove_punctuation)
    p.add(normalize_whitespace)
    return p
