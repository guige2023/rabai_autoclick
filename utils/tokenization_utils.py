"""
Natural language tokenization and text processing utilities.

Provides word tokenizers, sentence tokenizers, n-gram generation,
stopword removal, and basic NLP preprocessing.
"""

from __future__ import annotations

import re
from typing import Callable


def tokenize_words(text: str, lowercase: bool = True) -> list[str]:
    """
    Tokenize text into words.

    Args:
        text: Input text
        lowercase: Convert to lowercase

    Returns:
        List of word tokens.
    """
    if lowercase:
        text = text.lower()
    return re.findall(r"\b[a-zA-Z]+\b", text)


def tokenize_sentences(text: str) -> list[str]:
    """Split text into sentences."""
    sentences = re.split(r"[.!?]+\s+", text)
    return [s.strip() for s in sentences if s.strip()]


def tokenize_whitespace(text: str) -> list[str]:
    """Split on whitespace."""
    return text.split()


def tokenize_by_pattern(text: str, pattern: str) -> list[str]:
    """Split by regex pattern."""
    return re.split(pattern, text)


def char_ngrams(text: str, n: int = 3) -> list[str]:
    """
    Generate character n-grams.

    Args:
        text: Input text
        n: N-gram size

    Returns:
        List of character n-grams.
    """
    if len(text) < n:
        return [text]
    return [text[i:i+n] for i in range(len(text) - n + 1)]


def word_ngrams(words: list[str], n: int = 2) -> list[list[str]]:
    """
    Generate word n-grams.

    Args:
        words: List of words
        n: N-gram size

    Returns:
        List of word n-grams.
    """
    if len(words) < n:
        return []
    return [words[i:i+n] for i in range(len(words) - n + 1)]


def skip_ngrams(words: list[str], n: int = 2, skip: int = 1) -> list[list[str]]:
    """
    Generate skip n-grams.

    Args:
        words: List of words
        n: N-gram size
        skip: Number of words to skip

    Returns:
        List of skip n-grams.
    """
    result: list[list[str]] = []
    for i in range(len(words) - (n - 1) * (skip + 1) + 1):
        ngram = [words[i + j * (skip + 1)] for j in range(n)]
        result.append(ngram)
    return result


DEFAULT_STOPWORDS: set[str] = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
    "be", "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "must", "can", "need", "it", "its", "this",
    "that", "these", "those", "i", "you", "he", "she", "we", "they", "what",
    "which", "who", "whom", "when", "where", "why", "how", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such", "no",
    "nor", "not", "only", "own", "same", "so", "than", "too", "very",
    "just", "about", "into", "through", "during", "before", "after", "above",
    "below", "between", "under", "again", "further", "then", "once", "here",
    "there", "any", "up", "down", "out", "off", "over", "under", "because",
}


def remove_stopwords(
    words: list[str],
    stopwords: set[str] | None = None,
    case_sensitive: bool = False,
) -> list[str]:
    """
    Remove stopwords from word list.

    Args:
        words: List of words
        stopwords: Set of stopwords (default: DEFAULT_STOPWORDS)
        case_sensitive: Whether stopword matching is case-sensitive

    Returns:
        Filtered word list.
    """
    if stopwords is None:
        stopwords = DEFAULT_STOPWORDS
    if case_sensitive:
        return [w for w in words if w not in stopwords]
    return [w for w in words if w.lower() not in stopwords]


def stem_porter(word: str) -> str:
    """
    Simple Porter stemmer (simplified rules).

    Args:
        word: Input word

    Returns:
        Stemmed word.
    """
    if len(word) < 3:
        return word
    # Step 1a
    if word.endswith("sses"):
        word = word[:-2]
    elif word.endswith("ies"):
        word = word[:-2]
    elif word.endswith("ss"):
        pass
    elif word.endswith("s"):
        word = word[:-1]
    # Step 1b
    if word.endswith("eed") and len(word) > 4:
        word = word[:-1]
    # Step 1b continued
    if "v" in word:
        if word.endswith("ed"):
            word = word[:-2]
            if word.endswith("at") or word.endswith("bl") or word.endswith("iz"):
                word += "e"
        elif word.endswith("ing"):
            word = word[:-3]
    # Step 2
    suffixes = {
        "ational": "ate", "tional": "tion", "enci": "ence", "anci": "ance",
        "izer": "ize", "abli": "able", "alli": "al", "entli": "ent",
        "eli": "e", "ousli": "ous",
    }
    for suf, repl in suffixes.items():
        if word.endswith(suf):
            word = word[:-len(suf)] + repl
            break
    # Step 3
    suffixes3 = {
        "icate": "ic", "ative": "", "alize": "al", "iciti": "ic",
        "ical": "ic", "ful": "", "ness": "",
    }
    for suf, repl in suffixes3.items():
        if word.endswith(suf):
            word = word[:-len(suf)] + repl
            break
    # Step 4
    suffixes4 = [
        "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement",
        "ment", "ent", "ion", "ou", "ism", "ate", "iti", "ous", "ive", "ize",
    ]
    for suf in suffixes4:
        if word.endswith(suf):
            if word.endswith("ion") and len(word) > 4:
                word = word[:-3]
            elif len(word) > len(suf):
                word = word[:-len(suf)]
            break
    # Step 5a
    if word.endswith("e") and len(word) > 4:
        word = word[:-1]
    # Step 5b
    if len(word) > 3 and word.endswith("ll"):
        word = word[:-1]
    return word


def lemmatize_simple(word: str) -> str:
    """
    Simple lemmatization (rule-based).

    For production, use NLTK or spaCy.
    """
    word = word.lower()
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("ves"):
        return word[:-3] + "fe"
    if word.endswith("ses") or word.endswith("xes") or word.endswith("zes"):
        return word[:-2]
    if word.endswith("es") and len(word) > 3:
        return word[:-2]
    if word.endswith("ed") and len(word) > 4:
        if word.endswith("ied"):
            return word[:-3] + "y"
        return word[:-2]
    if word.endswith("ing") and len(word) > 5:
        return word[:-3]
    if word.endswith("ly") and len(word) > 4:
        return word[:-2]
    return word


def ngram_count(
    text: str,
    n: int = 2,
    lowercase: bool = True,
    remove_stop: bool = False,
) -> dict[str, int]:
    """
    Count n-gram frequencies.

    Args:
        text: Input text
        n: N-gram size
        lowercase: Convert to lowercase
        remove_stop: Remove stopwords

    Returns:
        Dictionary of n-gram -> count.
    """
    words = tokenize_words(text, lowercase=lowercase)
    if remove_stop:
        words = remove_stopwords(words)
    ngrams_list = word_ngrams(words, n)
    counts: dict[str, int] = {}
    for ng in ngrams_list:
        key = " ".join(ng)
        counts[key] = counts.get(key, 0) + 1
    return counts


def tf_idf_score(
    term: str,
    document: list[str],
    corpus: list[list[str]],
) -> float:
    """
    Compute TF-IDF score for a term.

    Args:
        term: Term to score
        document: List of terms in current document
        corpus: List of document term lists

    Returns:
        TF-IDF score.
    """
    tf = document.count(term) / len(document) if document else 0
    n_docs_with_term = sum(1 for doc in corpus if term in doc)
    idf = math.log(len(corpus) / n_docs_with_term) if n_docs_with_term > 0 else 0
    return tf * idf


def bag_of_words(
    words: list[str],
    max_features: int | None = None,
) -> dict[str, int]:
    """
    Create bag-of-words representation.

    Returns:
        Dictionary of word -> count.
    """
    counts: dict[str, int] = {}
    for w in words:
        counts[w] = counts.get(w, 0) + 1
    if max_features:
        sorted_words = sorted(counts.items(), key=lambda x: -x[1])
        counts = dict(sorted_words[:max_features])
    return counts


def word_frequency_rank(counts: dict[str, int]) -> list[tuple[str, int]]:
    """Rank words by frequency."""
    return sorted(counts.items(), key=lambda x: -x[1])


def text_similarity_jaccard(words1: list[str], words2: list[str]) -> float:
    """Jaccard similarity between two word lists."""
    set1 = set(words1)
    set2 = set(words2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def keyword_extraction_simple(
    text: str,
    top_n: int = 10,
    remove_stop: bool = True,
) -> list[str]:
    """
    Simple keyword extraction by frequency.

    Args:
        text: Input text
        top_n: Number of keywords
        remove_stop: Remove stopwords

    Returns:
        Top N keywords.
    """
    words = tokenize_words(text, lowercase=True)
    if remove_stop:
        words = remove_stopwords(words)
    counts = bag_of_words(words)
    ranked = word_frequency_rank(counts)
    return [word for word, _ in ranked[:top_n]]


import math
