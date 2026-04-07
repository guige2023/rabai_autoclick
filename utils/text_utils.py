"""Text processing utilities for RabAI AutoClick.

Provides:
- Text sanitization and cleaning
- String matching and similarity
- Text formatting
- Pattern extraction
"""

import difflib
import hashlib
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Callable, List, Optional, Tuple


def clean_whitespace(text: str, collapse: bool = True) -> str:
    """Clean whitespace in text.

    Args:
        text: Input text.
        collapse: If True, collapse multiple spaces.

    Returns:
        Cleaned text.
    """
    if collapse:
        text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


def remove_control_chars(text: str) -> str:
    """Remove control characters from text.

    Args:
        text: Input text.

    Returns:
        Text with control characters removed.
    """
    return ''.join(char for char in text if unicodedata.category(char)[0] != 'C' or char in '\n\r\t')


def truncate(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to maximum length.

    Args:
        text: Input text.
        max_length: Maximum length.
        suffix: Suffix to append if truncated.

    Returns:
        Truncated text.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def word_wrap(text: str, width: int = 80, break_long_words: bool = True) -> str:
    """Wrap text to specified width.

    Args:
        text: Input text.
        width: Maximum line width.
        break_long_words: If True, break long words.

    Returns:
        Wrapped text.
    """
    import textwrap
    return textwrap.fill(text, width=width, break_long_words=break_long_words)


def similarity(s1: str, s2: str) -> float:
    """Calculate similarity ratio between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity ratio (0.0 to 1.0).
    """
    return SequenceMatcher(None, s1, s2).ratio()


def jaro_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro similarity between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity score (0.0 to 1.0).
    """
    if s1 == s2:
        return 1.0

    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0

    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2

    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)

        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Edit distance.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text.

    Args:
        text: Input text.

    Returns:
        List of numbers as floats.
    """
    pattern = r'-?\d+\.?\d*'
    matches = re.findall(pattern, text)
    return [float(m) for m in matches if m]


def extract_urls(text: str) -> List[str]:
    """Extract URLs from text.

    Args:
        text: Input text.

    Returns:
        List of URLs.
    """
    pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    return re.findall(pattern, text)


def extract_emails(text: str) -> List[str]:
    """Extract email addresses from text.

    Args:
        text: Input text.

    Returns:
        List of email addresses.
    """
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.findall(pattern, text)


def slugify(text: str, lowercase: bool = True, max_length: Optional[int] = None) -> str:
    """Convert text to URL-friendly slug.

    Args:
        text: Input text.
        lowercase: If True, convert to lowercase.
        max_length: Maximum slug length.

    Returns:
        Slugified text.
    """
    # Normalize unicode
    text = unicodedata.normalize('NFKD', text)
    # Remove accents
    text = ''.join(c for c in text if not unicodedata.is_combining(c))
    # Replace spaces with hyphens
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    text = text.strip('-')

    if lowercase:
        text = text.lower()

    if max_length:
        text = text[:max_length].strip('-')

    return text


def md5_hash(text: str) -> str:
    """Calculate MD5 hash of text.

    Args:
        text: Input text.

    Returns:
        Hexadecimal hash string.
    """
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def sha256_hash(text: str) -> str:
    """Calculate SHA256 hash of text.

    Args:
        text: Input text.

    Returns:
        Hexadecimal hash string.
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def fuzzy_match(query: str, choices: List[str], limit: int = 5) -> List[Tuple[str, float]]:
    """Fuzzy match query against choices.

    Args:
        query: Search query.
        choices: List of strings to search.
        limit: Maximum number of results.

    Returns:
        List of (choice, score) tuples sorted by score.
    """
    scored = []
    query_lower = query.lower()

    for choice in choices:
        choice_lower = choice.lower()

        if query_lower == choice_lower:
            score = 1.0
        elif query_lower in choice_lower:
            score = 0.9
        elif choice_lower in query_lower:
            score = 0.8
        else:
            score = similarity(query_lower, choice_lower)

        scored.append((choice, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:limit]


def highlight_diff(text1: str, text2: str) -> List[Tuple[str, str]]:
    """Generate diff between two texts.

    Args:
        text1: First text.
        text2: Second text.

    Returns:
        List of (status, text) tuples where status is '-', '+', or ' '.
    """
    differ = difflib.Differ()
    diff = list(differ.compare(text1.splitlines(), text2.splitlines()))
    result = []

    for line in diff:
        if line.startswith('+'):
            result.append(('+', line[2:]))
        elif line.startswith('-'):
            result.append(('-', line[2:]))
        elif line.startswith('?'):
            continue
        else:
            result.append((' ', line[2:]))

    return result


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: camelCase string.

    Returns:
        snake_case string.
    """
    text = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    text = re.sub('(.)([a-z0-9])([A-Z])', r'\1_\2\3', text)
    return text.lower()


def snake_to_camel(text: str, capitalize_first: bool = False) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: snake_case string.
        capitalize_first: If True, capitalize first letter.

    Returns:
        camelCase string.
    """
    components = text.split('_')
    result = components[0]
    for component in components[1:]:
        result += component.capitalize()

    if capitalize_first:
        result = result.capitalize()

    return result


def remove_emoji(text: str) -> str:
    """Remove emoji characters from text.

    Args:
        text: Input text.

    Returns:
        Text with emojis removed.
    """
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.sub('', text)


def count_words(text: str) -> int:
    """Count words in text.

    Args:
        text: Input text.

    Returns:
        Word count.
    """
    return len(re.findall(r'\b\w+\b', text))


def count_lines(text: str) -> int:
    """Count lines in text.

    Args:
        text: Input text.

    Returns:
        Line count.
    """
    return len(text.splitlines())
