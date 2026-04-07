"""
String utilities v2 — advanced text processing.

Companion to string_utils.py. Adds fuzzy matching, string metrics,
and advanced text transformations.
"""

from __future__ import annotations

import re
import unicodedata


def levenshtein_distance(s: str, t: str) -> int:
    """
    Compute Levenshtein (edit) distance between two strings.

    Args:
        s: First string
        t: Second string

    Returns:
        Minimum edit distance

    Example:
        >>> levenshtein_distance("kitten", "sitting")
        3
    """
    m, n = len(s), len(t)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s[i - 1] == t[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)
    return dp[m][n]


def jaro_similarity(s: str, t: str) -> float:
    """
    Compute Jaro similarity coefficient (0-1).

    Args:
        s: First string
        t: Second string

    Returns:
        Similarity score
    """
    if s == t:
        return 1.0
    len_s, len_t = len(s), len(t)
    if len_s == 0 or len_t == 0:
        return 0.0
    match_dist = max(len_s, len_t) // 2 - 1
    s_matches = [False] * len_s
    t_matches = [False] * len_t
    matches = 0
    transpositions = 0
    for i in range(len_s):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len_t)
        for j in range(start, end):
            if t_matches[j] or s[i] != t[j]:
                continue
            s_matches[i] = True
            t_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len_s):
        if not s_matches[i]:
            continue
        while not t_matches[k]:
            k += 1
        if s[i] != t[k]:
            transpositions += 1
        k += 1
    return (matches / len_s + matches / len_t + (matches - transpositions / 2) / matches) / 3


def jaro_winkler_similarity(s: str, t: str, p: float = 0.1) -> float:
    """
    Jaro-Winkler similarity (gives higher score to prefix matches).

    Args:
        s: First string
        t: Second string
        p: Scaling factor (default 0.1)

    Returns:
        Similarity score (0-1)
    """
    jaro = jaro_similarity(s, t)
    prefix = 0
    for i in range(min(4, len(s), len(t))):
        if s[i] == t[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * p * (1 - jaro)


def damerau_levenshtein_distance(s: str, t: str) -> int:
    """
    Damerau-Levenshtein distance (allows transpositions).
    """
    m, n = len(s), len(t)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s[i - 1] == t[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)
            if i > 1 and j > 1 and s[i - 1] == t[j - 2] and s[i - 2] == t[j - 1]:
                dp[i][j] = min(dp[i][j], dp[i - 2][j - 2] + cost)
    return dp[m][n]


def normalize_whitespace(s: str) -> str:
    """Normalize whitespace: collapse multiple spaces/tabs/newlines."""
    return re.sub(r"\s+", " ", s).strip()


def remove_accents(s: str) -> str:
    """Remove accents/diacritics from Unicode string."""
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def camel_to_snake(s: str) -> str:
    """Convert CamelCase to snake_case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def snake_to_camel(s: str) -> str:
    """Convert snake_case to camelCase."""
    components = s.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def word_wrap(s: str, width: int = 80) -> list[str]:
    """Wrap text to specified width."""
    words = s.split()
    lines, line = [], []
    current_len = 0
    for word in words:
        if current_len + len(word) + len(line) <= width:
            line.append(word)
            current_len += len(word)
        else:
            if line:
                lines.append(" ".join(line))
            line = [word]
            current_len = len(word)
    if line:
        lines.append(" ".join(line))
    return lines


def truncate(s: str, width: int, suffix: str = "...") -> str:
    """Truncate string to width, appending suffix if truncated."""
    if len(s) <= width:
        return s
    return s[:width - len(suffix)].rstrip() + suffix


def is_palindrome(s: str, ignore_case: bool = True, ignore_non_alpha: bool = True) -> bool:
    """Check if string is a palindrome."""
    chars = []
    for c in s:
        if ignore_non_alpha and not c.isalnum():
            continue
        chars.append(c.lower() if ignore_case else c)
    return chars == chars[::-1]
