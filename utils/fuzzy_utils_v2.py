"""
Advanced fuzzy matching and string similarity v2.

Extends fuzzy_utils.py with phonetic matching, n-gram analysis,
fuzzy set operations, and approximate search indexing.
"""

from __future__ import annotations

import re
from typing import Callable


def soundex(word: str) -> str:
    """
    Compute Soundex code for a word (phonetic matching).

    Soundex maps similar-sounding names to the same code.
    """
    if not word:
        return ""
    word = word.upper()
    first = word[0]
    codes = {
        "B": "1", "F": "1", "P": "1", "V": "1",
        "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2", "S": "2", "X": "2", "Z": "2",
        "D": "3", "T": "3",
        "L": "4",
        "M": "5", "N": "5",
        "R": "6",
        "A": "", "E": "", "I": "", "O": "", "U": "", "H": "", "W": "", "Y": "",
    }
    coded = first
    prev = codes.get(first, "")
    for ch in word[1:]:
        code = codes.get(ch, "")
        if code and code != prev:
            coded += code
        if len(coded) >= 4:
            break
        if code:
            prev = code
    return (coded + "000")[:4]


def metaphone(word: str) -> str:
    """
    Compute Metaphone code for a word (more accurate than Soundex).

    Returns:
        4-character Metaphone code.
    """
    if not word:
        return ""
    word = word.upper()
    i = 0
    result = ""
    vowels = "AEIOU"
    while len(result) < 4:
        if i >= len(word):
            break
        ch = word[i]
        if ch in vowels:
            if i == 0:
                result += ch
        elif ch == "B":
            if i == len(word) - 1 or word[i - 1] == "M":
                pass
            else:
                result += "B"
        elif ch == "C":
            if word[i + 1:i + 2] in "IEH":
                if word[i + 1:i + 2] == "H":
                    result += "X"
                    i += 1
                else:
                    result += "S"
            else:
                result += "K"
        elif ch == "D":
            if word[i + 1:i + 2] == "G" and word[i + 2:i + 3] in "IEY":
                result += "J"
                i += 2
            else:
                result += "T"
        elif ch == "G":
            if word[i + 1:i + 2] == "H":
                if word[i + 2:i + 3] not in vowels:
                    i += 2
                    continue
            elif word[i + 1:i + 2] == "N":
                if i + 2 >= len(word) or word[i + 2:i + 3] not in ("E", "I"):
                    i += 1
                    continue
            elif word[i + 1:i + 2] in "IEY":
                result += "J"
                i += 1
                continue
            result += "K"
        elif ch == "K":
            if not (i > 0 and word[i - 1] == "C"):
                result += "K"
        elif ch == "P":
            if word[i + 1:i + 2] == "H":
                result += "F"
                i += 1
            else:
                result += "P"
        elif ch == "Q":
            result += "K"
        elif ch == "S":
            if word[i + 1:i + 3] == "CH":
                result += "X"
                i += 2
            else:
                result += "S"
        elif ch == "T":
            if word[i + 1:i + 3] == "CH":
                result += "X"
                i += 2
            elif word[i + 1:i + 2] == "H":
                result += "0"
            elif word[i + 1:i + 2] not in "IA":
                result += "T"
        elif ch == "V":
            result += "F"
        elif ch == "W":
            if word[i + 1:i + 2] in vowels:
                result += "W"
        elif ch == "X":
            result += "KS"
        elif ch == "Y":
            if word[i + 1:i + 2] in vowels:
                result += "Y"
        elif ch == "Z":
            result += "S"
        elif ch == "R":
            result += "R"
        i += 1
    return result


def ngram_similarity(s: str, t: str, n: int = 2) -> float:
    """
    N-gram similarity between two strings.

    Args:
        s: First string
        t: Second string
        n: N-gram size (default 2 = bigrams)

    Returns:
        Similarity score in [0, 1].
    """
    def get_ngrams(st: str, n: int) -> set[str]:
        if len(st) < n:
            return {st}
        return {st[i:i+n] for i in range(len(st) - n + 1)}

    ng_s = get_ngrams(s, n)
    ng_t = get_ngrams(t, n)
    if not ng_s and not ng_t:
        return 1.0
    if not ng_s or not ng_t:
        return 0.0
    intersection = len(ng_s & ng_t)
    return 2 * intersection / (len(ng_s) + len(ng_t))


def overlap_coefficient(s: str, t: str, n: int = 2) -> float:
    """Szymkiewicz-Simpson overlap coefficient."""
    def ngrams(st: str, n: int) -> set[str]:
        return {st[i:i+n] for i in range(max(0, len(st) - n + 1))}

    ng_s = ngrams(s, n)
    ng_t = ngrams(t, n)
    if not ng_s or not ng_t:
        return 0.0
    return len(ng_s & ng_t) / min(len(ng_s), len(ng_t))


def damerau_levenshtein_distance(s: str, t: str) -> int:
    """
    Damerau-Levenshtein distance (allows transpositions).

    Returns:
        Minimum number of operations (insert, delete, substitute, transpose).
    """
    m, n = len(s), len(t)
    dp: list[list[int]] = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s[i-1] == t[j-1] else 1
            dp[i][j] = min(
                dp[i-1][j] + 1,      # deletion
                dp[i][j-1] + 1,      # insertion
                dp[i-1][j-1] + cost  # substitution
            )
            # Transposition
            if i > 1 and j > 1 and s[i-1] == t[j-2] and s[i-2] == t[j-1]:
                dp[i][j] = min(dp[i][j], dp[i-2][j-2] + cost)
    return dp[m][n]


def needleman_wunsch(s: str, t: str, match: int = 1, mismatch: int = -1, gap: int = -1) -> float:
    """
    Needleman-Wunsch global sequence alignment score.

    Args:
        s: First sequence
        t: Second sequence
        match: Score for match
        mismatch: Score for mismatch
        gap: Score for gap penalty

    Returns:
        Alignment score.
    """
    m, n = len(s), len(t)
    dp: list[list[float]] = [[0.0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i * gap
    for j in range(n + 1):
        dp[0][j] = j * gap

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            match_score = match if s[i-1] == t[j-1] else mismatch
            dp[i][j] = max(
                dp[i-1][j-1] + match_score,
                dp[i-1][j] + gap,
                dp[i][j-1] + gap,
            )
    return dp[m][n]


def smith_waterman(s: str, t: str, match: int = 1, mismatch: int = -1, gap: int = -1) -> float:
    """
    Smith-Waterman local sequence alignment.

    Finds the best-scoring local alignment.
    """
    m, n = len(s), len(t)
    dp: list[list[float]] = [[0.0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            match_score = match if s[i-1] == t[j-1] else mismatch
            dp[i][j] = max(
                dp[i-1][j-1] + match_score,
                dp[i-1][j] + gap,
                dp[i][j-1] + gap,
                0.0,
            )
    return max(max(row) for row in dp)


def token_sort_ratio(s: str, t: str) -> float:
    """
    Token sort ratio - sort tokens alphabetically before comparing.

    Args:
        s: First string
        t: Second string

    Returns:
        Similarity score [0, 1].
    """
    tokens_s = sorted(s.lower().split())
    tokens_t = sorted(t.lower().split())
    joined_s = " ".join(tokens_s)
    joined_t = " ".join(tokens_t)
    max_len = max(len(joined_s), len(joined_t))
    if max_len == 0:
        return 1.0
    dist = damerau_levenshtein_distance(joined_s, joined_t)
    return 1.0 - dist / max_len


def token_set_ratio(s: str, t: str) -> float:
    """
    Token set ratio - intersection of tokens as primary comparison.

    Breaks strings into tokens and compares token sets.
    """
    tokens_s = set(s.lower().split())
    tokens_t = set(t.lower().split())
    intersection = tokens_s & tokens_t
    diff_s = tokens_s - tokens_t
    diff_t = tokens_t - tokens_s
    combined = " ".join(sorted(intersection))
    diff_s_str = " ".join(sorted(diff_s))
    diff_t_str = " ".join(sorted(diff_t))
    ratios = [
        ngram_similarity(combined, diff_s_str),
        ngram_similarity(combined, diff_t_str),
        ngram_similarity(diff_s_str, diff_t_str),
    ]
    return max(ratios) if ratios else 0.0


def partial_ratio(s: str, t: str) -> float:
    """Partial ratio - best partial match when one string is much shorter."""
    shorter = s if len(s) <= len(t) else t
    longer = t if len(s) <= len(t) else s
    if not shorter:
        return 0.0
    best = 0.0
    for i in range(len(longer) - len(shorter) + 1):
        substr = longer[i:i+len(shorter)]
        ratio = ngram_similarity(shorter, substr)
        if ratio > best:
            best = ratio
    return best


def weighted_token_sort_ratio(s: str, t: str) -> float:
    """Weighted version of token sort ratio."""
    tokens_s = s.lower().split()
    tokens_t = t.lower().split()
    if not tokens_s or not tokens_t:
        return 0.0
    # Sort and compare
    sorted_s = " ".join(sorted(tokens_s))
    sorted_t = " ".join(sorted(tokens_t))
    base_score = 1.0 - damerau_levenshtein_distance(sorted_s, sorted_t) / max(len(sorted_s), len(sorted_t))
    # Boost for matching common tokens
    common = set(tokens_s) & set(tokens_t)
    if common:
        base_score = min(1.0, base_score * (1.0 + 0.1 * len(common)))
    return base_score
