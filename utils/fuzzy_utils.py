"""
Fuzzy matching and string similarity utilities.

Provides Levenshtein distance, Jaro-Winkler, Dice coefficient,
fuzzy substring matching, and approximate string searching.
"""

from __future__ import annotations

from typing import Callable


def levenshtein_distance(s: str, t: str) -> int:
    """
    Compute the Levenshtein (edit) distance between two strings.

    Args:
        s: First string
        t: Second string

    Returns:
        Minimum number of single-character edits (insertions, deletions,
        substitutions) to transform s into t.

    Example:
        >>> levenshtein_distance("kitten", "sitting")
        3
    """
    m, n = len(s), len(t)
    if m == 0:
        return n
    if n == 0:
        return m

    dp: list[list[int]] = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s[i - 1] == t[j - 1] else 1
            dp[i][j] = min(
                dp[i - 1][j] + 1,       # deletion
                dp[i][j - 1] + 1,       # insertion
                dp[i - 1][j - 1] + cost  # substitution
            )
    return dp[m][n]


def levenshtein_similarity(s: str, t: str) -> float:
    """
    Compute normalized Levenshtein similarity [0, 1].

    Args:
        s: First string
        t: Second string

    Returns:
        Similarity score where 1.0 means identical, 0.0 means completely different.
    """
    if not s and not t:
        return 1.0
    dist = levenshtein_distance(s, t)
    max_len = max(len(s), len(t))
    return 1.0 - dist / max_len


def jaro_similarity(s: str, t: str) -> float:
    """
    Compute the Jaro similarity between two strings.

    Args:
        s: First string
        t: Second string

    Returns:
        Similarity score in [0, 1].

    Reference:
        Jaro, M.A. (1989). "Advances in Record-Linkage Methodology".
    """
    if s == t:
        return 1.0
    len_s, len_t = len(s), len(t)
    if len_s == 0 or len_t == 0:
        return 0.0

    match_distance = max(len_s, len_t) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s_matches = [False] * len_s
    t_matches = [False] * len_t
    matches = 0
    transpositions = 0

    for i in range(len_s):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len_t)
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

    return (
        matches / len_s
        + matches / len_t
        + (matches - transpositions / 2) / matches
    ) / 3


def jaro_winkler_similarity(s: str, t: str, p: float = 0.1) -> float:
    """
    Compute the Jaro-Winkler similarity.

    Jaro-Winkler gives more weight to matching prefixes.

    Args:
        s: First string
        t: Second string
        p: Scaling factor for prefix weight (default 0.1, max 0.25)

    Returns:
        Similarity score in [0, 1].
    """
    jaro = jaro_similarity(s, t)
    prefix_len = 0
    for i in range(min(len(s), len(t), 4)):
        if s[i] == t[i]:
            prefix_len += 1
        else:
            break
    return jaro + prefix_len * p * (1 - jaro)


def dice_coefficient(s: str, t: str) -> float:
    """
    Compute the Sorensen-Dice coefficient using bigrams.

    Args:
        s: First string
        t: Second string

    Returns:
        Score in [0, 1], where 1 means identical.
    """
    def bigrams(st: str) -> set[str]:
        return {st[i:i+2] for i in range(len(st) - 1)} if len(st) >= 2 else set()

    bi_s = bigrams(s)
    bi_t = bigrams(t)
    if not bi_s and not bi_t:
        return 1.0
    if not bi_s or not bi_t:
        return 0.0
    intersection = len(bi_s & bi_t)
    return 2 * intersection / (len(bi_s) + len(bi_t))


def cosine_similarity_strings(s: str, t: str) -> float:
    """
    Compute cosine similarity between two strings using character n-grams.

    Args:
        s: First string
        t: Second string
        n: N-gram size (default 2, character-level)

    Returns:
        Cosine similarity in [0, 1].
    """
    n = 2
    def ngrams(st: str, n: int) -> dict[str, int]:
        counts: dict[str, int] = {}
        for i in range(len(st) - n + 1):
            ng = st[i:i+n]
            counts[ng] = counts.get(ng, 0) + 1
        return counts

    ng_s = ngrams(s, n)
    ng_t = ngrams(t, n)
    all_keys = set(ng_s) | set(ng_t)

    dot_product = sum(ng_s.get(k, 0) * ng_t.get(k, 0) for k in all_keys)
    norm_s = math.sqrt(sum(v * v for v in ng_s.values()))
    norm_t = math.sqrt(sum(v * v for v in ng_t.values()))
    if norm_s == 0 or norm_t == 0:
        return 0.0
    return dot_product / (norm_s * norm_t)


def fuzzy_match(
    query: str,
    choices: list[str],
    scorer: Callable[[str, str], float] | None = None,
    threshold: float = 0.6,
) -> list[tuple[str, float]]:
    """
    Find fuzzy matches for query in a list of choices.

    Args:
        query: The string to match
        choices: List of candidate strings
        scorer: Similarity function (default: jaro_winkler_similarity)
        threshold: Minimum score to include (default 0.6)

    Returns:
        Sorted list of (choice, score) tuples above the threshold.
    """
    if scorer is None:
        scorer = jaro_winkler_similarity
    scored = [(c, scorer(query, c)) for c in choices]
    return sorted(((c, s) for c, s in scored if s >= threshold), key=lambda x: -x[1])


def longest_common_substring(s: str, t: str) -> str:
    """
    Find the longest common substring between two strings.

    Args:
        s: First string
        t: Second string

    Returns:
        The longest common substring.
    """
    m, n = len(s), len(t)
    if m == 0 or n == 0:
        return ""
    dp: list[list[int]] = [[0] * (n + 1) for _ in range(m + 1)]
    max_len = 0
    end_i = 0
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s[i-1] == t[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
                if dp[i][j] > max_len:
                    max_len = dp[i][j]
                    end_i = i
    return s[end_i - max_len:end_i]


def longest_common_subsequence(s: str, t: str) -> str:
    """
    Find the longest common subsequence between two strings.

    Args:
        s: First string
        t: Second string

    Returns:
        The longest common subsequence.
    """
    m, n = len(s), len(t)
    if m == 0 or n == 0:
        return ""
    dp: list[list[int]] = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s[i-1] == t[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])

    # Backtrack
    lcs = []
    i, j = m, n
    while i > 0 and j > 0:
        if s[i-1] == t[j-1]:
            lcs.append(s[i-1])
            i -= 1
            j -= 1
        elif dp[i-1][j] > dp[i][j-1]:
            i -= 1
        else:
            j -= 1
    return "".join(reversed(lcs))


import math
