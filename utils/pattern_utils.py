"""
Pattern matching and regular expression utilities.

Provides KMP, Boyer-Moore, Rabin-Karp, Aho-Corasick,
regex helpers, and wildcard/glob matching.
"""

from __future__ import annotations

import re
from typing import Callable


def kmp_search(text: str, pattern: str) -> list[int]:
    """
    Knuth-Morris-Pratt substring search.

    Args:
        text: Text to search in
        pattern: Pattern to find

    Returns:
        List of starting indices where pattern occurs.
    """
    if not pattern:
        return [0] if text else []
    if not text:
        return []

    # Build LPS (longest proper prefix which is also suffix) array
    lps = [0] * len(pattern)
    length = 0
    i = 1
    while i < len(pattern):
        if pattern[i] == pattern[length]:
            length += 1
            lps[i] = length
            i += 1
        else:
            if length != 0:
                length = lps[length - 1]
            else:
                lps[i] = 0
                i += 1

    # Search
    matches: list[int] = []
    i = 0  # index in text
    j = 0  # index in pattern
    while i < len(text):
        if pattern[j] == text[i]:
            i += 1
            j += 1
            if j == len(pattern):
                matches.append(i - j)
                j = lps[j - 1]
        else:
            if j != 0:
                j = lps[j - 1]
            else:
                i += 1
    return matches


def boyer_moore_search(text: str, pattern: str) -> list[int]:
    """
    Boyer-Moore substring search (bad character heuristic).

    Args:
        text: Text to search in
        pattern: Pattern to find

    Returns:
        List of starting indices where pattern occurs.
    """
    if not pattern:
        return [0] if text else []
    if not text or len(pattern) > len(text):
        return []

    # Build bad character table
    last: dict[str, int] = {}
    for i, ch in enumerate(pattern):
        last[ch] = i

    matches: list[int] = []
    i = 0  # index in text
    while i <= len(text) - len(pattern):
        j = len(pattern) - 1
        while j >= 0 and pattern[j] == text[i + j]:
            j -= 1
        if j < 0:
            matches.append(i)
            i += len(pattern) if i + len(pattern) < len(text) else 1
        else:
            last_occ = last.get(text[i + j], -1)
            if last_occ < j:
                i += j - last_occ
            else:
                i += 1
    return matches


def rabin_karp_search(
    text: str,
    pattern: str,
    base: int = 256,
    mod: int = 101,
) -> list[int]:
    """
    Rabin-Karp rolling hash substring search.

    Args:
        text: Text to search in
        pattern: Pattern to find
        base: Alphabet size
        mod: Modulus for hash

    Returns:
        List of starting indices where pattern occurs.
    """
    if not pattern:
        return [0] if text else []
    if not text or len(pattern) > len(text):
        return []

    n, m = len(text), len(pattern)
    h = pow(base, m - 1, mod)
    p_hash = 0
    t_hash = 0
    for i in range(m):
        p_hash = (p_hash * base + ord(pattern[i])) % mod
        t_hash = (t_hash * base + ord(text[i])) % mod

    matches: list[int] = []
    for i in range(n - m + 1):
        if p_hash == t_hash:
            if text[i:i + m] == pattern:
                matches.append(i)
        if i < n - m:
            t_hash = ((t_hash - ord(text[i]) * h) * base + ord(text[i + m])) % mod
            if t_hash < 0:
                t_hash += mod
    return matches


def aho_corasick_search(
    text: str,
    patterns: list[str],
) -> dict[str, list[int]]:
    """
    Aho-Corasick multi-pattern search.

    Args:
        text: Text to search in
        patterns: List of patterns to find

    Returns:
        Dictionary mapping pattern to list of match positions.
    """
    if not patterns:
        return {}

    # Build trie
    trie: dict = {}
    outputs: dict = {}
    for pid, pat in enumerate(patterns):
        node = trie
        for ch in pat:
            if ch not in node:
                node[ch] = {}
            node = node[ch]
        node["$"] = pat

    # Build failure links
    from collections import deque
    queue: deque = deque()
    for ch, child in trie.items():
        if ch != "$":
            queue.append(child)

    fail: dict = {}
    while queue:
        node = queue.popleft()
        for ch, child in node.items():
            if ch == "$":
                continue
            queue.append(child)
            fail_child = fail.get(node, trie)
            while fail_child and ch not in fail_child:
                fail_child = fail.get(fail_child, trie)
            fail[child] = fail_child.get(ch, trie) if fail_child else trie

    # Search
    result: dict[str, list[int]] = {p: [] for p in patterns}
    node = trie
    for i, ch in enumerate(text):
        while node and ch not in node:
            node = fail.get(node, trie)
        node = node.get(ch, trie)
        if "$" in node:
            pat = node["$"]
            result[pat].append(i - len(pat) + 1)
        # Follow output links
        temp = fail.get(node, trie)
        while temp:
            if "$" in temp:
                result[temp["$"]].append(i - len(temp["$"]) + 1)
            temp = fail.get(temp, trie)
    return result


def wildcard_match(pattern: str, text: str) -> bool:
    """
    Match text against wildcard pattern with * and ?.

    Args:
        pattern: Pattern using * (any chars) and ? (single char)
        text: Text to match

    Returns:
        True if pattern matches.
    """
    # Convert wildcard to regex
    regex_pattern = ""
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "*":
            regex_pattern += ".*"
        elif ch == "?":
            regex_pattern += "."
        else:
            regex_pattern += re.escape(ch)
        i += 1
    return bool(re.fullmatch(regex_pattern, text))


def glob_match(pattern: str, text: str) -> bool:
    """
    Match text against glob pattern (Unix-style).

    Supports: * (anything), ? (single char), ** (any path), [abc], [!abc].
    """
    # Convert glob to regex
    regex_pattern = ""
    i = 0
    in_bracket = False
    while i < len(pattern):
        ch = pattern[i]
        if ch == "*":
            if i + 1 < len(pattern) and pattern[i + 1] == "*":
                regex_pattern += ".*"
                i += 2
                continue
            regex_pattern += "[^/]*"
        elif ch == "?":
            regex_pattern += "."
        elif ch == "[":
            j = i + 1
            if j < len(pattern) and pattern[j] == "!":
                regex_pattern += "[^"
                j += 1
            elif j < len(pattern) and pattern[j] == "^":
                regex_pattern += "[^"
                j += 1
            else:
                regex_pattern += "["
            while j < len(pattern) and pattern[j] != "]":
                regex_pattern += re.escape(pattern[j])
                j += 1
            regex_pattern += "]"
            i = j
        else:
            regex_pattern += re.escape(ch)
        i += 1
    return bool(re.fullmatch(regex_pattern, text))


def longest_repeated_substring(s: str) -> str:
    """
    Find the longest repeated substring in s.

    Uses suffix array-like approach.
    """
    n = len(s)
    if n < 2:
        return ""
    lrs = ""
    for i in range(n):
        for j in range(i + 1, n):
            # Find common prefix of suffixes i and j
            k = 0
            while j + k < n and s[i + k] == s[j + k]:
                k += 1
            if k > len(lrs):
                lrs = s[i:i + k]
    return lrs


def regex_escape(s: str) -> str:
    """Escape special regex characters in string."""
    return re.escape(s)


def find_all_matches(text: str, pattern: str, overlapped: bool = False) -> list[str]:
    """
    Find all pattern matches in text.

    Args:
        text: Text to search
        pattern: Regex pattern
        overlapped: If True, allow overlapping matches

    Returns:
        List of matched strings.
    """
    matches = re.findall(pattern, text)
    if not overlapped:
        return matches
    results: list[str] = []
    for m in re.finditer(pattern, text):
        results.append(m.group())
    return results
