"""
Diff, match, and patch utilities for text comparison.

Provides text diff algorithms, fuzzy matching, and
patch generation for text synchronization.

Example:
    >>> from utils.diff_match_patch_utils import diff_texts, create_patch
    >>> changes = diff_texts("hello", "hello world")
"""

from __future__ import annotations

import difflib
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple, Union


@dataclass
class Diff:
    """Represents a single diff operation."""
    operation: int  # 0 = equal, 1 = insert, -1 = delete
    text: str

    EQUAL = 0
    INSERT = 1
    DELETE = -1


@dataclass
class Patch:
    """Represents a text patch."""
    start1: int
    start2: int
    length1: int
    length2: int
    diffs: List[Diff] = field(default_factory=list)


class TextDiff:
    """
    Text differencing with multiple algorithms.

    Supports character-level, word-level, and line-level diffs
    with various output formats.
    """

    def __init__(
        self,
        algorithm: str = "auto",
        ignore_whitespace: bool = False,
        ignore_case: bool = False,
    ) -> None:
        """
        Initialize the text differ.

        Args:
            algorithm: Diff algorithm ('auto', 'char', 'word', 'line').
            ignore_whitespace: Ignore whitespace differences.
            ignore_case: Ignore case differences.
        """
        self.algorithm = algorithm
        self.ignore_whitespace = ignore_whitespace
        self.ignore_case = ignore_case

    def diff(
        self,
        text1: str,
        text2: str,
    ) -> List[Diff]:
        """
        Compute the diff between two texts.

        Args:
            text1: Original text.
            text2: Modified text.

        Returns:
            List of Diff objects.
        """
        if self.ignore_case:
            text1 = text1.lower()
            text2 = text2.lower()

        if self.ignore_whitespace:
            text1 = self._normalize_whitespace(text1)
            text2 = self._normalize_whitespace(text2)

        if self.algorithm == "char":
            return self._diff_chars(text1, text2)
        elif self.algorithm == "word":
            return self._diff_words(text1, text2)
        elif self.algorithm == "line":
            return self._diff_lines(text1, text2)
        else:
            return self._diff_auto(text1, text2)

    @staticmethod
    def _diff_auto(text1: str, text2: str) -> List[Diff]:
        """Auto-select the best diff algorithm."""
        if "\n" in text1 or "\n" in text2:
            return TextDiff._diff_lines(text1, text2)
        if " " in text1 or " " in text2:
            return TextDiff._diff_words(text1, text2)
        return TextDiff._diff_chars(text1, text2)

    @staticmethod
    def _diff_chars(text1: str, text2: str) -> List[Diff]:
        """Character-level diff."""
        differ = difflib.Differ()
        result = list(differ.compare(text1, text2))

        diffs: List[Diff] = []
        for item in result:
            if item[0] == " ":
                diffs.append(Diff(Diff.EQUAL, item[2:]))
            elif item[0] == "+":
                diffs.append(Diff(Diff.INSERT, item[2:]))
            elif item[0] == "-":
                diffs.append(Diff(Diff.DELETE, item[2:]))

        return diffs

    @staticmethod
    def _diff_words(text1: str, text2: str) -> List[Diff]:
        """Word-level diff."""
        word_pattern = re.compile(r"\S+|\s+")

        words1 = word_pattern.findall(text1)
        words2 = word_pattern.findall(text2)

        differ = difflib.Differ()
        result = list(differ.compare(words1, words2))

        diffs: List[Diff] = []
        for item in result:
            if item[0] == " ":
                diffs.append(Diff(Diff.EQUAL, item[2:]))
            elif item[0] == "+":
                diffs.append(Diff(Diff.INSERT, item[2:]))
            elif item[0] == "-":
                diffs.append(Diff(Diff.DELETE, item[2:]))

        return diffs

    @staticmethod
    def _diff_lines(text1: str, text2: str) -> List[Diff]:
        """Line-level diff."""
        lines1 = text1.splitlines(keepends=True)
        lines2 = text2.splitlines(keepends=True)

        differ = difflib.Differ()
        result = list(differ.compare(lines1, lines2))

        diffs: List[Diff] = []
        for item in result:
            if item[0] == " ":
                diffs.append(Diff(Diff.EQUAL, item[2:]))
            elif item[0] == "+":
                diffs.append(Diff(Diff.INSERT, item[2:]))
            elif item[0] == "-":
                diffs.append(Diff(Diff.DELETE, item[2:]))

        return diffs

    @staticmethod
    def _normalize_whitespace(text: str) -> str:
        """Normalize whitespace in text."""
        return re.sub(r"\s+", " ", text).strip()

    def apply_diffs(
        self,
        diffs: List[Diff],
        base_text: str,
    ) -> str:
        """
        Apply a diff to base text to get the modified text.

        Args:
            diffs: List of diff operations.
            base_text: Original text.

        Returns:
            Modified text.
        """
        result: List[str] = []

        for diff in diffs:
            if diff.operation == Diff.EQUAL:
                result.append(diff.text)
            elif diff.operation == Diff.INSERT:
                result.append(diff.text)
            elif diff.operation == Diff.DELETE:
                pass

        return "".join(result)

    def format_unified(
        self,
        text1: str,
        text2: str,
        context_lines: int = 3,
    ) -> str:
        """
        Format diff as unified diff.

        Args:
            text1: Original text.
            text2: Modified text.
            context_lines: Number of context lines.

        Returns:
            Unified diff string.
        """
        lines1 = text1.splitlines(keepends=True)
        lines2 = text2.splitlines(keepends=True)

        import io
        output = io.StringIO()
        differ = difflib.unified_diff(
            lines1, lines2,
            n=context_lines,
        )
        output.writelines(differ)
        return output.getvalue()

    def format_context(
        self,
        text1: str,
        text2: str,
        context_lines: int = 3,
    ) -> str:
        """
        Format diff as context diff.

        Args:
            text1: Original text.
            text2: Modified text.
            context_lines: Number of context lines.

        Returns:
            Context diff string.
        """
        lines1 = text1.splitlines(keepends=True)
        lines2 = text2.splitlines(keepends=True)

        import io
        output = io.StringIO()
        differ = difflib.context_diff(
            lines1, lines2,
            n=context_lines,
        )
        output.writelines(differ)
        return output.getvalue()

    def stats(self, diffs: List[Diff]) -> dict:
        """
        Get statistics about a diff.

        Args:
            diffs: List of diff operations.

        Returns:
            Dictionary with statistics.
        """
        equal_len = sum(len(d.text) for d in diffs if d.operation == Diff.EQUAL)
        insert_len = sum(len(d.text) for d in diffs if d.operation == Diff.INSERT)
        delete_len = sum(len(d.text) for d in diffs if d.operation == Diff.DELETE)

        return {
            "equal_chars": equal_len,
            "insert_chars": insert_len,
            "delete_chars": delete_len,
            "total_changes": insert_len + delete_len,
            "change_ratio": (insert_len + delete_len) / max(1, equal_len + insert_len + delete_len),
        }


class FuzzyMatcher:
    """
    Fuzzy string matching with scoring.

    Provides similarity scoring and matching for approximate
    string comparisons.
    """

    def __init__(
        self,
        threshold: float = 0.6,
        ignore_case: bool = True,
    ) -> None:
        """
        Initialize the fuzzy matcher.

        Args:
            threshold: Minimum similarity score (0-1).
            ignore_case: Ignore case in comparisons.
        """
        self.threshold = threshold
        self.ignore_case = ignore_case

    def similarity(
        self,
        text1: str,
        text2: str,
    ) -> float:
        """
        Calculate similarity score between two strings.

        Args:
            text1: First string.
            text2: Second string.

        Returns:
            Similarity score between 0 and 1.
        """
        if self.ignore_case:
            text1 = text1.lower()
            text2 = text2.lower()

        if not text1 or not text2:
            return 0.0

        return difflib.SequenceMatcher(None, text1, text2).ratio()

    def match(
        self,
        text: str,
        candidates: List[str],
    ) -> List[Tuple[str, float]]:
        """
        Find best matches for text in candidate list.

        Args:
            text: Text to match.
            candidates: List of candidate strings.

        Returns:
            List of (candidate, score) tuples sorted by score.
        """
        scores: List[Tuple[str, float]] = []

        for candidate in candidates:
            score = self.similarity(text, candidate)
            if score >= self.threshold:
                scores.append((candidate, score))

        scores.sort(key=lambda x: x[1], reverse=True)
        return scores

    def closest(
        self,
        text: str,
        candidates: List[str],
    ) -> Optional[Tuple[str, float]]:
        """
        Find the closest match for text.

        Args:
            text: Text to match.
            candidates: List of candidate strings.

        Returns:
            (candidate, score) tuple or None.
        """
        matches = self.match(text, candidates)
        return matches[0] if matches else None


def diff_texts(
    text1: str,
    text2: str,
    algorithm: str = "auto",
) -> List[Diff]:
    """
    Convenience function to compute text diff.

    Args:
        text1: Original text.
        text2: Modified text.
        algorithm: Diff algorithm.

    Returns:
        List of Diff objects.
    """
    differ = TextDiff(algorithm=algorithm)
    return differ.diff(text1, text2)


def create_patch(
    text1: str,
    text2: str,
) -> List[Patch]:
    """
    Create patches from text diff.

    Args:
        text1: Original text.
        text2: Modified text.

    Returns:
        List of Patch objects.
    """
    differ = TextDiff()
    diffs = differ.diff(text1, text2)

    patch = Patch(
        start1=0,
        start2=0,
        length1=len(text1),
        length2=len(text2),
        diffs=diffs,
    )

    return [patch]


def apply_patch(
    text: str,
    patches: List[Patch],
) -> str:
    """
    Apply patches to text.

    Args:
        text: Original text.
        patches: Patches to apply.

    Returns:
        Patched text.
    """
    differ = TextDiff()
    if not patches:
        return text

    return differ.apply_diffs(patches[0].diffs, text)
