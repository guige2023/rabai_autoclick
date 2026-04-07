"""
Content Filter and Deduplication Action Module.

Filters and deduplicates scraped content using SimHash,
MinHash, and exact matching. Handles duplicate article detection
and near-duplicate content removal.

Example:
    >>> from content_filter_action import ContentFilter
    >>> filter = ContentFilter()
    >>> filter.add(content)
    >>> unique = filter.get_unique()
"""
from __future__ import annotations

import hashlib
import re
import simhash
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class ContentItem:
    """A content item with fingerprint."""
    content: str
    url: str = ""
    title: str = ""
    fingerprint: Optional[int] = None
    hash: str = ""


class ContentFilter:
    """Filter and deduplicate content using various strategies."""

    def __init__(
        self,
        strategy: str = "simhash",
        threshold: float = 0.9,
    ):
        self._items: list[ContentItem] = []
        self._fingerprints: dict[int, list[int]] = defaultdict(list)
        self._hashes: dict[str, ContentItem] = {}
        self._strategy = strategy
        self._threshold = threshold

    def add(self, content: str, url: str = "", title: str = "") -> bool:
        """
        Add content for deduplication checking.

        Returns:
            True if content is unique, False if duplicate
        """
        item = ContentItem(content=content, url=url, title=title)
        item.fingerprint = self._compute_fingerprint(content)
        item.hash = self._compute_hash(content)

        if self._strategy == "exact":
            if item.hash in self._hashes:
                return False
            self._hashes[item.hash] = item
        else:
            if self._find_similar(item.fingerprint) is not None:
                return False
            self._fingerprints[item.fingerprint].append(len(self._items))

        self._items.append(item)
        return True

    def _compute_fingerprint(self, content: str) -> int:
        """Compute SimHash fingerprint."""
        text = self._preprocess(content)
        return simhash.simhash(text.split())

    def _compute_hash(self, content: str) -> str:
        """Compute SHA256 hash."""
        return hashlib.sha256(content.encode()).hexdigest()

    def _preprocess(self, text: str) -> str:
        """Preprocess text for fingerprinting."""
        text = text.lower()
        text = re.sub(r"[^\w\s]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _find_similar(self, fingerprint: int) -> Optional[int]:
        """Find similar content using SimHash."""
        for stored_fp, indices in self._fingerprints.items():
            distance = simhash.simhash_distance(fingerprint, stored_fp)
            if distance <= 3:
                return indices[0]
        return None

    def get_unique(self) -> list[ContentItem]:
        """Get all unique content items."""
        return list(self._items)

    def get_duplicates(self) -> list[tuple[int, int]]:
        """Get pairs of duplicate indices."""
        duplicates: list[tuple[int, int]] = []
        seen: set[int] = set()
        for i, item in enumerate(self._items):
            if self._strategy == "exact":
                if item.hash in self._hashes and self._hashes[item.hash] is not item:
                    other_idx = self._fingerprints[item.hash][0]
                    if other_idx not in seen and i not in seen:
                        duplicates.append((other_idx, i))
                        seen.add(other_idx)
                        seen.add(i)
            else:
                similar = self._find_similar(item.fingerprint)
                if similar is not None and similar < i:
                    if similar not in seen and i not in seen:
                        duplicates.append((similar, i))
                        seen.add(similar)
                        seen.add(i)
        return duplicates

    def filter_duplicates(
        self,
        items: list[dict[str, str]],
        content_field: str = "content",
    ) -> list[dict[str, str]]:
        """Filter duplicates from list of content dicts."""
        unique: list[dict[str, str]] = []
        for item in items:
            content = item.get(content_field, "")
            if self.add(content):
                unique.append(item)
        return unique


class MinHashFilter:
    """Bloom filter for approximate set membership."""

    def __init__(self, size: int = 1000, hash_count: int = 7):
        self._size = size
        self._hash_count = hash_count
        self._bloom = [False] * size
        self._hash_funcs = self._generate_hash_funcs()

    def _generate_hash_funcs(self) -> list[Callable[[str], int]]:
        def make_hash(seed: int) -> Callable[[str], int]:
            def h(value: str) -> int:
                h = hashlib.sha256(f"{seed}_{value}".encode()).hexdigest()
                return int(h, 16) % self._size
            return h
        return [make_hash(i) for i in range(self._hash_count)]

    def add(self, value: str) -> None:
        """Add value to filter."""
        for h in self._hash_funcs:
            self._bloom[h(value)] = True

    def might_contain(self, value: str) -> bool:
        """Check if value might be in set."""
        return all(self._bloom[h(value)] for h in self._hash_funcs)

    def clear(self) -> None:
        """Reset filter."""
        self._bloom = [False] * self._size


class KeywordFilter:
    """Filter content based on keyword presence/absence."""

    def __init__(
        self,
        required: Optional[list[str]] = None,
        excluded: Optional[list[str]] = None,
        must_contain: Optional[list[str]] = None,
    ):
        self.required = [k.lower() for k in (required or [])]
        self.excluded = [k.lower() for k in (excluded or [])]
        self.must_contain = [k.lower() for k in (must_contain or [])]

    def matches(self, content: str) -> bool:
        """Check if content matches filter criteria."""
        text_lower = content.lower()

        if self.required:
            if not all(kw in text_lower for kw in self.required):
                return False

        if self.excluded:
            if any(kw in text_lower for kw in self.excluded):
                return False

        if self.must_contain:
            if not any(kw in text_lower for kw in self.must_contain):
                return False

        return True

    def filter_content(
        self,
        items: list[dict[str, str]],
        content_field: str = "content",
    ) -> list[dict[str, str]]:
        """Filter list of content items."""
        return [item for item in items if self.matches(item.get(content_field, ""))]


class ContentScorer:
    """Score content quality based on various heuristics."""

    def __init__(self):
        self._min_length = 100
        self._ideal_length = 1000

    def score(self, content: str, title: str = "") -> float:
        """
        Score content quality from 0 to 1.

        Factors:
        - Length (penalize too short or too long)
        - Title presence
        - Sentence structure
        - Word variety
        """
        score = 0.5

        length = len(content)
        if length < self._min_length:
            score *= 0.5
        elif length > 10000:
            score *= 0.8
        else:
            score *= 1.0

        word_count = len(content.split())
        if word_count < 50:
            score *= 0.5
        elif 100 <= word_count <= 2000:
            score *= 1.2

        if title:
            score *= 1.1

        sentence_count = len(re.findall(r"[.!?]+", content))
        if sentence_count > 0:
            avg_sentence_len = word_count / sentence_count
            if 10 <= avg_sentence_len <= 30:
                score *= 1.1

        words = content.lower().split()
        unique_words = len(set(words))
        if word_count > 0:
            vocab_ratio = unique_words / word_count
            if vocab_ratio > 0.3:
                score *= 1.1

        if re.search(r"\d{3}[-.\s]?\d{3}[-.\s]?\d{4}", content):
            score *= 0.9

        return min(1.0, max(0.0, score))

    def is_high_quality(self, content: str, title: str = "", threshold: float = 0.7) -> bool:
        """Check if content meets quality threshold."""
        return self.score(content, title) >= threshold


if __name__ == "__main__":
    content_filter = ContentFilter(strategy="exact")

    items = [
        "This is some unique content about cats",
        "This is some unique content about cats",
        "Different content about dogs",
    ]

    for item in items:
        is_unique = content_filter.add(item)
        print(f"Content added: unique={is_unique}")

    unique_items = content_filter.get_unique()
    print(f"\nUnique items: {len(unique_items)}")
    duplicates = content_filter.get_duplicates()
    print(f"Duplicates: {len(duplicates)}")
