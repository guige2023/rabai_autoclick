"""
Data Fuzzy Hash Action Module

Fuzzy hashing for similarity detection and data deduplication.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import hashlib
import re


class FuzzyHash:
    """Base fuzzy hash implementation."""

    @staticmethod
    def _normalize(text: str) -> str:
        """Normalize text for hashing."""
        text = text.lower()
        text = re.sub(r'[^a-z0-9]', '', text)
        return text

    @classmethod
    def simhash(cls, text: str, dim: int = 64) -> int:
        """Generate SimHash for text."""
        normalized = cls._normalize(text)
        if not normalized:
            return 0

        v = [0] * dim
        for i, char in enumerate(normalized):
            h = hashlib.md5(char.encode()).digest()
            for j in range(dim):
                bit = (h[j // 8] >> (j % 8)) & 1
                v[j] += 1 if bit else -1

        result = 0
        for i in range(dim):
            if v[i] > 0:
                result |= (1 << i)
        return result

    @classmethod
    def hamming_distance(cls, hash1: int, hash2: int) -> int:
        """Calculate Hamming distance between two hashes."""
        xor = hash1 ^ hash2
        return bin(xor).count('1')

    @classmethod
    def similarity(cls, hash1: int, hash2: int) -> float:
        """Calculate similarity between two hashes (0.0 to 1.0)."""
        dist = cls.hamming_distance(hash1, hash2)
        return 1.0 - (dist / 64.0)


class NGramFuzzyHash:
    """N-gram based fuzzy hash for string similarity."""

    def __init__(self, n: int = 3):
        self.n = n

    def _get_ngrams(self, text: str) -> set:
        """Extract n-grams from text."""
        text = text.lower().strip()
        ngrams = set()
        for i in range(len(text) - self.n + 1):
            ngrams.add(text[i:i+self.n])
        return ngrams

    def fingerprint(self, text: str) -> str:
        """Generate fingerprint from n-grams."""
        ngrams = self._get_ngrams(text)
        sorted_ngrams = sorted(ngrams)
        combined = ''.join(sorted_ngrams)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def similarity(self, text1: str, text2: str) -> float:
        """Calculate Jaccard similarity between two texts."""
        ngrams1 = self._get_ngrams(text1)
        ngrams2 = self._get_ngrams(text2)

        if not ngrams1 or not ngrams2:
            return 0.0

        intersection = len(ngrams1 & ngrams2)
        union = len(ngrams1 | ngrams2)
        return intersection / union if union > 0 else 0.0

    def find_similar(
        self,
        text: str,
        corpus: List[str],
        threshold: float = 0.5
    ) -> List[Tuple[str, float]]:
        """Find similar texts in corpus."""
        ngrams = self._get_ngrams(text)
        results = []

        for candidate in corpus:
            cand_ngrams = self._get_ngrams(candidate)
            if not cand_ngrams:
                continue

            intersection = len(ngrams & cand_ngrams)
            union = len(ngrams | cand_ngrams)
            sim = intersection / union if union > 0 else 0.0

            if sim >= threshold:
                results.append((candidate, sim))

        return sorted(results, key=lambda x: x[1], reverse=True)


class RabinKarpFuzzyHash:
    """Rabin-Karp rolling hash for fuzzy matching."""

    def __init__(self, window_size: int = 5, base: int = 256, mod: int = 101):
        self.window_size = window_size
        self.base = base
        self.mod = mod

    def _rolling_hash(self, text: str, start: int) -> int:
        """Calculate rolling hash for window starting at position."""
        h = 0
        for i in range(self.window_size):
            h = (h * self.base + ord(text[start + i])) % self.mod
        return h

    def fingerprints(self, text: str) -> List[int]:
        """Generate all fingerprints for text."""
        if len(text) < self.window_size:
            return []

        fingerprints = []
        h = self._rolling_hash(text, 0)

        for i in range(len(text) - self.window_size + 1):
            if i > 0:
                h = (h - ord(text[i-1]) * (self.base ** (self.window_size - 1))) % self.mod
                h = (h * self.base + ord(text[i + self.window_size - 1])) % self.mod
            fingerprints.append(h)

        return fingerprints

    def find_matches(
        self,
        pattern: str,
        text: str
    ) -> List[int]:
        """Find all positions where pattern matches."""
        pattern_hashes = set(self.fingerprints(pattern))
        text_hashes = self.fingerprints(text)

        matches = []
        for i, h in enumerate(text_hashes):
            if h in pattern_hashes:
                matches.append(i)
        return matches


class DataFuzzyHashAction:
    """
    Fuzzy hashing for similarity detection and deduplication.

    Example:
        hasher = DataFuzzyHashAction()
        h1 = hasher.simhash("hello world")
        h2 = hasher.simhash("hello world!")
        sim = hasher.similarity(h1, h2)

        ngram = hasher.create_ngram_hasher(n=3)
        sim = ngram.similarity("hello world", "hello world")
    """

    def __init__(self):
        self.simhasher = FuzzyHash()

    def simhash(self, text: str, dim: int = 64) -> int:
        """Generate SimHash."""
        return FuzzyHash.simhash(text, dim)

    def similarity(self, hash1: int, hash2: int) -> float:
        """Calculate hash similarity."""
        return FuzzyHash.similarity(hash1, hash2)

    def hamming_distance(self, hash1: int, hash2: int) -> int:
        """Calculate Hamming distance."""
        return FuzzyHash.hamming_distance(hash1, hash2)

    def create_ngram_hasher(self, n: int = 3) -> NGramFuzzyHash:
        """Create n-gram based fuzzy hasher."""
        return NGramFuzzyHash(n)

    def create_rabin_karp_hasher(self, window_size: int = 5) -> RabinKarpFuzzyHash:
        """Create Rabin-Karp rolling hasher."""
        return RabinKarpFuzzyHash(window_size)

    def deduplicate(
        self,
        texts: List[str],
        threshold: float = 0.8
    ) -> List[Tuple[int, str]]:
        """Deduplicate similar texts. Returns list of (index, representative) pairs."""
        if not texts:
            return []

        hasher = NGramFuzzyHash(n=3)
        representatives = []
        mappings = []

        for i, text in enumerate(texts):
            found = False
            for rep_idx, rep_text in representatives:
                sim = hasher.similarity(text, rep_text)
                if sim >= threshold:
                    mappings.append((i, rep_idx))
                    found = True
                    break

            if not found:
                new_idx = len(representatives)
                representatives.append((i, text))
                mappings.append((i, new_idx))

        return [(i, texts[idx]) for i, idx in mappings]
