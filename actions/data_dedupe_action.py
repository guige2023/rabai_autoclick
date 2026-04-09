"""Data Deduplication Action Module.

Provides data deduplication using various strategies including
exact match, fuzzy match, and content-defined chunking.
"""

from __future__ import annotations

import hashlib
import logging
import simhash
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class DedupStrategy(Enum):
    """Deduplication strategies."""
    EXACT = "exact"
    SIMHASH = "simhash"
    FINGERPRINT = "fingerprint"
    CONTENT_DEFINED = "content_defined"
    SAMPLING = "sampling"


@dataclass
class DedupResult:
    """Result of a deduplication operation."""
    is_duplicate: bool
    duplicate_of: Optional[str] = None
    similarity: float = 1.0
    method: DedupStrategy = DedupStrategy.EXACT


@dataclass
class DedupStats:
    """Deduplication statistics."""
    total_items: int = 0
    unique_items: int = 0
    duplicate_count: int = 0
    dedup_rate: float = 0.0


class ExactDeduplicator:
    """Exact match deduplication using hashes."""

    def __init__(self):
        self._hashes: Dict[str, str] = {}  # hash -> item_id
        self._reverse: Dict[str, str] = {}  # item_id -> hash

    def add(self, item_id: str, data: bytes) -> bool:
        """Add an item, return True if not duplicate."""
        h = hashlib.sha256(data).hexdigest()
        if h in self._hashes:
            return False
        self._hashes[h] = item_id
        self._reverse[item_id] = h
        return True

    def check(self, data: bytes) -> Tuple[bool, Optional[str]]:
        """Check if data is duplicate, return (is_dup, original_id)."""
        h = hashlib.sha256(data).hexdigest()
        original_id = self._hashes.get(h)
        return original_id is not None, original_id

    def get_hash(self, item_id: str) -> Optional[str]:
        """Get hash for an item ID."""
        return self._reverse.get(item_id)

    def remove(self, item_id: str) -> bool:
        """Remove an item."""
        h = self._reverse.pop(item_id, None)
        if h:
            self._hashes.pop(h, None)
            return True
        return False

    def count(self) -> int:
        """Return number of unique items."""
        return len(self._hashes)


class SimhashDeduplicator:
    """Fuzzy deduplication using SimHash for near-duplicate detection."""

    def __init__(self, threshold: float = 0.9):
        self._hashes: Dict[str, int] = {}  # item_id -> simhash value
        self._threshold = threshold
        self._bits = 64

    def add(self, item_id: str, text: str) -> bool:
        """Add an item, return True if not near-duplicate."""
        h = self._compute_simhash(text)
        for existing_id, existing_hash in self._hashes.items():
            similarity = self._compute_similarity(h, existing_hash)
            if similarity >= self._threshold:
                return False
        self._hashes[item_id] = h
        return True

    def check(self, text: str) -> Tuple[bool, Optional[str], float]:
        """Check if text is near-duplicate, return (is_dup, original_id, similarity)."""
        h = self._compute_simhash(text)
        best_similarity = 0.0
        best_id = None

        for existing_id, existing_hash in self._hashes.items():
            similarity = self._compute_similarity(h, existing_hash)
            if similarity > best_similarity:
                best_similarity = similarity
                best_id = existing_id

        if best_similarity >= self._threshold:
            return True, best_id, best_similarity
        return False, None, best_similarity

    def _compute_simhash(self, text: str) -> int:
        """Compute SimHash for text."""
        return simhash.simhash64(text)

    def _compute_similarity(self, h1: int, h2: int) -> float:
        """Compute Hamming similarity between two hashes."""
        xor = h1 ^ h2
        distance = bin(xor).count("1")
        return 1.0 - (distance / self._bits)

    def count(self) -> int:
        """Return number of unique items."""
        return len(self._hashes)


class FingerprintDeduplicator:
    """Deduplication using MinHash fingerprints."""

    def __init__(self, num_hashes: int = 128, threshold: float = 0.5):
        self._num_hashes = num_hashes
        self._threshold = threshold
        self._fingerprints: Dict[str, List[int]] = {}
        self._minhashes = simhash.MinHash(num_hashes=self._num_hashes)

    def add(self, item_id: str, text: str) -> bool:
        """Add an item using MinHash fingerprint."""
        minhash = simhash.MinHash(self._num_hashes)
        for word in text.split():
            minhash.update(word.encode("utf-8"))

        # Store fingerprint
        self._fingerprints[item_id] = minhash.digest()

        return True

    def check(self, text: str) -> Tuple[bool, Optional[str], float]:
        """Check if text is near-duplicate using MinHash."""
        minhash = simhash.MinHash(num_hashes=self._num_hashes)
        for word in text.split():
            minhash.update(word.encode("utf-8"))

        best_similarity = 0.0
        best_id = None

        for item_id, stored_fp in self._fingerprints.items():
            # Compute Jaccard similarity between minhashes
            similarity = minhash.jaccard(simhash.MinHash(stored_fp))
            if similarity > best_similarity:
                best_similarity = similarity
                best_id = item_id

        if best_similarity >= self._threshold:
            return True, best_id, best_similarity
        return False, None, best_similarity

    def count(self) -> int:
        """Return number of unique items."""
        return len(self._fingerprints)


class ContentDefinedChunker:
    """Content-defined chunking for deduplication at sub-document level."""

    def __init__(self, chunk_size: int = 1024, min_chunk: int = 256):
        self._chunk_size = chunk_size
        self._min_chunk = min_chunk
        self._chunk_hashes: Dict[str, Set[str]] = defaultdict(set)  # item_id -> chunk_hashes

    def _compute_chunks(self, data: bytes) -> List[bytes]:
        """Split data into content-defined chunks."""
        chunks = []
        i = 0
        while i < len(data):
            end = min(i + self._chunk_size, len(data))
            if end - i < self._min_chunk and chunks:
                chunks[-1] += data[i:end]
            else:
                chunks.append(data[i:end])
            i = end
        return chunks

    def add(self, item_id: str, data: bytes) -> Tuple[int, int]:
        """Add an item, return (total_chunks, new_unique_chunks)."""
        chunks = self._compute_chunks(data)
        chunk_hashes = set()
        new_count = 0

        for chunk in chunks:
            h = hashlib.sha256(chunk).hexdigest()
            chunk_hashes.add(h)

        for h in chunk_hashes:
            is_new = True
            for existing_hashes in self._chunk_hashes.values():
                if h in existing_hashes:
                    is_new = False
                    break
            if is_new:
                new_count += 1

        self._chunk_hashes[item_id] = chunk_hashes
        return len(chunks), new_count

    def get_shared_chunks(self, item_id: str) -> Set[str]:
        """Get chunk hashes shared with other items."""
        if item_id not in self._chunk_hashes:
            return set()
        my_chunks = self._chunk_hashes[item_id]
        shared = set()
        for existing_id, existing_chunks in self._chunk_hashes.items():
            if existing_id != item_id:
                shared.update(my_chunks & existing_chunks)
        return shared

    def count(self) -> int:
        """Return number of items."""
        return len(self._chunk_hashes)


class DataDedupeAction:
    """Main action class for data deduplication."""

    def __init__(self):
        self._exact = ExactDeduplicator()
        self._simhash = SimhashDeduplicator()
        self._fingerprint = FingerprintDeduplicator()
        self._chunker = ContentDefinedChunker()
        self._stats = DedupStats()
        self._default_strategy = DedupStrategy.EXACT

    def set_strategy(self, strategy: DedupStrategy) -> None:
        """Set the default deduplication strategy."""
        self._default_strategy = strategy

    def add_item(
        self,
        item_id: str,
        data: Any,
        strategy: Optional[DedupStrategy] = None
    ) -> bool:
        """Add an item, return True if not duplicate."""
        strat = strategy or self._default_strategy

        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            data_bytes = str(data).encode("utf-8")

        if strat == DedupStrategy.EXACT:
            result = self._exact.add(item_id, data_bytes)
        elif strat == DedupStrategy.SIMHASH:
            result = self._simhash.add(item_id, data_bytes.decode("utf-8", errors="ignore"))
        elif strat == DedupStrategy.CONTENT_DEFINED:
            _, new_count = self._chunker.add(item_id, data_bytes)
            result = new_count > 0
        else:
            result = self._fingerprint.add(item_id, data_bytes.decode("utf-8", errors="ignore"))

        self._stats.total_items += 1
        if result:
            self._stats.unique_items += 1
        else:
            self._stats.duplicate_count += 1

        if self._stats.total_items > 0:
            self._stats.dedup_rate = (self._stats.duplicate_count / self._stats.total_items) * 100

        return result

    def check_duplicate(
        self,
        data: Any,
        strategy: Optional[DedupStrategy] = None
    ) -> DedupResult:
        """Check if data is duplicate."""
        strat = strategy or self._default_strategy

        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
            data_text = data
        elif isinstance(data, bytes):
            data_bytes = data
            data_text = data.decode("utf-8", errors="ignore")
        else:
            data_bytes = str(data).encode("utf-8")
            data_text = str(data)

        if strat == DedupStrategy.EXACT:
            is_dup, original_id = self._exact.check(data_bytes)
            return DedupResult(is_duplicate=is_dup, duplicate_of=original_id, method=strat)
        elif strat == DedupStrategy.SIMHASH:
            is_dup, original_id, similarity = self._simhash.check(data_text)
            return DedupResult(is_duplicate=is_dup, duplicate_of=original_id, similarity=similarity, method=strat)
        elif strat == DedupStrategy.CONTENT_DEFINED:
            is_dup = False
            # Content-defined chunking doesn't support single-item check
            return DedupResult(is_duplicate=False, method=strat)
        else:
            is_dup, original_id, similarity = self._fingerprint.check(data_text)
            return DedupResult(is_duplicate=is_dup, duplicate_of=original_id, similarity=similarity, method=strat)

    def get_stats(self) -> DedupStats:
        """Return deduplication statistics."""
        return self._stats

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data deduplication action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform (add, check, stats)
                - data: Data to process
                - item_id: Item identifier
                - strategy: DedupStrategy value

        Returns:
            Dictionary with deduplication results.
        """
        operation = context.get("operation", "add")

        if operation == "add":
            item_id = context.get("item_id", "")
            data = context.get("data", "")
            strat_str = context.get("strategy", "exact")
            try:
                strat = DedupStrategy(strat_str)
            except ValueError:
                strat = self._default_strategy

            result = self.add_item(item_id, data, strat)
            return {
                "success": True,
                "is_unique": result,
                "stats": {
                    "total_items": self._stats.total_items,
                    "unique_items": self._stats.unique_items,
                    "duplicate_count": self._stats.duplicate_count,
                    "dedup_rate": round(self._stats.dedup_rate, 2)
                }
            }

        elif operation == "check":
            data = context.get("data", "")
            strat_str = context.get("strategy", "exact")
            try:
                strat = DedupStrategy(strat_str)
            except ValueError:
                strat = self._default_strategy

            result = self.check_duplicate(data, strat)
            return {
                "success": True,
                "is_duplicate": result.is_duplicate,
                "duplicate_of": result.duplicate_of,
                "similarity": round(result.similarity, 4),
                "method": result.method.value
            }

        elif operation == "stats":
            return {
                "success": True,
                "stats": {
                    "total_items": self._stats.total_items,
                    "unique_items": self._stats.unique_items,
                    "duplicate_count": self._stats.duplicate_count,
                    "dedup_rate": round(self._stats.dedup_rate, 2)
                }
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
