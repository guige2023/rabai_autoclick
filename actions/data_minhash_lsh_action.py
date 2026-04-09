"""Data MinHash LSH Action.

MinHash locality-sensitive hashing for fast approximate nearest
neighbor search and similarity estimation in large document sets.
"""
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import hashlib


@dataclass
class MinHashSignature:
    document_id: str
    signature: Tuple[int, ...]
    shingles: Set[str]


class DataMinHashLSHAction:
    """MinHash LSH for approximate set similarity."""

    def __init__(
        self,
        num_hashes: int = 128,
        shingle_size: int = 2,
        num_bands: int = 16,
        num_rows: int = 8,
    ) -> None:
        self.num_hashes = num_hashes
        self.shingle_size = shingle_size
        self.num_bands = num_bands
        self.num_rows = num_rows
        self._permutations: List[Tuple[int, int]] = []
        self._signatures: Dict[str, Tuple[int, ...]] = {}
        self._lsh_index: Dict[int, Set[str]] = {}
        self._generate_permutations()

    def _generate_permutations(self) -> None:
        self._permutations = []
        for i in range(self.num_hashes):
            a = (i * 19 + 7) % (2**32 - 1)
            b = (i * 31 + 13) % (2**32 - 1)
            self._permutations.append((a, b))

    def _shingle(self, text: str) -> Set[str]:
        text = text.lower()
        return {
            text[i : i + self.shingle_size]
            for i in range(len(text) - self.shingle_size + 1)
        }

    def _hash_shingle(self, shingle: str, seed: int = 0) -> int:
        combined = f"{shingle}:{seed}"
        return int(hashlib.sha256(combined.encode()).hexdigest(), 16)

    def _minhash(self, shingles: Set[str]) -> Tuple[int, ...]:
        if not shingles:
            return tuple(self.num_hashes * [2**32 - 1])
        sig = []
        for a, b in self._permutations:
            min_hash = min(
                (a * self._hash_shingle(s) + b) % (2**32 - 1) for s in shingles
            )
            sig.append(min_hash)
        return tuple(sig)

    def add_document(self, doc_id: str, text: str) -> MinHashSignature:
        shingles = self._shingle(text)
        sig = self._minhash(shingles)
        self._signatures[doc_id] = sig
        self._index_lsh(doc_id, sig)
        return MinHashSignature(document_id=doc_id, signature=sig, shingles=shingles)

    def _index_lsh(self, doc_id: str, sig: Tuple[int, ...]) -> None:
        for band_idx in range(self.num_bands):
            start = band_idx * self.num_rows
            end = start + self.num_rows
            band_sig = sig[start:end]
            band_hash = hash(
                tuple(band_sig)
            )
            if band_hash not in self._lsh_index:
                self._lsh_index[band_hash] = set()
            self._lsh_index[band_hash].add(doc_id)

    def estimate_similarity(self, doc_id1: str, doc_id2: str) -> float:
        sig1 = self._signatures.get(doc_id1)
        sig2 = self._signatures.get(doc_id2)
        if not sig1 or not sig2:
            return 0.0
        if len(sig1) != len(sig2):
            return 0.0
        matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
        return matches / len(sig1)

    def find_similar(
        self,
        doc_id: str,
        threshold: float = 0.5,
        max_results: int = 10,
    ) -> List[Tuple[str, float]]:
        sig = self._signatures.get(doc_id)
        if not sig:
            return []
        candidates: Set[str] = set()
        for band_idx in range(self.num_bands):
            start = band_idx * self.num_rows
            end = start + self.num_rows
            band_hash = hash(tuple(sig[start:end]))
            if band_hash in self._lsh_index:
                candidates.update(self._lsh_index[band_hash])
        candidates.discard(doc_id)
        similarities = []
        for cand in candidates:
            sim = self.estimate_similarity(doc_id, cand)
            if sim >= threshold:
                similarities.append((cand, sim))
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:max_results]
