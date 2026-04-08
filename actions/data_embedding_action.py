"""Data Embedding Action.

Generates vector embeddings from text and structured data.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import hashlib


@dataclass
class EmbeddingResult:
    text: str
    vector: List[float]
    model: str
    dimension: int
    metadata: Dict[str, Any]


class DataEmbeddingAction:
    """Generates and manages vector embeddings for data."""

    def __init__(
        self,
        model: str = "default",
        dimension: int = 384,
        normalize: bool = True,
    ) -> None:
        self.model = model
        self.dimension = dimension
        self.normalize = normalize
        self._cache: Dict[str, List[float]] = {}
        self._embed_fn: Optional[Callable[[str], List[float]]] = None

    def set_embed_fn(self, fn: Callable[[str], List[float]]) -> None:
        self._embed_fn = fn

    def _mock_embed(self, text: str) -> List[float]:
        h = hashlib.sha256(text.encode()).digest()
        vec = [float(b) / 255.0 for b in h[:self.dimension]]
        if self.normalize:
            norm = sum(v * v for v in vec) ** 0.5
            vec = [v / norm for v in vec]
        return vec

    def embed(self, text: str, cache: bool = True) -> EmbeddingResult:
        if cache and text in self._cache:
            return EmbeddingResult(
                text=text,
                vector=self._cache[text],
                model=self.model,
                dimension=self.dimension,
                metadata={"cached": True},
            )
        if self._embed_fn:
            vector = self._embed_fn(text)
        else:
            vector = self._mock_embed(text)
        if cache:
            self._cache[text] = vector
        return EmbeddingResult(
            text=text,
            vector=vector,
            model=self.model,
            dimension=self.dimension,
            metadata={"cached": False},
        )

    def embed_batch(self, texts: List[str]) -> List[EmbeddingResult]:
        return [self.embed(t) for t in texts]

    def cosine_similarity(self, a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        return dot / (norm_a * norm_b) if norm_a and norm_b else 0.0

    def find_similar(
        self,
        query: str,
        candidates: List[str],
        top_k: int = 5,
    ) -> List[tuple]:
        query_vec = self.embed(query).vector
        scored = []
        for candidate in candidates:
            cand_vec = self.embed(candidate).vector
            score = self.cosine_similarity(query_vec, cand_vec)
            scored.append((candidate, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

    def clear_cache(self) -> None:
        self._cache.clear()
