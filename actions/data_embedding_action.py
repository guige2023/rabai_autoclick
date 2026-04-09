"""Data embedding action module for RabAI AutoClick.

Provides data embedding operations:
- DataEmbeddingAction: Generate embeddings from text
- DataEmbeddingBatchAction: Batch embedding generation
- DataEmbeddingSearchAction: Search by embedding similarity
- DataEmbeddingClusterAction: Cluster embeddings
"""

import math
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataEmbeddingAction(BaseAction):
    """Generate embeddings from text data."""
    action_type = "data_embedding"
    display_name = "数据嵌入向量"
    description = "从文本生成嵌入向量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            model = params.get("model", "simple")
            normalize = params.get("normalize", True)
            dimension = params.get("dimension", 128)

            if not text:
                return ActionResult(success=False, message="text is required")

            if model == "simple":
                embedding = self._simple_embedding(text, dimension)
            elif model == "tfidf_style":
                embedding = self._tfidf_embedding(text, dimension)
            elif model == "hash":
                embedding = self._hash_embedding(text, dimension)
            else:
                embedding = self._simple_embedding(text, dimension)

            if normalize:
                norm = math.sqrt(sum(e ** 2 for e in embedding))
                if norm > 0:
                    embedding = [e / norm for e in embedding]

            return ActionResult(
                success=True,
                message=f"Generated {dimension}-dim embedding",
                data={"embedding": embedding, "dimension": dimension, "model": model, "text_length": len(text)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {e}")

    def _simple_embedding(self, text: str, dimension: int) -> List[float]:
        """Simple word-based embedding."""
        words = text.lower().split()
        embedding = [0.0] * dimension
        for i, word in enumerate(words[:dimension]):
            hash_val = int(hashlib.md5(word.encode()).hexdigest()[:8], 16)
            embedding[i % dimension] += (hash_val % 1000) / 1000.0
        return embedding

    def _tfidf_embedding(self, text: str, dimension: int) -> List[float]:
        """TF-IDF style embedding."""
        words = text.lower().split()
        word_freq = {}
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1

        embedding = [0.0] * dimension
        for i, word in enumerate(words[:dimension]):
            hash_val = int(hashlib.md5(word.encode()).hexdigest()[:8], 16)
            idx = hash_val % dimension
            tf = word_freq[word] / len(words)
            embedding[idx] += tf * ((hash_val % 1000) / 1000.0)

        return embedding

    def _hash_embedding(self, text: str, dimension: int) -> List[float]:
        """Hash-based deterministic embedding."""
        embedding = [0.0] * dimension
        text_bytes = text.encode()
        for i in range(0, len(text_bytes), 8):
            chunk = text_bytes[i:i + 8]
            if len(chunk) < 8:
                chunk = chunk + b'\x00' * (8 - len(chunk))
            hash_val = int.from_bytes(chunk[:8], 'big')
            idx = hash_val % dimension
            embedding[idx] = (embedding[idx] + (hash_val % 1000) / 1000.0) % 1.0
        return embedding


class DataEmbeddingBatchAction(BaseAction):
    """Batch embedding generation."""
    action_type = "data_embedding_batch"
    display_name = "批量嵌入向量生成"
    description = "批量生成嵌入向量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            texts = params.get("texts", [])
            model = params.get("model", "simple")
            dimension = params.get("dimension", 128)
            normalize = params.get("normalize", True)

            if not texts:
                return ActionResult(success=False, message="texts list is required")

            from concurrent.futures import ThreadPoolExecutor, as_completed

            embeddings = []
            for text in texts:
                emb = self._simple_embedding(text, dimension) if model == "simple" else self._hash_embedding(text, dimension)
                if normalize:
                    norm = math.sqrt(sum(e ** 2 for e in emb))
                    if norm > 0:
                        emb = [e / norm for e in emb]
                embeddings.append(emb)

            return ActionResult(
                success=True,
                message=f"Generated {len(embeddings)} embeddings",
                data={"embeddings": embeddings, "count": len(embeddings), "dimension": dimension}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch embedding error: {e}")

    def _simple_embedding(self, text: str, dimension: int) -> List[float]:
        import hashlib
        words = text.lower().split()
        embedding = [0.0] * dimension
        for i, word in enumerate(words[:dimension]):
            hash_val = int(hashlib.md5(word.encode()).hexdigest()[:8], 16)
            embedding[i % dimension] += (hash_val % 1000) / 1000.0
        return embedding

    def _hash_embedding(self, text: str, dimension: int) -> List[float]:
        import hashlib
        embedding = [0.0] * dimension
        text_bytes = text.encode()
        for i in range(0, len(text_bytes), 8):
            chunk = text_bytes[i:i + 8]
            if len(chunk) < 8:
                chunk = chunk + b'\x00' * (8 - len(chunk))
            hash_val = int.from_bytes(chunk[:8], 'big')
            idx = hash_val % dimension
            embedding[idx] = (embedding[idx] + (hash_val % 1000) / 1000.0) % 1.0
        return embedding


class DataEmbeddingSearchAction(BaseAction):
    """Search by embedding similarity."""
    action_type = "data_embedding_search"
    display_name = "嵌入向量搜索"
    description = "基于嵌入向量相似度搜索"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", "")
            documents = params.get("documents", [])
            dimension = params.get("dimension", 128)
            top_k = params.get("top_k", 5)
            metric = params.get("metric", "cosine")

            if not query:
                return ActionResult(success=False, message="query is required")

            query_emb = self._simple_embedding(query.lower(), dimension)

            results = []
            for i, doc in enumerate(documents):
                if isinstance(doc, dict):
                    text = doc.get("text", str(doc))
                    doc_id = doc.get("id", i)
                else:
                    text = str(doc)
                    doc_id = i

                doc_emb = self._simple_embedding(text.lower(), dimension)

                if metric == "cosine":
                    similarity = self._cosine_similarity(query_emb, doc_emb)
                elif metric == "euclidean":
                    similarity = -self._euclidean_distance(query_emb, doc_emb)
                else:
                    similarity = self._cosine_similarity(query_emb, doc_emb)

                results.append({"id": doc_id, "text": text, "similarity": similarity})

            results.sort(key=lambda x: x["similarity"], reverse=True)
            top_results = results[:top_k]

            return ActionResult(
                success=True,
                message=f"Found top {len(top_results)} matches",
                data={"results": top_results, "total": len(documents), "top_k": top_k}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding search error: {e}")

    def _simple_embedding(self, text: str, dimension: int) -> List[float]:
        import hashlib
        words = text.lower().split()
        embedding = [0.0] * dimension
        for i, word in enumerate(words[:dimension]):
            hash_val = int(hashlib.md5(word.encode()).hexdigest()[:8], 16)
            embedding[i % dimension] += (hash_val % 1000) / 1000.0
        return embedding

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x ** 2 for x in a))
        norm_b = math.sqrt(sum(x ** 2 for x in b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def _euclidean_distance(self, a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


class DataEmbeddingClusterAction(BaseAction):
    """Cluster embeddings using k-means style."""
    action_type = "data_embedding_cluster"
    display_name = "嵌入向量聚类"
    description = "对嵌入向量进行聚类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            embeddings = params.get("embeddings", [])
            k = params.get("k", 3)
            max_iterations = params.get("max_iterations", 100)
            dimension = params.get("dimension", 128)

            if not embeddings:
                return ActionResult(success=False, message="embeddings list is required")

            if len(embeddings) < k:
                return ActionResult(success=False, message=f"Need at least {k} embeddings")

            centroids = embeddings[:k]
            assignments = [0] * len(embeddings)

            for _ in range(max_iterations):
                changed = False
                for i, emb in enumerate(embeddings):
                    distances = [self._euclidean(e, c) for c in centroids]
                    new_cluster = distances.index(min(distances))
                    if new_cluster != assignments[i]:
                        assignments[i] = new_cluster
                        changed = True

                if not changed:
                    break

                for c in range(k):
                    cluster_points = [embeddings[i] for i in range(len(embeddings)) if assignments[i] == c]
                    if cluster_points:
                        centroids[c] = [sum(p[d] for p in cluster_points) / len(cluster_points) for d in range(dimension)]

            clusters = {c: [] for c in range(k)}
            for i, assignment in enumerate(assignments):
                clusters[assignment].append(i)

            return ActionResult(
                success=True,
                message=f"Clustered into {k} groups",
                data={"clusters": clusters, "centroids": centroids, "assignments": assignments, "k": k}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clustering error: {e}")

    def _euclidean(self, a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
