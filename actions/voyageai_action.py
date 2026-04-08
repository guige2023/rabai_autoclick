"""VoyageAI embedding action module for RabAI AutoClick.

Provides state-of-the-art text embeddings via Voyage AI API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VoyageAIEmbeddingAction(BaseAction):
    """Generate text embeddings via Voyage AI API.

    Supports multiple embedding models optimized for different use cases.
    """
    action_type = "voyageai_embedding"
    display_name = "VoyageAI嵌入"
    description = "VoyageAI文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: VoyageAI API key
                - texts: List of strings to embed
                - model: Model name (default: voyage-2)
                  Options: voyage-2, voyage-2-lite, voyage-code-2, voyage-multimodal-3
                - truncation: Whether to truncate long inputs

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('VOYAGE_API_KEY')
        texts = params.get('texts', [])
        model = params.get('model', 'voyage-2')
        truncation = params.get('truncation', True)

        if not api_key:
            return ActionResult(success=False, message="VOYAGE_API_KEY is required")
        if not texts:
            return ActionResult(success=False, message="texts list is required")

        try:
            from voyageai import client as voyage_client
        except ImportError:
            return ActionResult(success=False, message="voyageai package not installed. Run: pip install voyageai")

        vo = voyage_client(api_key=api_key)
        start = time.time()
        try:
            response = vo.embed(
                texts=texts,
                model=model,
                truncation=truncation,
            )
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Generated {len(response.embeddings)} embeddings",
                data={
                    'embeddings': response.embeddings,
                    'model': model,
                    'dimensions': response.embeddings[0].shape[-1] if hasattr(response.embeddings[0], 'shape') else len(response.embeddings[0]),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"VoyageAI embedding error: {str(e)}")


class VoyageAIRerankAction(BaseAction):
    """Rerank documents using Voyage AI API."""
    action_type = "voyageai_rerank"
    display_name = "VoyageAI重排序"
    description = "VoyageAI文档重排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Rerank documents.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: VoyageAI API key
                - query: Search query
                - documents: List of document strings
                - model: Rerank model
                - top_k: Number of results

        Returns:
            ActionResult with reranked results.
        """
        api_key = params.get('api_key') or os.environ.get('VOYAGE_API_KEY')
        query = params.get('query', '')
        documents = params.get('documents', [])
        model = params.get('model', 'rerank-2')
        top_k = params.get('top_k', 5)

        if not api_key:
            return ActionResult(success=False, message="VOYAGE_API_KEY is required")
        if not query or not documents:
            return ActionResult(success=False, message="query and documents are required")

        try:
            from voyageai import client as voyage_client
        except ImportError:
            return ActionResult(success=False, message="voyageai package not installed")

        vo = voyage_client(api_key=api_key)
        start = time.time()
        try:
            response = vo.rerank(
                query=query, documents=documents, model=model, top_k=top_k
            )
            results = [{'index': r.index, 'relevance_score': r.relevance_score} for r in response.results]
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Reranked {len(results)} documents",
                data={'results': results, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"VoyageAI rerank error: {str(e)}")
