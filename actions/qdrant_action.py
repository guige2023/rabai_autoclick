"""Qdrant vector database action module for RabAI AutoClick.

Provides Qdrant operations for vector search and storage.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QdrantSearchAction(BaseAction):
    """Execute vector similarity search via Qdrant."""
    action_type = "qdrant_search"
    display_name = "Qdrant向量搜索"
    description = "Qdrant向量数据库查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Qdrant vector search.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Qdrant server URL
                - api_key: Qdrant API key
                - collection: Collection name
                - vector: Query vector
                - limit: Max results
                - score_threshold: Minimum similarity score

        Returns:
            ActionResult with search results.
        """
        url = params.get('url', 'http://localhost:6333')
        api_key = params.get('api_key', '')
        collection = params.get('collection', '')
        vector = params.get('vector', [])
        limit = params.get('limit', 10)
        score_threshold = params.get('score_threshold', 0.0)

        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not vector:
            return ActionResult(success=False, message="vector is required")

        try:
            from qdrant_client import QdrantClient
        except ImportError:
            return ActionResult(success=False, message="qdrant-client not installed. Run: pip install qdrant-client")

        start = time.time()
        try:
            client = QdrantClient(url=url, api_key=api_key) if api_key else QdrantClient(url=url)
            results = client.search(
                collection_name=collection,
                query_vector=vector,
                limit=limit,
                score_threshold=score_threshold,
            )
            duration = time.time() - start
            hits = [{'id': r.id, 'score': r.score, 'payload': r.payload} for r in results]
            return ActionResult(
                success=True, message=f"Found {len(hits)} results",
                data={'hits': hits, 'collection': collection}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Qdrant error: {str(e)}")
