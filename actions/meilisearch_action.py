"""Meilisearch action module for RabAI AutoClick.

Provides Meilisearch operations for full-text search and indexing.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MeilisearchSearchAction(BaseAction):
    """Execute full-text search via Meilisearch."""
    action_type = "meilisearch_search"
    display_name = "Meilisearch搜索"
    description = "Meilisearch全文搜索"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Meilisearch query.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Meilisearch host URL
                - api_key: Meilisearch API key
                - index: Index name
                - query: Search query
                - limit: Max results
                - attributes_to_retrieve: Fields to return

        Returns:
            ActionResult with search results.
        """
        url = params.get('url', 'http://localhost:7700')
        api_key = params.get('api_key', '')
        index_name = params.get('index', '')
        query = params.get('query', '')
        limit = params.get('limit', 20)

        if not index_name:
            return ActionResult(success=False, message="index is required")
        if not query:
            return ActionResult(success=False, message="query is required")

        try:
            import meilisearch
        except ImportError:
            return ActionResult(success=False, message="meilisearch not installed. Run: pip install meilisearch")

        start = time.time()
        try:
            client = meilisearch.Client(url, api_key) if api_key else meilisearch.Client(url)
            index = client.index(index_name)
            results = index.search(query, {'limit': limit})
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Found {results.get('estimatedTotalHits', 0)} hits",
                data={
                    'hits': results.get('hits', []),
                    'query': query,
                    'processing_time_ms': results.get('processingTimeMs', 0),
                    'total_hits': results.get('estimatedTotalHits', 0),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Meilisearch error: {str(e)}")
