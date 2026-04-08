"""OpenSearch action module for RabAI AutoClick.

Provides OpenSearch operations for full-text search and document indexing.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OpenSearchAction(BaseAction):
    """Execute OpenSearch queries and indexing."""
    action_type = "opensearch_query"
    display_name = "OpenSearch查询"
    description = "OpenSearch全文搜索"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OpenSearch query.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: OpenSearch endpoint URL
                - index: Index name
                - query: OpenSearch query DSL dict
                - size: Number of results

        Returns:
            ActionResult with search results.
        """
        url = params.get('url', '')
        index = params.get('index', '')
        query = params.get('query', {'match_all': {}})
        size = params.get('size', 10)

        if not url:
            return ActionResult(success=False, message="url is required")
        if not index:
            return ActionResult(success=False, message="index is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            search_url = f"{url.rstrip('/')}/{index}/_search"
            payload = {'query': query, 'size': size}
            response = requests.post(search_url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Found {len(hits)} results",
                data={
                    'hits': [{'_id': h['_id'], '_source': h['_source'], '_score': h.get('_score')} for h in hits],
                    'total': data.get('hits', {}).get('total', {}).get('value', 0),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenSearch error: {str(e)}")
