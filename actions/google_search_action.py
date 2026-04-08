"""Google Custom Search action module for RabAI AutoClick.

Provides programmatic Google search via Google Custom Search JSON API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GoogleSearchAction(BaseAction):
    """Execute search via Google Custom Search JSON API."""
    action_type = "google_search"
    display_name = "Google搜索"
    description = "Google自定义搜索API"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Google search.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Google API key
                - cx: Custom Search Engine ID
                - query: Search query
                - num: Number of results (1-10)

        Returns:
            ActionResult with search results.
        """
        api_key = params.get('api_key') or os.environ.get('GOOGLE_API_KEY')
        cx = params.get('cx', '')
        query = params.get('query', '')
        num = params.get('num', 10)

        if not api_key:
            return ActionResult(success=False, message="GOOGLE_API_KEY is required")
        if not cx:
            return ActionResult(success=False, message="cx (Custom Search Engine ID) is required")
        if not query:
            return ActionResult(success=False, message="query is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.get(
                'https://www.googleapis.com/customsearch/v1',
                params={'key': api_key, 'cx': cx, 'q': query, 'num': min(num, 10)},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            results = data.get('items', [])
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Found {len(results)} results",
                data={'results': results, 'query': query, 'total_results': data.get('searchInformation', {}).get('totalResults')},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Google search error: {str(e)}")
