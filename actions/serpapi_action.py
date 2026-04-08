"""SerpAPI search action module for RabAI AutoClick.

Provides Google and other search engine results via SerpAPI.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SerpAPISearchAction(BaseAction):
    """Execute web search via SerpAPI for Google, Bing, and other engines."""
    action_type = "serpapi_search"
    display_name = "SerpAPI搜索"
    description = "SerpAPI多引擎搜索"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SerpAPI search.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: SerpAPI API key
                - query: Search query
                - engine: Search engine (google, bing, yahoo, etc.)
                - num_results: Number of results

        Returns:
            ActionResult with search results.
        """
        api_key = params.get('api_key') or os.environ.get('SERPAPI_API_KEY')
        query = params.get('query', '')
        engine = params.get('engine', 'google')
        num_results = params.get('num_results', 10)

        if not api_key:
            return ActionResult(success=False, message="SERPAPI_API_KEY is required")
        if not query:
            return ActionResult(success=False, message="query is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.get(
                'https://serpapi.com/search',
                params={
                    'q': query,
                    'api_key': api_key,
                    'engine': engine,
                    'num': num_results,
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            results = data.get('organic_results', [])
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Found {len(results)} results",
                data={'results': results, 'query': query, 'engine': engine},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SerpAPI error: {str(e)}")
