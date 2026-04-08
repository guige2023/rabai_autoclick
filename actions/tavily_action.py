"""Tavily AI search action module for RabAI AutoClick.

Provides AI-optimized web search via Tavily API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TavilySearchAction(BaseAction):
    """Execute AI-optimized web search via Tavily API.

    Returns concise, relevant results optimized for AI agents.
    """
    action_type = "tavily_search"
    display_name = "Tavily搜索"
    description = "Tavily AI搜索引擎"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Tavily search.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Tavily API key
                - query: Search query
                - search_depth: 'basic' or 'advanced'
                - max_results: Maximum number of results (1-20)
                - include_answer: Include AI-generated answer
                - include_domains: List of domains to include
                - exclude_domains: List of domains to exclude

        Returns:
            ActionResult with search results.
        """
        api_key = params.get('api_key') or os.environ.get('TAVILY_API_KEY')
        query = params.get('query', '')
        search_depth = params.get('search_depth', 'basic')
        max_results = params.get('max_results', 5)
        include_answer = params.get('include_answer', False)
        include_domains = params.get('include_domains', [])
        exclude_domains = params.get('exclude_domains', [])

        if not api_key:
            return ActionResult(success=False, message="TAVILY_API_KEY is required")
        if not query:
            return ActionResult(success=False, message="query is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.post(
                'https://api.tavily.com/search',
                json={
                    'api_key': api_key,
                    'query': query,
                    'search_depth': search_depth,
                    'max_results': max_results,
                    'include_answer': include_answer,
                    'include_domains': include_domains,
                    'exclude_domains': exclude_domains,
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Found {len(data.get('results', []))} results",
                data={
                    'results': data.get('results', []),
                    'answer': data.get('answer'),
                    'query': query,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Tavily search error: {str(e)}")
