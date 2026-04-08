"""Apache Pinot action module for RabAI AutoClick.

Provides Apache Pinot query operations for real-time analytics.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PinotQueryAction(BaseAction):
    """Execute PQL queries on Apache Pinot."""
    action_type = "pinot_query"
    display_name = "Pinot查询"
    description = "Apache Pinot实时分析查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Pinot PQL query.

        Args:
            context: Execution context.
            params: Dict with keys:
                - broker_url: Pinot broker URL
                - query: PQL query string
                - query_options: Optional query options

        Returns:
            ActionResult with query results.
        """
        broker_url = params.get('broker_url', '')
        query = params.get('query', '')
        query_options = params.get('query_options', {})

        if not broker_url:
            return ActionResult(success=False, message="broker_url is required")
        if not query:
            return ActionResult(success=False, message="query is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.post(
                f"{broker_url.rstrip('/')}/query",
                json={'pql': query, 'queryOptions': query_options},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            num_rows = len(data.get('results', []))
            return ActionResult(
                success=True, message=f"Query returned {num_rows} rows",
                data={'results': data.get('results', []), 'num_rows': num_rows}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pinot query error: {str(e)}")
