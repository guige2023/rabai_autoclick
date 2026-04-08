"""Notion database action module for RabAI AutoClick.

Provides Notion database query and record operations.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotionDatabaseQueryAction(BaseAction):
    """Query Notion database with filters and sorts."""
    action_type = "notion_database_query"
    display_name = "Notion数据库查询"
    description = "Notion数据库查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Query Notion database.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Notion API key
                - database_id: Database ID
                - filter: Notion filter object
                - sorts: Notion sorts list
                - page_size: Results per page (max 100)

        Returns:
            ActionResult with query results.
        """
        api_key = params.get('api_key', '') or os.environ.get('NOTION_API_KEY')
        database_id = params.get('database_id', '')
        filter_obj = params.get('filter', None)
        sorts = params.get('sorts', [])
        page_size = params.get('page_size', 100)

        if not api_key:
            return ActionResult(success=False, message="NOTION_API_KEY is required")
        if not database_id:
            return ActionResult(success=False, message="database_id is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            url = f"https://api.notion.com/v1/databases/{database_id}/query"
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Notion-Version': '2022-06-28',
                'Content-Type': 'application/json',
            }
            payload: Dict[str, Any] = {'page_size': min(page_size, 100)}
            if filter_obj:
                payload['filter'] = filter_obj
            if sorts:
                payload['sorts'] = sorts

            all_results = []
            while url:
                resp = requests.post(url, json=payload, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                all_results.extend(data.get('results', []))
                url = data.get('next_cursor') and f"https://api.notion.com/v1/databases/{database_id}/query?start_cursor={data['next_cursor']}"
                payload = {}

            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Query returned {len(all_results)} results",
                data={'count': len(all_results), 'results': all_results}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notion query error: {str(e)}")
