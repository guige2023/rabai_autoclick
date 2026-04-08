"""Notion page/database action module for RabAI AutoClick.

Provides Notion API operations for pages and databases.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotionPageAction(BaseAction):
    """Create and update pages in Notion."""
    action_type = "notion_page"
    display_name = "Notion页面"
    description = "Notion页面创建与更新"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create or update Notion page.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Notion API key
                - action: 'create' or 'update'
                - parent_id: Parent page or database ID
                - properties: Page properties
                - content: Rich text content blocks

        Returns:
            ActionResult with page data.
        """
        api_key = params.get('api_key') or os.environ.get('NOTION_API_KEY')
        action = params.get('action', 'create')
        parent_id = params.get('parent_id', '')
        properties = params.get('properties', {})
        content = params.get('content', [])

        if not api_key:
            return ActionResult(success=False, message="NOTION_API_KEY is required")
        if not parent_id:
            return ActionResult(success=False, message="parent_id is required")

        try:
            from notion_client import NotionClient
        except ImportError:
            return ActionResult(success=False, message="notion_client not installed. Run: pip install notion_client")

        start = time.time()
        try:
            client = NotionClient(token=api_key)
            if action == 'create':
                payload = {
                    'parent': {'page_id': parent_id} if len(parent_id) == 32 else {'database_id': parent_id},
                    'properties': properties,
                }
                if content:
                    payload['children'] = content
                result = client.create_page(**payload)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Notion page created",
                    data={'page_id': result.get('id')}, duration=duration
                )
            else:
                return ActionResult(success=False, message=f"Action '{action}' not yet implemented")
        except Exception as e:
            return ActionResult(success=False, message=f"Notion API error: {str(e)}")
