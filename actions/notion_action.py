"""Notion action module for RabAI AutoClick.

Provides Notion API operations for pages, databases, and blocks.
"""

import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotionAction(BaseAction):
    """Notion API operations.
    
    Supports reading and writing Notion pages, databases,
    and block content via the Notion API.
    """
    action_type = "notion"
    display_name = "Notion笔记"
    description = "Notion页面与数据库操作"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Notion operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'create_page', 'get_page', 'update_page', 'query_database', 'create_database'
                - token: Notion API token (or env NOTION_TOKEN)
                - page_id: Page ID for operations
                - database_id: Database ID for query/create
                - properties: Page properties dict
                - content: Block content for page body
        
        Returns:
            ActionResult with operation result.
        """
        token = params.get('token') or os.environ.get('NOTION_TOKEN')
        if not token:
            return ActionResult(success=False, message="Notion token required (set NOTION_TOKEN env)")
        
        command = params.get('command', 'get_page')
        page_id = params.get('page_id')
        database_id = params.get('database_id')
        properties = params.get('properties', {})
        content = params.get('content', [])
        title = params.get('title', properties.get('title', 'Untitled'))
        
        base_url = "https://api.notion.com/v1"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        }
        
        if command == 'create_page':
            if not database_id:
                return ActionResult(success=False, message="database_id required for create_page")
            return self._notion_create_page(base_url, headers, database_id, properties, title)
        
        if command == 'get_page':
            if not page_id:
                return ActionResult(success=False, message="page_id required for get_page")
            return self._notion_get_page(base_url, headers, page_id)
        
        if command == 'update_page':
            if not page_id:
                return ActionResult(success=False, message="page_id required for update_page")
            return self._notion_update_page(base_url, headers, page_id, properties)
        
        if command == 'query_database':
            if not database_id:
                return ActionResult(success=False, message="database_id required for query_database")
            return self._notion_query_database(base_url, headers, database_id)
        
        if command == 'create_database':
            if not page_id:
                return ActionResult(success=False, message="page_id (parent) required for create_database")
            return self._notion_create_database(base_url, headers, page_id, properties, title)
        
        if command == 'append_blocks':
            if not page_id:
                return ActionResult(success=False, message="page_id required for append_blocks")
            return self._notion_append_blocks(base_url, headers, page_id, content)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _notion_request(self, base_url: str, headers: Dict, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Make HTTP request to Notion API."""
        from urllib.request import Request, urlopen
        
        url = f"{base_url}/{endpoint}"
        req_data = json.dumps(data).encode('utf-8') if data else None
        request = Request(url, data=req_data, headers=headers, method=method)
        with urlopen(request, timeout=15) as resp:
            return json.loads(resp.read().decode())
    
    def _notion_create_page(self, base_url: str, headers: Dict, database_id: str, properties: Dict, title: str) -> ActionResult:
        """Create a new Notion page in a database."""
        try:
            props = {}
            for key, value in properties.items():
                if isinstance(value, str):
                    props[key] = {'title': [{'text': {'content': value}}]}
                elif isinstance(value, int):
                    props[key] = {'number': value}
                elif isinstance(value, bool):
                    props[key] = {'checkbox': value}
                else:
                    props[key] = {'rich_text': [{'text': {'content': str(value)}}]}
            
            payload = {
                'parent': {'database_id': database_id},
                'properties': props
            }
            
            result = self._notion_request(base_url, headers, 'POST', 'pages', payload)
            page_id = result.get('id', '')
            return ActionResult(
                success=True,
                message=f"Created Notion page {page_id[:8]}...",
                data={'page_id': page_id, 'url': result.get('url')}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create page: {e}")
    
    def _notion_get_page(self, base_url: str, headers: Dict, page_id: str) -> ActionResult:
        """Get Notion page."""
        try:
            result = self._notion_request(base_url, headers, 'GET', f'pages/{page_id}')
            props = result.get('properties', {})
            return ActionResult(
                success=True,
                message=f"Retrieved page {page_id[:8]}...",
                data={'page_id': page_id, 'properties': props, 'url': result.get('url')}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get page: {e}")
    
    def _notion_update_page(self, base_url: str, headers: Dict, page_id: str, properties: Dict) -> ActionResult:
        """Update Notion page properties."""
        try:
            props = {}
            for key, value in properties.items():
                if isinstance(value, str):
                    props[key] = {'title': [{'text': {'content': value}}]}
                elif isinstance(value, int):
                    props[key] = {'number': value}
                elif isinstance(value, bool):
                    props[key] = {'checkbox': value}
                else:
                    props[key] = {'rich_text': [{'text': {'content': str(value)}}]}
            
            payload = {'properties': props}
            result = self._notion_request(base_url, headers, 'PATCH', f'pages/{page_id}', payload)
            return ActionResult(success=True, message=f"Updated page {page_id[:8]}...", data={'page_id': result.get('id')})
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to update page: {e}")
    
    def _notion_query_database(self, base_url: str, headers: Dict, database_id: str) -> ActionResult:
        """Query Notion database."""
        try:
            result = self._notion_request(base_url, headers, 'POST', f'databases/{database_id}/query', {})
            pages = result.get('results', [])
            return ActionResult(
                success=True,
                message=f"Query returned {len(pages)} pages",
                data={'pages': [{'id': p.get('id'), 'properties': p.get('properties', {})} for p in pages], 'count': len(pages)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to query database: {e}")
    
    def _notion_create_database(self, base_url: str, headers: Dict, parent_page_id: str, properties: Dict, title: str) -> ActionResult:
        """Create a new Notion database."""
        try:
            props = {'Name': {'title': [{'text': {'content': 'Name'}}]}}
            for key, value_type in properties.items():
                if value_type == 'number':
                    props[key] = {'number': {}}
                elif value_type == 'checkbox':
                    props[key] = {'checkbox': {}}
                else:
                    props[key] = {'rich_text': {}}
            
            payload = {
                'parent': {'page_id': parent_page_id},
                'title': [{'text': {'content': title}}],
                'properties': props
            }
            result = self._notion_request(base_url, headers, 'POST', 'databases', payload)
            db_id = result.get('id', '')
            return ActionResult(success=True, message=f"Created database {db_id[:8]}...", data={'database_id': db_id})
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create database: {e}")
    
    def _notion_append_blocks(self, base_url: str, headers: Dict, page_id: str, content: List[Dict]) -> ActionResult:
        """Append blocks to a page."""
        try:
            blocks = []
            for item in content:
                if isinstance(item, str):
                    blocks.append({'type': 'paragraph', 'paragraph': {'rich_text': [{'type': 'text', 'text': {'content': item}}]}})
                elif isinstance(item, dict):
                    blocks.append(item)
            
            payload = {'children': blocks}
            result = self._notion_request(base_url, headers, 'PATCH', f'blocks/{page_id}/children', payload)
            return ActionResult(success=True, message=f"Appended {len(blocks)} blocks", data={'blocks': result.get('results', [])})
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to append blocks: {e}")
