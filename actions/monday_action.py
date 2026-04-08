"""Monday.com integration for RabAI AutoClick.

Provides actions to manage boards, items, groups, and updates on Monday.com.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MondayBoardAction(BaseAction):
    """Manage Monday.com boards - create, query, and update boards.

    Uses Monday.com GraphQL API v2.
    """
    action_type = "monday_board"
    display_name = "Monday看板管理"
    description = "创建、查询、更新Monday.com看板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Monday.com boards.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Monday.com API token
                - operation: create | get | list | update
                - board_id: Board ID for get/update operations
                - name: Board name (for create)
                - board_kind: public | private | share (for create)
                - description: Board description
                - workspace_id: Workspace ID

        Returns:
            ActionResult with board data.
        """
        api_key = params.get('api_key') or os.environ.get('MONDAY_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="MONDAY_API_KEY is required")

        import urllib.request
        import urllib.error

        query_map = {
            'list': '''
                query { boards { id name state kind description created_at workspace { id name } } }
            ''',
            'get': f'''
                query {{ boards(ids: [{params.get('board_id', 0)}]) {{ id name state kind description columns {{ id title type }} items {{ id name column_values {{ id column {{ id title }} text }} }} }} }}
            ''',
        }

        mutation_map = {
            'create': f'''
                mutation {{
                    create_board(
                        board_name: "{params.get('name', 'New Board')}",
                        board_kind: {params.get('board_kind', 'private').upper()},
                        description: "{params.get('description', '')}"
                    ) {{ id name state }}
                }}
            ''',
            'update': f'''
                mutation {{
                    change_column_value(
                        board_id: {params.get('board_id', 0)},
                        item_id: {params.get('item_id', 0)},
                        column_id: "{params.get('column_id', '')}",
                        value: "{{\\\"text\\\": \\\"{params.get('value', '')}\\\"}}"
                    ) {{ id }}
                }}
            ''',
        }

        query = query_map.get(operation, query_map['list']) if operation in query_map else mutation_map.get(operation, query_map['list'])

        try:
            req = urllib.request.Request(
                'https://api.monday.com/v2',
                data=json.dumps({'query': query}).encode('utf-8'),
                headers={
                    'Authorization': api_key,
                    'Content-Type': 'application/json'
                }
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            if 'errors' in data:
                return ActionResult(success=False, message=str(data['errors']))

            return ActionResult(
                success=True,
                message=f"Board operation '{operation}' completed",
                data=data.get('data', {})
            )
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Monday API error: {e.code}", data={'body': body})
        except Exception as e:
            return ActionResult(success=False, message=f"Monday error: {str(e)}")


class MondayItemAction(BaseAction):
    """Manage Monday.com items - create, update, move, archive.

    Handles item lifecycle operations.
    """
    action_type = "monday_item"
    display_name = "Monday项目操作"
    description = "创建、更新、移动、归档Monday.com项目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Monday.com items.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Monday.com API token
                - operation: create | update | move | archive | delete
                - board_id: Board ID
                - item_id: Item ID (for update/move/archive)
                - group_id: Group ID (for create)
                - item_name: Name for new item
                - column_values: Dict of column_id: value pairs
                - target_group_id: Target group for move

        Returns:
            ActionResult with item data.
        """
        api_key = params.get('api_key') or os.environ.get('MONDAY_API_KEY')
        operation = params.get('operation', 'create')

        if not api_key:
            return ActionResult(success=False, message="MONDAY_API_KEY is required")

        import urllib.request
        import urllib.error

        mutations = {
            'create': f'''
                mutation {{
                    create_item(
                        board_id: {params.get('board_id', 0)},
                        group_id: "{params.get('group_id', '')}",
                        item_name: "{params.get('item_name', 'New Item')}",
                        column_values: "{json.dumps(params.get('column_values', {}))}"
                    ) {{ id name column_values {{ id text }} }}
                }}
            ''',
            'update': f'''
                mutation {{
                    change_column_value(
                        board_id: {params.get('board_id', 0)},
                        item_id: {params.get('item_id', 0)},
                        column_id: "{params.get('column_id', '')}",
                        value: "{{\\\"text\\\": \\\"{params.get('value', '')}\\\"}}"
                    ) {{ id }}
                }}
            ''',
            'move': f'''
                mutation {{
                    move_item_to_group(
                        board_id: {params.get('board_id', 0)},
                        item_id: {params.get('item_id', 0)},
                        group_id: "{params.get('target_group_id', '')}"
                    ) {{ id }}
                }}
            ''',
            'archive': f'''
                mutation {{
                    archive_item(board_id: {params.get('board_id', 0)}, item_id: {params.get('item_id', 0)}) {{ id archived }}
                }}
            ''',
        }

        mutation = mutations.get(operation)
        if not mutation:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        try:
            req = urllib.request.Request(
                'https://api.monday.com/v2',
                data=json.dumps({'query': mutation}).encode('utf-8'),
                headers={
                    'Authorization': api_key,
                    'Content-Type': 'application/json'
                }
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            if 'errors' in data:
                return ActionResult(success=False, message=str(data['errors']))

            return ActionResult(
                success=True,
                message=f"Item operation '{operation}' completed",
                data=data.get('data', {})
            )
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"Monday API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"Monday error: {str(e)}")


class MondayUpdatesAction(BaseAction):
    """Manage Monday.com updates and comments on items.

    Allows posting, listing, and managing updates.
    """
    action_type = "monday_updates"
    display_name = "Monday更新评论"
    description = "管理Monday.com项目的更新和评论"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Monday.com updates.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Monday.com API token
                - operation: create | list
                - item_id: Item ID
                - body: Update body text
                - board_id: Board ID for listing

        Returns:
            ActionResult with update data.
        """
        api_key = params.get('api_key') or os.environ.get('MONDAY_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="MONDAY_API_KEY is required")

        import urllib.request
        import urllib.error

        if operation == 'create':
            query = f'''
                mutation {{
                    create_update(
                        item_id: {params.get('item_id', 0)},
                        body: "{params.get('body', '')}"
                    ) {{ id created_at body }}
                }}
            '''
        else:
            query = f'''
                query {{
                    items(ids: [{params.get('item_id', 0)}]) {{
                        updates {{ id body created_at author {{ id name }} }}
                    }}
                }}
            '''

        try:
            req = urllib.request.Request(
                'https://api.monday.com/v2',
                data=json.dumps({'query': query}).encode('utf-8'),
                headers={
                    'Authorization': api_key,
                    'Content-Type': 'application/json'
                }
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            if 'errors' in data:
                return ActionResult(success=False, message=str(data['errors']))

            return ActionResult(
                success=True,
                message=f"Update operation '{operation}' completed",
                data=data.get('data', {})
            )
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"Monday API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"Monday error: {str(e)}")


class MondayWebhookAction(BaseAction):
    """Create and manage Monday.com webhooks.

    Enables event-driven automation when board data changes.
    """
    action_type = "monday_webhook"
    display_name = "Monday Webhook"
    description = "创建和管理Monday.com Webhook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Monday.com webhooks.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Monday.com API token
                - operation: create | list | delete
                - board_id: Board ID
                - webhook_url: URL to receive events
                - event: Event type (create_item, update_item, etc.)

        Returns:
            ActionResult with webhook data.
        """
        api_key = params.get('api_key') or os.environ.get('MONDAY_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="MONDAY_API_KEY is required")

        import urllib.request
        import urllib.error

        if operation == 'create':
            query = f'''
                mutation {{
                    create_webhook(
                        board_id: {params.get('board_id', 0)},
                        url: "{params.get('webhook_url', '')}",
                        event: {params.get('event', 'create_item')}
                    ) {{ id webhook {{ id }} }}
                }}
            '''
        elif operation == 'delete':
            query = f'''
                mutation {{
                    delete_webhook(webhook_id: {params.get('webhook_id', 0)}) {{ id }}
                }}
            '''
        else:
            query = f'''
                query {{
                    boards(ids: [{params.get('board_id', 0)}]) {{
                        webhooks {{ id target_url event }}
                    }}
                }}
            '''

        try:
            req = urllib.request.Request(
                'https://api.monday.com/v2',
                data=json.dumps({'query': query}).encode('utf-8'),
                headers={
                    'Authorization': api_key,
                    'Content-Type': 'application/json'
                }
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            if 'errors' in data:
                return ActionResult(success=False, message=str(data['errors']))

            return ActionResult(
                success=True,
                message=f"Webhook operation '{operation}' completed",
                data=data.get('data', {})
            )
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"Monday API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"Monday error: {str(e)}")
