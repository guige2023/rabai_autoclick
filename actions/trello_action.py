"""Trello action module for RabAI AutoClick.

Provides Trello API operations for board, list, and card management.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TrelloCardAction(BaseAction):
    """Create and manage Trello cards."""
    action_type = "trello_card"
    display_name = "Trello卡片"
    description = "Trello卡片管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create or update Trello card.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Trello API key
                - token: Trello token
                - action: 'create', 'update', 'move', 'archive'
                - list_id: List ID
                - name: Card name
                - desc: Card description
                - card_id: Card ID (for update/move/archive)
                - dest_list_id: Destination list ID (for move action)

        Returns:
            ActionResult with card data.
        """
        api_key = params.get('api_key', '') or os.environ.get('TRELLO_API_KEY')
        token = params.get('token', '') or os.environ.get('TRELLO_TOKEN')
        action = params.get('action', 'create')
        list_id = params.get('list_id', '')
        name = params.get('name', '')
        desc = params.get('desc', '')
        card_id = params.get('card_id', '')
        dest_list_id = params.get('dest_list_id', '')

        if not api_key or not token:
            return ActionResult(success=False, message="api_key and token are required")

        if action == 'create' and not list_id:
            return ActionResult(success=False, message="list_id is required for create")

        if action in ('update', 'move', 'archive') and not card_id:
            return ActionResult(success=False, message="card_id is required for this action")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        base = 'https://api.trello.com/1'
        auth = {'key': api_key, 'token': token}
        start = time.time()
        try:
            if action == 'create':
                resp = requests.post(
                    f"{base}/cards",
                    params={**auth, 'idList': list_id, 'name': name, 'desc': desc},
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                duration = time.time() - start
                return ActionResult(
                    success=True, message=f"Created card {data['id']}",
                    data={'card_id': data['id'], 'short_url': data['shortUrl']}, duration=duration
                )
            elif action == 'update':
                resp = requests.put(
                    f"{base}/cards/{card_id}",
                    params={**auth, 'name': name, 'desc': desc},
                    timeout=30
                )
                resp.raise_for_status()
                duration = time.time() - start
                return ActionResult(success=True, message=f"Updated card {card_id}", data={'card_id': card_id}, duration=duration)
            elif action == 'move':
                if not dest_list_id:
                    return ActionResult(success=False, message="dest_list_id required for move")
                resp = requests.put(
                    f"{base}/cards/{card_id}",
                    params={**auth, 'idList': dest_list_id},
                    timeout=30
                )
                resp.raise_for_status()
                duration = time.time() - start
                return ActionResult(success=True, message=f"Moved card {card_id}", data={'card_id': card_id}, duration=duration)
            elif action == 'archive':
                resp = requests.put(
                    f"{base}/cards/{card_id}/closed",
                    params={**auth, 'value': 'true'},
                    timeout=30
                )
                resp.raise_for_status()
                duration = time.time() - start
                return ActionResult(success=True, message=f"Archived card {card_id}", data={'card_id': card_id}, duration=duration)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Trello error: {str(e)}")
