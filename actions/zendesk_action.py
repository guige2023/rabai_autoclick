"""Zendesk action module for RabAI AutoClick.

Provides Zendesk API operations for ticket management and user data.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ZendeskTicketAction(BaseAction):
    """Create and update Zendesk tickets."""
    action_type = "zendesk_ticket"
    display_name = "Zendesk工单"
    description = "Zendesk工单管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create or update Zendesk ticket.

        Args:
            context: Execution context.
            params: Dict with keys:
                - subdomain: Zendesk subdomain
                - email: Zendesk email
                - api_token: Zendesk API token
                - action: 'create' or 'update'
                - ticket_data: Ticket fields (subject, comment, requester_id, etc.)
                - ticket_id: Ticket ID (for update action)

        Returns:
            ActionResult with ticket data.
        """
        subdomain = params.get('subdomain', '')
        email = params.get('email', '')
        api_token = params.get('api_token', '')
        action = params.get('action', 'create')
        ticket_data = params.get('ticket_data', {})
        ticket_id = params.get('ticket_id', '')

        if not subdomain or not email or not api_token:
            return ActionResult(success=False, message="subdomain, email, and api_token are required")
        if action == 'create' and not ticket_data:
            return ActionResult(success=False, message="ticket_data is required for create")

        try:
            import requests
            from requests.auth import HTTPBasicAuth
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        auth = HTTPBasicAuth(f"{email}/token", api_token)
        base_url = f"https://{subdomain}.zendesk.com/api/v2"
        try:
            if action == 'create':
                response = requests.post(
                    f"{base_url}/tickets.json",
                    json={'ticket': ticket_data},
                    auth=auth, timeout=30
                )
                response.raise_for_status()
                data = response.json()
                duration = time.time() - start
                return ActionResult(
                    success=True, message=f"Created ticket {data['ticket']['id']}",
                    data={'ticket_id': data['ticket']['id'], 'url': data['ticket']['url']}, duration=duration
                )
            elif action == 'update':
                if not ticket_id:
                    return ActionResult(success=False, message="ticket_id required for update")
                response = requests.put(
                    f"{base_url}/tickets/{ticket_id}.json",
                    json={'ticket': ticket_data},
                    auth=auth, timeout=30
                )
                response.raise_for_status()
                data = response.json()
                duration = time.time() - start
                return ActionResult(
                    success=True, message=f"Updated ticket {ticket_id}",
                    data={'ticket_id': data['ticket']['id']}, duration=duration
                )
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Zendesk error: {str(e)}")
