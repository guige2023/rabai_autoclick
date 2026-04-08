"""Iterable email action module for RabAI AutoClick.

Provides Iterable API operations for email marketing and campaigns.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IterableTriggerAction(BaseAction):
    """Trigger email campaigns via Iterable API."""
    action_type = "iterable_trigger"
    display_name = "Iterable触发"
    description = "Iterable邮件营销触发"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Trigger Iterable email.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Iterable API key
                - campaign_id: Campaign ID to trigger
                - email: Recipient email
                - data_fields: Optional user data fields

        Returns:
            ActionResult with trigger result.
        """
        api_key = params.get('api_key', '') or os.environ.get('ITERABLE_API_KEY')
        campaign_id = params.get('campaign_id', '')
        email = params.get('email', '')
        data_fields = params.get('data_fields', {})

        if not api_key:
            return ActionResult(success=False, message="ITERABLE_API_KEY is required")
        if not campaign_id or not email:
            return ActionResult(success=False, message="campaign_id and email are required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.post(
                'https://api.iterable.com/api/campaigns/trigger/send',
                json={
                    'apiKey': api_key,
                    'campaignId': campaign_id,
                    'email': email,
                    'dataFields': data_fields,
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Iterable email triggered",
                data={'success': data.get('success', False)}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Iterable error: {str(e)}")
