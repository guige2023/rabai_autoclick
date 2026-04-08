"""Resend email action module for RabAI AutoClick.

Provides Resend API operations for transactional email sending.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ResendEmailAction(BaseAction):
    """Send emails via Resend API."""
    action_type = "resend_email"
    display_name = "Resend邮件"
    description = "Resend邮件发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Resend email.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Resend API key
                - to: Recipient email (string or list)
                - from_email: Sender email
                - subject: Email subject
                - html: HTML body
                - text: Plain text body
                - reply_to: Optional reply-to address

        Returns:
            ActionResult with send result.
        """
        api_key = params.get('api_key', '') or os.environ.get('RESEND_API_KEY')
        to = params.get('to', '')
        from_email = params.get('from_email', '')
        subject = params.get('subject', '')
        html = params.get('html', '')
        text = params.get('text', '')
        reply_to = params.get('reply_to', '')

        if not api_key:
            return ActionResult(success=False, message="RESEND_API_KEY is required")
        if not to or not from_email or not subject:
            return ActionResult(success=False, message="to, from_email, and subject are required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            to_list = [to] if isinstance(to, str) else to
            payload: Dict[str, Any] = {
                'from': from_email,
                'to': to_list,
                'subject': subject,
            }
            if html:
                payload['html'] = html
            if text:
                payload['text'] = text
            if reply_to:
                payload['reply_to'] = reply_to

            response = requests.post(
                'https://api.resend.com/emails',
                json=payload,
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Resend email sent",
                data={'id': data.get('id'), 'from': data.get('from')}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Resend error: {str(e)}")
