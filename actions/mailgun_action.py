"""Mailgun email action module for RabAI AutoClick.

Provides Mailgun API operations for transactional email sending.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MailgunEmailAction(BaseAction):
    """Send emails via Mailgun API."""
    action_type = "mailgun_email"
    display_name = "Mailgun邮件"
    description = "Mailgun事务邮件发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Mailgun email.

        Args:
            context: Execution context.
            params: Dict with keys:
                - domain: Mailgun domain
                - api_key: Mailgun API key
                - to: Recipient email (or list)
                - from_email: Sender email
                - subject: Email subject
                - html: HTML body
                - text: Plain text body

        Returns:
            ActionResult with send result.
        """
        domain = params.get('domain', '')
        api_key = params.get('api_key', '') or os.environ.get('MAILGUN_API_KEY')
        to = params.get('to', '')
        from_email = params.get('from_email', '')
        subject = params.get('subject', '')
        html = params.get('html', '')
        text = params.get('text', '')

        if not domain or not api_key:
            return ActionResult(success=False, message="domain and api_key are required")
        if not to:
            return ActionResult(success=False, message="to is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            to_list = [to] if isinstance(to, str) else to
            data = {
                'from': from_email or f"mailgun@{domain}",
                'to': to_list,
                'subject': subject,
            }
            if html:
                data['html'] = html
            if text:
                data['text'] = text

            response = requests.post(
                f"https://api.mailgun.net/v3/{domain}/messages",
                auth=('api', api_key),
                data=data,
                timeout=30
            )
            response.raise_for_status()
            data_resp = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Mailgun email sent",
                data={'id': data_resp.get('id'), 'message': data_resp.get('message')}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Mailgun error: {str(e)}")
