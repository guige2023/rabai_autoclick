"""SendGrid email action module for RabAI AutoClick.

Provides SendGrid API operations for transactional email sending.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SendGridEmailAction(BaseAction):
    """Send transactional emails via SendGrid API."""
    action_type = "sendgrid_email"
    display_name = "SendGrid邮件"
    description = "SendGrid事务邮件发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send SendGrid email.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: SendGrid API key
                - to: Recipient email (or list)
                - from_email: Sender email
                - from_name: Sender name
                - subject: Email subject
                - html_content: HTML body
                - text_content: Plain text body
                - template_id: Optional SendGrid template ID

        Returns:
            ActionResult with send result.
        """
        api_key = params.get('api_key', '') or os.environ.get('SENDGRID_API_KEY')
        to = params.get('to', '')
        from_email = params.get('from_email', '')
        from_name = params.get('from_name', '')
        subject = params.get('subject', '')
        html_content = params.get('html_content', '')
        text_content = params.get('text_content', '')
        template_id = params.get('template_id', '')
        dynamic_data = params.get('dynamic_data', {})

        if not api_key:
            return ActionResult(success=False, message="SENDGRID_API_KEY is required")
        if not to or not from_email or not subject:
            return ActionResult(success=False, message="to, from_email, and subject are required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
            }
            to_list = [to] if isinstance(to, str) else to
            payload: Dict[str, Any] = {
                'personalizations': [{'to': [{'email': e} for e in to_list]}],
                'from': {'email': from_email, 'name': from_name} if from_name else {'email': from_email},
                'subject': subject,
            }
            if template_id:
                payload['template_id'] = template_id
                if dynamic_data:
                    payload['personalizations'][0]['dynamic_template_data'] = dynamic_data
            else:
                if html_content:
                    payload['content'] = [{'type': 'text/html', 'value': html_content}]
                if text_content:
                    payload.setdefault('content', []).append({'type': 'text/plain', 'value': text_content})

            response = requests.post(
                'https://api.sendgrid.com/v3/mail/send',
                json=payload,
                headers=headers,
                timeout=30
            )
            duration = time.time() - start
            if response.status_code in (200, 201, 202):
                return ActionResult(
                    success=True, message=f"Email sent to {len(to_list)} recipient(s)",
                    data={'status_code': response.status_code}, duration=duration
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"SendGrid error {response.status_code}: {response.text[:200]}",
                    data={'status_code': response.status_code}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"SendGrid error: {str(e)}")
