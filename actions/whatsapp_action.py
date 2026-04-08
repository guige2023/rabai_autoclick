"""WhatsApp message action module for RabAI AutoClick.

Provides WhatsApp business API operations for sending messages.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WhatsAppSendAction(BaseAction):
    """Send WhatsApp messages via Twilio or WhatsApp Business API."""
    action_type = "whatsapp_send"
    display_name = "WhatsApp发送"
    description = "WhatsApp消息发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send WhatsApp message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - account_sid: Twilio Account SID
                - auth_token: Twilio Auth Token
                - from_: Sender WhatsApp number
                - to: Recipient WhatsApp number
                - body: Message text

        Returns:
            ActionResult with message result.
        """
        account_sid = params.get('account_sid') or os.environ.get('TWILIO_ACCOUNT_SID')
        auth_token = params.get('auth_token') or os.environ.get('TWILIO_AUTH_TOKEN')
        from_num = params.get('from_', '')
        to = params.get('to', '')
        body = params.get('body', '')

        if not account_sid or not auth_token:
            return ActionResult(success=False, message="TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN are required")
        if not from_num or not to:
            return ActionResult(success=False, message="from_ and to numbers are required")
        if not body:
            return ActionResult(success=False, message="body is required")

        try:
            from twilio.rest import Client as TwilioClient
        except ImportError:
            return ActionResult(success=False, message="twilio not installed. Run: pip install twilio")

        start = time.time()
        try:
            client = TwilioClient(account_sid, auth_token)
            message = client.messages.create(
                from_=f'whatsapp:{from_num}',
                body=body,
                to=f'whatsapp:{to}'
            )
            duration = time.time() - start
            return ActionResult(
                success=True, message="WhatsApp message sent",
                data={
                    'sid': message.sid,
                    'status': message.status,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"WhatsApp error: {str(e)}")
