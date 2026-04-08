"""Twilio action module for RabAI AutoClick.

Provides SMS, voice, and WhatsApp messaging via Twilio API.
"""

import json
import base64
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TwilioAction(BaseAction):
    """Twilio API integration for SMS, voice, and messaging.

    Supports sending SMS, making calls, sending WhatsApp messages,
    and managing call workflows.

    Args:
        config: Twilio configuration containing account_sid and auth_token
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.account_sid = self.config.get("account_sid", "")
        self.auth_token = self.config.get("auth_token", "")
        self.from_number = self.config.get("from_number", "")
        self.api_base = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Twilio API."""
        url = f"{self.api_base}/{endpoint}"
        credentials = f"{self.account_sid}:{self.auth_token}"
        credentials_b64 = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {credentials_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        from urllib.parse import urlencode
        body = urlencode(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = response.read().decode("utf-8")
                from urllib.parse import parse_qs
                return dict(parse_qs(result))
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def send_sms(
        self,
        to: str,
        body: str,
        from_number: Optional[str] = None,
        status_callback: Optional[str] = None,
    ) -> ActionResult:
        """Send an SMS message.

        Args:
            to: Recipient phone number
            body: Message body
            from_number: Optional sender number (uses config default)
            status_callback: Optional webhook for status updates

        Returns:
            ActionResult with message SID
        """
        if not self.account_sid or not self.auth_token:
            return ActionResult(success=False, error="Missing account_sid or auth_token")

        from_ = from_number or self.from_number
        if not from_:
            return ActionResult(success=False, error="Missing from_number")

        data = {
            "To": to,
            "From": from_,
            "Body": body,
        }
        if status_callback:
            data["StatusCallback"] = status_callback

        result = self._make_request("POST", "Messages.json", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"message_sid": result.get("sid", "")})

    def send_whatsapp(
        self,
        to: str,
        body: str,
        from_number: Optional[str] = None,
    ) -> ActionResult:
        """Send a WhatsApp message.

        Args:
            to: Recipient WhatsApp number
            body: Message body
            from_number: Optional sender WhatsApp number

        Returns:
            ActionResult with message SID
        """
        def format_whatsapp_number(number: str) -> str:
            if number.startswith("whatsapp:"):
                return number
            return f"whatsapp:{number}"

        from_ = format_whatsapp_number(from_number or self.from_number)
        to = format_whatsapp_number(to)

        return self.send_sms(to, body, from_)

    def make_call(
        self,
        to: str,
        url: str,
        from_number: Optional[str] = None,
        status_callback: Optional[str] = None,
        method: str = "GET",
    ) -> ActionResult:
        """Make a phone call.

        Args:
            to: Recipient phone number
            url: TwiML URL for call handling
            from_number: Optional caller ID
            status_callback: Webhook for call status
            method: HTTP method for TwiML URL (GET or POST)

        Returns:
            ActionResult with call SID
        """
        if not self.account_sid or not self.auth_token:
            return ActionResult(success=False, error="Missing account_sid or auth_token")

        from_ = from_number or self.from_number
        if not from_:
            return ActionResult(success=False, error="Missing from_number")

        data = {
            "To": to,
            "From": from_,
            "Url": url,
            "Method": method,
        }
        if status_callback:
            data["StatusCallback"] = status_callback

        result = self._make_request("POST", "Calls.json", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"call_sid": result.get("sid", "")})

    def get_call(self, call_sid: str) -> ActionResult:
        """Get call details.

        Args:
            call_sid: Call SID

        Returns:
            ActionResult with call data
        """
        if not self.account_sid:
            return ActionResult(success=False, error="Missing account_sid")

        result = self._make_request("GET", f"Calls/{call_sid}.json")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def get_message(self, message_sid: str) -> ActionResult:
        """Get message details.

        Args:
            message_sid: Message SID

        Returns:
            ActionResult with message data
        """
        if not self.account_sid:
            return ActionResult(success=False, error="Missing account_sid")

        result = self._make_request("GET", f"Messages/{message_sid}.json")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def list_messages(
        self,
        to: Optional[str] = None,
        from_number: Optional[str] = None,
        limit: int = 20,
    ) -> ActionResult:
        """List messages.

        Args:
            to: Optional filter by recipient
            from_number: Optional filter by sender
            limit: Maximum number of messages

        Returns:
            ActionResult with messages list
        """
        if not self.account_sid:
            return ActionResult(success=False, error="Missing account_sid")

        data = {"PageSize": limit}
        if to:
            data["To"] = to
        if from_number:
            data["From"] = from_number

        result = self._make_request("GET", "Messages.json", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"messages": result.get("messages", [])}) 

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Twilio operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "send_sms": self.send_sms,
            "send_whatsapp": self.send_whatsapp,
            "make_call": self.make_call,
            "get_call": self.get_call,
            "get_message": self.get_message,
            "list_messages": self.list_messages,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
