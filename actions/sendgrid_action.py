"""SendGrid action module for RabAI AutoClick.

Provides email automation via SendGrid API including
transactional emails, templates, and contact management.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SendGridAction(BaseAction):
    """SendGrid API integration for email automation.

    Supports sending transactional emails, using dynamic templates,
    managing contacts, and handling email events.

    Args:
        config: SendGrid configuration containing api_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.from_email = self.config.get("from_email", "")
        self.from_name = self.config.get("from_name", "")
        self.api_base = "https://api.sendgrid.com/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to SendGrid API."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result if isinstance(result, list) else result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def send_email(
        self,
        to: Union[str, List[str]],
        subject: str,
        content: str,
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        reply_to: Optional[str] = None,
        attachments: Optional[List[Dict]] = None,
    ) -> ActionResult:
        """Send a plain text or HTML email.

        Args:
            to: Recipient email or list of recipients
            subject: Email subject
            content: Email body content
            cc: Optional CC recipients
            bcc: Optional BCC recipients
            reply_to: Optional reply-to address
            attachments: Optional list of attachment dicts

        Returns:
            ActionResult with send status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        if isinstance(to, str):
            to = [{"email": to}]
        else:
            to = [{"email": addr} for addr in to]

        msg = {
            "personalizations": [{"to": to, "subject": subject}],
            "from": {"email": self.from_email},
            "content": [{"type": "text/plain", "value": content}],
        }

        if self.from_name:
            msg["from"]["name"] = self.from_name
        if cc:
            if isinstance(cc, str):
                cc = [cc]
            msg["personalizations"][0]["cc"] = [{"email": addr} for addr in cc]
        if bcc:
            if isinstance(bcc, str):
                bcc = [bcc]
            msg["personalizations"][0]["bcc"] = [{"email": addr} for addr in bcc]
        if reply_to:
            msg["reply_to"] = {"email": reply_to}
        if attachments:
            msg["attachments"] = attachments

        result = self._make_request("POST", "mail/send", data=msg)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"message": "Email sent successfully"})

    def send_template(
        self,
        to: Union[str, List[str]],
        template_id: str,
        dynamic_data: Dict[str, Any],
        cc: Optional[Union[str, List[str]]] = None,
    ) -> ActionResult:
        """Send an email using a dynamic template.

        Args:
            to: Recipient email or list of recipients
            template_id: SendGrid template ID
            dynamic_data: Data for template placeholders
            cc: Optional CC recipients

        Returns:
            ActionResult with send status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        if isinstance(to, str):
            to = [{"email": to}]
        else:
            to = [{"email": addr} for addr in to]

        msg = {
            "personalizations": [{"to": to, "dynamic_template_data": dynamic_data}],
            "from": {"email": self.from_email},
            "template_id": template_id,
        }

        if self.from_name:
            msg["from"]["name"] = self.from_name
        if cc:
            if isinstance(cc, str):
                cc = [cc]
            msg["personalizations"][0]["cc"] = [{"email": addr} for addr in cc]

        result = self._make_request("POST", "mail/send", data=msg)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"message": "Template email sent"})

    def add_contact(
        self,
        email: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        custom_fields: Optional[Dict] = None,
    ) -> ActionResult:
        """Add or update a contact.

        Args:
            email: Contact email
            first_name: First name
            last_name: Last name
            custom_fields: Custom field values

        Returns:
            ActionResult with contact operation status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        contacts = [{"email": email}]
        if first_name:
            contacts[0]["first_name"] = first_name
        if last_name:
            contacts[0]["last_name"] = last_name
        if custom_fields:
            contacts[0]["custom_fields"] = custom_fields

        result = self._make_request(
            "PUT", "marketing/contacts", data={"contacts": contacts}
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"message": "Contact updated"})

    def list_contacts(
        self,
        page_size: int = 100,
    ) -> ActionResult:
        """List contacts from marketing campaigns.

        Args:
            page_size: Number of contacts per page

        Returns:
            ActionResult with contacts list
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "POST", "marketing/contacts/searches/primary/exports",
            data={"list_metadata": {"name": "temp_export"}, "contacts_metadata": []}
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def get_stats(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> ActionResult:
        """Get email statistics.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)

        Returns:
            ActionResult with stats data
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "GET", f"stats?start_date={start_date}"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"stats": result})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute SendGrid operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "send_email": self.send_email,
            "send_template": self.send_template,
            "add_contact": self.add_contact,
            "list_contacts": self.list_contacts,
            "get_stats": self.get_stats,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
