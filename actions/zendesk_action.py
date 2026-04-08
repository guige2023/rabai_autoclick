"""Zendesk action module for RabAI AutoClick.

Provides customer service and support operations via Zendesk API
for ticket management and customer engagement.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import base64

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ZendeskAction(BaseAction):
    """Zendesk API integration for support and customer service.

    Supports ticket CRUD, user management, macros, and views.

    Args:
        config: Zendesk configuration containing subdomain, email, and api_token
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.subdomain = self.config.get("subdomain", "")
        self.email = self.config.get("email", "")
        self.api_token = self.config.get("api_token", "")
        self.api_base = f"https://{self.subdomain}.zendesk.com/api/v2"
        credentials = f"{self.email}/token:{self.api_token}"
        token = base64.b64encode(credentials.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Zendesk."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def create_ticket(
        self,
        subject: str,
        body: str,
        requester_id: Optional[int] = None,
        assignee_id: Optional[int] = None,
        priority: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> ActionResult:
        """Create a support ticket.

        Args:
            subject: Ticket subject
            body: Ticket description
            requester_id: Requester user ID
            assignee_id: Assignee user ID
            priority: Priority (low, normal, high, urgent)
            tags: List of tags

        Returns:
            ActionResult with created ticket
        """
        if not self.api_token:
            return ActionResult(success=False, error="Missing api_token")

        ticket = {"subject": subject, "comment": {"body": body}}
        if requester_id:
            ticket["requester_id"] = requester_id
        if assignee_id:
            ticket["assignee_id"] = assignee_id
        if priority:
            ticket["priority"] = priority
        if tags:
            ticket["tags"] = tags

        result = self._make_request("POST", "tickets.json", data={"ticket": ticket})
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        ticket_data = result.get("ticket", {})
        return ActionResult(success=True, data={"ticket_id": ticket_data.get("id")})

    def get_ticket(self, ticket_id: int) -> ActionResult:
        """Get a ticket by ID."""
        if not self.api_token:
            return ActionResult(success=False, error="Missing api_token")

        result = self._make_request("GET", f"tickets/{ticket_id}.json")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("ticket", {}))

    def update_ticket(
        self,
        ticket_id: int,
        **kwargs,
    ) -> ActionResult:
        """Update a ticket.

        Args:
            ticket_id: Ticket ID
            **kwargs: Fields to update (subject, body, status, priority, etc.)

        Returns:
            ActionResult with updated ticket
        """
        if not self.api_token:
            return ActionResult(success=False, error="Missing api_token")

        ticket = {k: v for k, v in kwargs.items() if v is not None}
        result = self._make_request(
            "PUT", f"tickets/{ticket_id}.json", data={"ticket": ticket}
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("ticket", {}))

    def list_tickets(
        self,
        status: Optional[str] = None,
        assignee_id: Optional[int] = None,
        limit: int = 100,
    ) -> ActionResult:
        """List tickets with optional filters.

        Args:
            status: Filter by status (open, pending, solved, etc.)
            assignee_id: Filter by assignee
            limit: Maximum tickets to return

        Returns:
            ActionResult with tickets list
        """
        if not self.api_token:
            return ActionResult(success=False, error="Missing api_token")

        endpoint = "tickets.json"
        params = f"?sort_by=created_at&sort_order=desc&per_page={limit}"
        if status:
            params += f"&status={status}"
        if assignee_id:
            params += f"&assignee_id={assignee_id}"

        result = self._make_request("GET", endpoint + params)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        tickets = result.get("tickets", [])
        return ActionResult(success=True, data={"tickets": tickets})

    def add_comment(
        self,
        ticket_id: int,
        body: str,
        public: bool = True,
    ) -> ActionResult:
        """Add a comment to a ticket.

        Args:
            ticket_id: Ticket ID
            body: Comment body
            public: Whether comment is public

        Returns:
            ActionResult with update status
        """
        if not self.api_token:
            return ActionResult(success=False, error="Missing api_token")

        data = {
            "ticket": {
                "comment": {"body": body, "public": public}
            }
        }
        result = self._make_request(
            "PUT", f"tickets/{ticket_id}.json", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"comment_added": True})

    def search_tickets(self, query: str) -> ActionResult:
        """Search tickets.

        Args:
            query: Search query

        Returns:
            ActionResult with matching tickets
        """
        if not self.api_token:
            return ActionResult(success=False, error="Missing api_token")

        encoded = query.replace(" ", "+")
        result = self._make_request(
            "GET", f"search.json?query={encoded}&sort_by=created_at"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"results": result.get("results", [])})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Zendesk operation."""
        operations = {
            "create_ticket": self.create_ticket,
            "get_ticket": self.get_ticket,
            "update_ticket": self.update_ticket,
            "list_tickets": self.list_tickets,
            "add_comment": self.add_comment,
            "search_tickets": self.search_tickets,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
