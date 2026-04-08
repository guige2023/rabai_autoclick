"""Intercom API integration for customer messaging and support.

Handles Intercom operations including conversations, contacts,
messages, tags, and team management.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


@dataclass
class IntercomConfig:
    """Configuration for Intercom API client."""
    access_token: str
    app_id: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3


@dataclass
class IntercomContact:
    """Represents an Intercom contact."""
    id: str
    type: str
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    external_id: Optional[str] = None
    custom_attributes: dict = field(default_factory=dict)
    created_at: Optional[int] = None
    last_seen_at: Optional[int] = None


@dataclass
class IntercomConversation:
    """Represents an Intercom conversation."""
    id: str
    state: str
    title: Optional[str] = None
    contact_ids: list[str] = field(default_factory=list)
    admin_assignee_id: Optional[str] = None
    team_assignee_id: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    source_type: Optional[str] = None


@dataclass
class IntercomMessage:
    """Represents a message in a conversation."""
    id: str
    conversation_id: str
    author_type: str
    body: str
    created_at: Optional[int] = None
    attachments: list[str] = field(default_factory=list)


class IntercomAPIError(Exception):
    """Raised when Intercom API returns an error."""
    def __init__(self, message: str, code: Optional[str] = None, response: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.response = response or {}


class IntercomAction:
    """Intercom API client for messaging and support operations."""

    BASE_URL = "https://api.intercom.io"

    def __init__(self, config: IntercomConfig):
        """Initialize Intercom client with access token.

        Args:
            config: IntercomConfig with access token and optional app ID
        """
        if requests is None:
            raise ImportError("requests library required: pip install requests")

        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {config.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make authenticated request to Intercom API.

        Args:
            method: HTTP method
            endpoint: API endpoint path
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON response

        Raises:
            IntercomAPIError: On API error responses
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        retries = self.config.max_retries

        while retries > 0:
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=self.config.timeout,
                    **kwargs
                )

                if response.status_code == 429:
                    retries -= 1
                    import time
                    time.sleep(2 ** (self.config.max_retries - retries))
                    continue

                if response.status_code >= 400:
                    error_data = response.json() if response.content else {}
                    raise IntercomAPIError(
                        message=error_data.get("message", "Request failed"),
                        code=error_data.get("type"),
                        response=error_data
                    )

                return response.json() if response.content else {}

            except requests.RequestException as e:
                retries -= 1
                if retries == 0:
                    raise IntercomAPIError(f"Request failed: {e}")

    def create_contact(self, email: Optional[str] = None,
                       name: Optional[str] = None,
                       phone: Optional[str] = None,
                       external_id: Optional[str] = None,
                       custom_attributes: Optional[dict] = None) -> IntercomContact:
        """Create a new contact.

        Args:
            email: Contact email address
            name: Contact full name
            phone: Contact phone number
            external_id: External ID for sync
            custom_attributes: Custom field values

        Returns:
            Created IntercomContact object
        """
        payload: dict[str, Any] = {"role": "user"}

        if email:
            payload["email"] = email

        if name:
            payload["name"] = name

        if phone:
            payload["phone"] = phone

        if external_id:
            payload["external_id"] = external_id

        if custom_attributes:
            payload.update(custom_attributes)

        data = self._request("POST", "contacts", json=payload)

        return IntercomContact(
            id=data.get("id", ""),
            type=data.get("type", ""),
            name=data.get("name"),
            email=data.get("email"),
            phone=data.get("phone"),
            external_id=data.get("external_id"),
            custom_attributes=data.get("custom_attributes", {}),
            created_at=data.get("created_at"),
            last_seen_at=data.get("last_seen_at")
        )

    def get_contact(self, contact_id: str) -> IntercomContact:
        """Get contact by ID.

        Args:
            contact_id: Contact ID

        Returns:
            IntercomContact object
        """
        data = self._request("GET", f"contacts/{contact_id}")

        return IntercomContact(
            id=data.get("id", ""),
            type=data.get("type", ""),
            name=data.get("name"),
            email=data.get("email"),
            phone=data.get("phone"),
            external_id=data.get("external_id"),
            custom_attributes=data.get("custom_attributes", {}),
            created_at=data.get("created_at"),
            last_seen_at=data.get("last_seen_at")
        )

    def update_contact(self, contact_id: str, **kwargs) -> IntercomContact:
        """Update an existing contact.

        Args:
            contact_id: Contact ID
            **kwargs: Fields to update (email, name, phone, custom_attributes, etc.)

        Returns:
            Updated IntercomContact object
        """
        payload = {k: v for k, v in kwargs.items() if v is not None}
        data = self._request("PUT", f"contacts/{contact_id}", json=payload)

        return IntercomContact(
            id=data.get("id", ""),
            type=data.get("type", ""),
            name=data.get("name"),
            email=data.get("email"),
            phone=data.get("phone"),
            external_id=data.get("external_id"),
            custom_attributes=data.get("custom_attributes", {}),
            created_at=data.get("created_at"),
            last_seen_at=data.get("last_seen_at")
        )

    def search_contacts(self, query: str, limit: int = 50) -> list[IntercomContact]:
        """Search contacts by email or name.

        Args:
            query: Search query string
            limit: Maximum results

        Returns:
            List of matching IntercomContact objects
        """
        payload = {
            "query": {
                "field": "email",
                "operator": "~",
                "value": query
            },
            "pagination": {"per_page": min(limit, 100)}
        }

        data = self._request("POST", "contacts/search", json=payload)

        contacts = []
        for item in data.get("data", []):
            contacts.append(IntercomContact(
                id=item.get("id", ""),
                type=item.get("type", ""),
                name=item.get("name"),
                email=item.get("email"),
                phone=item.get("phone"),
                external_id=item.get("external_id"),
                custom_attributes=item.get("custom_attributes", {}),
                created_at=item.get("created_at"),
                last_seen_at=item.get("last_seen_at")
            ))

        return contacts

    def create_conversation(self, from_contact_id: str,
                            admin_id: Optional[str] = None,
                            subject: Optional[str] = None,
                            body: Optional[str] = None) -> IntercomConversation:
        """Create a new conversation.

        Args:
            from_contact_id: Contact initiating the conversation
            admin_id: Admin ID to assign
            subject: Conversation subject
            body: Initial message body

        Returns:
            Created IntercomConversation object
        """
        payload: dict[str, Any] = {
            "from": {"type": "contact", "id": from_contact_id}
        }

        if admin_id:
            payload["to"] = {"type": "admin", "id": admin_id}

        if subject:
            payload["subject"] = subject

        if body:
            payload["body"] = body

        data = self._request("POST", "conversations", json=payload)

        return IntercomConversation(
            id=data.get("id", ""),
            state=data.get("state", ""),
            title=data.get("title"),
            contact_ids=[c.get("id") for c in data.get("contacts", {}).get("contacts", [])],
            admin_assignee_id=data.get("admin_assignee_id"),
            team_assignee_id=data.get("team_assignee_id"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            source_type=data.get("source", {}).get("type") if data.get("source") else None
        )

    def reply_to_conversation(self, conversation_id: str,
                               message_type: str,
                               body: str,
                               admin_id: Optional[str] = None,
                               attachment_urls: Optional[list[str]] = None) -> IntercomMessage:
        """Reply to a conversation.

        Args:
            conversation_id: Conversation ID
            message_type: Type of reply ('comment', 'note', 'assignment')
            body: Reply body
            admin_id: Admin sending the reply
            attachment_urls: Optional file attachment URLs

        Returns:
            Created IntercomMessage object
        """
        payload: dict[str, Any] = {
            "message_type": message_type,
            "body": body,
            "type": "admin"
        }

        if admin_id:
            payload["admin_id"] = admin_id

        if attachment_urls:
            payload["attachment_urls"] = attachment_urls

        data = self._request("POST", f"conversations/{conversation_id}/reply", json=payload)

        return IntercomMessage(
            id=data.get("id", ""),
            conversation_id=conversation_id,
            author_type=data.get("author", {}).get("type", "") if data.get("author") else "",
            body=data.get("body", ""),
            created_at=data.get("created_at"),
            attachments=data.get("attachments", [])
        )

    def get_conversation(self, conversation_id: str) -> IntercomConversation:
        """Get conversation by ID with messages.

        Args:
            conversation_id: Conversation ID

        Returns:
            IntercomConversation object
        """
        data = self._request("GET", f"conversations/{conversation_id}")

        return IntercomConversation(
            id=data.get("id", ""),
            state=data.get("state", ""),
            title=data.get("title"),
            contact_ids=[c.get("id") for c in data.get("contacts", {}).get("contacts", [])],
            admin_assignee_id=data.get("admin_assignee_id"),
            team_assignee_id=data.get("team_assignee_id"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            source_type=data.get("source", {}).get("type") if data.get("source") else None
        )

    def list_conversations(self, state: Optional[str] = None,
                            admin_id: Optional[str] = None,
                            limit: int = 50) -> list[IntercomConversation]:
        """List conversations with filters.

        Args:
            state: Filter by state ('open', 'closed', 'snoozed')
            admin_id: Filter by assigned admin
            limit: Maximum results

        Returns:
            List of IntercomConversation objects
        """
        params: dict[str, Any] = {"per_page": min(limit, 150)}

        if state:
            params["state"] = state

        if admin_id:
            params["admin_id"] = admin_id

        data = self._request("GET", "conversations", params=params)

        conversations = []
        for item in data.get("conversations", []):
            conversations.append(IntercomConversation(
                id=item.get("id", ""),
                state=item.get("state", ""),
                title=item.get("title"),
                contact_ids=[c.get("id") for c in item.get("contacts", {}).get("contacts", [])],
                admin_assignee_id=item.get("admin_assignee_id"),
                team_assignee_id=item.get("team_assignee_id"),
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at"),
                source_type=item.get("source", {}).get("type") if item.get("source") else None
            ))

        return conversations

    def add_tag(self, tag_id: str, contact_ids: list[str]) -> bool:
        """Add a tag to contacts.

        Args:
            tag_id: Tag ID to apply
            contact_ids: List of contact IDs

        Returns:
            True if successful
        """
        payload = {
            "id": tag_id,
            "contacts": [{"id": cid} for cid in contact_ids]
        }

        self._request("POST", "contacts/tags", json=payload)
        return True

    def create_tag(self, name: str) -> dict:
        """Create a new tag.

        Args:
            name: Tag name

        Returns:
            Created tag object with id and name
        """
        payload = {"name": name}
        return self._request("POST", "tags", json=payload)
