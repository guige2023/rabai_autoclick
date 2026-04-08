"""Notion API integration for database/pages management.

Handles Notion workspace operations including database queries,
page creation/updates, block manipulation, and search functionality.
"""

from typing import Any, Optional
import json
import logging
from datetime import datetime
from dataclasses import dataclass, field

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


@dataclass
class NotionConfig:
    """Configuration for Notion API client."""
    api_key: str
    api_version: str = "2022-06-30"
    timeout: int = 30
    max_retries: int = 3


@dataclass
class NotionPage:
    """Represents a Notion page."""
    id: str
    title: str
    url: str
    created_time: Optional[str] = None
    last_edited_time: Optional[str] = None
    properties: dict = field(default_factory=dict)
    cover_url: Optional[str] = None
    icon: Optional[dict] = None


@dataclass
class NotionDatabase:
    """Represents a Notion database."""
    id: str
    title: str
    url: str
    properties: dict = field(default_factory=dict)
    schema: dict = field(default_factory=dict)


class NotionAPIError(Exception):
    """Raised when Notion API returns an error."""
    def __init__(self, message: str, status_code: Optional[int] = None, code: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class NotionAction:
    """Notion API client for workspace operations."""

    BASE_URL = "https://api.notion.com/v1"

    def __init__(self, config: NotionConfig):
        """Initialize Notion client with configuration.

        Args:
            config: NotionConfig with API key and settings
        """
        if requests is None:
            raise ImportError("requests library required: pip install requests")

        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {config.api_key}",
            "Notion-Version": config.api_version,
            "Content-Type": "application/json"
        })

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make authenticated request to Notion API.

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: API endpoint path
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON response

        Raises:
            NotionAPIError: On API error responses
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

                if not response.ok:
                    error_data = response.json() if response.content else {}
                    raise NotionAPIError(
                        message=error_data.get("message", "Unknown error"),
                        status_code=response.status_code,
                        code=error_data.get("code")
                    )

                return response.json() if response.content else {}

            except requests.RequestException as e:
                retries -= 1
                if retries == 0:
                    raise NotionAPIError(f"Request failed: {e}")

        raise NotionAPIError("Max retries exceeded")

    def search(self, query: Optional[str] = None, filter_type: Optional[str] = None,
               page_size: int = 100) -> list[dict]:
        """Search pages and databases in workspace.

        Args:
            query: Search query string
            filter_type: Filter by type ('page' or 'database')
            page_size: Number of results per page (max 100)

        Returns:
            List of search results with page/database info
        """
        payload: dict[str, Any] = {"page_size": min(page_size, 100)}

        if query:
            payload["query"] = query

        if filter_type:
            payload["filter"] = {"property": "object", "value": filter_type}

        results = []
        cursor = None

        while True:
            if cursor:
                payload["start_cursor"] = cursor

            data = self._request("POST", "search", json=payload)
            results.extend(data.get("results", []))

            cursor = data.get("next_cursor")
            if not cursor:
                break

        return results

    def get_page(self, page_id: str) -> NotionPage:
        """Retrieve a page by ID.

        Args:
            page_id: Notion page ID (32 characters without dashes)

        Returns:
            NotionPage object with page details
        """
        data = self._request("GET", f"pages/{page_id}")
        return self._parse_page(data)

    def get_page_property(self, page_id: str, property_id: str) -> dict:
        """Retrieve a specific page property.

        Args:
            page_id: Notion page ID
            property_id: Property ID or name

        Returns:
            Property data from page
        """
        return self._request("GET", f"pages/{page_id}/properties/{property_id}")

    def create_page(self, parent_id: str, parent_type: str = "database_id",
                    properties: Optional[dict] = None,
                    children: Optional[list[dict]] = None) -> NotionPage:
        """Create a new page in a database or as child of a page.

        Args:
            parent_id: Parent database or page ID
            parent_type: 'database_id' or 'page_id'
            properties: Page properties as dict
            children: Optional list of block objects

        Returns:
            Created NotionPage object
        """
        parent = {parent_type: parent_id}
        payload: dict[str, Any] = {"parent": parent}

        if properties:
            payload["properties"] = properties

        if children:
            payload["children"] = children

        data = self._request("POST", "pages", json=payload)
        return self._parse_page(data)

    def update_page(self, page_id: str, properties: Optional[dict] = None,
                    archived: Optional[bool] = None) -> NotionPage:
        """Update page properties or archive status.

        Args:
            page_id: Notion page ID
            properties: Properties to update
            archived: Set archive status

        Returns:
            Updated NotionPage object
        """
        payload: dict[str, Any] = {}

        if properties:
            payload["properties"] = properties

        if archived is not None:
            payload["archived"] = archived

        data = self._request("PATCH", f"pages/{page_id}", json=payload)
        return self._parse_page(data)

    def get_database(self, database_id: str) -> NotionDatabase:
        """Retrieve a database schema.

        Args:
            database_id: Notion database ID

        Returns:
            NotionDatabase object with schema info
        """
        data = self._request("GET", f"databases/{database_id}")
        return self._parse_database(data)

    def query_database(self, database_id: str,
                       filter_clause: Optional[dict] = None,
                       sorts: Optional[list[dict]] = None,
                       page_size: int = 100,
                       start_cursor: Optional[str] = None) -> tuple[list[dict], Optional[str]]:
        """Query a database with optional filter and sort.

        Args:
            database_id: Notion database ID
            filter_clause: Notion filter object
            sorts: List of sort objects
            page_size: Results per page (max 100)
            start_cursor: Pagination cursor

        Returns:
            Tuple of (results list, next_cursor or None)
        """
        payload: dict[str, Any] = {"page_size": min(page_size, 100)}

        if filter_clause:
            payload["filter"] = filter_clause

        if sorts:
            payload["sorts"] = sorts

        if start_cursor:
            payload["start_cursor"] = start_cursor

        data = self._request("POST", f"databases/{database_id}/query", json=payload)

        return data.get("results", []), data.get("next_cursor")

    def append_block(self, block_id: str, children: list[dict]) -> dict:
        """Append child blocks to a parent block.

        Args:
            block_id: Parent block ID
            children: List of block objects to append

        Returns:
            Append response with created blocks
        """
        payload = {"children": children}
        return self._request("PATCH", f"blocks/{block_id}/children", json=payload)

    def get_block_children(self, block_id: str) -> list[dict]:
        """Retrieve all child blocks of a block.

        Args:
            block_id: Parent block ID

        Returns:
            List of child block objects
        """
        results = []
        cursor = None

        while True:
            endpoint = f"blocks/{block_id}/children"
            if cursor:
                endpoint += f"?start_cursor={cursor}"

            data = self._request("GET", endpoint)
            results.extend(data.get("results", []))

            cursor = data.get("next_cursor")
            if not cursor:
                break

        return results

    def _parse_page(self, data: dict) -> NotionPage:
        """Parse API response into NotionPage object."""
        title_property = data.get("properties", {}).get("title", {}).get("title", [])
        title = "".join(t.get("plain_text", "") for t in title_property) if title_property else "Untitled"

        properties = {}
        for key, value in data.get("properties", {}).items():
            properties[key] = self._simplify_property(value)

        return NotionPage(
            id=data.get("id", ""),
            title=title,
            url=data.get("url", ""),
            created_time=data.get("created_time"),
            last_edited_time=data.get("last_edited_time"),
            properties=properties,
            cover_url=data.get("cover", {}).get("external", {}).get("url") if data.get("cover") else None,
            icon=data.get("icon")
        )

    def _parse_database(self, data: dict) -> NotionDatabase:
        """Parse API response into NotionDatabase object."""
        title_property = data.get("title", [])
        title = "".join(t.get("plain_text", "") for t in title_property) if title_property else "Untitled"

        return NotionDatabase(
            id=data.get("id", ""),
            title=title,
            url=data.get("url", ""),
            properties=data.get("properties", {}),
            schema=data.get("properties", {})
        )

    def _simplify_property(self, prop: dict) -> Any:
        """Simplify property value to plain Python type."""
        prop_type = prop.get("type", "")
        value = prop.get(prop_type, {})

        if prop_type == "title":
            return "".join(t.get("plain_text", "") for t in value) if value else ""
        elif prop_type == "rich_text":
            return "".join(t.get("plain_text", "") for t in value) if value else ""
        elif prop_type in ("number", "checkbox", "email", "url", "phone_number"):
            return value
        elif prop_type == "select":
            return value.get("name") if value else None
        elif prop_type == "multi_select":
            return [s.get("name") for s in value] if value else []
        elif prop_type == "date":
            return value.get("start") if value else None
        elif prop_type == "relation":
            return [r.get("id") for r in value] if value else []
        elif prop_type == "people":
            return [p.get("id") for p in value] if value else []
        elif prop_type == "files":
            return [f.get("name") for f in value] if value else []
        else:
            return str(value) if value else None
