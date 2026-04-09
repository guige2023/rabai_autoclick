"""
API Notion Action Module.

Provides Notion API integration for database management,
page operations, content blocks, and workspace automation.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class BlockType(Enum):
    """Notion block types."""
    PARAGRAPH = "paragraph"
    HEADING_1 = "heading_1"
    HEADING_2 = "heading_2"
    HEADING_3 = "heading_3"
    BULLETED_LIST_ITEM = "bulleted_list_item"
    NUMBERED_LIST_ITEM = "numbered_list_item"
    TO_DO = "to_do"
    TOGGLE = "toggle"
    CODE = "code"
    QUOTE = "quote"
    CALLOUT = "callout"
    DIVIDER = "divider"
    IMAGE = "image"
    FILE = "file"
    BOOKMARK = "bookmark"


@dataclass
class NotionConfig:
    """Notion client configuration."""
    token: str = ""
    version: str = "2022-06-28"
    timeout: float = 30.0
    retry_attempts: int = 3


@dataclass
class RichText:
    """Rich text content in Notion."""
    text: str
    link: Optional[str] = None
    annotations: dict[str, Any] = field(default_factory=lambda: {
        "bold": False,
        "italic": False,
        "strikethrough": False,
        "underline": False,
        "code": False,
        "color": "default",
    })


@dataclass
class Block:
    """Notion block representation."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: BlockType = BlockType.PARAGRAPH
    content: list[RichText] = field(default_factory=list)
    children: list[Block] = field(default_factory=list)
    checked: bool = False
    language: str = "plain text"
    has_children: bool = False


@dataclass
class Page:
    """Notion page representation."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    title: str = ""
    properties: dict[str, Any] = field(default_factory=dict)
    parent_id: Optional[str] = None
    parent_type: str = "page_id"
    created_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_edited_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    archived: bool = False
    url: str = ""


@dataclass
class Database:
    """Notion database representation."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    title: str = ""
    properties: dict[str, dict[str, Any]] = field(default_factory=dict)
    parent_id: Optional[str] = None
    parent_type: str = "page_id"
    created_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    url: str = ""


class NotionClient:
    """Notion API client."""

    def __init__(self, config: Optional[NotionConfig] = None):
        self.config = config or NotionConfig()
        self._pages: dict[str, Page] = {}
        self._databases: dict[str, Database] = {}
        self._blocks: dict[str, Block] = {}

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[dict] = None,
    ) -> dict[str, Any]:
        """Make API request to Notion."""
        await asyncio.sleep(0.02)
        return {"status": 200, "data": {}}

    async def get_page(self, page_id: str) -> Page:
        """Get a page by ID."""
        if page_id in self._pages:
            return self._pages[page_id]
        data = await self._request("GET", f"/pages/{page_id}")
        return Page(id=page_id, title=data["data"].get("properties", {}).get("title", [{}])[0].get("title", [{}])[0].get("plain_text", "Untitled"))

    async def create_page(
        self,
        parent_id: str,
        parent_type: str = "page_id",
        properties: Optional[dict[str, Any]] = None,
        children: Optional[list[dict]] = None,
    ) -> Page:
        """Create a new page."""
        page = Page(
            parent_id=parent_id,
            parent_type=parent_type,
            properties=properties or {},
        )
        self._pages[page.id] = page
        data = await self._request("POST", "/pages", {
            "parent": {"type": parent_type, parent_type: parent_id},
            "properties": properties or {},
            "children": children or [],
        })
        return page

    async def update_page(
        self,
        page_id: str,
        properties: Optional[dict[str, Any]] = None,
        archived: bool = False,
    ) -> Page:
        """Update a page."""
        if page_id not in self._pages:
            raise Exception(f"Page {page_id} not found")
        page = self._pages[page_id]
        if properties:
            page.properties.update(properties)
        page.archived = archived
        page.last_edited_time = datetime.now(timezone.utc)
        await self._request("PATCH", f"/pages/{page_id}", {"properties": properties, "archived": archived})
        return page

    async def archive_page(self, page_id: str) -> bool:
        """Archive a page."""
        await self.update_page(page_id, archived=True)
        return True

    async def get_database(self, database_id: str) -> Database:
        """Get a database by ID."""
        if database_id in self._databases:
            return self._databases[database_id]
        data = await self._request("GET", f"/databases/{database_id}")
        return Database(id=database_id, title=data["data"].get("title", [{}])[0].get("plain_text", "Untitled"))

    async def create_database(
        self,
        parent_page_id: str,
        title: str,
        properties: dict[str, dict[str, Any]],
    ) -> Database:
        """Create a new database."""
        database = Database(
            parent_id=parent_page_id,
            title=title,
            properties=properties,
        )
        self._databases[database.id] = database
        await self._request("POST", "/databases", {
            "parent": {"type": "page_id", "page_id": parent_page_id},
            "title": [{"type": "text", "text": {"content": title}}],
            "properties": properties,
        })
        return database

    async def query_database(
        self,
        database_id: str,
        filter: Optional[dict] = None,
        sorts: Optional[list[dict]] = None,
        page_size: int = 100,
        start_cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        """Query a database."""
        await asyncio.sleep(0.02)
        database_pages = [p for p in self._pages.values() if p.parent_id == database_id]
        return {
            "results": database_pages,
            "has_more": False,
            "next_cursor": None,
        }

    async def create_database_entry(
        self,
        database_id: str,
        properties: dict[str, Any],
    ) -> Page:
        """Create a new entry in database."""
        page = Page(
            parent_id=database_id,
            parent_type="database_id",
            properties=properties,
        )
        self._pages[page.id] = page
        await self._request("POST", "/pages", {
            "parent": {"type": "database_id", "database_id": database_id},
            "properties": properties,
        })
        return page

    async def get_block_children(self, block_id: str) -> list[Block]:
        """Get children of a block."""
        blocks = [b for b in self._blocks.values() if b.id == block_id]
        if blocks:
            return blocks[0].children
        return []

    async def append_block_children(
        self,
        block_id: str,
        children: list[dict[str, Any]],
    ) -> list[Block]:
        """Append children to a block."""
        blocks = []
        for child_data in children:
            block = Block(
                type=BlockType(child_data.get("type", "paragraph")),
                content=[RichText(text=child_data.get("text", ""))],
            )
            blocks.append(block)
            self._blocks[block.id] = block
        await self._request("PATCH", f"/blocks/{block_id}/children", {"children": children})
        return blocks

    async def update_block(
        self,
        block_id: str,
        updates: dict[str, Any],
    ) -> Block:
        """Update a block."""
        if block_id in self._blocks:
            block = self._blocks[block_id]
            if "text" in updates:
                block.content = [RichText(text=updates["text"])]
            await self._request("PATCH", f"/blocks/{block_id}", updates)
            return block
        raise Exception(f"Block {block_id} not found")

    async def delete_block(self, block_id: str) -> bool:
        """Delete a block."""
        if block_id in self._blocks:
            del self._blocks[block_id]
        await self._request("DELETE", f"/blocks/{block_id}")
        return True

    async def search(
        self,
        query: str,
        filter_type: Optional[str] = None,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """Search pages and databases."""
        await asyncio.sleep(0.02)
        results = []
        for page in self._pages.values():
            if query.lower() in page.title.lower():
                results.append(page)
        return {"results": results, "has_more": False}


def build_paragraph_block(text: str, annotations: Optional[dict] = None) -> dict[str, Any]:
    """Build a paragraph block."""
    return {
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": text}}],
            "annotations": annotations or {},
        },
    }


def build_heading_block(text: str, level: int = 1) -> dict[str, Any]:
    """Build a heading block."""
    heading_type = f"heading_{level}"
    return {
        "type": heading_type,
        heading_type: {
            "rich_text": [{"type": "text", "text": {"content": text}}],
        },
    }


def build_todo_block(text: str, checked: bool = False) -> dict[str, Any]:
    """Build a to-do block."""
    return {
        "type": "to_do",
        "to_do": {
            "rich_text": [{"type": "text", "text": {"content": text}}],
            "checked": checked,
        },
    }


async def demo():
    """Demo Notion integration."""
    client = NotionClient()

    page = await client.create_page(
        parent_id="parent-page-id",
        properties={"title": [{"title": [{"text": {"content": "New Page"}}]}]},
    )
    print(f"Created page: {page.id}")


if __name__ == "__main__":
    asyncio.run(demo())
