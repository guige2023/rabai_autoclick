"""Notion Integration Action Module.

Provides Notion workspace operations including database queries,
page creation, content updates, and block manipulation.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class BlockType(Enum):
    """Notion block types."""
    PARAGRAPH = "paragraph"
    HEADING_1 = "heading_1"
    HEADING_2 = "heading_2"
    HEADING_3 = "heading_3"
    BULLETED_LIST = "bulleted_list_item"
    NUMBERED_LIST = "numbered_list_item"
    TODO = "to_do"
    TOGGLE = "toggle"
    CODE = "code"
    QUOTE = "quote"
    CALLOUT = "callout"
    DIVIDER = "divider"
    IMAGE = "image"
    FILE = "file"
    VIDEO = "video"


@dataclass
class NotionBlock:
    """Notion block structure."""
    id: str
    type: BlockType
    content: str
    children: List["NotionBlock"] = field(default_factory=list)
    checked: bool = False
    language: str = "plain"


@dataclass
class NotionPage:
    """Notion page structure."""
    id: str
    title: str
    url: str
    parent_id: str
    properties: Dict[str, Any] = field(default_factory=dict)
    blocks: List[NotionBlock] = field(default_factory=list)
    created_time: float = field(default_factory=time.time)
    last_edited_time: float = field(default_factory=time.time)


@dataclass
class NotionDatabase:
    """Notion database structure."""
    id: str
    title: str
    url: str
    properties: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class OperationResult:
    """Notion operation result."""
    success: bool
    operation: str
    id: Optional[str]
    data: Any = None
    message: str = ""
    duration_ms: float = 0.0


class NotionStore:
    """In-memory Notion data store (simulated)."""

    def __init__(self):
        self._pages: Dict[str, NotionPage] = {}
        self._databases: Dict[str, NotionDatabase] = {}
        self._blocks: Dict[str, List[NotionBlock]] = {}

    def create_page(self, title: str, parent_id: str,
                    properties: Optional[Dict[str, Any]] = None,
                    blocks: Optional[List[Dict[str, Any]]] = None) -> NotionPage:
        """Create a new page."""
        page_id = uuid.uuid4().hex
        page = NotionPage(
            id=page_id,
            title=title,
            url=f"https://notion.so/{page_id}",
            parent_id=parent_id,
            properties=properties or {},
            blocks=[]
        )
        self._pages[page_id] = page

        if blocks:
            page_blocks = []
            for b in blocks:
                block = NotionBlock(
                    id=uuid.uuid4().hex,
                    type=BlockType(b.get("type", "paragraph")),
                    content=b.get("content", ""),
                    checked=b.get("checked", False)
                )
                page_blocks.append(block)
            self._blocks[page_id] = page_blocks
            page.blocks = page_blocks

        return page

    def get_page(self, page_id: str) -> Optional[NotionPage]:
        """Get page by ID."""
        return self._pages.get(page_id)

    def update_page(self, page_id: str,
                    title: Optional[str] = None,
                    properties: Optional[Dict[str, Any]] = None) -> OperationResult:
        """Update page properties."""
        start = time.time()
        page = self._pages.get(page_id)
        if not page:
            return OperationResult(
                success=False,
                operation="update_page",
                id=page_id,
                message="Page not found",
                duration_ms=(time.time() - start) * 1000
            )

        if title:
            page.title = title
        if properties:
            page.properties.update(properties)
        page.last_edited_time = time.time()

        return OperationResult(
            success=True,
            operation="update_page",
            id=page_id,
            message=f"Updated page {page_id}",
            duration_ms=(time.time() - start) * 1000
        )

    def archive_page(self, page_id: str) -> OperationResult:
        """Archive a page."""
        start = time.time()
        page = self._pages.get(page_id)
        if not page:
            return OperationResult(
                success=False,
                operation="archive_page",
                id=page_id,
                message="Page not found",
                duration_ms=(time.time() - start) * 1000
            )

        del self._pages[page_id]
        return OperationResult(
            success=True,
            operation="archive_page",
            id=page_id,
            message=f"Archived page {page_id}",
            duration_ms=(time.time() - start) * 1000
        )

    def create_database(self, title: str, parent_id: str,
                        properties: Dict[str, Dict[str, Any]]) -> NotionDatabase:
        """Create a new database."""
        db_id = uuid.uuid4().hex
        db = NotionDatabase(
            id=db_id,
            title=title,
            url=f"https://notion.so/{db_id}",
            properties=properties
        )
        self._databases[db_id] = db
        return db

    def get_database(self, db_id: str) -> Optional[NotionDatabase]:
        """Get database by ID."""
        return self._databases.get(db_id)

    def query_database(self, db_id: str,
                       filter: Optional[Dict[str, Any]] = None,
                       sorts: Optional[List[Dict[str, Any]]] = None,
                       limit: int = 100) -> List[NotionPage]:
        """Query database and return matching pages."""
        pages = [p for p in self._pages.values() if p.parent_id == db_id]

        if filter:
            for f in filter:
                prop = f.get("property")
                cond = f.get("condition", {})
                op = cond.get("operator", "equals")
                value = cond.get("value")

                if prop in ["title", "Name"]:
                    pages = [p for p in pages if self._match_text(p.title, op, value)]
                elif prop in p.properties:
                    pages = [p for p in pages if self._match_value(p.properties.get(prop), op, value)]

        return pages[:limit]

    def _match_text(self, text: str, op: str, value: Any) -> bool:
        """Match text with operator."""
        if op == "equals":
            return text == value
        elif op == "contains":
            return value in text
        elif op == "starts_with":
            return text.startswith(value)
        elif op == "ends_with":
            return text.endswith(value)
        return True

    def _match_value(self, prop_value: Any, op: str, filter_value: Any) -> bool:
        """Match property value with operator."""
        if op == "equals":
            return prop_value == filter_value
        elif op == "contains":
            return filter_value in str(prop_value)
        return True

    def add_block(self, page_id: str, block_type: str,
                  content: str, after_block_id: Optional[str] = None) -> NotionBlock:
        """Add block to page."""
        block = NotionBlock(
            id=uuid.uuid4().hex,
            type=BlockType(block_type),
            content=content
        )

        if page_id not in self._blocks:
            self._blocks[page_id] = []

        if after_block_id:
            idx = next((i for i, b in enumerate(self._blocks[page_id]) if b.id == after_block_id), -1)
            self._blocks[page_id].insert(idx + 1, block)
        else:
            self._blocks[page_id].append(block)

        if page_id in self._pages:
            self._pages[page_id].blocks = self._blocks[page_id]

        return block

    def update_block(self, block_id: str, page_id: str,
                     content: Optional[str] = None,
                     checked: Optional[bool] = None) -> OperationResult:
        """Update block content."""
        start = time.time()
        blocks = self._blocks.get(page_id, [])
        block = next((b for b in blocks if b.id == block_id), None)

        if not block:
            return OperationResult(
                success=False,
                operation="update_block",
                id=block_id,
                message="Block not found",
                duration_ms=(time.time() - start) * 1000
            )

        if content is not None:
            block.content = content
        if checked is not None:
            block.checked = checked

        return OperationResult(
            success=True,
            operation="update_block",
            id=block_id,
            message=f"Updated block {block_id}",
            duration_ms=(time.time() - start) * 1000
        )

    def delete_block(self, block_id: str, page_id: str) -> OperationResult:
        """Delete block from page."""
        start = time.time()
        blocks = self._blocks.get(page_id, [])
        block = next((b for b in blocks if b.id == block_id), None)

        if not block:
            return OperationResult(
                success=False,
                operation="delete_block",
                id=block_id,
                message="Block not found",
                duration_ms=(time.time() - start) * 1000
            )

        self._blocks[page_id].remove(block)

        return OperationResult(
            success=True,
            operation="delete_block",
            id=block_id,
            message=f"Deleted block {block_id}",
            duration_ms=(time.time() - start) * 1000
        )


_global_store = NotionStore()


class NotionAction:
    """Notion integration action.

    Example:
        action = NotionAction()

        page = action.create_page("My Page", parent_id="parent-123")
        action.add_block(page.id, "heading_1", "Welcome")
        action.add_block(page.id, "paragraph", "This is content...")

        db = action.create_database("Tasks", parent_id="parent-456", properties={
            "Name": {"type": "title"},
            "Status": {"type": "select", "options": ["Todo", "Done"]},
            "Due": {"type": "date"}
        })

        pages = action.query_database(db.id, filter=[...])
    """

    def __init__(self, store: Optional[NotionStore] = None):
        self._store = store or _global_store

    def create_page(self, title: str, parent_id: str,
                    properties: Optional[Dict[str, Any]] = None,
                    blocks: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Create a new page.

        Args:
            title: Page title
            parent_id: Parent page or database ID
            properties: Page properties
            blocks: Initial blocks

        Returns:
            Dict with page info
        """
        try:
            page = self._store.create_page(title, parent_id, properties, blocks)
            return {
                "success": True,
                "page": {
                    "id": page.id,
                    "title": page.title,
                    "url": page.url,
                    "parent_id": page.parent_id,
                    "properties": page.properties,
                    "created_time": page.created_time
                },
                "message": f"Created page: {title}"
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def get_page(self, page_id: str) -> Dict[str, Any]:
        """Get page by ID.

        Args:
            page_id: Page ID

        Returns:
            Dict with page info
        """
        page = self._store.get_page(page_id)
        if page:
            return {
                "success": True,
                "page": {
                    "id": page.id,
                    "title": page.title,
                    "url": page.url,
                    "properties": page.properties,
                    "blocks": [
                        {
                            "id": b.id,
                            "type": b.type.value,
                            "content": b.content,
                            "checked": b.checked
                        }
                        for b in page.blocks
                    ],
                    "created_time": page.created_time,
                    "last_edited_time": page.last_edited_time
                }
            }
        return {"success": False, "message": "Page not found"}

    def update_page(self, page_id: str,
                    title: Optional[str] = None,
                    properties: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Update page properties.

        Args:
            page_id: Page ID
            title: New title
            properties: Properties to update

        Returns:
            Dict with operation result
        """
        result = self._store.update_page(page_id, title, properties)
        return {
            "success": result.success,
            "message": result.message,
            "duration_ms": result.duration_ms
        }

    def archive_page(self, page_id: str) -> Dict[str, Any]:
        """Archive a page.

        Args:
            page_id: Page ID

        Returns:
            Dict with operation result
        """
        result = self._store.archive_page(page_id)
        return {
            "success": result.success,
            "message": result.message,
            "duration_ms": result.duration_ms
        }

    def create_database(self, title: str, parent_id: str,
                        properties: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Create a new database.

        Args:
            title: Database title
            parent_id: Parent page ID
            properties: Database schema

        Returns:
            Dict with database info
        """
        try:
            db = self._store.create_database(title, parent_id, properties)
            return {
                "success": True,
                "database": {
                    "id": db.id,
                    "title": db.title,
                    "url": db.url,
                    "properties": db.properties
                },
                "message": f"Created database: {title}"
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def get_database(self, db_id: str) -> Dict[str, Any]:
        """Get database by ID.

        Args:
            db_id: Database ID

        Returns:
            Dict with database info
        """
        db = self._store.get_database(db_id)
        if db:
            return {
                "success": True,
                "database": {
                    "id": db.id,
                    "title": db.title,
                    "url": db.url,
                    "properties": db.properties
                }
            }
        return {"success": False, "message": "Database not found"}

    def query_database(self, db_id: str,
                       filter: Optional[List[Dict[str, Any]]] = None,
                       sorts: Optional[List[Dict[str, Any]]] = None,
                       limit: int = 100) -> Dict[str, Any]:
        """Query database.

        Args:
            db_id: Database ID
            filter: Filter conditions
            sorts: Sort conditions
            limit: Maximum results

        Returns:
            Dict with matching pages
        """
        try:
            pages = self._store.query_database(db_id, filter, sorts, limit)
            return {
                "success": True,
                "pages": [
                    {
                        "id": p.id,
                        "title": p.title,
                        "url": p.url,
                        "properties": p.properties,
                        "created_time": p.created_time
                    }
                    for p in pages
                ],
                "count": len(pages)
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def add_block(self, page_id: str, block_type: str, content: str,
                  after_block_id: Optional[str] = None) -> Dict[str, Any]:
        """Add block to page.

        Args:
            page_id: Page ID
            block_type: Block type
            content: Block content
            after_block_id: Insert after this block

        Returns:
            Dict with block info
        """
        try:
            block = self._store.add_block(page_id, block_type, content, after_block_id)
            return {
                "success": True,
                "block": {
                    "id": block.id,
                    "type": block.type.value,
                    "content": block.content
                },
                "message": f"Added {block_type} block"
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def update_block(self, block_id: str, page_id: str,
                     content: Optional[str] = None,
                     checked: Optional[bool] = None) -> Dict[str, Any]:
        """Update block content.

        Args:
            block_id: Block ID
            page_id: Page ID
            content: New content
            checked: Checkbox state

        Returns:
            Dict with operation result
        """
        result = self._store.update_block(block_id, page_id, content, checked)
        return {
            "success": result.success,
            "message": result.message,
            "duration_ms": result.duration_ms
        }

    def delete_block(self, block_id: str, page_id: str) -> Dict[str, Any]:
        """Delete block from page.

        Args:
            block_id: Block ID
            page_id: Page ID

        Returns:
            Dict with operation result
        """
        result = self._store.delete_block(block_id, page_id)
        return {
            "success": result.success,
            "message": result.message,
            "duration_ms": result.duration_ms
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute Notion action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "create_page", "get_page", "update_page", "archive_page",
                         "create_database", "get_database", "query_database",
                         "add_block", "update_block", "delete_block"
            - title: Page/database title
            - parent_id: Parent page or database ID
            - page_id: Page ID
            - db_id: Database ID
            - properties: Page/database properties
            - blocks: Initial blocks (for create_page)
            - block_type: Block type (for add_block)
            - content: Block content (for add_block, update_block)
            - block_id: Block ID (for update_block, delete_block)
            - after_block_id: Insert after block ID
            - filter: Query filter
            - limit: Query limit

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "")
    action = NotionAction()

    try:
        if operation == "create_page":
            title = params.get("title", "")
            parent_id = params.get("parent_id", "")
            if not title or not parent_id:
                return {"success": False, "message": "title and parent_id required"}
            return action.create_page(
                title=title,
                parent_id=parent_id,
                properties=params.get("properties"),
                blocks=params.get("blocks")
            )

        elif operation == "get_page":
            page_id = params.get("page_id", "")
            if not page_id:
                return {"success": False, "message": "page_id required"}
            return action.get_page(page_id)

        elif operation == "update_page":
            page_id = params.get("page_id", "")
            if not page_id:
                return {"success": False, "message": "page_id required"}
            return action.update_page(
                page_id=page_id,
                title=params.get("title"),
                properties=params.get("properties")
            )

        elif operation == "archive_page":
            page_id = params.get("page_id", "")
            if not page_id:
                return {"success": False, "message": "page_id required"}
            return action.archive_page(page_id)

        elif operation == "create_database":
            title = params.get("title", "")
            parent_id = params.get("parent_id", "")
            properties = params.get("properties", {})
            if not title or not parent_id:
                return {"success": False, "message": "title and parent_id required"}
            return action.create_database(title, parent_id, properties)

        elif operation == "get_database":
            db_id = params.get("db_id", "")
            if not db_id:
                return {"success": False, "message": "db_id required"}
            return action.get_database(db_id)

        elif operation == "query_database":
            db_id = params.get("db_id", "")
            if not db_id:
                return {"success": False, "message": "db_id required"}
            return action.query_database(
                db_id=db_id,
                filter=params.get("filter"),
                sorts=params.get("sorts"),
                limit=params.get("limit", 100)
            )

        elif operation == "add_block":
            page_id = params.get("page_id", "")
            block_type = params.get("block_type", "paragraph")
            content = params.get("content", "")
            if not page_id:
                return {"success": False, "message": "page_id required"}
            return action.add_block(
                page_id=page_id,
                block_type=block_type,
                content=content,
                after_block_id=params.get("after_block_id")
            )

        elif operation == "update_block":
            block_id = params.get("block_id", "")
            page_id = params.get("page_id", "")
            if not block_id or not page_id:
                return {"success": False, "message": "block_id and page_id required"}
            return action.update_block(
                block_id=block_id,
                page_id=page_id,
                content=params.get("content"),
                checked=params.get("checked")
            )

        elif operation == "delete_block":
            block_id = params.get("block_id", "")
            page_id = params.get("page_id", "")
            if not block_id or not page_id:
                return {"success": False, "message": "block_id and page_id required"}
            return action.delete_block(block_id, page_id)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Notion error: {str(e)}"}
