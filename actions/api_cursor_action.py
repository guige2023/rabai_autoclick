"""
API Cursor Action Module.

Implements cursor-based pagination for API responses,
providing efficient navigation through large result sets.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import base64
import json
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CursorDirection(Enum):
    """Direction for cursor navigation."""
    FORWARD = auto()
    BACKWARD = auto()
    AROUND = auto()


@dataclass
class Cursor:
    """Represents a pagination cursor."""
    position: str
    index: int
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def encode(self) -> str:
        """Encode cursor to string."""
        data = {
            "pos": self.position,
            "idx": self.index,
            "ts": self.timestamp.isoformat() if self.timestamp else None,
            "meta": self.metadata,
        }
        json_str = json.dumps(data, default=str)
        return base64.urlsafe_b64encode(json_str.encode()).decode()

    @classmethod
    def decode(cls, cursor_str: str) -> Optional["Cursor"]:
        """Decode cursor from string."""
        try:
            json_str = base64.urlsafe_b64decode(cursor_str.encode()).decode()
            data = json.loads(json_str)
            return cls(
                position=data["pos"],
                index=data["idx"],
                timestamp=datetime.fromisoformat(data["ts"]) if data.get("ts") else None,
                metadata=data.get("meta", {}),
            )
        except Exception as e:
            logger.warning(f"Failed to decode cursor: {e}")
            return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "position": self.position,
            "index": self.index,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "metadata": self.metadata,
        }


@dataclass
class Page(Generic[T]):
    """A page of results."""
    items: List[T]
    cursor: Optional[Cursor]
    has_next: bool
    has_previous: bool
    total_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "items": self.items,
            "cursor": self.cursor.to_dict() if self.cursor else None,
            "has_next": self.has_next,
            "has_previous": self.has_previous,
            "total_count": self.total_count,
        }


@dataclass
class PaginationConfig:
    """Configuration for pagination."""
    page_size: int = 20
    max_page_size: int = 100
    default_direction: CursorDirection = CursorDirection.FORWARD
    include_total: bool = False
    cursor_ttl_seconds: Optional[float] = None


class ApiCursorAction(Generic[T]):
    """
    Implements cursor-based pagination for API responses.

    This action provides efficient pagination through large result sets
    using cursor-based navigation, which is more performant than offset-based
    pagination for large datasets.

    Example:
        >>> cursor_action = ApiCursorAction(page_size=10)
        >>> async def fetch_items(cursor):
        ...     return await db.fetch_some(limit=11, cursor=cursor)
        >>> page = await cursor_action.fetch(fetch_items, None)
        >>> print(f"Got {len(page.items)} items")
        >>> next_page = await cursor_action.fetch_next(fetch_items, page.cursor)
    """

    def __init__(self, config: Optional[PaginationConfig] = None):
        """
        Initialize the API Cursor Action.

        Args:
            config: Optional pagination configuration.
        """
        self.config = config or PaginationConfig()

    async def fetch(
        self,
        fetcher: Callable[[Optional[str]], Tuple[List[T], Optional[str]]],
        cursor_str: Optional[str],
        direction: Optional[CursorDirection] = None,
        page_size: Optional[int] = None,
    ) -> Page[T]:
        """
        Fetch a page of results.

        Args:
            fetcher: Function to fetch items, takes cursor string, returns (items, next_cursor).
            cursor_str: Optional cursor for navigation.
            direction: Navigation direction.
            page_size: Override default page size.

        Returns:
            Page of results.
        """
        direction = direction or self.config.default_direction
        size = min(page_size or self.config.page_size, self.config.max_page_size)

        actual_size = size + 1

        cursor = None
        if cursor_str:
            cursor = Cursor.decode(cursor_str)
            if cursor and direction == CursorDirection.BACKWARD:
                cursor.index -= 1

        items, next_cursor = fetcher(cursor_str)

        has_more = len(items) > size
        if has_more:
            items = items[:size]

        has_next = has_more
        has_previous = cursor is not None and cursor.index > 0

        result_cursor = None
        if items and next_cursor:
            idx = cursor.index + 1 if cursor else 0
            result_cursor = Cursor(
                position=next_cursor,
                index=idx,
                timestamp=datetime.now(timezone.utc),
            )

        return Page(
            items=items,
            cursor=result_cursor,
            has_next=has_next,
            has_previous=has_previous,
        )

    async def fetch_next(
        self,
        fetcher: Callable[[Optional[str]], Tuple[List[T], Optional[str]]],
        current_cursor: Cursor,
    ) -> Page[T]:
        """Fetch the next page."""
        return await self.fetch(
            fetcher,
            current_cursor.encode(),
            CursorDirection.FORWARD,
        )

    async def fetch_previous(
        self,
        fetcher: Callable[[Optional[str]], Tuple[List[T], Optional[str]]],
        current_cursor: Cursor,
    ) -> Page[T]:
        """Fetch the previous page."""
        return await self.fetch(
            fetcher,
            current_cursor.encode(),
            CursorDirection.BACKWARD,
        )

    async def fetch_around(
        self,
        fetcher: Callable[[Optional[str]], Tuple[List[T], Optional[str]]],
        center_cursor: Cursor,
        size: Optional[int] = None,
    ) -> Page[T]:
        """
        Fetch a page centered around a cursor position.

        Args:
            fetcher: Function to fetch items.
            center_cursor: Center position cursor.
            size: Page size.

        Returns:
            Page centered around cursor.
        """
        size = size or self.config.page_size

        before_cursor = self._create_before_cursor(center_cursor, size // 2)
        after_cursor = self._create_after_cursor(center_cursor, size // 2)

        items_before, _ = fetcher(before_cursor)
        items_after, _ = fetcher(after_cursor)

        items = items_before + items_after

        return Page(
            items=items[:size],
            cursor=center_cursor,
            has_next=True,
            has_previous=True,
        )

    def _create_before_cursor(self, center: Cursor, offset: int) -> Optional[str]:
        """Create cursor for items before center."""
        new_index = max(0, center.index - offset)
        if new_index == center.index:
            return None

        cursor = Cursor(
            position=center.position,
            index=new_index,
            timestamp=center.timestamp,
        )
        return cursor.encode()

    def _create_after_cursor(self, center: Cursor, offset: int) -> Optional[str]:
        """Create cursor for items after center."""
        cursor = Cursor(
            position=center.position,
            index=center.index + offset,
            timestamp=center.timestamp,
        )
        return cursor.encode()

    def create_initial_cursor(self) -> Cursor:
        """Create an initial cursor for first page."""
        return Cursor(
            position="start",
            index=0,
            timestamp=datetime.now(timezone.utc),
        )

    def validate_cursor(self, cursor_str: str) -> bool:
        """Validate a cursor string."""
        cursor = Cursor.decode(cursor_str)
        return cursor is not None

    def get_cursor_info(self, cursor_str: str) -> Optional[Dict[str, Any]]:
        """Get information about a cursor."""
        cursor = Cursor.decode(cursor_str)
        if cursor is None:
            return None

        return {
            "position": cursor.position,
            "index": cursor.index,
            "timestamp": cursor.timestamp.isoformat() if cursor.timestamp else None,
            "metadata": cursor.metadata,
            "encoded": cursor_str,
        }

    def merge_cursors(self, cursor1: Cursor, cursor2: Cursor) -> Cursor:
        """Merge two cursors (for bidirectional pagination)."""
        return Cursor(
            position=f"{cursor1.position}:{cursor2.position}",
            index=cursor1.index,
            timestamp=cursor1.timestamp,
            metadata={
                "forward": cursor1.to_dict(),
                "backward": cursor2.to_dict(),
            },
        )

    def split_cursor(self, merged: Cursor) -> Tuple[Cursor, Optional[Cursor]]:
        """Split a merged cursor into forward and backward."""
        if ":" not in merged.position:
            return merged, None

        parts = merged.position.split(":")
        forward = Cursor(
            position=parts[0],
            index=merged.index,
            timestamp=merged.timestamp,
            metadata=merged.metadata.get("forward", {}),
        )

        backward_meta = merged.metadata.get("backward", {})
        backward = Cursor(
            position=backward_meta.get("position", parts[1] if len(parts) > 1 else ""),
            index=backward_meta.get("index", merged.index - 1),
            timestamp=datetime.fromisoformat(backward_meta["timestamp"]) if backward_meta.get("timestamp") else None,
        )

        return forward, backward


def create_cursor_action(page_size: int = 20, **kwargs) -> ApiCursorAction:
    """Factory function to create an ApiCursorAction."""
    config = PaginationConfig(page_size=page_size, **kwargs)
    return ApiCursorAction(config=config)
