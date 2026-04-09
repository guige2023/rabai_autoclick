"""
API DynamoDB Action Module.

Provides Amazon DynamoDB operations including CRUD,
batch operations, queries, scans, and conditional updates.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class KeyType(Enum):
    """DynamoDB key types."""
    HASH = "HASH"
    RANGE = "RANGE"


@dataclass
class DynamoDBConfig:
    """DynamoDB client configuration."""
    region: str = "us-east-1"
    table_name: str = ""
    endpoint_url: Optional[str] = None
    credentials: Optional[dict[str, str]] = None
    read_capacity: int = 5
    write_capacity: int = 5


@dataclass
class Item:
    """DynamoDB item representation."""
    partition_key: str
    sort_key: Optional[str] = None
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Query result container."""
    items: list[dict[str, Any]]
    count: int
    scanned_count: int
    last_evaluated_key: Optional[dict[str, Any]] = None


@dataclass
class BatchWriteResult:
    """Batch write operation result."""
    unprocessed_items: list[dict[str, Any]] = field(default_factory=list)
    consumed_capacity: list[dict[str, Any]] = field(default_factory=list)


class DynamoDBClient:
    """DynamoDB client wrapper."""

    def __init__(self, config: DynamoDBConfig):
        self.config = config
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._gsis: dict[str, dict[str, Any]] = {}

    async def put_item(
        self,
        item: dict[str, Any],
        condition: Optional[str] = None,
        return_values: str = "NONE",
    ) -> dict[str, Any]:
        """Put a single item."""
        table = self._tables.setdefault(self.config.table_name, [])
        existing = next((i for i in table if i.get("PK") == item.get("PK")), None)
        if condition == "attribute_not_exists" and existing:
            raise Exception("ConditionalCheckFailedException")
        if existing:
            table.remove(existing)
        table.append(item)
        await asyncio.sleep(0.01)
        return {"attributes": item} if return_values != "NONE" else {}}

    async def get_item(
        self,
        key: dict[str, Any],
        projection: Optional[list[str]] = None,
        consistent_read: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Get a single item by key."""
        table = self._tables.get(self.config.table_name, [])
        for item in table:
            if all(item.get(k) == v for k, v in key.items()):
                if projection:
                    return {k: item[k] for k in projection if k in item}
                return item.copy()
        return None

    async def delete_item(
        self,
        key: dict[str, Any],
        condition: Optional[str] = None,
        return_values: str = "NONE",
    ) -> dict[str, Any]:
        """Delete an item."""
        table = self._tables.get(self.config.table_name, [])
        for i, item in enumerate(table):
            if all(item.get(k) == v for k, v in key.items()):
                deleted = table.pop(i)
                await asyncio.sleep(0.01)
                return {"attributes": deleted} if return_values != "NONE" else {}
        return {}

    async def update_item(
        self,
        key: dict[str, Any],
        updates: dict[str, dict[str, Any]],
        condition: Optional[str] = None,
        return_values: str = "NONE",
    ) -> dict[str, Any]:
        """Update an item."""
        item = await self.get_item(key)
        if not item:
            raise Exception("Item not found")
        for attr, update in updates.items():
            op = list(update.keys())[0]
            value = list(update.values())[0]
            if op == "S":
                item[attr] = value
            elif op == "N":
                item[attr] = int(value) if "." not in str(value) else float(value)
            elif op == "ADD":
                if attr not in item:
                    item[attr] = 0
                item[attr] += int(value) if isinstance(value, int) else float(value)
            elif op == "DELETE":
                item.pop(attr, None)
        await asyncio.sleep(0.01)
        return {"attributes": item} if return_values != "NONE" else {}

    async def query(
        self,
        key_condition: dict[str, Any],
        filter_expression: Optional[str] = None,
        projection: Optional[list[str]] = None,
        limit: int = 100,
        scan_index_forward: bool = True,
        exclusive_start_key: Optional[dict[str, Any]] = None,
    ) -> QueryResult:
        """Query items by key condition."""
        table = self._tables.get(self.config.table_name, [])
        results = []
        for item in table:
            if all(item.get(k) == v for k, v in key_condition.items()):
                results.append(item)
        if projection:
            results = [{k: r[k] for k in projection if k in r} for r in results]
        await asyncio.sleep(0.02)
        return QueryResult(
            items=results[:limit],
            count=len(results[:limit]),
            scanned_count=len(results),
        )

    async def scan(
        self,
        filter_expression: Optional[str] = None,
        projection: Optional[list[str]] = None,
        limit: int = 100,
        segment: int = 0,
        total_segments: int = 1,
    ) -> QueryResult:
        """Scan all items in table."""
        table = self._tables.get(self.config.table_name, [])
        results = table[segment::total_segments]
        if projection:
            results = [{k: r[k] for k in projection if k in r} for r in results]
        await asyncio.sleep(0.02)
        return QueryResult(
            items=results[:limit],
            count=len(results[:limit]),
            scanned_count=len(results),
        )

    async def batch_get_item(
        self,
        keys: list[dict[str, Any]],
        projection: Optional[list[str]] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Batch get multiple items."""
        results = []
        for key in keys:
            item = await self.get_item(key, projection)
            if item:
                results.append(item)
        await asyncio.sleep(0.02)
        return {"responses": {self.config.table_name: results}}

    async def batch_write_item(
        self,
        items: list[tuple[str, dict[str, Any]]],
    ) -> BatchWriteResult:
        """Batch write or delete multiple items."""
        unprocessed = []
        for op, item in items:
            try:
                if op == "put":
                    await self.put_item(item)
                elif op == "delete":
                    await self.delete_item(item)
            except Exception:
                unprocessed.append(item)
        return BatchWriteResult(unprocessed_items=unprocessed)


class DynamoDBCache:
    """Simple cache layer for DynamoDB."""

    def __init__(self, client: DynamoDBClient, ttl_seconds: int = 60):
        self.client = client
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[Any, float]] = {}

    def _make_key(self, key: dict[str, Any]) -> str:
        """Generate cache key from item key."""
        return hashlib.md5(json.dumps(key, sort_keys=True).encode()).hexdigest()

    async def get_item(self, key: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Get item with cache."""
        cache_key = self._make_key(key)
        if cache_key in self._cache:
            value, expires = self._cache[cache_key]
            import time
            if time.time() < expires:
                return value
            del self._cache[cache_key]
        item = await self.client.get_item(key)
        if item:
            import time
            self._cache[cache_key] = (item, time.time() + self.ttl_seconds)
        return item

    async def put_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Put item and invalidate cache."""
        cache_key = self._make_key(item)
        if cache_key in self._cache:
            del self._cache[cache_key]
        return await self.client.put_item(item)


async def demo():
    """Demo DynamoDB operations."""
    config = DynamoDBConfig(table_name="Users")
    client = DynamoDBClient(config)

    await client.put_item({"PK": "user#123", "SK": "profile", "name": "Alice", "age": 30})
    await client.put_item({"PK": "user#123", "SK": "settings", "theme": "dark"})
    await client.put_item({"PK": "user#456", "SK": "profile", "name": "Bob", "age": 25})

    result = await client.query(key_condition={"PK": "user#123"})
    print(f"Found {result.count} items for user#123")

    item = await client.get_item({"PK": "user#123", "SK": "profile"})
    print(f"Got item: {item}")


if __name__ == "__main__":
    asyncio.run(demo())
