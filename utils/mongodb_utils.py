"""MongoDB utilities: connection management, CRUD helpers, aggregation pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "MongoConfig",
    "MongoClient",
    "escape_key",
    "unescape_key",
    "build_filter",
]


@dataclass
class MongoConfig:
    """MongoDB connection configuration."""

    uri: str = "mongodb://localhost:27017"
    database: str = "default"
    username: str | None = None
    password: str | None = None
    auth_source: str = "admin"
    max_pool_size: int = 100
    min_pool_size: int = 10
    server_selection_timeout_ms: int = 5000


def escape_key(key: str) -> str:
    """Escape dots and dollar signs in field names for MongoDB."""
    return key.replace(".", "\\.").replace("$", "\\$")


def unescape_key(key: str) -> str:
    """Restore escaped dots and dollar signs."""
    return key.replace("\\.", ".").replace("\\$", "$")


def build_filter(
    eq: dict[str, Any] | None = None,
    ne: dict[str, Any] | None = None,
    gt: dict[str, Any] | None = None,
    gte: dict[str, Any] | None = None,
    lt: dict[str, Any] | None = None,
    lte: dict[str, Any] | None = None,
    contains: dict[str, str] | None = None,
    in_list: dict[str, list[Any]] | None = None,
    exists: dict[str, bool] | None = None,
    regex: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a MongoDB filter document from keyword args."""
    flt: dict[str, Any] = {}
    if eq:
        flt.update(eq)
    if ne:
        for k, v in (ne or {}).items():
            flt[k] = {"$ne": v}
    if gt:
        for k, v in (gt or {}).items():
            flt.setdefault(k, {})["$gt"] = v
    if gte:
        for k, v in (gte or {}).items():
            flt.setdefault(k, {})["$gte"] = v
    if lt:
        for k, v in (lt or {}).items():
            flt.setdefault(k, {})["$lt"] = v
    if lte:
        for k, v in (lte or {}).items():
            flt.setdefault(k, {})["$lte"] = v
    if contains:
        for k, v in (contains or {}).items():
            flt[k] = {"$regex": v, "$options": "i"}
    if in_list:
        for k, v in (in_list or {}).items():
            flt[k] = {"$in": v}
    if exists:
        for k, v in (exists or {}).items():
            flt[k] = {"$exists": v}
    if regex:
        for k, v in (regex or {}).items():
            flt[k] = {"$regex": v}
    return flt


class MongoClient:
    """Lightweight MongoDB client wrapper with CRUD helpers."""

    def __init__(self, config: MongoConfig | None = None) -> None:
        self.config = config or MongoConfig()
        self._client: Any = None

    def connect(self) -> Any:
        """Establish connection to MongoDB."""
        try:
            from pymongo import MongoClient as PyMongoClient
        except ImportError:
            return None

        self._client = PyMongoClient(
            self.config.uri,
            maxPoolSize=self.config.max_pool_size,
            minPoolSize=self.config.min_pool_size,
            serverSelectionTimeoutMS=self.config.server_selection_timeout_ms,
        )
        return self._client

    @property
    def db(self) -> Any:
        """Get the database handle."""
        if self._client is None:
            self.connect()
        return self._client[self.config.database]

    def find(
        self,
        collection: str,
        filter_dict: dict[str, Any] | None = None,
        projection: list[str] | None = None,
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
        skip: int = 0,
    ) -> list[dict[str, Any]]:
        """Query documents from a collection."""
        cursor = self.db[collection].find(
            filter_dict or {},
            {k: 1 for k in projection} if projection else None,
        )
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)

    def find_one(
        self,
        collection: str,
        filter_dict: dict[str, Any] | None = None,
        projection: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document."""
        return self.db[collection].find_one(
            filter_dict,
            {k: 1 for k in projection} if projection else None,
        )

    def insert_one(self, collection: str, document: dict[str, Any]) -> str:
        """Insert a single document, return its _id."""
        result = self.db[collection].insert_one(document)
        return str(result.inserted_id)

    def insert_many(self, collection: str, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents, return their _ids."""
        result = self.db[collection].insert_many(documents)
        return [str(id) for id in result.inserted_ids]

    def update_one(
        self,
        collection: str,
        filter_dict: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> int:
        """Update a single document, return matched count."""
        result = self.db[collection].update_one(filter_dict, {"$set": update}, upsert=upsert)
        return result.matched_count

    def update_many(
        self,
        collection: str,
        filter_dict: dict[str, Any],
        update: dict[str, Any],
    ) -> int:
        """Update multiple documents, return matched count."""
        result = self.db[collection].update_many(filter_dict, {"$set": update})
        return result.matched_count

    def delete_one(self, collection: str, filter_dict: dict[str, Any]) -> int:
        """Delete a single document, return deleted count."""
        result = self.db[collection].delete_one(filter_dict)
        return result.deleted_count

    def delete_many(self, collection: str, filter_dict: dict[str, Any]) -> int:
        """Delete multiple documents, return deleted count."""
        result = self.db[collection].delete_many(filter_dict)
        return result.deleted_count

    def count(self, collection: str, filter_dict: dict[str, Any] | None = None) -> int:
        """Count documents matching filter."""
        return self.db[collection].count_documents(filter_dict or {})

    def aggregate(self, collection: str, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run an aggregation pipeline."""
        return list(self.db[collection].aggregate(pipeline))
