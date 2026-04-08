"""MongoDB integration for document database operations.

Handles MongoDB operations including CRUD on collections,
aggregation pipelines, indexing, and change streams.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    from pymongo.errors import PyMongoError
except ImportError:
    MongoClient = None
    PyMongoError = Exception

logger = logging.getLogger(__name__)


class SortOrder(Enum):
    """Sort order enumeration."""
    ASC = ASCENDING
    DESC = DESCENDING


@dataclass
class MongoConfig:
    """Configuration for MongoDB connection."""
    connection_string: str
    database: str = "default"
    max_pool_size: int = 50
    min_pool_size: int = 10
    server_selection_timeout_ms: int = 5000
    connect_timeout_ms: int = 5000


@dataclass
class MongoQuery:
    """Query specification for MongoDB operations."""
    filter: dict = field(default_factory=dict)
    projection: Optional[dict] = None
    sort: Optional[list[tuple[str, int]]] = None
    skip: int = 0
    limit: Optional[int] = None
    batch_size: Optional[int] = None


@dataclass
class MongoIndex:
    """Represents a MongoDB index."""
    name: str
    keys: list[tuple[str, int]]
    unique: bool = False
    sparse: bool = False
    ttl: Optional[int] = None


class MongoAPIError(Exception):
    """Raised when MongoDB operations fail."""
    def __init__(self, message: str, error_code: Optional[str] = None):
        super().__init__(message)
        self.error_code = error_code


class MongoAction:
    """MongoDB client for document database operations."""

    def __init__(self, config: MongoConfig):
        """Initialize MongoDB client with configuration.

        Args:
            config: MongoConfig with connection string and database

        Raises:
            ImportError: If pymongo is not installed
            MongoAPIError: On connection failure
        """
        if MongoClient is None:
            raise ImportError("pymongo required: pip install pymongo")

        self.config = config
        self._client: Optional[MongoClient] = None
        self._db = None

    def connect(self) -> None:
        """Establish connection to MongoDB.

        Raises:
            MongoAPIError: On connection failure
        """
        try:
            self._client = MongoClient(
                self.config.connection_string,
                maxPoolSize=self.config.max_pool_size,
                minPoolSize=self.config.min_pool_size,
                serverSelectionTimeoutMS=self.config.server_selection_timeout_ms,
                connectTimeoutMS=self.config.connect_timeout_ms
            )
            self._client.admin.command("ping")
            self._db = self._client[self.config.database]
            logger.info(f"Connected to MongoDB: {self.config.database}")

        except Exception as e:
            raise MongoAPIError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("Disconnected from MongoDB")

    @property
    def db(self):
        """Get database reference, connect if needed."""
        if self._db is None:
            self.connect()
        return self._db

    def insert_one(self, collection: str, document: dict) -> str:
        """Insert a single document.

        Args:
            collection: Collection name
            document: Document to insert

        Returns:
            Inserted document ID as string

        Raises:
            MongoAPIError: On insert failure
        """
        try:
            result = self.db[collection].insert_one(document)
            return str(result.inserted_id)

        except PyMongoError as e:
            raise MongoAPIError(f"Insert failed: {e}")

    def insert_many(self, collection: str, documents: list[dict],
                    ordered: bool = True) -> list[str]:
        """Insert multiple documents.

        Args:
            collection: Collection name
            documents: List of documents to insert
            ordered: If True, stop on first error

        Returns:
            List of inserted document IDs as strings

        Raises:
            MongoAPIError: On insert failure
        """
        try:
            result = self.db[collection].insert_many(documents, ordered=ordered)
            return [str(id) for id in result.inserted_ids]

        except PyMongoError as e:
            raise MongoAPIError(f"Batch insert failed: {e}")

    def find_one(self, collection: str, query: dict,
                 projection: Optional[dict] = None) -> Optional[dict]:
        """Find a single document.

        Args:
            collection: Collection name
            query: Filter query
            projection: Optional field projection

        Returns:
            Document dict or None if not found
        """
        try:
            result = self.db[collection].find_one(query, projection)
            return self._serialize_document(result)

        except PyMongoError as e:
            raise MongoAPIError(f"Find one failed: {e}")

    def find(self, collection: str, query: MongoQuery) -> list[dict]:
        """Find documents matching query.

        Args:
            collection: Collection name
            query: MongoQuery with filter, projection, sort, skip, limit

        Returns:
            List of matching documents
        """
        try:
            cursor = self.db[collection].find(
                filter=query.filter,
                projection=query.projection
            )

            if query.sort:
                cursor = cursor.sort(query.sort)

            if query.skip:
                cursor = cursor.skip(query.skip)

            if query.limit:
                cursor = cursor.limit(query.limit)

            if query.batch_size:
                cursor = cursor.batch_size(query.batch_size)

            return [self._serialize_document(doc) for doc in cursor]

        except PyMongoError as e:
            raise MongoAPIError(f"Find failed: {e}")

    def count(self, collection: str, query: dict = None) -> int:
        """Count documents matching query.

        Args:
            collection: Collection name
            query: Filter query (empty dict for all)

        Returns:
            Document count
        """
        try:
            return self.db[collection].count_documents(query or {})

        except PyMongoError as e:
            raise MongoAPIError(f"Count failed: {e}")

    def update_one(self, collection: str, query: dict,
                   update: dict, upsert: bool = False) -> int:
        """Update a single document.

        Args:
            collection: Collection name
            query: Filter query
            update: Update operations ($set, $inc, etc.)
            upsert: Create if not exists

        Returns:
            Number of modified documents
        """
        try:
            result = self.db[collection].update_one(query, update, upsert=upsert)
            return result.modified_count

        except PyMongoError as e:
            raise MongoAPIError(f"Update one failed: {e}")

    def update_many(self, collection: str, query: dict,
                    update: dict, upsert: bool = False) -> int:
        """Update multiple documents.

        Args:
            collection: Collection name
            query: Filter query
            update: Update operations
            upsert: Create if not exists

        Returns:
            Number of modified documents
        """
        try:
            result = self.db[collection].update_many(query, update, upsert=upsert)
            return result.modified_count

        except PyMongoError as e:
            raise MongoAPIError(f"Update many failed: {e}")

    def delete_one(self, collection: str, query: dict) -> int:
        """Delete a single document.

        Args:
            collection: Collection name
            query: Filter query

        Returns:
            Number of deleted documents
        """
        try:
            result = self.db[collection].delete_one(query)
            return result.deleted_count

        except PyMongoError as e:
            raise MongoAPIError(f"Delete one failed: {e}")

    def delete_many(self, collection: str, query: dict) -> int:
        """Delete multiple documents.

        Args:
            collection: Collection name
            query: Filter query

        Returns:
            Number of deleted documents
        """
        try:
            result = self.db[collection].delete_many(query)
            return result.deleted_count

        except PyMongoError as e:
            raise MongoAPIError(f"Delete many failed: {e}")

    def aggregate(self, collection: str, pipeline: list[dict],
                  allow_disk_use: bool = True) -> list[dict]:
        """Execute aggregation pipeline.

        Args:
            collection: Collection name
            pipeline: MongoDB aggregation pipeline
            allow_disk_use: Allow disk usage for large operations

        Returns:
            List of aggregated documents
        """
        try:
            cursor = self.db[collection].aggregate(
                pipeline,
                allowDiskUse=allow_disk_use
            )
            return [self._serialize_document(doc) for doc in cursor]

        except PyMongoError as e:
            raise MongoAPIError(f"Aggregation failed: {e}")

    def create_index(self, collection: str, index: MongoIndex) -> str:
        """Create an index on a collection.

        Args:
            collection: Collection name
            index: MongoIndex specification

        Returns:
            Created index name
        """
        try:
            result = self.db[collection].create_index(
                keys=index.keys,
                unique=index.unique,
                sparse=index.sparse,
                name=index.name if index.name != "default" else None,
                expireAfterSeconds=index.ttl
            )
            return result

        except PyMongoError as e:
            raise MongoAPIError(f"Create index failed: {e}")

    def list_indexes(self, collection: str) -> list[MongoIndex]:
        """List all indexes on a collection.

        Args:
            collection: Collection name

        Returns:
            List of MongoIndex objects
        """
        try:
            indexes = self.db[collection].list_indexes()
            result = []

            for idx in indexes:
                result.append(MongoIndex(
                    name=idx["name"],
                    keys=list(idx["key"].items()),
                    unique=idx.get("unique", False),
                    sparse=idx.get("sparse", False),
                    ttl=idx.get("expireAfterSeconds")
                ))

            return result

        except PyMongoError as e:
            raise MongoAPIError(f"List indexes failed: {e}")

    def drop_index(self, collection: str, index_name: str) -> bool:
        """Drop an index from a collection.

        Args:
            collection: Collection name
            index_name: Name of index to drop

        Returns:
            True if successful
        """
        try:
            self.db[collection].drop_index(index_name)
            return True

        except PyMongoError as e:
            raise MongoAPIError(f"Drop index failed: {e}")

    def bulk_write(self, collection: str, operations: list[dict]) -> dict:
        """Execute bulk write operations.

        Args:
            collection: Collection name
            operations: List of bulk operation dicts

        Returns:
            Bulk write result summary
        """
        try:
            from pymongo import InsertOne, UpdateOne, DeleteOne

            processed_ops = []
            for op in operations:
                op_type = op.get("type")
                if op_type == "insert":
                    processed_ops.append(InsertOne(op["document"]))
                elif op_type == "update":
                    processed_ops.append(UpdateOne(
                        op["query"],
                        op["update"],
                        upsert=op.get("upsert", False)
                    ))
                elif op_type == "delete":
                    processed_ops.append(DeleteOne(op["query"]))

            result = self.db[collection].bulk_write(processed_ops)

            return {
                "inserted": result.inserted_count,
                "modified": result.modified_count,
                "deleted": result.deleted_count,
                "upserted": result.upserted_count
            }

        except PyMongoError as e:
            raise MongoAPIError(f"Bulk write failed: {e}")

    def distinct(self, collection: str, field: str,
                 query: Optional[dict] = None) -> list:
        """Get distinct values for a field.

        Args:
            collection: Collection name
            field: Field name
            query: Optional filter query

        Returns:
            List of distinct values
        """
        try:
            return self.db[collection].distinct(field, query or {})

        except PyMongoError as e:
            raise MongoAPIError(f"Distinct failed: {e}")

    def exists(self, collection: str, query: dict) -> bool:
        """Check if document exists matching query.

        Args:
            collection: Collection name
            query: Filter query

        Returns:
            True if at least one document matches
        """
        return self.count(collection, query) > 0

    def _serialize_document(self, doc: Optional[dict]) -> Optional[dict]:
        """Convert MongoDB document to JSON-serializable dict.

        Args:
            doc: MongoDB document

        Returns:
            Serialized document with ObjectId as strings
        """
        if doc is None:
            return None

        result = {}
        for key, value in doc.items():
            if key == "_id":
                result["_id"] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = self._serialize_document(value)
            elif isinstance(value, list):
                result[key] = [
                    self._serialize_document(v) if isinstance(v, dict) else str(v) if hasattr(v, "__str__") else v
                    for v in value
                ]
            else:
                result[key] = value

        return result
