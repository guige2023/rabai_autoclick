"""MongoDB action module for RabAI AutoClick.

Provides MongoDB document database operations for CRUD,
aggregation pipelines, indexing, and collection management.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MongoDBConfig:
    """MongoDB connection configuration."""
    host: str = "localhost"
    port: int = 27017
    username: str = ""
    password: str = ""
    database: str = ""
    auth_source: str = "admin"
    direct_connection: bool = True
    timeout: int = 30000


class MongoDBConnection:
    """Manages MongoDB connection lifecycle."""
    
    def __init__(self, config: MongoDBConfig):
        self.config = config
        self._client = None
        self._db = None
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> tuple:
        """Establish MongoDB connection."""
        try:
            try:
                from pymongo import MongoClient
                from pymongo.errors import ConnectionFailure, OperationFailure
            except ImportError:
                return False, "pymongo not installed. Install with: pip install pymongo"
            
            try:
                if self.config.username and self.config.password:
                    uri = (f"mongodb://{self.config.username}:{self.config.password}"
                          f"@{self.config.host}:{self.config.port}/"
                          f"?authSource={self.config.auth_source}")
                else:
                    uri = f"mongodb://{self.config.host}:{self.config.port}/"
                
                self._client = MongoClient(
                    uri,
                    directConnection=self.config.direct_connection,
                    serverSelectionTimeoutMS=self.config.timeout
                )
                
                self._client.admin.command("ping")
                self._db = self._client[self.config.database] if self.config.database else None
                self._connected = True
                return True, "Connected"
                
            except ConnectionFailure as e:
                return False, f"Connection failed: {str(e)}"
            except OperationFailure as e:
                return False, f"Authentication failed: {str(e)}"
                
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        self._connected = False
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._db = None
    
    def find(
        self,
        collection: str,
        query: Dict = None,
        projection: Dict = None,
        limit: int = 0,
        skip: int = 0,
        sort: List = None
    ) -> tuple:
        """Find documents in collection."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            coll = self._db[collection]
            cursor = coll.find(query or {}, projection or {})
            
            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            
            results = list(cursor)
            return True, results, ""
        except Exception as e:
            return False, [], str(e)
    
    def find_one(
        self,
        collection: str,
        query: Dict = None,
        projection: Dict = None
    ) -> tuple:
        """Find single document."""
        if not self._connected:
            return False, None, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.find_one(query or {}, projection or {})
            return True, result, ""
        except Exception as e:
            return False, None, str(e)
    
    def insert_one(self, collection: str, document: Dict) -> tuple:
        """Insert single document."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.insert_one(document)
            return True, {
                "inserted_id": str(result.inserted_id),
                "acknowledged": result.acknowledged
            }
        except Exception as e:
            return False, str(e)
    
    def insert_many(self, collection: str, documents: List[Dict]) -> tuple:
        """Insert multiple documents."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.insert_many(documents)
            return True, {
                "inserted_ids": [str(id) for id in result.inserted_ids],
                "count": len(result.inserted_ids)
            }
        except Exception as e:
            return False, str(e)
    
    def update_one(
        self,
        collection: str,
        query: Dict,
        update: Dict,
        upsert: bool = False
    ) -> tuple:
        """Update single document."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.update_one(query, {"$set": update}, upsert=upsert)
            return True, {
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except Exception as e:
            return False, str(e)
    
    def update_many(
        self,
        collection: str,
        query: Dict,
        update: Dict,
        upsert: bool = False
    ) -> tuple:
        """Update multiple documents."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.update_many(query, {"$set": update}, upsert=upsert)
            return True, {
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except Exception as e:
            return False, str(e)
    
    def delete_one(self, collection: str, query: Dict) -> tuple:
        """Delete single document."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.delete_one(query)
            return True, {"deleted": result.deleted_count}
        except Exception as e:
            return False, str(e)
    
    def delete_many(self, collection: str, query: Dict) -> tuple:
        """Delete multiple documents."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.delete_many(query)
            return True, {"deleted": result.deleted_count}
        except Exception as e:
            return False, str(e)
    
    def aggregate(self, collection: str, pipeline: List[Dict]) -> tuple:
        """Run aggregation pipeline."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            coll = self._db[collection]
            cursor = coll.aggregate(pipeline)
            results = list(cursor)
            return True, results, ""
        except Exception as e:
            return False, [], str(e)
    
    def count(self, collection: str, query: Dict = None) -> tuple:
        """Count documents in collection."""
        if not self._connected:
            return False, 0, "Not connected"
        
        try:
            coll = self._db[collection]
            count = coll.count_documents(query or {})
            return True, count, ""
        except Exception as e:
            return False, 0, str(e)
    
    def create_index(self, collection: str, keys: List, unique: bool = False) -> tuple:
        """Create index on collection."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            coll = self._db[collection]
            result = coll.create_index(keys, unique=unique)
            return True, result
        except Exception as e:
            return False, str(e)
    
    def list_collections(self) -> tuple:
        """List all collections in database."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            collections = self._db.list_collection_names()
            return True, collections, ""
        except Exception as e:
            return False, [], str(e)


class MongoDBAction(BaseAction):
    """Action for MongoDB operations.
    
    Features:
        - Connect to MongoDB servers
        - CRUD operations (Create, Read, Update, Delete)
        - Aggregation pipelines
        - Index management
        - Collection operations
        - Bulk operations
    
    Note: Requires pymongo library.
    Install with: pip install pymongo
    """
    
    def __init__(self, config: Optional[MongoDBConfig] = None):
        """Initialize MongoDB action.
        
        Args:
            config: MongoDB configuration.
        """
        super().__init__()
        self.config = config or MongoDBConfig()
        self._connection: Optional[MongoDBConnection] = None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute MongoDB operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, find, find_one,
                           insert, insert_many, update, update_many, delete, delete_many,
                           aggregate, count, create_index, list_collections)
                - collection: Collection name
                - query: Query filter
                - document: Document data
                - pipeline: Aggregation pipeline
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "find":
                return self._find(params)
            elif operation == "find_one":
                return self._find_one(params)
            elif operation == "insert":
                return self._insert_one(params)
            elif operation == "insert_many":
                return self._insert_many(params)
            elif operation == "update":
                return self._update_one(params)
            elif operation == "update_many":
                return self._update_many(params)
            elif operation == "delete":
                return self._delete_one(params)
            elif operation == "delete_many":
                return self._delete_many(params)
            elif operation == "aggregate":
                return self._aggregate(params)
            elif operation == "count":
                return self._count(params)
            elif operation == "create_index":
                return self._create_index(params)
            elif operation == "list_collections":
                return self._list_collections(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"MongoDB operation failed: {str(e)}")
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish MongoDB connection."""
        config = MongoDBConfig(
            host=params.get("host", self.config.host),
            port=params.get("port", self.config.port),
            username=params.get("username", self.config.username),
            password=params.get("password", self.config.password),
            database=params.get("database", self.config.database),
            auth_source=params.get("auth_source", self.config.auth_source),
            timeout=params.get("timeout", self.config.timeout)
        )
        
        self._connection = MongoDBConnection(config)
        success, message = self._connection.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to MongoDB at {config.host}:{config.port}",
                data={
                    "host": config.host,
                    "port": config.port,
                    "database": config.database,
                    "connected": True
                }
            )
        else:
            self._connection = None
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close MongoDB connection."""
        if not self._connection:
            return ActionResult(success=True, message="No active connection")
        
        self._connection.disconnect()
        self._connection = None
        
        return ActionResult(success=True, message="Disconnected")
    
    def _find(self, params: Dict[str, Any]) -> ActionResult:
        """Find documents in collection."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        if not collection:
            return ActionResult(success=False, message="collection is required")
        
        query = params.get("query")
        projection = params.get("projection")
        limit = params.get("limit", 0)
        skip = params.get("skip", 0)
        sort = params.get("sort")
        
        success, results, error = self._connection.find(
            collection, query, projection, limit, skip, sort
        )
        
        if success:
            return ActionResult(
                success=True,
                message=f"Found {len(results)} documents",
                data={"documents": results, "count": len(results)}
            )
        else:
            return ActionResult(success=False, message=f"Find error: {error}")
    
    def _find_one(self, params: Dict[str, Any]) -> ActionResult:
        """Find single document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        if not collection:
            return ActionResult(success=False, message="collection is required")
        
        query = params.get("query")
        projection = params.get("projection")
        
        success, result, error = self._connection.find_one(collection, query, projection)
        
        if success:
            if result:
                return ActionResult(
                    success=True,
                    message="Document found",
                    data={"document": result}
                )
            else:
                return ActionResult(
                    success=True,
                    message="No document found",
                    data={"document": None}
                )
        else:
            return ActionResult(success=False, message=f"Find one error: {error}")
    
    def _insert_one(self, params: Dict[str, Any]) -> ActionResult:
        """Insert single document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        document = params.get("document", {})
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not document:
            return ActionResult(success=False, message="document is required")
        
        success, result = self._connection.insert_one(collection, document)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Inserted document: {result.get('inserted_id')}",
                data=result
            )
        else:
            return ActionResult(success=False, message=f"Insert error: {result}")
    
    def _insert_many(self, params: Dict[str, Any]) -> ActionResult:
        """Insert multiple documents."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        documents = params.get("documents", [])
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not documents:
            return ActionResult(success=False, message="documents list is required")
        
        success, result = self._connection.insert_many(collection, documents)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Inserted {result.get('count', 0)} documents",
                data=result
            )
        else:
            return ActionResult(success=False, message=f"Insert many error: {result}")
    
    def _update_one(self, params: Dict[str, Any]) -> ActionResult:
        """Update single document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        query = params.get("query", {})
        update = params.get("update", {})
        upsert = params.get("upsert", False)
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not update:
            return ActionResult(success=False, message="update data is required")
        
        success, result = self._connection.update_one(collection, query, update, upsert)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Updated {result.get('modified', 0)} document(s)",
                data=result
            )
        else:
            return ActionResult(success=False, message=f"Update error: {result}")
    
    def _update_many(self, params: Dict[str, Any]) -> ActionResult:
        """Update multiple documents."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        query = params.get("query", {})
        update = params.get("update", {})
        upsert = params.get("upsert", False)
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not update:
            return ActionResult(success=False, message="update data is required")
        
        success, result = self._connection.update_many(collection, query, update, upsert)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Updated {result.get('modified', 0)} document(s)",
                data=result
            )
        else:
            return ActionResult(success=False, message=f"Update many error: {result}")
    
    def _delete_one(self, params: Dict[str, Any]) -> ActionResult:
        """Delete single document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        query = params.get("query", {})
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        
        success, result = self._connection.delete_one(collection, query)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Deleted {result.get('deleted', 0)} document(s)",
                data=result
            )
        else:
            return ActionResult(success=False, message=f"Delete error: {result}")
    
    def _delete_many(self, params: Dict[str, Any]) -> ActionResult:
        """Delete multiple documents."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        query = params.get("query", {})
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        
        success, result = self._connection.delete_many(collection, query)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Deleted {result.get('deleted', 0)} document(s)",
                data=result
            )
        else:
            return ActionResult(success=False, message=f"Delete many error: {result}")
    
    def _aggregate(self, params: Dict[str, Any]) -> ActionResult:
        """Run aggregation pipeline."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        pipeline = params.get("pipeline", [])
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not pipeline:
            return ActionResult(success=False, message="pipeline is required")
        
        success, results, error = self._connection.aggregate(collection, pipeline)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Aggregation returned {len(results)} results",
                data={"results": results, "count": len(results)}
            )
        else:
            return ActionResult(success=False, message=f"Aggregation error: {error}")
    
    def _count(self, params: Dict[str, Any]) -> ActionResult:
        """Count documents."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        query = params.get("query")
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        
        success, count, error = self._connection.count(collection, query)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Count: {count} documents",
                data={"count": count}
            )
        else:
            return ActionResult(success=False, message=f"Count error: {error}")
    
    def _create_index(self, params: Dict[str, Any]) -> ActionResult:
        """Create index."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        collection = params.get("collection", "")
        keys = params.get("keys", [])
        unique = params.get("unique", False)
        
        if not collection:
            return ActionResult(success=False, message="collection is required")
        if not keys:
            return ActionResult(success=False, message="keys is required")
        
        success, result = self._connection.create_index(collection, keys, unique)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Index created: {result}",
                data={"index": result}
            )
        else:
            return ActionResult(success=False, message=f"Create index error: {result}")
    
    def _list_collections(self, params: Dict[str, Any]) -> ActionResult:
        """List collections."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        success, collections, error = self._connection.list_collections()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Found {len(collections)} collections",
                data={"collections": collections, "count": len(collections)}
            )
        else:
            return ActionResult(success=False, message=f"List collections error: {error}")
