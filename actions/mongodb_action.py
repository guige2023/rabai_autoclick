"""MongoDB action module for RabAI AutoClick.

Provides MongoDB operations including collection manipulation,
CRUD operations, indexing, and aggregation pipelines.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MongoDBClient:
    """MongoDB client wrapper with connection and operation management.
    
    Provides methods for common MongoDB operations.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        database: str = "test",
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_database: str = "admin",
        replica_set: Optional[str] = None,
        server_selection_timeout_ms: int = 5000
    ) -> None:
        """Initialize MongoDB client.
        
        Args:
            host: MongoDB server hostname.
            port: MongoDB server port.
            database: Default database name.
            username: Optional authentication username.
            password: Optional authentication password.
            auth_database: Authentication database.
            replica_set: Optional replica set name.
            server_selection_timeout_ms: Server selection timeout.
        """
        self.host = host
        self.port = port
        self.database_name = database
        self.username = username
        self.password = password
        self.auth_database = auth_database
        self.replica_set = replica_set
        self.server_selection_timeout_ms = server_selection_timeout_ms
        self._client: Optional[Any] = None
        self._db: Optional[Any] = None
    
    def connect(self) -> bool:
        """Establish connection to MongoDB server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ImportError(
                "pymongo is required for MongoDB support. Install with: pip install pymongo"
            )
        
        try:
            if self.username and self.password:
                uri = (
                    f"mongodb://{self.username}:{self.password}@"
                    f"{self.host}:{self.port}/{self.database_name}"
                    f"?authSource={self.auth_database}"
                )
            else:
                uri = f"mongodb://{self.host}:{self.port}/{self.database_name}"
            
            if self.replica_set:
                uri += f"?replicaSet={self.replica_set}"
            
            self._client = MongoClient(
                uri,
                serverSelectionTimeoutMS=self.server_selection_timeout_ms
            )
            
            self._client.admin.command("ping")
            
            self._db = self._client[self.database_name]
            
            return True
        
        except Exception:
            self._client = None
            self._db = None
            return False
    
    def disconnect(self) -> bool:
        """Close the MongoDB connection.
        
        Returns:
            True if disconnection successful.
        """
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
            self._db = None
        return True
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        if not self._client:
            return False
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            self._client = None
            self._db = None
            return False
    
    def _require_db(self) -> Any:
        """Ensure an active connection and database exist."""
        if not self._db:
            raise RuntimeError("Not connected to MongoDB")
        return self._db
    
    def list_databases(self) -> List[str]:
        """List all databases on the server.
        
        Returns:
            List of database names.
        """
        if not self._client:
            raise RuntimeError("Not connected to MongoDB")
        
        return self._client.list_database_names()
    
    def list_collections(self, database: Optional[str] = None) -> List[str]:
        """List all collections in a database.
        
        Args:
            database: Optional database name (uses default if not provided).
            
        Returns:
            List of collection names.
        """
        if database:
            db = self._client[database]
        else:
            db = self._require_db()
        
        return db.list_collection_names()
    
    def create_collection(
        self,
        name: str,
        capped: bool = False,
        size: Optional[int] = None,
        max: Optional[int] = None,
        database: Optional[str] = None
    ) -> Any:
        """Create a new collection.
        
        Args:
            name: Collection name.
            capped: Whether to create a capped collection.
            size: Maximum size in bytes for capped collection.
            max: Maximum number of documents for capped collection.
            database: Optional database name.
            
        Returns:
            Created collection object.
        """
        db = self._client[database] if database else self._require_db()
        
        kwargs: Dict[str, Any] = {}
        if capped:
            kwargs["capped"] = True
            if size:
                kwargs["size"] = size
            if max:
                kwargs["max"] = max
        
        return db.create_collection(name, **kwargs)
    
    def drop_collection(
        self,
        name: str,
        database: Optional[str] = None
    ) -> bool:
        """Drop a collection.
        
        Args:
            name: Collection name.
            database: Optional database name.
            
        Returns:
            True if dropped successfully.
        """
        db = self._client[database] if database else self._require_db()
        db[name].drop()
        return True
    
    def insert_one(
        self,
        collection: str,
        document: Dict[str, Any],
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Insert a single document.
        
        Args:
            collection: Collection name.
            document: Document to insert.
            database: Optional database name.
            
        Returns:
            Insert result with inserted_id.
        """
        db = self._client[database] if database else self._require_db()
        result = db[collection].insert_one(document)
        
        return {
            "inserted_id": str(result.inserted_id),
            "acknowledged": result.acknowledged
        }
    
    def insert_many(
        self,
        collection: str,
        documents: List[Dict[str, Any]],
        ordered: bool = False,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Insert multiple documents.
        
        Args:
            collection: Collection name.
            documents: List of documents to insert.
            ordered: Whether to insert in order (stop on error if True).
            database: Optional database name.
            
        Returns:
            Insert result with inserted_ids.
        """
        db = self._client[database] if database else self._require_db()
        result = db[collection].insert_many(documents, ordered=ordered)
        
        return {
            "inserted_ids": [str(id) for id in result.inserted_ids],
            "acknowledged": result.acknowledged
        }
    
    def find_one(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Find a single document.
        
        Args:
            collection: Collection name.
            query: Query filter.
            projection: Optional field projection.
            database: Optional database name.
            
        Returns:
            Found document or None.
        """
        db = self._client[database] if database else self._require_db()
        return db[collection].find_one(query, projection)
    
    def find_many(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: int = 0,
        skip: int = 0,
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Find multiple documents.
        
        Args:
            collection: Collection name.
            query: Query filter.
            projection: Optional field projection.
            sort: Optional sort specification.
            limit: Maximum number of documents to return.
            skip: Number of documents to skip.
            database: Optional database name.
            
        Returns:
            List of found documents.
        """
        db = self._client[database] if database else self._require_db()
        cursor = db[collection].find(query, projection)
        
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def count(
        self,
        collection: str,
        query: Dict[str, Any] = None,
        database: Optional[str] = None
    ) -> int:
        """Count documents matching a query.
        
        Args:
            collection: Collection name.
            query: Optional query filter.
            database: Optional database name.
            
        Returns:
            Count of matching documents.
        """
        db = self._client[database] if database else self._require_db()
        return db[collection].count_documents(query or {})
    
    def update_one(
        self,
        collection: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update a single document.
        
        Args:
            collection: Collection name.
            query: Query filter.
            update: Update operation ($set, $inc, etc.).
            upsert: Create document if not found.
            database: Optional database name.
            
        Returns:
            Update result.
        """
        db = self._client[database] if database else self._require_db()
        result = db[collection].update_one(query, update, upsert=upsert)
        
        return {
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted_id": str(result.upserted_id) if result.upserted_id else None
        }
    
    def update_many(
        self,
        collection: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update multiple documents.
        
        Args:
            collection: Collection name.
            query: Query filter.
            update: Update operation.
            upsert: Create document if not found.
            database: Optional database name.
            
        Returns:
            Update result.
        """
        db = self._client[database] if database else self._require_db()
        result = db[collection].update_many(query, update, upsert=upsert)
        
        return {
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted_id": str(result.upserted_id) if result.upserted_id else None
        }
    
    def delete_one(
        self,
        collection: str,
        query: Dict[str, Any],
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Delete a single document.
        
        Args:
            collection: Collection name.
            query: Query filter.
            database: Optional database name.
            
        Returns:
            Delete result.
        """
        db = self._client[database] if database else self._require_db()
        result = db[collection].delete_one(query)
        
        return {"deleted": result.deleted_count}
    
    def delete_many(
        self,
        collection: str,
        query: Dict[str, Any],
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Delete multiple documents.
        
        Args:
            collection: Collection name.
            query: Query filter.
            database: Optional database name.
            
        Returns:
            Delete result.
        """
        db = self._client[database] if database else self._require_db()
        result = db[collection].delete_many(query)
        
        return {"deleted": result.deleted_count}
    
    def aggregate(
        self,
        collection: str,
        pipeline: List[Dict[str, Any]],
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Execute an aggregation pipeline.
        
        Args:
            collection: Collection name.
            pipeline: Aggregation pipeline.
            database: Optional database name.
            
        Returns:
            List of aggregated documents.
        """
        db = self._client[database] if database else self._require_db()
        cursor = db[collection].aggregate(pipeline)
        return list(cursor)
    
    def create_index(
        self,
        collection: str,
        keys: List[Tuple[str, int]],
        unique: bool = False,
        name: Optional[str] = None,
        database: Optional[str] = None
    ) -> str:
        """Create an index on a collection.
        
        Args:
            collection: Collection name.
            keys: List of (field, direction) tuples.
            unique: Whether to create a unique index.
            name: Optional index name.
            database: Optional database name.
            
        Returns:
            Name of created index.
        """
        db = self._client[database] if database else self._require_db()
        return db[collection].create_index(keys, unique=unique, name=name)
    
    def list_indexes(
        self,
        collection: str,
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all indexes on a collection.
        
        Args:
            collection: Collection name.
            database: Optional database name.
            
        Returns:
            List of index specifications.
        """
        db = self._client[database] if database else self._require_db()
        return list(db[collection].list_indexes())
    
    def drop_index(
        self,
        collection: str,
        index_name: str,
        database: Optional[str] = None
    ) -> bool:
        """Drop an index from a collection.
        
        Args:
            collection: Collection name.
            index_name: Name of the index to drop.
            database: Optional database name.
            
        Returns:
            True if dropped successfully.
        """
        db = self._client[database] if database else self._require_db()
        db[collection].drop_index(index_name)
        return True


class MongoDBAction(BaseAction):
    """MongoDB action for document database operations.
    
    Supports CRUD operations, aggregation, and indexing.
    """
    action_type: str = "mongodb"
    display_name: str = "MongoDB动作"
    description: str = "MongoDB文档数据库操作，支持CRUD、聚合和索引"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[MongoDBClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute MongoDB operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "list_databases":
                return self._list_databases(start_time)
            elif operation == "list_collections":
                return self._list_collections(params, start_time)
            elif operation == "insert_one":
                return self._insert_one(params, start_time)
            elif operation == "insert_many":
                return self._insert_many(params, start_time)
            elif operation == "find_one":
                return self._find_one(params, start_time)
            elif operation == "find_many":
                return self._find_many(params, start_time)
            elif operation == "count":
                return self._count(params, start_time)
            elif operation == "update_one":
                return self._update_one(params, start_time)
            elif operation == "update_many":
                return self._update_many(params, start_time)
            elif operation == "delete_one":
                return self._delete_one(params, start_time)
            elif operation == "delete_many":
                return self._delete_many(params, start_time)
            elif operation == "aggregate":
                return self._aggregate(params, start_time)
            elif operation == "create_index":
                return self._create_index(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to MongoDB server."""
        host = params.get("host", "localhost")
        port = params.get("port", 27017)
        database = params.get("database", "test")
        username = params.get("username")
        password = params.get("password")
        
        self._client = MongoDBClient(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password
        )
        
        success = self._client.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to MongoDB: {database}",
                data={"host": host, "port": port, "database": database},
                duration=time.time() - start_time
            )
        else:
            self._client = None
            return ActionResult(
                success=False,
                message=f"Failed to connect to MongoDB at {host}:{port}",
                duration=time.time() - start_time
            )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from MongoDB server."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from MongoDB",
            duration=time.time() - start_time
        )
    
    def _require_client(self) -> MongoDBClient:
        """Ensure a MongoDB client exists."""
        if not self._client:
            raise RuntimeError("Not connected to MongoDB. Use 'connect' operation first.")
        return self._client
    
    def _list_databases(self, start_time: float) -> ActionResult:
        """List all databases."""
        client = self._require_client()
        
        try:
            databases = client.list_databases()
            
            return ActionResult(
                success=True,
                message=f"Found {len(databases)} databases",
                data={"databases": databases, "count": len(databases)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list databases: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_collections(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all collections."""
        client = self._require_client()
        database = params.get("database")
        
        try:
            collections = client.list_collections(database=database)
            
            return ActionResult(
                success=True,
                message=f"Found {len(collections)} collections",
                data={"collections": collections, "count": len(collections)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list collections: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _insert_one(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Insert a single document."""
        client = self._require_client()
        collection = params.get("collection", "")
        document = params.get("document", {})
        
        if not collection or not document:
            return ActionResult(
                success=False,
                message="collection and document are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.insert_one(
                collection=collection,
                document=document,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Inserted document: {result['inserted_id']}",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Insert failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _insert_many(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Insert multiple documents."""
        client = self._require_client()
        collection = params.get("collection", "")
        documents = params.get("documents", [])
        ordered = params.get("ordered", False)
        
        if not collection or not documents:
            return ActionResult(
                success=False,
                message="collection and documents are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.insert_many(
                collection=collection,
                documents=documents,
                ordered=ordered,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Inserted {len(result['inserted_ids'])} documents",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Insert many failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _find_one(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find a single document."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        projection = params.get("projection")
        
        if not collection:
            return ActionResult(
                success=False,
                message="collection is required",
                duration=time.time() - start_time
            )
        
        try:
            document = client.find_one(
                collection=collection,
                query=query,
                projection=projection,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message="Document found" if document else "Document not found",
                data={"document": document, "found": document is not None},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Find failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _find_many(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find multiple documents."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        projection = params.get("projection")
        sort = params.get("sort")
        limit = params.get("limit", 0)
        skip = params.get("skip", 0)
        
        if not collection:
            return ActionResult(
                success=False,
                message="collection is required",
                duration=time.time() - start_time
            )
        
        try:
            documents = client.find_many(
                collection=collection,
                query=query,
                projection=projection,
                sort=sort,
                limit=limit,
                skip=skip,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Found {len(documents)} documents",
                data={"documents": documents, "count": len(documents)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Find many failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _count(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Count documents."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        
        if not collection:
            return ActionResult(
                success=False,
                message="collection is required",
                duration=time.time() - start_time
            )
        
        try:
            count = client.count(
                collection=collection,
                query=query,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Count: {count}",
                data={"count": count},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Count failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _update_one(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update a single document."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        update = params.get("update", {})
        upsert = params.get("upsert", False)
        
        if not collection or not query or not update:
            return ActionResult(
                success=False,
                message="collection, query, and update are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.update_one(
                collection=collection,
                query=query,
                update=update,
                upsert=upsert,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Matched {result['matched']}, modified {result['modified']}",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Update failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _update_many(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update multiple documents."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        update = params.get("update", {})
        upsert = params.get("upsert", False)
        
        if not collection or not query or not update:
            return ActionResult(
                success=False,
                message="collection, query, and update are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.update_many(
                collection=collection,
                query=query,
                update=update,
                upsert=upsert,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Matched {result['matched']}, modified {result['modified']}",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Update many failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _delete_one(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a single document."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        
        if not collection or not query:
            return ActionResult(
                success=False,
                message="collection and query are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.delete_one(
                collection=collection,
                query=query,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Deleted {result['deleted']} document(s)",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delete failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _delete_many(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete multiple documents."""
        client = self._require_client()
        collection = params.get("collection", "")
        query = params.get("query", {})
        
        if not collection or not query:
            return ActionResult(
                success=False,
                message="collection and query are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.delete_many(
                collection=collection,
                query=query,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Deleted {result['deleted']} document(s)",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delete many failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _aggregate(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute aggregation pipeline."""
        client = self._require_client()
        collection = params.get("collection", "")
        pipeline = params.get("pipeline", [])
        
        if not collection or not pipeline:
            return ActionResult(
                success=False,
                message="collection and pipeline are required",
                duration=time.time() - start_time
            )
        
        try:
            results = client.aggregate(
                collection=collection,
                pipeline=pipeline,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Aggregation returned {len(results)} results",
                data={"results": results, "count": len(results)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Aggregation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_index(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an index."""
        client = self._require_client()
        collection = params.get("collection", "")
        keys = params.get("keys", [])
        unique = params.get("unique", False)
        name = params.get("name")
        
        if not collection or not keys:
            return ActionResult(
                success=False,
                message="collection and keys are required",
                duration=time.time() - start_time
            )
        
        try:
            index_name = client.create_index(
                collection=collection,
                keys=keys,
                unique=unique,
                name=name,
                database=params.get("database")
            )
            
            return ActionResult(
                success=True,
                message=f"Created index: {index_name}",
                data={"index_name": index_name},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Create index failed: {str(e)}",
                duration=time.time() - start_time
            )
