"""Elasticsearch action module for RabAI AutoClick.

Provides Elasticsearch operations for indexing, searching,
and managing documents in Elasticsearch clusters.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ElasticsearchConfig:
    """Elasticsearch connection configuration."""
    hosts: List[str] = field(default_factory=lambda: ["http://localhost:9200"])
    username: str = ""
    password: str = ""
    api_key: str = ""
    timeout: float = 30.0
    max_retries: int = 3
    retry_on_timeout: bool = True
    index: str = ""


class ElasticsearchConnection:
    """Manages Elasticsearch connection lifecycle."""
    
    def __init__(self, config: ElasticsearchConfig):
        self.config = config
        self._client = None
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> tuple:
        """Establish Elasticsearch connection."""
        try:
            try:
                from elasticsearch import Elasticsearch
                
                es_config = {
                    "hosts": self.config.hosts,
                    "timeout": self.config.timeout,
                    "max_retries": self.config.max_retries,
                    "retry_on_timeout": self.config.retry_on_timeout
                }
                
                if self.config.username and self.config.password:
                    es_config["basic_auth"] = (self.config.username, self.config.password)
                elif self.config.api_key:
                    es_config["api_key"] = self.config.api_key
                
                self._client = Elasticsearch(**es_config)
                
                if self._client.ping():
                    self._connected = True
                    return True, "Connected"
                else:
                    return False, "Ping failed"
                    
            except ImportError:
                return False, "elasticsearch not installed. Install with: pip install elasticsearch"
                
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect(self) -> None:
        """Close Elasticsearch connection."""
        self._connected = False
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
    
    def create_index(self, index_name: str, mappings: Optional[Dict] = None,
                    settings: Optional[Dict] = None) -> tuple:
        """Create an index with mappings and settings."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            body = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings
            
            if body:
                self._client.indices.create(index=index_name, body=body)
            else:
                self._client.indices.create(index=index_name)
            
            return True, f"Index {index_name} created"
        except Exception as e:
            return False, f"Create index error: {str(e)}"
    
    def delete_index(self, index_name: str) -> tuple:
        """Delete an index."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._client.indices.delete(index=index_name)
            return True, f"Index {index_name} deleted"
        except Exception as e:
            return False, f"Delete index error: {str(e)}"
    
    def index_document(self, index_name: str, doc_id: Optional[str],
                      document: Dict, refresh: bool = False) -> tuple:
        """Index a single document."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            if doc_id:
                result = self._client.index(
                    index=index_name,
                    id=doc_id,
                    document=document,
                    refresh=refresh
                )
            else:
                result = self._client.index(
                    index=index_name,
                    document=document,
                    refresh=refresh
                )
            
            return True, {
                "_id": result.get("_id"),
                "_index": result.get("_index"),
                "result": result.get("result")
            }
        except Exception as e:
            return False, f"Index document error: {str(e)}"
    
    def bulk_index(self, index_name: str, documents: List[Dict],
                  id_field: Optional[str] = None) -> tuple:
        """Bulk index multiple documents."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            from elasticsearch.helpers import bulk
            
            actions = []
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                actions.append(action)
            
            success, failed = bulk(self._client, actions)
            
            return True, {"indexed": success, "failed": len(failed) if failed else 0}
        except Exception as e:
            return False, f"Bulk index error: {str(e)}"
    
    def search(self, index_name: str, query: Dict, size: int = 10,
              from_: int = 0, sort: Optional[List] = None) -> tuple:
        """Search for documents."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            body = {"query": query, "size": size, "from": from_}
            if sort:
                body["sort"] = sort
            
            result = self._client.search(index=index_name, body=body)
            
            hits = []
            for hit in result["hits"]["hits"]:
                hits.append({
                    "_id": hit["_id"],
                    "_score": hit.get("_score"),
                    "_source": hit["_source"]
                })
            
            total = result["hits"]["total"]
            if isinstance(total, dict):
                total = total.get("value", 0)
            
            return True, hits, {
                "total": total,
                "max_score": result["hits"]["max_score"]
            }
        except Exception as e:
            return False, [], f"Search error: {str(e)}"
    
    def get_document(self, index_name: str, doc_id: str) -> tuple:
        """Get a single document by ID."""
        if not self._connected:
            return False, None, "Not connected"
        
        try:
            result = self._client.get(index=index_name, id=doc_id)
            return True, {
                "_id": result["_id"],
                "_source": result["_source"]
            }, None
        except Exception as e:
            return False, None, f"Get document error: {str(e)}"
    
    def delete_document(self, index_name: str, doc_id: str,
                       refresh: bool = False) -> tuple:
        """Delete a document by ID."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._client.delete(index=index_name, id=doc_id, refresh=refresh)
            return True, f"Document {doc_id} deleted"
        except Exception as e:
            return False, f"Delete document error: {str(e)}"
    
    def update_document(self, index_name: str, doc_id: str,
                       doc: Dict, upsert: bool = False) -> tuple:
        """Update a document."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            body = {"doc": doc}
            if upsert:
                body["doc_as_upsert"] = True
            
            result = self._client.update(index=index_name, id=doc_id, body=body)
            return True, {
                "_id": result.get("_id"),
                "result": result.get("result")
            }
        except Exception as e:
            return False, f"Update document error: {str(e)}"
    
    def refresh_index(self, index_name: str) -> tuple:
        """Refresh an index."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._client.indices.refresh(index=index_name)
            return True, f"Index {index_name} refreshed"
        except Exception as e:
            return False, f"Refresh error: {str(e)}"
    
    def get_index_stats(self, index_name: str) -> tuple:
        """Get index statistics."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            stats = self._client.indices.stats(index=index_name)
            return True, {
                "doc_count": stats["_all"]["primaries"]["docs"]["count"],
                "size_bytes": stats["_all"]["primaries"]["store"]["size_in_bytes"]
            }
        except Exception as e:
            return False, f"Stats error: {str(e)}"


class ElasticsearchAction(BaseAction):
    """Action for Elasticsearch operations.
    
    Features:
        - Connect to Elasticsearch clusters
        - Index management (create, delete)
        - Document CRUD operations
        - Full-text search
        - Bulk indexing
        - Aggregations
        - Mapping management
    
    Note: Requires elasticsearch library.
    Install with: pip install elasticsearch
    """
    
    def __init__(self, config: Optional[ElasticsearchConfig] = None):
        """Initialize Elasticsearch action.
        
        Args:
            config: Elasticsearch configuration.
        """
        super().__init__()
        self.config = config or ElasticsearchConfig()
        self._connection: Optional[ElasticsearchConnection] = None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute Elasticsearch operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, create_index,
                           delete_index, index, bulk_index, search, get, delete,
                           update, refresh, stats)
                - index: Index name
                - document: Document data
                - query: Search query
                - id: Document ID
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "create_index":
                return self._create_index(params)
            elif operation == "delete_index":
                return self._delete_index(params)
            elif operation == "index":
                return self._index(params)
            elif operation == "bulk_index":
                return self._bulk_index(params)
            elif operation == "search":
                return self._search(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "update":
                return self._update(params)
            elif operation == "refresh":
                return self._refresh(params)
            elif operation == "stats":
                return self._stats(params)
            elif operation == "list_indices":
                return self._list_indices(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"Elasticsearch operation failed: {str(e)}")
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish Elasticsearch connection."""
        hosts = params.get("hosts", self.config.hosts)
        if isinstance(hosts, str):
            hosts = [hosts]
        
        config = ElasticsearchConfig(
            hosts=hosts,
            username=params.get("username", self.config.username),
            password=params.get("password", self.config.password),
            api_key=params.get("api_key", self.config.api_key),
            timeout=params.get("timeout", self.config.timeout),
            index=params.get("index", self.config.index)
        )
        
        self._connection = ElasticsearchConnection(config)
        success, message = self._connection.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to Elasticsearch at {hosts[0]}",
                data={"hosts": hosts, "connected": True}
            )
        else:
            self._connection = None
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close Elasticsearch connection."""
        if not self._connection:
            return ActionResult(success=True, message="No active connection")
        
        self._connection.disconnect()
        self._connection = None
        
        return ActionResult(success=True, message="Disconnected")
    
    def _create_index(self, params: Dict[str, Any]) -> ActionResult:
        """Create an index."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index name required")
        
        mappings = params.get("mappings")
        settings = params.get("settings")
        
        success, message = self._connection.create_index(index, mappings, settings)
        
        if success:
            return ActionResult(success=True, message=message, data={"index": index})
        else:
            return ActionResult(success=False, message=message)
    
    def _delete_index(self, params: Dict[str, Any]) -> ActionResult:
        """Delete an index."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index name required")
        
        success, message = self._connection.delete_index(index)
        
        if success:
            return ActionResult(success=True, message=message)
        else:
            return ActionResult(success=False, message=message)
    
    def _index(self, params: Dict[str, Any]) -> ActionResult:
        """Index a document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        document = params.get("document", {})
        doc_id = params.get("id")
        refresh = params.get("refresh", False)
        
        if not index:
            return ActionResult(success=False, message="index is required")
        if not document:
            return ActionResult(success=False, message="document is required")
        
        success, result = self._connection.index_document(index, doc_id, document, refresh)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Document indexed: {result.get('_id')}",
                data=result
            )
        else:
            return ActionResult(success=False, message=result)
    
    def _bulk_index(self, params: Dict[str, Any]) -> ActionResult:
        """Bulk index documents."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        documents = params.get("documents", [])
        id_field = params.get("id_field")
        
        if not index:
            return ActionResult(success=False, message="index is required")
        if not documents:
            return ActionResult(success=False, message="documents list is required")
        
        success, result = self._connection.bulk_index(index, documents, id_field)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Bulk indexed {result['indexed']} documents",
                data=result
            )
        else:
            return ActionResult(success=False, message=result)
    
    def _search(self, params: Dict[str, Any]) -> ActionResult:
        """Search for documents."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        query = params.get("query", {"match_all": {}})
        size = params.get("size", 10)
        from_ = params.get("from_", 0)
        sort = params.get("sort")
        
        if not index:
            return ActionResult(success=False, message="index is required")
        
        success, hits, meta = self._connection.search(index, query, size, from_, sort)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Search returned {len(hits)} hits (total: {meta['total']})",
                data={"hits": hits, "total": meta["total"], "max_score": meta.get("max_score")}
            )
        else:
            return ActionResult(success=False, message=meta)
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get a document by ID."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        doc_id = params.get("id", "")
        
        if not index or not doc_id:
            return ActionResult(success=False, message="index and id are required")
        
        success, result, error = self._connection.get_document(index, doc_id)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Document {doc_id} retrieved",
                data=result
            )
        else:
            return ActionResult(success=False, message=error)
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        doc_id = params.get("id", "")
        refresh = params.get("refresh", False)
        
        if not index or not doc_id:
            return ActionResult(success=False, message="index and id are required")
        
        success, message = self._connection.delete_document(index, doc_id, refresh)
        
        if success:
            return ActionResult(success=True, message=message)
        else:
            return ActionResult(success=False, message=message)
    
    def _update(self, params: Dict[str, Any]) -> ActionResult:
        """Update a document."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        doc_id = params.get("id", "")
        doc = params.get("doc", {})
        upsert = params.get("upsert", False)
        
        if not index or not doc_id:
            return ActionResult(success=False, message="index and id are required")
        
        success, result = self._connection.update_document(index, doc_id, doc, upsert)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Document {doc_id} updated",
                data=result
            )
        else:
            return ActionResult(success=False, message=result)
    
    def _refresh(self, params: Dict[str, Any]) -> ActionResult:
        """Refresh an index."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index is required")
        
        success, message = self._connection.refresh_index(index)
        
        if success:
            return ActionResult(success=True, message=message)
        else:
            return ActionResult(success=False, message=message)
    
    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get index statistics."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        index = params.get("index", self.config.index)
        if not index:
            return ActionResult(success=False, message="index is required")
        
        success, result = self._connection.get_index_stats(index)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Stats for {index}",
                data=result
            )
        else:
            return ActionResult(success=False, message=result)
    
    def _list_indices(self, params: Dict[str, Any]) -> ActionResult:
        """List all indices."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        try:
            result = self._connection._client.indices.list()
            indices = list(result.keys())
            
            return ActionResult(
                success=True,
                message=f"Found {len(indices)} indices",
                data={"indices": indices, "count": len(indices)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"List indices error: {str(e)}")
