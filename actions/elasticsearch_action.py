"""Elasticsearch action module for RabAI AutoClick.

Provides Elasticsearch operations including
index management, document CRUD, and search queries.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class SearchHit:
    """Represents a single search result hit.
    
    Attributes:
        index: Index name.
        id: Document ID.
        score: Relevance score.
        source: Document source data.
    """
    index: str
    id: str
    score: float
    source: Dict[str, Any]


class ElasticsearchClient:
    """Elasticsearch client for search and analytics operations.
    
    Provides methods for connecting to Elasticsearch,
    managing indices, and executing searches.
    """
    
    def __init__(
        self,
        hosts: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
        verify_certs: bool = True
    ) -> None:
        """Initialize Elasticsearch client.
        
        Args:
            hosts: List of host URLs.
            api_key: API key for authentication.
            username: Username for basic auth.
            password: Password for basic auth.
            timeout: Request timeout in seconds.
            verify_certs: Whether to verify SSL certificates.
        """
        self.hosts = hosts or ["http://localhost:9200"]
        self.api_key = api_key
        self.username = username
        self.password = password
        self.timeout = timeout
        self.verify_certs = verify_certs
        self._client: Optional[Any] = None
    
    def connect(self) -> bool:
        """Connect to Elasticsearch cluster.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            from elasticsearch import Elasticsearch
        except ImportError:
            raise ImportError(
                "elasticsearch is required. Install with: pip install elasticsearch"
            )
        
        try:
            kwargs: Dict[str, Any] = {
                "hosts": self.hosts,
                "request_timeout": self.timeout,
                "verify_certs": self.verify_certs
            }
            
            if self.api_key:
                kwargs["api_key"] = self.api_key
            elif self.username and self.password:
                kwargs["basic_auth"] = (self.username, self.password)
            
            self._client = Elasticsearch(**kwargs)
            
            return self._client.ping()
        
        except Exception:
            self._client = None
            return False
    
    def disconnect(self) -> None:
        """Close the Elasticsearch connection."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
    
    def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Create a new index.
        
        Args:
            index: Index name.
            mappings: Optional field mappings.
            settings: Optional index settings.
            
        Returns:
            True if created successfully.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            body: Dict[str, Any] = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings
            
            if body:
                self._client.indices.create(index=index, body=body)
            else:
                self._client.indices.create(index=index)
            
            return True
        
        except Exception as e:
            raise Exception(f"Create index failed: {str(e)}")
    
    def delete_index(self, index: str) -> bool:
        """Delete an index.
        
        Args:
            index: Index name.
            
        Returns:
            True if deleted successfully.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            self._client.indices.delete(index=index)
            return True
        except Exception:
            return False
    
    def list_indices(self, pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all indices.
        
        Args:
            pattern: Optional index pattern filter.
            
        Returns:
            List of index information.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            if pattern:
                indices = self._client.indices.get(index=pattern)
            else:
                indices = self._client.indices.get(index="_all")
            
            return [
                {
                    "name": name,
                    "aliases": list(info.get("aliases", {}).keys()),
                    "mappings": info.get("mappings", {}),
                    "settings": info.get("settings", {}).get("index", {})
                }
                for name, info in indices.items()
            ]
        
        except Exception as e:
            raise Exception(f"List indices failed: {str(e)}")
    
    def index_document(
        self,
        index: str,
        document: Dict[str, Any],
        id: Optional[str] = None,
        refresh: bool = False
    ) -> str:
        """Index a document.
        
        Args:
            index: Target index name.
            document: Document data.
            id: Optional document ID.
            refresh: Whether to refresh immediately.
            
        Returns:
            Document ID.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            kwargs: Dict[str, Any] = {
                "index": index,
                "document": document,
                "refresh": refresh
            }
            
            if id:
                kwargs["id"] = id
            
            result = self._client.index(**kwargs)
            return result.get("_id", "")
        
        except Exception as e:
            raise Exception(f"Index document failed: {str(e)}")
    
    def bulk_index(
        self,
        index: str,
        documents: List[Dict[str, Any]],
        id_field: Optional[str] = None,
        refresh: bool = False
    ) -> Dict[str, Any]:
        """Bulk index multiple documents.
        
        Args:
            index: Target index name.
            documents: List of documents.
            id_field: Optional field to use as document ID.
            refresh: Whether to refresh immediately.
            
        Returns:
            Bulk operation statistics.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            from elasticsearch.helpers import bulk
            
            actions = []
            for doc in documents:
                action = {
                    "_index": index,
                    "_source": doc
                }
                
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                
                actions.append(action)
            
            success, failed = bulk(
                self._client,
                actions,
                refresh=refresh,
                raise_on_error=False
            )
            
            return {"indexed": success, "failed": len(failed) if failed else 0}
        
        except Exception as e:
            raise Exception(f"Bulk index failed: {str(e)}")
    
    def get_document(self, index: str, id: str) -> Optional[Dict[str, Any]]:
        """Get a document by ID.
        
        Args:
            index: Index name.
            id: Document ID.
            
        Returns:
            Document data or None if not found.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            result = self._client.get(index=index, id=id)
            return result.get("_source")
        except Exception:
            return None
    
    def update_document(
        self,
        index: str,
        id: str,
        doc: Dict[str, Any],
        refresh: bool = False
    ) -> bool:
        """Update a document.
        
        Args:
            index: Index name.
            id: Document ID.
            doc: Partial document update.
            refresh: Whether to refresh immediately.
            
        Returns:
            True if updated successfully.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            self._client.update(index=index, id=id, doc=doc, refresh=refresh)
            return True
        except Exception:
            return False
    
    def delete_document(self, index: str, id: str, refresh: bool = False) -> bool:
        """Delete a document.
        
        Args:
            index: Index name.
            id: Document ID.
            refresh: Whether to refresh immediately.
            
        Returns:
            True if deleted successfully.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            self._client.delete(index=index, id=id, refresh=refresh)
            return True
        except Exception:
            return False
    
    def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None,
        source: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Execute a search query.
        
        Args:
            index: Index name.
            query: Query DSL.
            size: Number of results.
            from_: Offset for pagination.
            sort: Optional sort criteria.
            source: Optional source filtering.
            
        Returns:
            Search results with hits.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            body: Dict[str, Any] = {
                "query": query,
                "size": size,
                "from": from_
            }
            
            if sort:
                body["sort"] = sort
            
            if source:
                body["_source"] = source
            
            result = self._client.search(index=index, body=body)
            
            hits = result.get("hits", {})
            total = hits.get("total", {})
            if isinstance(total, dict):
                total = total.get("value", 0)
            
            return {
                "total": total,
                "hits": [
                    {
                        "index": hit.get("_index"),
                        "id": hit.get("_id"),
                        "score": hit.get("_score", 0),
                        "source": hit.get("_source", {})
                    }
                    for hit in hits.get("hits", [])
                ]
            }
        
        except Exception as e:
            raise Exception(f"Search failed: {str(e)}")
    
    def search_template(
        self,
        index: str,
        id: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a search template.
        
        Args:
            index: Index name.
            id: Template ID.
            params: Template parameters.
            
        Returns:
            Search results.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            result = self._client.search_template(
                index=index,
                id=id,
                params=params or {}
            )
            
            hits = result.get("hits", {})
            return {
                "total": hits.get("total", {}).get("value", 0),
                "hits": [
                    {
                        "index": hit.get("_index"),
                        "id": hit.get("_id"),
                        "score": hit.get("_score", 0),
                        "source": hit.get("_source", {})
                    }
                    for hit in hits.get("hits", [])
                ]
            }
        
        except Exception as e:
            raise Exception(f"Search template failed: {str(e)}")
    
    def count(self, index: str, query: Optional[Dict[str, Any]] = None) -> int:
        """Count documents matching a query.
        
        Args:
            index: Index name.
            query: Optional query filter.
            
        Returns:
            Document count.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            body = {"query": query} if query else None
            result = self._client.count(index=index, body=body)
            return result.get("count", 0)
        except Exception:
            return 0
    
    def refresh_index(self, index: str) -> bool:
        """Refresh an index to make recent changes searchable.
        
        Args:
            index: Index name.
            
        Returns:
            True if successful.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            self._client.indices.refresh(index=index)
            return True
        except Exception:
            return False
    
    def put_mapping(
        self,
        index: str,
        mappings: Dict[str, Any]
    ) -> bool:
        """Update index mappings.
        
        Args:
            index: Index name.
            mappings: Mapping definitions.
            
        Returns:
            True if successful.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            self._client.indices.put_mapping(index=index, body=mappings)
            return True
        except Exception as e:
            raise Exception(f"Put mapping failed: {str(e)}")
    
    def get_cluster_health(self) -> Dict[str, Any]:
        """Get cluster health information.
        
        Returns:
            Cluster health status.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            return self._client.cluster.health()
        except Exception as e:
            raise Exception(f"Get cluster health failed: {str(e)}")
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster statistics.
        
        Returns:
            Cluster statistics.
        """
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
        
        try:
            return self._client.cluster.stats()
        except Exception as e:
            raise Exception(f"Get cluster stats failed: {str(e)}")


class ElasticsearchAction(BaseAction):
    """Elasticsearch action for search and analytics operations.
    
    Supports index management, document CRUD, and search queries.
    """
    action_type: str = "elasticsearch"
    display_name: str = "Elasticsearch动作"
    description: str = "Elasticsearch搜索和索引管理操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[ElasticsearchClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Elasticsearch operation.
        
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
            elif operation == "create_index":
                return self._create_index(params, start_time)
            elif operation == "delete_index":
                return self._delete_index(params, start_time)
            elif operation == "list_indices":
                return self._list_indices(params, start_time)
            elif operation == "index_document":
                return self._index_document(params, start_time)
            elif operation == "bulk_index":
                return self._bulk_index(params, start_time)
            elif operation == "get_document":
                return self._get_document(params, start_time)
            elif operation == "update_document":
                return self._update_document(params, start_time)
            elif operation == "delete_document":
                return self._delete_document(params, start_time)
            elif operation == "search":
                return self._search(params, start_time)
            elif operation == "count":
                return self._count(params, start_time)
            elif operation == "refresh_index":
                return self._refresh_index(params, start_time)
            elif operation == "cluster_health":
                return self._cluster_health(start_time)
            elif operation == "cluster_stats":
                return self._cluster_stats(start_time)
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
                message=f"Elasticsearch operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Elasticsearch."""
        hosts = params.get("hosts", ["http://localhost:9200"])
        api_key = params.get("api_key")
        username = params.get("username")
        password = params.get("password")
        
        self._client = ElasticsearchClient(
            hosts=hosts,
            api_key=api_key,
            username=username,
            password=password
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Elasticsearch" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Elasticsearch."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Elasticsearch",
            duration=time.time() - start_time
        )
    
    def _create_index(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new index."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index is required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_index(
                index=index,
                mappings=params.get("mappings"),
                settings=params.get("settings")
            )
            return ActionResult(
                success=success,
                message=f"Index created: {index}" if success else "Create index failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_index(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete an index."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_index(index)
            return ActionResult(
                success=success,
                message=f"Index deleted: {index}" if success else "Delete index failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_indices(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all indices."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        pattern = params.get("pattern")
        
        try:
            indices = self._client.list_indices(pattern)
            return ActionResult(
                success=True,
                message=f"Found {len(indices)} indices",
                data={"indices": indices, "count": len(indices)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _index_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Index a document."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        document = params.get("document", {})
        doc_id = params.get("id")
        refresh = params.get("refresh", False)
        
        if not index or not document:
            return ActionResult(success=False, message="index and document are required", duration=time.time() - start_time)
        
        try:
            doc_id = self._client.index_document(
                index=index,
                document=document,
                id=doc_id,
                refresh=refresh
            )
            return ActionResult(
                success=True,
                message=f"Document indexed: {doc_id}",
                data={"id": doc_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _bulk_index(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Bulk index documents."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        documents = params.get("documents", [])
        id_field = params.get("id_field")
        refresh = params.get("refresh", False)
        
        if not index or not documents:
            return ActionResult(success=False, message="index and documents are required", duration=time.time() - start_time)
        
        try:
            result = self._client.bulk_index(
                index=index,
                documents=documents,
                id_field=id_field,
                refresh=refresh
            )
            return ActionResult(
                success=True,
                message=f"Bulk indexed {result.get('indexed', 0)} documents",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a document by ID."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        doc_id = params.get("id", "")
        
        if not index or not doc_id:
            return ActionResult(success=False, message="index and id are required", duration=time.time() - start_time)
        
        try:
            document = self._client.get_document(index, doc_id)
            return ActionResult(
                success=document is not None,
                message=f"Document found: {doc_id}" if document else f"Document not found: {doc_id}",
                data={"document": document},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _update_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update a document."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        doc_id = params.get("id", "")
        doc = params.get("doc", {})
        refresh = params.get("refresh", False)
        
        if not index or not doc_id or not doc:
            return ActionResult(success=False, message="index, id, and doc are required", duration=time.time() - start_time)
        
        try:
            success = self._client.update_document(index, doc_id, doc, refresh)
            return ActionResult(
                success=success,
                message=f"Document updated: {doc_id}" if success else "Update failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a document."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        doc_id = params.get("id", "")
        refresh = params.get("refresh", False)
        
        if not index or not doc_id:
            return ActionResult(success=False, message="index and id are required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_document(index, doc_id, refresh)
            return ActionResult(
                success=success,
                message=f"Document deleted: {doc_id}" if success else "Delete failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _search(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a search query."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        query = params.get("query", {"match_all": {}})
        size = params.get("size", 10)
        from_val = params.get("from", 0)
        
        if not index:
            return ActionResult(success=False, message="index is required", duration=time.time() - start_time)
        
        try:
            result = self._client.search(
                index=index,
                query=query,
                size=size,
                from_=from_val,
                sort=params.get("sort"),
                source=params.get("source")
            )
            return ActionResult(
                success=True,
                message=f"Found {result.get('total', 0)} hits",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _count(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Count documents."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index is required", duration=time.time() - start_time)
        
        try:
            count = self._client.count(index, params.get("query"))
            return ActionResult(
                success=True,
                message=f"Count: {count}",
                data={"count": count},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _refresh_index(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Refresh an index."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        index = params.get("index", "")
        if not index:
            return ActionResult(success=False, message="index is required", duration=time.time() - start_time)
        
        try:
            success = self._client.refresh_index(index)
            return ActionResult(
                success=success,
                message="Index refreshed" if success else "Refresh failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _cluster_health(self, start_time: float) -> ActionResult:
        """Get cluster health."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            health = self._client.get_cluster_health()
            return ActionResult(
                success=True,
                message=f"Cluster status: {health.get('status', 'unknown')}",
                data=health,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _cluster_stats(self, start_time: float) -> ActionResult:
        """Get cluster statistics."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            stats = self._client.get_cluster_stats()
            return ActionResult(
                success=True,
                message="Cluster stats retrieved",
                data=stats,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
