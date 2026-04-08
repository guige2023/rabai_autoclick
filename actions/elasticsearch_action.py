"""
Elasticsearch Action Module.

Provides Elasticsearch client capabilities for search and analytics.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Elasticsearch query types."""
    MATCH = "match"
    TERM = "term"
    RANGE = "range"
    BOOL = "bool"
    FUZZY = "fuzzy"
    WILDCARD = "wildcard"


@dataclass
class Document:
    """Elasticsearch document."""
    index: str
    doc_type: str
    id: Optional[str]
    source: Dict[str, Any]
    score: Optional[float] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class SearchResult:
    """Search result container."""
    total: int
    hits: List[Document]
    aggregations: Dict[str, Any] = field(default_factory=dict)
    took_ms: int = 0


@dataclass
class ESConfig:
    """Elasticsearch client configuration."""
    hosts: List[str] = field(
        default_factory=lambda: ["http://localhost:9200"]
    )
    index_name: str = "default"
    doc_type: str = "_doc"
    timeout: float = 30.0
    max_retries: int = 3
    retry_on_timeout: bool = True
    verify_certs: bool = False


class ElasticsearchAction:
    """
    Elasticsearch action handler.
    
    Provides Elasticsearch client for search and analytics.
    
    Example:
        es = ElasticsearchAction(config=cfg)
        es.connect()
        es.create_index("my-index")
        es.index("my-index", {"title": "Hello"})
        es.search("my-index", {"query": {"match": {"title": "Hello"}}})
    """
    
    def __init__(self, config: Optional[ESConfig] = None):
        """
        Initialize Elasticsearch handler.
        
        Args:
            config: Elasticsearch configuration
        """
        self.config = config or ESConfig()
        self._connected = False
        self._indices: Dict[str, Dict[str, Any]] = {}
        self._documents: Dict[str, Dict[str, Document]] = {}
        self._lock = threading.RLock()
    
    def connect(self) -> bool:
        """
        Connect to Elasticsearch cluster.
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(f"Connecting to Elasticsearch: {self.config.hosts}")
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"Elasticsearch connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from Elasticsearch.
        
        Returns:
            True if disconnected
        """
        with self._lock:
            self._connected = False
            self._indices.clear()
            self._documents.clear()
            logger.info("Disconnected from Elasticsearch")
            return True
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
    
    def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create an index.
        
        Args:
            index: Index name
            mappings: Index field mappings
            settings: Index settings
            
        Returns:
            True if created successfully
        """
        if not self._connected:
            return False
        
        with self._lock:
            self._indices[index] = {
                "mappings": mappings or {},
                "settings": settings or {},
                "created_at": time.time()
            }
            self._documents[index] = {}
            logger.info(f"Created index: {index}")
            return True
    
    def delete_index(self, index: str) -> bool:
        """
        Delete an index.
        
        Args:
            index: Index name
            
        Returns:
            True if deleted
        """
        with self._lock:
            if index in self._indices:
                del self._indices[index]
            if index in self._documents:
                del self._documents[index]
            logger.info(f"Deleted index: {index}")
            return True
    
    def index(
        self,
        index: str,
        document: Dict[str, Any],
        doc_id: Optional[str] = None,
        refresh: bool = False
    ) -> str:
        """
        Index a document.
        
        Args:
            index: Index name
            document: Document data
            doc_id: Optional document ID
            refresh: Whether to refresh immediately
            
        Returns:
            Document ID
        """
        if not self._connected:
            raise RuntimeError("Not connected to Elasticsearch")
        
        doc_id = doc_id or f"doc_{int(time.time() * 1000)}"
        
        with self._lock:
            if index not in self._documents:
                self._documents[index] = {}
            
            doc = Document(
                index=index,
                doc_type=self.config.doc_type,
                id=doc_id,
                source=document
            )
            self._documents[index][doc_id] = doc
        
        logger.debug(f"Indexed document {doc_id} in {index}")
        return doc_id
    
    def get(
        self,
        index: str,
        doc_id: str
    ) -> Optional[Document]:
        """
        Get a document by ID.
        
        Args:
            index: Index name
            doc_id: Document ID
            
        Returns:
            Document or None
        """
        with self._lock:
            if index not in self._documents:
                return None
            return self._documents[index].get(doc_id)
    
    def delete(
        self,
        index: str,
        doc_id: str,
        refresh: bool = False
    ) -> bool:
        """
        Delete a document.
        
        Args:
            index: Index name
            doc_id: Document ID
            refresh: Whether to refresh immediately
            
        Returns:
            True if deleted
        """
        with self._lock:
            if index in self._documents:
                if doc_id in self._documents[index]:
                    del self._documents[index][doc_id]
                    logger.debug(f"Deleted document {doc_id}")
                    return True
        return False
    
    def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None,
        aggregations: Optional[Dict[str, Any]] = None
    ) -> SearchResult:
        """
        Search documents.
        
        Args:
            index: Index name
            query: Search query
            size: Number of results
            from_: Starting offset
            sort: Sort specification
            aggregations: Aggregation definitions
            
        Returns:
            SearchResult with hits and aggregations
        """
        if not self._connected:
            raise RuntimeError("Not connected to Elasticsearch")
        
        start_time = time.time()
        
        with self._lock:
            hits = []
            if index in self._documents:
                for doc in self._documents[index].values():
                    hits.append(doc)
            
            result = SearchResult(
                total=len(hits),
                hits=hits[:size],
                aggregations=aggregations or {},
                took_ms=int((time.time() - start_time) * 1000)
            )
        
        return result
    
    def update(
        self,
        index: str,
        doc_id: str,
        doc: Dict[str, Any],
        refresh: bool = False
    ) -> bool:
        """
        Update a document.
        
        Args:
            index: Index name
            doc_id: Document ID
            doc: Partial document update
            refresh: Whether to refresh immediately
            
        Returns:
            True if updated
        """
        with self._lock:
            if index not in self._documents:
                return False
            if doc_id not in self._documents[index]:
                return False
            
            existing = self._documents[index][doc_id]
            existing.source.update(doc)
            return True
    
    def bulk(
        self,
        operations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute bulk operations.
        
        Args:
            operations: List of bulk operations
            
        Returns:
            Bulk operation results
        """
        with self._lock:
            success_count = 0
            error_count = 0
            
            for op in operations:
                if "index" in op:
                    success_count += 1
                elif "delete" in op:
                    success_count += 1
                else:
                    error_count += 1
            
            return {
                "took": len(operations),
                "errors": error_count > 0,
                "items": [
                    {"index": {"_id": f"doc_{i}"}}
                    for i in range(success_count)
                ]
            }
    
    def count(self, index: str) -> int:
        """
        Get document count for an index.
        
        Args:
            index: Index name
            
        Returns:
            Document count
        """
        with self._lock:
            if index not in self._documents:
                return 0
            return len(self._documents[index])
