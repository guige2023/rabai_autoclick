"""API metadata action module for RabAI AutoClick.

Provides metadata handling for API operations:
- ApiMetadataExtractor: Extract metadata from API responses
- ApiMetadataStore: Store and manage API metadata
- ApiSchemaValidator: Validate API schemas and metadata
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class ApiMetadata:
    """API metadata."""
    endpoint: str
    method: str
    status_code: int
    content_type: str
    content_length: int
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    custom_fields: Dict[str, Any] = field(default_factory=dict)


class MetadataExtractor:
    """Extract metadata from API responses."""
    
    def __init__(self):
        self._stats = {"total_extractions": 0, "successful_extractions": 0}
        self._lock = threading.Lock()
    
    def extract(self, response: Any) -> Optional[ApiMetadata]:
        """Extract metadata from API response."""
        with self._lock:
            self._stats["total_extractions"] += 1
        
        try:
            endpoint = getattr(response, 'url', 'unknown')
            method = getattr(response, 'method', 'GET')
            status_code = getattr(response, 'status_code', 0)
            
            headers = {}
            if hasattr(response, 'headers'):
                headers = dict(response.headers)
            
            content_type = headers.get('Content-Type', headers.get('content-type', ''))
            
            content_length = 0
            if hasattr(response, 'headers') and 'Content-Length' in response.headers:
                try:
                    content_length = int(response.headers['Content-Length'])
                except (ValueError, TypeError):
                    pass
            
            metadata = ApiMetadata(
                endpoint=endpoint,
                method=method,
                status_code=status_code,
                content_type=content_type,
                content_length=content_length,
                headers=headers,
            )
            
            with self._lock:
                self._stats["successful_extractions"] += 1
            
            return metadata
            
        except Exception as e:
            logging.error(f"Metadata extraction failed: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics."""
        with self._lock:
            return dict(self._stats)


class ApiMetadataStore:
    """Store and manage API metadata."""
    
    def __init__(self, max_entries: int = 1000):
        self.max_entries = max_entries
        self._metadata: Dict[str, ApiMetadata] = {}
        self._by_endpoint: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
        self._stats = {"total_stored": 0, "total_retrieved": 0}
    
    def store(self, key: str, metadata: ApiMetadata):
        """Store metadata."""
        with self._lock:
            self._metadata[key] = metadata
            self._by_endpoint[metadata.endpoint].append(key)
            
            if len(self._metadata) > self.max_entries:
                oldest_key = next(iter(self._metadata))
                old_meta = self._metadata.pop(oldest_key)
                if oldest_key in self._by_endpoint[old_meta.endpoint]:
                    self._by_endpoint[old_meta.endpoint].remove(oldest_key)
            
            self._stats["total_stored"] += 1
    
    def get(self, key: str) -> Optional[ApiMetadata]:
        """Get metadata by key."""
        with self._lock:
            self._stats["total_retrieved"] += 1
            return self._metadata.get(key)
    
    def get_by_endpoint(self, endpoint: str) -> List[ApiMetadata]:
        """Get all metadata for endpoint."""
        with self._lock:
            keys = self._by_endpoint.get(endpoint, [])
            return [self._metadata[k] for k in keys if k in self._metadata]
    
    def list_all(self) -> List[Tuple[str, ApiMetadata]]:
        """List all stored metadata."""
        with self._lock:
            return list(self._metadata.items())
    
    def clear(self):
        """Clear all metadata."""
        with self._lock:
            self._metadata.clear()
            self._by_endpoint.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        with self._lock:
            return {
                "total_entries": len(self._metadata),
                "endpoints_tracked": len(self._by_endpoint),
                **{k: v for k, v in self._stats.items()},
            }


class ApiMetadataAction(BaseAction):
    """API metadata action."""
    action_type = "api_metadata"
    display_name = "API元数据"
    description = "API元数据提取与管理"
    
    def __init__(self):
        super().__init__()
        self._extractor = MetadataExtractor()
        self._store: Optional[ApiMetadataStore] = None
        self._lock = threading.Lock()
    
    def _get_store(self) -> ApiMetadataStore:
        """Get or create metadata store."""
        with self._lock:
            if self._store is None:
                self._store = ApiMetadataStore()
            return self._store
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute metadata operation."""
        try:
            command = params.get("command", "extract")
            store = self._get_store()
            
            if command == "extract":
                response = params.get("response")
                if response:
                    metadata = self._extractor.extract(response)
                    if metadata:
                        key = hashlib.md5(f"{metadata.endpoint}:{metadata.timestamp}".encode()).hexdigest()[:16]
                        store.store(key, metadata)
                        return ActionResult(success=True, data={"endpoint": metadata.endpoint, "status": metadata.status_code})
                return ActionResult(success=False, message="Response required")
            
            elif command == "get":
                key = params.get("key")
                metadata = store.get(key)
                if metadata:
                    return ActionResult(success=True, data={"endpoint": metadata.endpoint, "status": metadata.status_code})
                return ActionResult(success=False, message="Metadata not found")
            
            elif command == "by_endpoint":
                endpoint = params.get("endpoint")
                results = store.get_by_endpoint(endpoint)
                return ActionResult(success=True, data={"count": len(results)})
            
            elif command == "stats":
                stats = store.get_stats()
                extractor_stats = self._extractor.get_stats()
                return ActionResult(success=True, data={"store": stats, "extractor": extractor_stats})
            
            elif command == "clear":
                store.clear()
                return ActionResult(success=True)
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiMetadataAction error: {str(e)}")
