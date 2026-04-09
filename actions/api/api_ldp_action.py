"""API Linked Data Platform (LDP) Action Module.

Provides Linked Data Platform capabilities for API resources,
enabling semantic linking, resource discovery, and RDF metadata handling.

Example:
    >>> from actions.api.api_ldp_action import APILDPConnector
    >>> connector = APILDPConnector()
    >>> await connector.create_container(container_url)
"""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import threading


class RDFMediaType(Enum):
    """RDF serialization formats."""
    TURTLE = ("turtle", ".ttl")
    JSON_LD = ("json-ld", ".jsonld")
    RDF_XML = ("rdf+xml", ".rdf")
    N3 = ("n3", ".n3")
    N_TRIPLES = ("ntriples", ".nt")
    HTML = ("html", ".html")


@dataclass
class LDPResource:
    """Represents an LDP resource.
    
    Attributes:
        uri: Resource URI
        resource_type: Type of resource (BasicContainer, DirectContainer, etc.)
        title: Human-readable title
        created: Creation timestamp
        modified: Last modification timestamp
        etag: Entity tag for caching
        content_type: MIME type of the resource
        linked_resources: URIs of resources linked from this one
    """
    uri: str
    resource_type: str = "Resource"
    title: str = ""
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    etag: str = ""
    content_type: str = "application/ld+json"
    linked_resources: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LDPPrefix:
    """Namespace prefix mapping.
    
    Attributes:
        prefix: The prefix string (e.g., 'ex', 'foaf')
        namespace: Full namespace URI
    """
    prefix: str
    namespace: str


@dataclass
class LDPConfig:
    """Configuration for LDP operations.
    
    Attributes:
        base_uri: Base URI for the LDP server
        default_content_type: Default content type for resources
        validate_resources: Whether to validate RDF on creation/update
        enable_prefer_headers: Whether to honor Prefer headers
        default_prefixes: Default namespace prefixes
    """
    base_uri: str = "http://localhost:8080/ldp/"
    default_content_type: str = "application/ld+json"
    validate_resources: bool = True
    enable_prefer_headers: bool = True
    default_prefixes: List[LDPPrefix] = field(default_factory=list)


class APILDPConnector:
    """Handles Linked Data Platform operations for APIs.
    
    Provides capabilities for creating containers, managing resources,
    handling RDF metadata, and performing semantic queries.
    
    Attributes:
        config: Current LDP configuration
    
    Example:
        >>> connector = APILDPConnector()
        >>> container = await connector.create_container("/data/")
    """
    
    # Default namespace prefixes
    DEFAULT_PREFIXES = [
        LDPPrefix("ldp", "http://www.w3.org/ns/ldp#"),
        LDPPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        LDPPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
        LDPPrefix("dc", "http://purl.org/dc/terms/"),
        LDPPrefix("foaf", "http://xmlns.com/foaf/0.1/"),
    ]
    
    def __init__(self, config: Optional[LDPConfig] = None):
        """Initialize the LDP connector.
        
        Args:
            config: LDP configuration. Uses defaults if not provided.
        """
        self.config = config or LDPConfig()
        self._resources: Dict[str, LDPResource] = {}
        self._containers: Dict[str, List[str]] = {}
        self._lock = threading.RLock()
        
        # Initialize with default prefixes
        if not self.config.default_prefixes:
            self.config.default_prefixes = self.DEFAULT_PREFIXES.copy()
    
    def _normalize_uri(self, uri: str) -> str:
        """Normalize a URI to be relative to base.
        
        Args:
            uri: URI to normalize
        
        Returns:
            Normalized URI
        """
        if uri.startswith(self.config.base_uri):
            return uri[len(self.config.base_uri):].lstrip("/")
        return uri
    
    def _resolve_uri(self, path: str) -> str:
        """Resolve a path to a full URI.
        
        Args:
            path: Relative path
        
        Returns:
            Full URI
        """
        if path.startswith("http"):
            return path
        return f"{self.config.base_uri.rstrip('/')}/{path.lstrip('/')}"
    
    def _generate_etag(self, content: Any) -> str:
        """Generate an ETag for content.
        
        Args:
            content: Content to hash
        
        Returns:
            ETag string
        """
        content_str = json.dumps(content, sort_keys=True, default=str)
        return f'"{hashlib.md5(content_str.encode()).hexdigest()}"'
    
    async def create_container(
        self,
        path: str,
        title: str = "",
        membership_resource: Optional[str] = None,
        has_member_relation: Optional[str] = None
    ) -> LDPResource:
        """Create an LDP container.
        
        Args:
            path: Container path (relative or absolute)
            title: Container title
            membership_resource: URI of the membership resource
            has_member_relation: Predicate relating container to members
        
        Returns:
            Created container resource
        
        Raises:
            ValueError: If container already exists or path invalid
        """
        normalized = self._normalize_uri(path)
        
        with self._lock:
            if normalized in self._resources:
                raise ValueError(f"Container already exists: {normalized}")
            
            uri = self._resolve_uri(normalized)
            container = LDPResource(
                uri=uri,
                resource_type="Container",
                title=title,
                created=datetime.now(),
                modified=datetime.now(),
                etag=self._generate_etag({"path": normalized}),
                content_type="application/ld+json",
                metadata={
                    "membership_resource": membership_resource,
                    "has_member_relation": has_member_relation,
                    "is_container": True
                }
            )
            
            self._resources[normalized] = container
            self._containers[normalized] = []
        
        return container
    
    async def add_resource(
        self,
        container_path: str,
        slug: str,
        content: Dict[str, Any],
        content_type: Optional[str] = None,
        linked_to: Optional[List[str]] = None
    ) -> LDPResource:
        """Add a resource to a container.
        
        Args:
            container_path: Path to the containing container
            slug: Slug/identifier for the resource
            content: Resource content (JSON-LD compatible)
            content_type: Optional content type override
            linked_to: URIs of resources this links to
        
        Returns:
            Created resource
        
        Raises:
            ValueError: If container doesn't exist or slug conflicts
        """
        normalized_container = self._normalize_uri(container_path)
        resource_id = f"{normalized_container}/{slug}"
        
        with self._lock:
            if normalized_container not in self._resources:
                raise ValueError(f"Container not found: {normalized_container}")
            
            if resource_id in self._resources:
                raise ValueError(f"Resource already exists: {resource_id}")
            
            uri = self._resolve_uri(resource_id)
            resource = LDPResource(
                uri=uri,
                resource_type="Resource",
                title=content.get("name", content.get("title", slug)),
                created=datetime.now(),
                modified=datetime.now(),
                etag=self._generate_etag(content),
                content_type=content_type or self.config.default_content_type,
                linked_resources=linked_to or [],
                metadata={"content": content}
            )
            
            self._resources[resource_id] = resource
            self._containers[normalized_container].append(resource_id)
        
        return resource
    
    async def get_resource(self, path: str, include_metadata: bool = True) -> Optional[Dict[str, Any]]:
        """Get a resource by path.
        
        Args:
            path: Resource path
            include_metadata: Whether to include LDP metadata
        
        Returns:
            Resource data with optional metadata
        """
        normalized = self._normalize_uri(path)
        
        with self._lock:
            resource = self._resources.get(normalized)
            
            if not resource:
                return None
            
            result: Dict[str, Any] = {}
            
            if include_metadata:
                result["@context"] = "http://www.w3.org/ns/ldp"
                result["@id"] = resource.uri
                result["@type"] = resource.resource_type
                result["dc:title"] = resource.title
                result["dc:created"] = resource.created.isoformat() if resource.created else None
                result["dc:modified"] = resource.modified.isoformat() if resource.modified else None
                result["etag"] = resource.etag
            
            if resource.metadata.get("content"):
                result.update(resource.metadata["content"])
            elif include_metadata:
                # Just metadata, no content stored
                pass
            
            return result
    
    async def update_resource(self, path: str, content: Dict[str, Any]) -> LDPResource:
        """Update an existing resource.
        
        Args:
            path: Resource path
            content: New content
        
        Returns:
            Updated resource
        
        Raises:
            ValueError: If resource not found
        """
        normalized = self._normalize_uri(path)
        
        with self._lock:
            resource = self._resources.get(normalized)
            
            if not resource:
                raise ValueError(f"Resource not found: {normalized}")
            
            resource.content = content
            resource.modified = datetime.now()
            resource.etag = self._generate_etag(content)
            resource.metadata["content"] = content
        
        return resource
    
    async def delete_resource(self, path: str) -> bool:
        """Delete a resource.
        
        Args:
            path: Resource path
        
        Returns:
            True if deleted, False if not found
        """
        normalized = self._normalize_uri(path)
        
        with self._lock:
            if normalized not in self._resources:
                return False
            
            resource = self._resources[normalized]
            
            # Remove from container
            container_path = normalized.rsplit("/", 1)[0]
            if container_path in self._containers:
                self._containers[container_path] = [
                    r for r in self._containers[container_path] if r != normalized
                ]
            
            # Remove linked references
            for other_path, other in list(self._resources.items()):
                if normalized in other.linked_resources:
                    other.linked_resources.remove(normalized)
            
            del self._resources[normalized]
            return True
    
    async def find_resources(
        self,
        container_path: Optional[str] = None,
        rdf_type: Optional[str] = None,
        linked_from: Optional[str] = None,
        limit: int = 100
    ) -> List[LDPResource]:
        """Find resources matching criteria.
        
        Args:
            container_path: Filter to specific container
            rdf_type: Filter by RDF type
            linked_from: Filter to resources linked from specific URI
            limit: Maximum results
        
        Returns:
            List of matching resources
        """
        results: List[LDPResource] = []
        
        with self._lock:
            for path, resource in self._resources.items():
                # Filter by container
                if container_path:
                    normalized = self._normalize_uri(container_path)
                    if not path.startswith(normalized + "/"):
                        continue
                
                # Filter by type
                if rdf_type and resource.resource_type != rdf_type:
                    continue
                
                # Filter by link
                if linked_from:
                    normalized_linked = self._normalize_uri(linked_from)
                    if normalized_linked not in resource.linked_resources:
                        continue
                
                results.append(resource)
                
                if len(results) >= limit:
                    break
        
        return results
    
    async def get_container_members(
        self,
        container_path: str,
        prefer_minimal: bool = False
    ) -> List[str]:
        """Get member URIs of a container.
        
        Args:
            container_path: Container path
            prefer_minimal: Whether to return minimal member references
        
        Returns:
            List of member URIs
        """
        normalized = self._normalize_uri(container_path)
        
        with self._lock:
            members = self._containers.get(normalized, [])
            
            if prefer_minimal:
                # Return just IDs
                return [self._resolve_uri(m) for m in members]
            
            return [self._resolve_uri(m) for m in members]
    
    def expand_prefix(self, prefixed: str) -> str:
        """Expand a prefixed term to full URI.
        
        Args:
            prefixed: Prefixed term (e.g., 'foaf:Person')
        
        Returns:
            Expanded URI
        """
        if ":" in prefixed:
            prefix, local = prefixed.split(":", 1)
            for p in self.config.default_prefixes:
                if p.prefix == prefix:
                    return p.namespace + local
        return prefixed
    
    def collapse_uri(self, uri: str) -> str:
        """Collapse a full URI to a prefixed term if possible.
        
        Args:
            uri: Full URI to collapse
        
        Returns:
            Prefixed term or original URI
        """
        for p in self.config.default_prefixes:
            if uri.startswith(p.namespace):
                return f"{p.prefix}:" + uri[len(p.namespace):]
        return uri
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get LDP statistics.
        
        Returns:
            Statistics dictionary
        """
        with self._lock:
            resources = list(self._resources.values())
            containers = [r for r in resources if "Container" in r.resource_type]
            
            all_links: Set[str] = set()
            for r in resources:
                all_links.update(r.linked_resources)
            
            return {
                "total_resources": len(resources),
                "total_containers": len(containers),
                "total_links": len(all_links),
                "base_uri": self.config.base_uri
            }
